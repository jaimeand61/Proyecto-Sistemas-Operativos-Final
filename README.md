 # Sistema de Categorización Meteorológica — Bogotá

**Proyecto Final · Sistemas Operativos 2026-30**  
Pontificia Universidad Javeriana · Departamento de Ingeniería de Sistemas  
Docente: John Corredor, Ph.D.

---

## Integrantes

| Nombre | Repositorio |
|---|---|
| Federico Restrepo Guzmán | [@federico204](https://github.com/federico204/Proyecto-Clima-SO) |
| Jaime Andrés Molina Villamarín | [@jaimeand61](https://github.com/jaimeand61/Proyecto-Sistemas-Operativos) |
| Nicholas Ruiz Zhilkin | [@Nivk-Debug](https://github.com/Nivk-Debug/Proyecto-SO) |

---

## Descripción

Sistema concurrente en C que simula una red de estaciones meteorológicas para la ciudad de Bogotá. Recibe datos de humedad, presión atmosférica y punto de rocío desde múltiples agentes independientes, los procesa de forma concurrente y clasifica el clima en las categorías **Lluvioso**, **Nublado** o **Fresco**.

Conceptos aplicados:
- Comunicación entre procesos mediante **pipes nominales (FIFOs)**
- Concurrencia con **hilos POSIX (pthreads)**
- Sincronización con **semáforos POSIX** bajo el patrón **productor-consumidor** con buffer acotado

---

## Estructura del Proyecto

```
.
├── common.h        # Estructura Medicion, macros de rangos, includes compartidos
├── agenteM.c       # Proceso Agente de Medición
├── monitor.c       # Proceso Monitor / Control de Categorización
├── Makefile        # Compilación de ambos ejecutables
├── sensor1.csv     # Archivo de prueba (estación de ejemplo)
├── informe_SO.pdf  # Informe del proyecto
└── README.md
```

---

## Compilación

```bash
make
```

Para limpiar binarios, el consolidado y el pipe:

```bash
make clean
```

> `clean` elimina: `agenteM`, `monitor`, `consolidado.csv`, `miPipe`, `pipeNom`

---

## Uso

### 1. Iniciar el Monitor (siempre primero)

```bash
./monitor -b <tamBuffer> -p <pipeNom>
```

| Flag | Descripción |
|---|---|
| `-b` | Tamaño del buffer acotado por estación |
| `-p` | Nombre del pipe nominal (FIFO) |

### 2. Lanzar los Agentes (en terminales separadas)

```bash
./agenteM -f <archivo.csv> -t <segundos> -p <pipeNom>
```

| Flag | Descripción |
|---|---|
| `-f` | Archivo CSV con mediciones del sensor |
| `-t` | Segundos de espera entre mediciones y antes de enviar señal de fin |
| `-p` | Nombre del pipe nominal (debe coincidir con el monitor) |

> Las flags pueden pasarse en **cualquier orden** gracias a `getopt()`.

### Ejemplo con múltiples agentes

```bash
# Terminal 1 — levantar el monitor primero
./monitor -b 4 -p miPipe

# Terminal 2 — agente estación EK
./agenteM -f sensor1.csv -t 1 -p miPipe

# Terminal 3 — agente estación ET (simultáneo)
./agenteM -f sensorET.csv -t 1 -p miPipe
```

**Salida esperada:**
```
... Control de Categorizacion Meteorologica !!!
...
Parte Meteorologico Bogota: "Lluvioso"
Fin del Monitor !!!
```

---

## Formato del CSV de Sensores

```
ESTACION,HUMEDAD,ROCIO,PRESION,HORA
EK,85,7,750,08:00:00
EK,92,10,748,10:00:00
.
```

La línea con punto (`.`) indica el fin del archivo. El agente esperará `-t` segundos adicionales y enviará la señal de fin al monitor.

---

## Rangos Aceptables (IDEAM)

| Parámetro | Unidad | Mínimo | Máximo |
|---|---|---|---|
| Humedad Relativa | % | 77 | 100 |
| Presión Atmosférica | hPa | 740 | 760 |
| Punto de Rocío | °C | 3 | 12 |

Las mediciones fuera de rango son **descartadas por el agente** y no se transmiten al monitor.

---

## Categorías Meteorológicas

| Categoría | Humedad (%) | Rocío (°C) | Presión (hPa) |
|---|---|---|---|
| Lluvioso | > 90 | > 9 | < 750 |
| Nublado | 80–95 | > 8 | 751 |
| Fresco | < 80 | 5–8 | > 754 |

---

## Salidas del Sistema

- **Consola:** mensajes de inicio/fin de cada agente, alarmas de horas faltantes y parte meteorológico final.
- **`consolidado.csv`:** mediciones válidas procesadas. Ejemplo:

```
EK,90,9,750,08:00:00
ET,90,9,750,08:00:00
EK,89,8,749,09:01:00
```

---

## Decisiones de Diseño

1. **Pipe nominal único compartido** — todos los agentes escriben al mismo FIFO; el Hilo Recolector los lee y distribuye por estación.
2. **Detección dinámica de estaciones** — el monitor registra automáticamente cada estación nueva y le asigna un buffer y un Hilo Consumidor.
3. **Señal de fin con `es_fin`** — se reutiliza la estructura `Medicion` con `es_fin = 1` en lugar de señales UNIX adicionales.
4. **Detección de horas faltantes** — el Hilo Consumidor emite una alarma si entre dos mediciones consecutivas hay más de dos horas sin datos.
5. **Validación de llamadas al sistema** — `open`, `write`, `read`, `sem_init`, etc. validan su retorno; en caso de error invocan `perror()` y terminan con `EXIT_FAILURE`.
6. **Liberación de recursos** — al finalizar, el monitor destruye semáforos y mutex, elimina el FIFO y libera la memoria dinámica.

---

## Dependencias

- GCC con soporte C11
- POSIX threads (`-lpthread`)
- POSIX real-time extensions (`-lrt`)
- Linux

---

## Referencias

- Kerrisk, M. (2010). *The Linux Programming Interface*. No Starch Press.
- Butenhof, D. R. (1997). *Programming with POSIX Threads*. Addison-Wesley.
- Departamento de Ingeniería de Sistemas. (2026). *Enunciado Proyecto: Sistema de Categorización Meteorológica*, Sistemas Operativos 2026-30.
****
