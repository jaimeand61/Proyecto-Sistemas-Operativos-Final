/*
 * monitor.c - Monitor / Control de Categorización Meteorológica
 * Sistema de Categorización Meteorológica - Bogotá
 * Sistemas Operativos 2026-30
 *
 * Uso: ./monitor -b tamBuffer -p pipeNom
 *
 * Arquitectura de hilos:
 *   - Hilo Recolector : lee del pipe nominal y deposita en el buffer
 *                       de la estación correspondiente (productor).
 *   - Hilo Consumidor : uno por estación registrada dinámicamente;
 *                       lee del buffer de su estación, escribe en el
 *                       archivo consolidado y detecta horas faltantes.
 *   - Hilo Main       : espera al recolector y a los consumidores,
 *                       luego imprime el reporte final de categoría.
 *
 * Sincronización: semáforos POSIX + mutex POSIX.
 * Patrón productor/consumidor con buffer acotado (ring buffer) por estación.
 */

#include "common.h"
#include <time.h>

/* ------------------------------------------------------------------ */
/* Constantes                                                          */
/* ------------------------------------------------------------------ */
#define MAX_ESTACIONES  16
#define HORA_LEN         9          /* "HH:MM:SS\0" */
#define ARCHIVO_CONSOL  "consolidado.csv"

/* ------------------------------------------------------------------ */
/* Buffer acotado por estación                                         */
/* ------------------------------------------------------------------ */
typedef struct {
    char     id[5];
    Medicion *slots;            /* Ring buffer de Medicion              */
    int       tam;
    int       in;               /* Índice productor                     */
    int       out;              /* Índice consumidor                    */
    sem_t     empty;            /* Slots disponibles para el productor  */
    sem_t     full;             /* Slots con dato para el consumidor    */
    pthread_mutex_t mutex;      /* Protege in, out y campos de estado   */

    /* Estadísticas */
    long sum_h, sum_r, sum_p;
    int  total;

    /* Detección de horas faltantes */
    char ultima_hora[HORA_LEN];
} BufferEstacion;

/* ================================================================== */
/* Variables globales                                                  */
/* ================================================================== */
static int            tam_buffer   = 0;
static char          *pipeNom      = NULL;
static FILE          *log_consol   = NULL;

static pthread_mutex_t mutex_log;   /* Protege fwrite al consolidado    */
static pthread_mutex_t mutex_est;   /* Protege acceso a estaciones[]    */

static BufferEstacion estaciones[MAX_ESTACIONES];
static int            num_estaciones = 0;
static pthread_t      hilos_consumidores[MAX_ESTACIONES];

/*
 * agentes_por_estacion[i]: cuántos agentes están activos enviando datos
 * para la estación i. Se incrementa la primera vez que llega un dato de
 * una combinación (estación, agente) nueva.
 * agentes_fin_por_est[i]: cuántas señales es_fin llegaron para la estación i.
 */
static int agentes_por_estacion[MAX_ESTACIONES];
static int agentes_fin_por_est[MAX_ESTACIONES];
static int total_agentes  = 0;
static int total_fin       = 0;

/* ================================================================== */
/* Prototipos                                                          */
/* ================================================================== */
static void  depositar(int idx, const Medicion *m);
static void *hilo_consumidor(void *arg);
static void *hilo_recolector(void *arg);

/* ------------------------------------------------------------------ */
/* Convierte "HH:MM:SS" a minutos desde medianoche                    */
/* ------------------------------------------------------------------ */
static int hora_a_minutos(const char *h) {
    int hh = 0, mm = 0, ss = 0;
    sscanf(h, "%d:%d:%d", &hh, &mm, &ss);
    return hh * 60 + mm;
}

/* ------------------------------------------------------------------ */
/* Registra una estación nueva y lanza su hilo consumidor.            */
/* Retorna índice. Debe llamarse con mutex_est tomado.                */
/* ------------------------------------------------------------------ */
static int registrar_estacion(const char *id) {
    /* Buscar si ya existe */
    for (int i = 0; i < num_estaciones; i++) {
        if (strncmp(estaciones[i].id, id, sizeof(estaciones[i].id)) == 0)
            return i;
    }

    if (num_estaciones >= MAX_ESTACIONES) {
        fprintf(stderr, "monitor: límite de estaciones alcanzado\n");
        return -1;
    }

    int idx = num_estaciones;
    BufferEstacion *be = &estaciones[idx];

    strncpy(be->id, id, sizeof(be->id) - 1);
    be->id[sizeof(be->id) - 1] = '\0';
    be->tam    = tam_buffer;
    be->in     = 0;
    be->out    = 0;
    be->sum_h  = 0; be->sum_r = 0; be->sum_p = 0;
    be->total  = 0;
    memset(be->ultima_hora, 0, HORA_LEN);

    agentes_por_estacion[idx] = 0;
    agentes_fin_por_est[idx]  = 0;

    be->slots = malloc(sizeof(Medicion) * (size_t)tam_buffer);
    if (!be->slots) { perror("monitor: malloc"); return -1; }

    if (sem_init(&be->empty, 0, (unsigned)tam_buffer) != 0) {
        perror("sem_init empty"); free(be->slots); return -1;
    }
    if (sem_init(&be->full, 0, 0) != 0) {
        perror("sem_init full");
        sem_destroy(&be->empty); free(be->slots); return -1;
    }
    if (pthread_mutex_init(&be->mutex, NULL) != 0) {
        perror("pthread_mutex_init estación");
        sem_destroy(&be->empty); sem_destroy(&be->full);
        free(be->slots); return -1;
    }

    if (pthread_create(&hilos_consumidores[idx], NULL,
                       hilo_consumidor, (void *)(intptr_t)idx) != 0) {
        perror("monitor: pthread_create consumidor");
        pthread_mutex_destroy(&be->mutex);
        sem_destroy(&be->empty); sem_destroy(&be->full);
        free(be->slots); return -1;
    }

    num_estaciones++;
    return idx;
}

/* ================================================================== */
/* Hilo consumidor                                                     */
/* ================================================================== */
static void *hilo_consumidor(void *arg) {
    int idx = (int)(intptr_t)arg;
    BufferEstacion *be = &estaciones[idx];

    while (1) {
        /* Bloquear si el buffer está vacío */
        sem_wait(&be->full);

        pthread_mutex_lock(&be->mutex);

        /* Extraer siguiente ítem del ring buffer */
        Medicion m = be->slots[be->out];
        be->out = (be->out + 1) % be->tam;

        pthread_mutex_unlock(&be->mutex);
        sem_post(&be->empty);   /* Liberar slot para el recolector */

        /* El centinela (es_fin=1) indica que no hay más datos reales */
        if (m.es_fin) {
            break;
        }

        pthread_mutex_lock(&be->mutex);

        /* Acumular estadísticas (solo el consumidor escribe estos campos) */
        be->sum_h += m.humedad;
        be->sum_r += m.rocio;
        be->sum_p += m.presion;
        be->total++;

        /* Detección de horas consecutivas faltantes (>= 120 min = 2 horas).
         * Calculamos dentro del mutex para proteger ultima_hora. */
        int  hay_alarma = 0;
        char hora_ant[HORA_LEN]  = {0};
        char hora_curr[HORA_LEN] = {0};

        if (be->ultima_hora[0] != '\0') {
            int ant  = hora_a_minutos(be->ultima_hora);
            int curr = hora_a_minutos(m.hora);
            if (curr - ant >= 120) {
                hay_alarma = 1;
                strncpy(hora_ant,  be->ultima_hora, HORA_LEN - 1);
                strncpy(hora_curr, m.hora,          HORA_LEN - 1);
            }
        }
        strncpy(be->ultima_hora, m.hora, HORA_LEN - 1);
        be->ultima_hora[HORA_LEN - 1] = '\0';

        pthread_mutex_unlock(&be->mutex);

        /* Imprimir alarma usando mutex_log para salida limpia */
        if (hay_alarma) {
            pthread_mutex_lock(&mutex_log);
            printf("Hay datos faltantes, por favor revisar el sensor %s "
                   "(entre %s y %s).\n",
                   be->id, hora_ant, hora_curr);
            fflush(stdout);
            pthread_mutex_unlock(&mutex_log);
        }

        /* Escribir en el archivo consolidado (sección crítica global) */
        pthread_mutex_lock(&mutex_log);
        fprintf(log_consol, "%s,%d,%d,%d,%s\n",
                m.estacion, m.humedad, m.rocio, m.presion, m.hora);
        fflush(log_consol);
        pthread_mutex_unlock(&mutex_log);
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/* Depositar medición en el buffer de una estación (productor)        */
/* ------------------------------------------------------------------ */
static void depositar(int idx, const Medicion *m) {
    BufferEstacion *be = &estaciones[idx];
    sem_wait(&be->empty);               /* Bloquear si buffer lleno  */
    pthread_mutex_lock(&be->mutex);
    be->slots[be->in] = *m;
    be->in = (be->in + 1) % be->tam;
    pthread_mutex_unlock(&be->mutex);
    sem_post(&be->full);                /* Notificar dato disponible */
}

/* ================================================================== */
/* Hilo recolector                                                     */
/* ================================================================== */
static void *hilo_recolector(void *arg) {
    (void)arg;

    /* Crear el pipe nominal; EEXIST no es error */
    if (mkfifo(pipeNom, 0666) != 0 && errno != EEXIST) {
        perror("monitor: mkfifo");
        return NULL;
    }

    /*
     * open() en lectura se bloquea hasta que al menos un agente abra
     * el extremo escritura. Esto garantiza sincronización de arranque.
     */
    int fd = open(pipeNom, O_RDONLY);
    if (fd < 0) {
        perror("monitor: open pipe");
        return NULL;
    }

    Medicion m;
    ssize_t  n;

    while ((n = read(fd, &m, sizeof(Medicion))) > 0) {

        if ((size_t)n < sizeof(Medicion)) {
            fprintf(stderr, "monitor: lectura parcial (%zd bytes), ignorada\n", n);
            continue;
        }

        pthread_mutex_lock(&mutex_est);

        if (m.es_fin) {
            /*
             * Señal de fin de un agente para su estación.
             * Registrar la estación si no se ha visto todavía
             * (agente que envió solo es_fin sin datos previos).
             */
            int idx_fin = -1;
            for (int i = 0; i < num_estaciones; i++) {
                if (strncmp(estaciones[i].id, m.estacion,
                            sizeof(estaciones[i].id)) == 0) {
                    idx_fin = i;
                    break;
                }
            }

            /* FIX: si el agente no envió datos, registrar la estación ahora
             * para contabilizar correctamente el fin. */
            if (idx_fin < 0) {
                int antes = num_estaciones;
                idx_fin = registrar_estacion(m.estacion);
                if (idx_fin >= 0 && num_estaciones > antes) {
                    agentes_por_estacion[idx_fin] = 1;
                    total_agentes++;
                }
            }

            if (idx_fin >= 0) {
                agentes_fin_por_est[idx_fin]++;
                /* Si llegan más es_fin que agentes registrados para esta
                 * estación, hay un agente extra: ajustar total_agentes */
                if (agentes_fin_por_est[idx_fin] >
                        agentes_por_estacion[idx_fin]) {
                    agentes_por_estacion[idx_fin]++;
                    total_agentes++;
                }
                total_fin++;
            } else {
                /* No se pudo registrar la estación, aun así contamos el fin */
                total_fin++;
                total_agentes++;
            }

            /* FIX: leer total_agentes/total_fin dentro del mutex */
            int fin_local    = total_fin;
            int agentes_local = total_agentes;

            pthread_mutex_unlock(&mutex_est);

            if (agentes_local > 0 && fin_local >= agentes_local) {
                break;   /* Todos los agentes terminaron */
            }
            continue;
        }

        /* Registrar o localizar la estación. */
        int antes = num_estaciones;
        int idx   = registrar_estacion(m.estacion);
        if (num_estaciones > antes) {
            /* Estación completamente nueva */
            agentes_por_estacion[idx] = 1;
            total_agentes++;
        }

        pthread_mutex_unlock(&mutex_est);

        if (idx < 0) continue;

        /* Depositar en el buffer de la estación */
        depositar(idx, &m);
    }

    if (n < 0) perror("monitor: read pipe");

    close(fd);

    /* Señalizar terminación a todos los consumidores depositando un
     * centinela (es_fin=1) en el buffer de cada estación.
     * El consumidor procesa todos los ítems reales antes de encontrar
     * el centinela y salir del loop. */
    pthread_mutex_lock(&mutex_est);
    int total = num_estaciones;
    pthread_mutex_unlock(&mutex_est);

    for (int i = 0; i < total; i++) {
        Medicion centinela;
        memset(&centinela, 0, sizeof(Medicion));
        centinela.es_fin = 1;
        strncpy(centinela.estacion, estaciones[i].id,
                sizeof(centinela.estacion) - 1);
        depositar(i, &centinela);
    }

    return NULL;
}

/* ================================================================== */
/* Categorización final (Tabla 2)                                     */
/* ================================================================== */
static void imprimir_reporte(void) {
    long total_h = 0, total_r = 0, total_p = 0;
    int  total_m = 0;

    for (int i = 0; i < num_estaciones; i++) {
        total_h += estaciones[i].sum_h;
        total_r += estaciones[i].sum_r;
        total_p += estaciones[i].sum_p;
        total_m += estaciones[i].total;
    }

    if (total_m == 0) {
        printf("No se recibieron mediciones válidas.\n");
        printf("Fin del Monitor\xe2\x80\xa6!!!\n");   /* … U+2026 */
        fflush(stdout);
        return;
    }

    float ph = (float)total_h / total_m;
    float pr = (float)total_r / total_m;
    float pp = (float)total_p / total_m;

    /*
     * Categorización (Tabla 2)
     *   Lluvioso : H > 90,   R > 9,   P < 750
     *   Nublado  : H 80-95,  R > 8,   P = 751
     *   Fresco   : H < 80,   R 5-8,   P > 754
     */
    const char *categoria;

    if (ph > 90.0f && pr > 9.0f && pp < 750.0f) {
        categoria = "Lluvioso";
    } else if (ph >= 80.0f && ph <= 95.0f &&
               pr > 8.0f &&
               pp >= 750.5f && pp <= 751.5f) {
        categoria = "Nublado";
    } else if (ph < 80.0f &&
               pr >= 5.0f && pr <= 8.0f &&
               pp > 754.0f) {
        categoria = "Fresco";
    } else {
        /*
         * Los promedios no caen exactamente en ninguna categoría.
         * Se selecciona la más cercana por distancia euclidiana al
         * punto central de cada categoría:
         *   Lluvioso → (H=95, R=10.5, P=745)
         *   Nublado  → (H=87.5, R=8.5, P=751)
         *   Fresco   → (H=75,   R=6.5, P=757)
         */
        float dL = (ph-95.0f)*(ph-95.0f) + (pr-10.5f)*(pr-10.5f) + (pp-745.0f)*(pp-745.0f);
        float dN = (ph-87.5f)*(ph-87.5f) + (pr- 8.5f)*(pr- 8.5f) + (pp-751.0f)*(pp-751.0f);
        float dF = (ph-75.0f)*(ph-75.0f) + (pr- 6.5f)*(pr- 6.5f) + (pp-757.0f)*(pp-757.0f);

        if (dL <= dN && dL <= dF)      categoria = "Lluvioso";
        else if (dN <= dL && dN <= dF) categoria = "Nublado";
        else                           categoria = "Fresco";
    }

    printf("Parte Meteorológico Bogotá: \"%s\"\n", categoria);
    printf("Fin del Monitor\xe2\x80\xa6!!!\n");   /* … U+2026 */
    fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* Liberación de recursos                                             */
/* ------------------------------------------------------------------ */
static void liberar_recursos(void) {
    for (int i = 0; i < num_estaciones; i++) {
        sem_destroy(&estaciones[i].empty);
        sem_destroy(&estaciones[i].full);
        pthread_mutex_destroy(&estaciones[i].mutex);
        free(estaciones[i].slots);
        estaciones[i].slots = NULL;
    }
    pthread_mutex_destroy(&mutex_log);
    pthread_mutex_destroy(&mutex_est);
    if (pipeNom) unlink(pipeNom);
}

/* ================================================================== */
/* main                                                               */
/* ================================================================== */
int main(int argc, char *argv[]) {
    int opt;

    while ((opt = getopt(argc, argv, "b:p:")) != -1) {
        switch (opt) {
            case 'b': tam_buffer = atoi(optarg); break;
            case 'p': pipeNom    = optarg;       break;
            default:
                fprintf(stderr, "Uso: %s -b tamBuffer -p pipeNom\n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (tam_buffer <= 0) {
        fprintf(stderr, "Error: tamBuffer debe ser > 0.\n");
        return EXIT_FAILURE;
    }
    if (!pipeNom) {
        fprintf(stderr, "Error: debe especificarse -p pipeNom.\n");
        return EXIT_FAILURE;
    }

    /* Inicializar mutexes globales */
    if (pthread_mutex_init(&mutex_log, NULL) != 0 ||
        pthread_mutex_init(&mutex_est, NULL) != 0) {
        perror("monitor: pthread_mutex_init global");
        return EXIT_FAILURE;
    }

    /* Crear archivo consolidado */
    log_consol = fopen(ARCHIVO_CONSOL, "w");
    if (!log_consol) {
        perror("monitor: fopen consolidado.csv");
        return EXIT_FAILURE;
    }
    time_t ahora = time(NULL);
    char   ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&ahora));
    fprintf(log_consol,
            "hora_consolidado,%s\n"
            "estacion,humedad,rocio,presion,hora\n", ts);
    fflush(log_consol);

    printf("... Control de Categorización Meteorológica!!!\n");
    fflush(stdout);

    /* Crear hilo recolector */
    pthread_t h_recolector;
    if (pthread_create(&h_recolector, NULL, hilo_recolector, NULL) != 0) {
        perror("monitor: pthread_create recolector");
        fclose(log_consol);
        return EXIT_FAILURE;
    }

    /* Esperar a que el recolector termine */
    pthread_join(h_recolector, NULL);

    /* Esperar a que todos los hilos consumidores terminen */
    pthread_mutex_lock(&mutex_est);
    int n = num_estaciones;
    pthread_mutex_unlock(&mutex_est);

    for (int i = 0; i < n; i++) {
        pthread_join(hilos_consumidores[i], NULL);
    }

    /* Cerrar consolidado */
    fclose(log_consol);
    log_consol = NULL;

    /* Imprimir reporte */
    imprimir_reporte();

    /* Liberar recursos y eliminar pipe */
    liberar_recursos();

    return EXIT_SUCCESS;
}
