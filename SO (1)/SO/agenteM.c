/*
 * agenteM.c - Agente de Mediciones
 * Sistema de Categorización Meteorológica - Bogotá
 * Sistemas Operativos 2026-30
 *
 * Uso: ./agenteM -f nombreArchivo -t tiempo -p pipeNom
 *
 * Lee un archivo CSV de mediciones de una estación meteorológica,
 * filtra los valores fuera de rango (Tabla 1) y envía las mediciones
 * válidas al proceso monitor a través de un pipe nominal.
 * Al finalizar, envía una señal de fin (es_fin=1) al monitor.
 */

#include "common.h"

/* ------------------------------------------------------------------ */
static void uso(const char *prog) {
    fprintf(stderr, "Uso: %s -f nombreArchivo -t tiempo -p pipeNom\n", prog);
    exit(EXIT_FAILURE);
}

/* ================================================================== */
int main(int argc, char *argv[]) {

    char *archivo = NULL;
    char *pipeNom = NULL;
    int   tiempo  = -1;
    int   opt;

    /* Argumentos en orden libre gracias a getopt */
    while ((opt = getopt(argc, argv, "f:t:p:")) != -1) {
        switch (opt) {
            case 'f': archivo = optarg;       break;
            case 't': tiempo  = atoi(optarg); break;
            case 'p': pipeNom = optarg;       break;
            default:  uso(argv[0]);
        }
    }

    /* Validación de argumentos obligatorios */
    if (archivo == NULL || pipeNom == NULL || tiempo < 0) {
        fprintf(stderr, "Error: faltan argumentos obligatorios.\n");
        uso(argv[0]);
    }

    /* Abrir el archivo de sensores */
    FILE *fp = fopen(archivo, "r");
    if (!fp) {
        perror("agenteM: fopen archivo sensores");
        return EXIT_FAILURE;
    }

    /*
     * Abrir el pipe nominal en escritura.
     * Esta llamada se bloquea hasta que el monitor abra el extremo de lectura.
     */
    int fd = open(pipeNom, O_WRONLY);
    if (fd < 0) {
        perror("agenteM: open pipe nominal");
        fclose(fp);
        return EXIT_FAILURE;
    }

    printf("... Agente de Medición en proceso!!!\n");
    fflush(stdout);

    char     linea[128];
    Medicion m;
    char     estacion_id[5] = {0};   /* Guardamos el id para el mensaje final */
    int      enviadas    = 0;
    int      rechazadas  = 0;

    while (fgets(linea, sizeof(linea), fp)) {

        /* Quitar el salto de línea final */
        linea[strcspn(linea, "\n")] = '\0';
        linea[strcspn(linea, "\r")] = '\0';

        /* Punto de fin de archivo:
         * El enunciado indica esperar un tiempo adicional -t antes de finalizar. */
        if (linea[0] == '.') {
            sleep(tiempo);
            break;
        }

        /* Ignorar líneas vacías o comentarios */
        if (linea[0] == '\0' || linea[0] == '#') {
            continue;
        }

        /* Parsear CSV: estacion,humedad,rocio,presion,hora */
        memset(&m, 0, sizeof(Medicion));
        if (sscanf(linea, "%4[^,],%d,%d,%d,%8s",
                   m.estacion, &m.humedad, &m.rocio,
                   &m.presion, m.hora) != 5) {
            fprintf(stderr, "agenteM: línea con formato inválido, ignorada: '%s'\n",
                    linea);
            continue;
        }

        /* Guardar id de estación para el mensaje de fin */
        strncpy(estacion_id, m.estacion, sizeof(estacion_id) - 1);

        /* Validar rangos aceptables (Tabla 1) */
        int hum_ok = (m.humedad >= HUM_MIN && m.humedad <= HUM_MAX);
        int roc_ok = (m.rocio   >= ROC_MIN && m.rocio   <= ROC_MAX);
        int pre_ok = (m.presion >= PRE_MIN && m.presion <= PRE_MAX);

        if (hum_ok && roc_ok && pre_ok) {
            m.es_fin = 0;
            ssize_t w = write(fd, &m, sizeof(Medicion));
            if (w < 0) {
                perror("agenteM: write pipe");
                break;
            }
            enviadas++;
        } else {
            rechazadas++;
        }

        /* Esperar antes de leer la siguiente línea */
        sleep(tiempo);
    }

    /* Enviar señal de fin al monitor */
    memset(&m, 0, sizeof(Medicion));
    m.es_fin = 1;
    strncpy(m.estacion, estacion_id, sizeof(m.estacion) - 1);
    if (write(fd, &m, sizeof(Medicion)) < 0) {
        perror("agenteM: write señal de fin");
    }

    /* Liberar recursos */
    close(fd);
    fclose(fp);

    /* Mensaje de finalización requerido por el enunciado */
    printf("Fin de Lectura de Sensores de la Estación %s!!!\n",
           estacion_id[0] ? estacion_id : "DESCONOCIDA");
    fflush(stdout);

    return EXIT_SUCCESS;
}
