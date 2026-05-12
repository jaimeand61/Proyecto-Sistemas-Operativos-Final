/*
 * common.h - Definiciones compartidas entre agenteM y monitor
 * Sistema de Categorización Meteorológica - Bogotá
 * Sistemas Operativos 2026-30
 */

#ifndef COMMON_H
#define COMMON_H

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>

/* ---------- Estructura de medición enviada por el pipe ---------- */
typedef struct {
    char estacion[5];   /* Identificador de estación: EK, ET, EU, etc. */
    int  humedad;       /* Humedad relativa (%)                         */
    int  rocio;         /* Punto de rocío (°C)                          */
    int  presion;       /* Presión atmosférica (hPa)                    */
    char hora[9];       /* Hora de medición HH:MM:SS                    */
    int  es_fin;        /* 1 = señal de fin de agente, 0 = dato válido  */
} Medicion;

/* ---------- Rangos aceptables (Tabla 1) ---------- */
#define HUM_MIN  77
#define HUM_MAX  100
#define PRE_MIN  740
#define PRE_MAX  760
#define ROC_MIN  3
#define ROC_MAX  12

#endif /* COMMON_H */
