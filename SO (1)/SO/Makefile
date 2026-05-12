# Makefile - Sistema de Categorización Meteorológica
# Sistemas Operativos 2026-30
#
# Uso:
#   make          → compila agenteM y monitor
#   make clean    → elimina binarios, consolidado y pipe

CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -g
LIBS    = -lpthread -lrt

TARGETS = agenteM monitor

.PHONY: all clean

all: $(TARGETS)

agenteM: agenteM.c common.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

monitor: monitor.c common.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

clean:
	rm -f $(TARGETS) consolidado.csv miPipe pipeNom
