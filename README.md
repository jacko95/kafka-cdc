# Kafka CDC

Questo repository dimostra l'implementazione di **Change Data Capture (CDC)** utilizzando Apache Kafka. Cattura le modifiche da un database MySQL e le rende disponibili su topic Kafka, che vengono poi consumati e processati con diversi framework.

## Overview

- **Obiettivo**: Fornire esempi pratici di pipeline CDC con Kafka come sistema di messaggistica centrale.
- **Tecnologie utilizzate**:
    - Python (FastAPI, Flask, Spark)
    - Java (Apache Flink)
    - Docker & Docker Compose
    - MySQL (database sorgente)
    - Kafka Connect + Debezium (per il CDC)

Il tutto è containerizzato per un setup rapido e riproducibile.

## Struttura del Repository

- `fastAPI/`        → Servizio FastAPI per consumare e processare i topic Kafka
- `flask/`          → Applicazione Flask per il consumo Kafka
- `flink/`          → Job Apache Flink per l'elaborazione di stream
- `mysql/`          → Configurazioni, schema e script di inizializzazione MySQL
- `spark/`          → Applicazione Apache Spark (batch/streaming) su Kafka
- `docker-compose.yml` → Definizione dei servizi (Kafka, Zookeeper, MySQL, Kafka Connect, ecc.)
- `init-connector.sh` → Script per registrare il connettore Debezium (CDC MySQL → Kafka)

## Prerequisiti

- Docker e Docker Compose installati
- Conoscenza base di Kafka e concetti di CDC

## Installazione e Avvio

1. Clona il repository:
   ```bash
   git clone https://github.com/jacko95/kafka-cdc.git
   cd kafka-cdc
   ```

2. Avvia i container:
    ```Bash 
    docker-compose up -d
    ```