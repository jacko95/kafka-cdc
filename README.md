# Kafka CDC – Change Data Capture con Apache Kafka

Questo repository dimostra l'implementazione di una pipeline **Change Data Capture (CDC)** utilizzando **Apache Kafka** come backbone di messaggistica.  

Le modifiche (insert, update, delete) su un database **MySQL** vengono catturate in tempo reale tramite **Debezium** e pubblicate su topic Kafka.  
Successivamente, questi eventi vengono consumati e processati da diversi framework per mostrare approcci reali di elaborazione dati in streaming.

Perfetto per imparare CDC, Kafka Connect, Debezium e stream processing con vari tool.

Questa architettura è perfetta per scenari production come:

- **Sincronizzazione dati tra database** (es. MySQL → data warehouse)
- **Cache invalidation** (aggiorna Redis/Memcached quando cambiano i dati)
- **Audit log** distribuito e immutabile
- **Feed di eventi per microservizi** (event-driven architecture)
- **Data integration** verso lakehouse (es. Delta Lake, Iceberg)
- **Monitoring e alerting** su cambiamenti critici
- **Aggiornamento in tempo reale di modelli Machine Learning**  
  Gli eventi CDC possono alimentare feature store (es. Feast) o triggerare ritraining online.  
  Un modello deployato (es. via TensorFlow Serving, MLflow o KServe) può essere aggiornato automaticamente non appena arrivano nuovi dati etichettati o pattern significativi, abilitando **online learning** o **continual learning**.

## Tecnologie Utilizzate

- **MySQL** → Database sorgente
- **Debezium** + **Kafka Connect** → Cattura delle modifiche (CDC)
- **Apache Kafka** → Messaggistica e streaming
- **Python**:
  - FastAPI (API REST per consumo dati)
  - Flask (applicazione web semplice)
  - Apache Spark (elaborazione batch/streaming)
- **Java**:
  - Apache Flink (stream processing avanzato)
- **Docker** & **Docker Compose** → Containerizzazione completa

## Struttura del Repository
```text
kafka-cdc/
├── fastAPI/             # Servizio FastAPI per consumo Kafka
├── flask/               # Applicazione Flask per consumo Kafka
├── flink/               # Job Apache Flink per streaming
├── mysql/               # Database MySQL + schema e script
├── spark/               # Applicazione Apache Spark
├── docker-compose.yml   # Definizione dei servizi CDC  
├── init-connector.sh    # Script per registrare il connettore Debezium
├── README.md  
```

## Come Funziona il Flusso CDC

1. **MySQL** → Contiene i dati di esempio (es. tabella `utenti`).
2. **Debezium** (in Kafka Connect) → Monitora il binlog di MySQL e cattura ogni modifica.
3. **Kafka Topic** → Gli eventi CDC vengono pubblicati (es. `mysql.db_cdc.utenti`).
4. **Consumer** → Le varie applicazioni (`fastAPI`, `flask`, `flink`, `spark`) leggono dal topic e processano i dati in tempo reale o batch.

## Prerequisiti

- Docker installato (versione recente)
- Almeno 8GB RAM liberi (per tutti i container)
- Conoscenza base di Kafka, Docker e concetti di CDC

## Installazione e Avvio

1. Clona il repository:
    ```bash
    git clone https://github.com/jacko95/kafka-cdc.git
    cd kafka-cdc
    ```

2. Avvia l'intera infrastruttura:
    ```bash
    docker-compose up -d
    ```

3. Registra il connettore Debezium (attendi che Kafka Connect sia pronto):Bash./init-connector.sh

4. Verifica lo stato del connettore:Bashcurl http://localhost:8083/connectors/mysql-connector/status

5. Genera alcune modifiche nel database MySQL (esempio):
    ```bash
    docker exec -it kafka-cdc-mysql-1 mysql -u root -proot -e "USE inventory"; 
    INSERT INTO users (name, email) VALUES ('Mario Rossi', 'mario@example.com');
    ```

## Come Funziona il Flusso CDC – Spiegato senza giri di parole

Questa pipeline fa una cosa sola: quando qualcuno modifica un record nel database MySQL (aggiunge, cambia o cancella una riga), **l’app lo vede immediatamente** e lo trasforma in un evento che tutti i consumer possono leggere in tempo reale. Ecco il percorso esatto, passo dopo passo:

## Come Funziona il Flusso CDC – Tabella completa (con tutti i passi)

Flusso preciso: MySQL → Debezium → Kafka → Flink/Spark leggono → codice Java (Flink) e Python (Spark) elabora e spara POST → Flask/FastAPI ricevono e aggiornano.

| Passo | Componente                  | Cosa fa esattamente                                                                                   | Dettagli chiave (codice)                       | Dove lo vedi in azione                          | Comando / URL per controllare live                                                                 |
|------|-----------------------------|-------------------------------------------------------------------------------------------------------|------------------------------------------------|------------------------------------------------|----------------------------------------------------------------------------------------------------|
| 1    | **MySQL**                   | INSERT / UPDATE / DELETE su tabella (es. `utenti`)                                                    | Query SQL                                      | Container MySQL                                | `docker exec -it kafka-cdc-mysql-1 mysql -u root -proot inventory`                                  |
| 2    | **MySQL Binlog**            | Registra la modifica nel binlog                                                                       | Config interna MySQL                           | Interno                                        | Attivo di default                                                                                  |
| 3    | **Debezium (Kafka Connect)**| Legge binlog → genera evento JSON CDC                                                                 | Debezium connector                             | Container Kafka Connect                        | `docker ps \| grep 8083` → `docker logs -f <nome-container-connect>`                               |
| 4    | **Apache Kafka**            | Pubblica evento su topic (es. `dbserver1.inventory.utenti`)                                           | Broker                                         | Kafka                                          | Console consumer: `docker exec -it kafka-cdc-kafka-1 kafka-console-consumer ...`                    |
| 5    | **Lettura da Kafka**        | **Flink** e **Spark** consumano gli eventi grezzi dal topic                                           | Flink: codice Java (KafkaSource/Consumer)<br>Spark: codice Python (Structured Streaming readStream) | Flink TaskManager / Spark driver               | Log Flink: `docker logs -f kafka-cdc-flink-taskmanager-1`<br>Log Spark: `docker logs -f kafka-cdc-spark-1` |
| 6    | **Elaborazione**            | Flink e Spark processano (aggregazioni, window, filtri, conteggi, ecc.)                                | Codice Java in Flink<br>Codice Python in Spark | Stessi container                               | UI Flink: http://localhost:8081<br>UI Spark: http://localhost:4040                                 |
| 7    | **Invio risultati (POST)**  | **Codice Java di Flink** e **codice Python di Spark** sparano richieste HTTP POST con i risultati elaborati verso Flask e FastAPI | Java: HttpClient o simili<br>Python: requests.post() | Log di Flink e Spark                           | Nei log vedi righe tipo:<br>`[Java] POST to http://fastapi:8000/result ...`<br>`[Python] requests.post('http://flask:5000/update', json=...)` |
| 8    | **Ricezione POST**          | Flask e FastAPI ricevono le POST, aggiornano stato interno (dashboard, stats, ecc.)                    | Endpoint Flask/FastAPI (es. /update o /result)  | Container flask / fastapi                      | Log: `docker logs -f kafka-cdc-flask-1` → "Received POST from Spark/Flink"<br>Stesso per fastapi    |
| 9    | **Esposizione finale**      | Flask mostra dashboard web aggiornata<br>FastAPI espone API REST con dati elaborati                   | HTML/JSON response                             | Browser / curl                                 | Flask: http://localhost:5000<br>FastAPI: http://localhost:8000/docs o /stats                       |
| 10   | **Utente**                  | Vedi in tempo reale eventi grezzi + risultati aggregati/elaborati                                     | Il tuo browser                                 | Refresh o curl                                 | Tutto si aggiorna automaticamente quando arrivano i POST da Flink/Spark                            |

### Chiarimento definitivo
- **Flink legge da Kafka con codice Java** → elabora → **il Java spara POST** a Flask/FastAPI.
- **Spark legge da Kafka con codice Python** → elabora → **il Python spara POST** a Flask/FastAPI.
- Flask e FastAPI ricevono questi POST e diventano il "pannello di controllo" centrale con dati arricchiti.
- FastAPI/Flask possono anche leggere direttamente da Kafka per eventi grezzi (se nel codice), ma il flusso principale è Flink/Spark → POST.

### Esempio reale di cosa vedi arrivare su Kafka
Quando fai questo comando:

```bash
docker exec -it kafka-cdc-mysql-1 mysql -u root -proot -e "USE inventory; UPDATE utenti SET email = 'nuova@email.it' WHERE id = 1;"
