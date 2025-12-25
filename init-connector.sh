#!/bin/sh

set -e

CONNECTOR_NAME="mysql-cdc"

echo "⏳ Attendo Kafka Connect su http://debezium:8083 ..."

until curl -s http://debezium:8083/ > /dev/null; do
  echo "Kafka Connect non pronto, riprovo tra 5s..."
  sleep 5
done

echo "✅ Kafka Connect è online"

# Se il connettore esiste già, lo elimino
if curl -s http://debezium:8083/connectors/${CONNECTOR_NAME} > /dev/null; then
  echo "⚠️ Connettore ${CONNECTOR_NAME} già presente, lo elimino"
  curl -s -X DELETE http://debezium:8083/connectors/${CONNECTOR_NAME}
  sleep 2
fi

echo "🚀 Registro il connettore Debezium MySQL (${CONNECTOR_NAME})"

curl -f -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-cdc",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",

      "database.hostname": "mysql",
      "database.port": "3306",
      "database.user": "cdc_user",
      "database.password": "cdc_password",
      "database.server.id": "1",

      "topic.prefix": "mysql",
      "database.include.list": "db_cdc",

      "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
      "schema.history.internal.kafka.topic": "schema-changes.db_cdc",

      "snapshot.mode": "initial"
    }
  }'

echo "✅ Connettore ${CONNECTOR_NAME} registrato con successo"
