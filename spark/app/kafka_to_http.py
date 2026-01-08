# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import *
# import json  # <-- AGGIUNGI QUESTO
# import requests
#
# spark = SparkSession.builder \
#     .appName("KafkaToHTTP") \
#     .master("local[*]") \
#     .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_http") \
#     .getOrCreate()
#
# # Schema per Debezium (COMPLETO)
# debezium_schema = StructType([
#     StructField("schema", StringType(), True),
#     StructField("payload", StructType([
#         StructField("before", StringType(), True),
#         StructField("after", StringType(), True),
#         StructField("source", StringType(), True),
#         StructField("op", StringType(), True),
#         StructField("ts_ms", LongType(), True)
#     ]), True)
# ])
#
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "mysql.db_cdc.utenti") \
#     .option("startingOffsets", "latest") \
#     .load()
#
# def send_batch(df, epoch_id):
#     print(f"\n=== BATCH {epoch_id} ===")
#     records = df.collect()
#     print(f"Record ricevuti: {len(records)}")
#
#     for i, row in enumerate(records):
#         try:
#             # 1. Decodifica la stringa
#             json_string = row.value.decode('utf-8')
#             print(f"\n[{epoch_id}-{i}] JSON grezzo (primi 200 caratteri):")
#             print(json_string[:200])
#
#             # 2. PARSALA in oggetto Python
#             data = json.loads(json_string)
#
#             # 3. Invia l'oggetto Python (non la stringa!)
#             response = requests.post(
#                 "http://host.docker.internal:5000/events",
#                 json=data,  # <-- NOTA: json=, non data=
#                 timeout=5
#             )
#
#             print(f"✅ Invio OK: {response.status_code}")
#
#         except json.JSONDecodeError as e:
#             print(f"❌ ERRORE JSON: {e}")
#             print(f"Stringa problematica: {json_string[:100]}")
#
#         except requests.exceptions.ConnectionError:
#             print(f"❌ Server non raggiungibile")
#
#         except Exception as e:
#             print(f"⚠️  Errore: {type(e).__name__}: {str(e)[:100]}")
#
# query = df.writeStream \
#     .foreachBatch(send_batch) \
#     .trigger(processingTime="10 seconds") \
#     .start()
#
# print("=== STREAMING AVVIATO ===")
# print("In attesa di dati da Kafka...")
# print("Per testare, inserisci dati in MySQL:")
# print("docker exec mysql mysql -u cdc_user -pcdc_password -e 'INSERT INTO db_cdc.utenti (name) VALUES (\"test\");'")
#
# query.awaitTermination()

# -----------------------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, get_json_object
from pyspark.sql.types import *
import json
import requests

spark = SparkSession.builder \
    .appName("KafkaToHTTP-Clean") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_clean") \
    .getOrCreate()

# Leggi da Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "mysql.db_cdc.utenti") \
    .option("startingOffsets", "earliest") \
    .load()

def send_batch(df, epoch_id):
    print(f"\n{'='*60}")
    print(f"BATCH {epoch_id}")
    print(f"{'='*60}")
    
    if df.isEmpty():
        print("Nessun dato")
        return
    
    # Converti ogni record
    records = df.select(
        col("value").cast("string").alias("json_str")
    ).collect()
    
    for i, record in enumerate(records):
        try:
            json_str = record.json_str
            
            # DEBUG: mostra cosa stai inviando
            print(f"\n[{i+1}/{len(records)}] Invio...")
            
            # Invia la stringa JSON DIRETTAMENTE come raw data
            # (alcuni server preferiscono così)
            response = requests.post(
                "http://host.docker.internal:5000/events",
                data=json_str,  # NOTA: data=, non json=
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            
            print(f"✅ Status: {response.status_code}")
        
        except requests.exceptions.ConnectionError:
            print(f"❌ Server non raggiungibile su host.docker.internal:5000")
            print("   Avvia un server con: python -m http.server 5000")
            return
        
        except Exception as e:
            print(f"⚠️  Errore: {e}")
            continue

# Avvia lo streaming
query = df.writeStream \
    .foreachBatch(send_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Kafka -> HTTP Streaming avviato")
print("📍 Target: http://host.docker.internal:5000/events")
print("⏳ In attesa di dati...")

query.awaitTermination()

# -----------------------------------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import *
# import requests
#
# # spark = SparkSession.builder \
# #     .appName("KafkaToHTTP") \
# #     .getOrCreate()
# spark = SparkSession.builder.appName("KafkaToHTTP").master("local[*]").config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_http").getOrCreate()
#
# schema = StructType() \
#     .add("after", StringType()) \
#     .add("op", StringType())
#
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "mysql.db_cdc.utenti") \
#     .load()
#
# def send_batch(df, epoch_id):
#     for row in df.collect():
#         try:
#             requests.post("http://host.docker.internal:5000/events",
#             # requests.post("http://localhost:5000/events",
#                           json=row.value.decode('utf-8'),
#                           timeout=5)
#         except:
#             pass
#
# query = df.writeStream \
#     .foreachBatch(send_batch) \
#     .start()
#
# query.awaitTermination()

# -----------------------------------------------------------------------------------------------------------------

# val streamingDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "mysql.db_cdc.utenti").option("startingOffsets", "earliest").load();
# val query = streamingDF.selectExpr("CAST(value AS STRING) as json_message").writeStream.outputMode("append").format("console").option("truncate", false).start();
#
# val batchDF = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "mysql.db_cdc.utenti").option("startingOffsets", "earliest").option("endingOffsets", "latest").load().limit(5);
# batchDF.selectExpr("CAST(value AS STRING) as json").show(false)