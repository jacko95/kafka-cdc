# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("KafkaDebezium").getOrCreate()
#
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("subscribe", "mysql.db_cdc.utenti") \
#     .option("startingOffsets", "earliest") \
#     .load()
#
# df.selectExpr("CAST(value AS STRING)") \
#     .writeStream \
#     .format("console") \
#     .start() \
#     .awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StringType
#
# spark = (
#     SparkSession.builder
#     .appName("KafkaDebeziumReader")
#     .getOrCreate()
# )
#
# spark.sparkContext.setLogLevel("WARN")
#
# # Legge da Kafka
# df = (
#     spark.readStream
#     .format("kafka")
#     # .option("kafka.bootstrap.servers", "kafka:29092")
#     .option("kafka.bootstrap.servers", "kafka:9092")
#     .option("subscribe", "mysql.db_cdc.utenti")
#     .option("startingOffsets", "earliest")
#     .load()
# )
#
# # Kafka value è binaria → stringa JSON
# json_df = df.select(
#     col("topic"),
#     col("partition"),
#     col("offset"),
#     col("timestamp"),
#     col("key").cast(StringType()).alias("key"),
#     col("value").cast(StringType()).alias("value")
# )
#
# # Output a console
# query = (
#     json_df.writeStream
#     .format("console")
#     .option("truncate", "false")
#     .start()
# )
#
# query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("DebeziumCDCReader") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "mysql.db_cdc.test") \
    .option("startingOffsets", "earliest") \
    .load()

df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

