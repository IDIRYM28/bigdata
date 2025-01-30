from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json, when, to_date, lit, from_utc_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import boto3
import json
from io import BytesIO

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "bigdata"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_BUCKET = "warehouse"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
USD_TO_EUR_RATE = 0.92
DEFAULT_TIMEZONE = "Europe/Paris"

def write_to_minio(df, epoch_id):
    df_json = df.toJSON().collect()
    if not df_json:
        return

    # Initialisation du client MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Création d'un fichier JSON en mémoire
    json_data = "\n".join(df_json).encode("utf-8")
    file_name = f"data_batch_{epoch_id}.json"
    s3_client.put_object(Bucket=MINIO_BUCKET, Key=file_name, Body=BytesIO(json_data))

    print(f"Uploaded {file_name} to MinIO")

# Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

schema = StructType() \
        .add("id_transaction", StringType()) \
        .add("type_transaction", StringType()) \
        .add("montant", DoubleType()) \
        .add("devise", StringType()) \
        .add("date", StringType()) \
        .add("lieu", StringType()) \
        .add("moyen_paiement", StringType()) \
        .add("details", StructType()
             .add("produit", StringType())
             .add("quantite", DoubleType())
             .add("prix_unitaire", DoubleType())) \
        .add("utilisateur", StructType()
             .add("id_utilisateur", StringType())
             .add("nom", StringType())
             .add("adresse", StringType())
             .add("email", StringType()))

# Lecture du streaming Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")


# ✅ 2. Remplacer `devise` par `"EUR"`
df_parsed = df_parsed.withColumn("devise", lit("EUR"))
df_parsed = df_parsed.withColumn("montant", col("montant") * lit(USD_TO_EUR_RATE))
# ✅ 3. Convertir `date` de String à Date (format `yyyy-MM-dd`)

df_parsed = df_parsed.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
df_parsed = df_parsed.filter(col("utilisateur.adresse").isNotNull())
# Écriture dans MinIO
df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
df_parsed.writeStream \
    .foreachBatch(write_to_minio) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
