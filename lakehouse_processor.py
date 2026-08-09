import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Initialize PySpark Session with Delta Lake Support
spark = SparkSession.builder \
    .appName("Realtime-Financial-Lakehouse-Vector-Engine") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Ingestion Schema
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("merchant", StringType(), False),
    StructField("risk_score", DoubleType(), False),
    StructField("compliance_notes", StringType(), True),
    StructField("event_timestamp", StringType(), False)
])

# Initialize Embedder & Qdrant Client for Micro-batch Processing
embedder = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient(host="localhost", port=6333)

COLLECTION_NAME = "compliance_audit_vectors"
if not qdrant.collection_exists(COLLECTION_NAME):
    qdrant.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
    )

def process_micro_batch(df, batch_id):
    """Executes dual-path write: Delta Lake + Qdrant Vector Upsert."""
    if df.isEmpty():
        return

    print(f"\n--- Processing Micro-Batch ID: {batch_id} ---")
    
    # 1. Delta Lake Write (ACID Lakehouse Storage)
    df.write.format("delta").mode("append").save("data/delta/financial_transactions")
    print(f"[Lakehouse] Persisted micro-batch {batch_id} to Delta Lake.")

    # 2. Extract High-Risk Transactions for AI Vector Indexing
    high_risk_rows = df.filter(col("risk_score") > 0.70).collect()
    
    if high_risk_rows:
        points = []
        for idx, row in enumerate(high_risk_rows):
            vector = embedder.encode(row["compliance_notes"]).tolist()
            point_id = abs(hash(row["transaction_id"])) % (10 ** 8)
            
            points.append(PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "transaction_id": row["transaction_id"],
                    "account_id": row["account_id"],
                    "amount": row["amount"],
                    "risk_score": row["risk_score"],
                    "compliance_notes": row["compliance_notes"]
                }
            ))

        qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
        print(f"[Vector Engine] Upserted {len(points)} high-risk compliance records into Qdrant.")

# Stream Processing Pipeline
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = parsed_stream.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "data/checkpoints/financial_stream") \
    .start()

query.awaitTermination()