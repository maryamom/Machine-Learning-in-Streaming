#!/usr/bin/env python3
"""Phase 1: Stream from Kafka, load pre-trained model, apply to each event, output predictions."""
import os
import sys

import joblib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType, StringType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(SCRIPT_DIR, "model.pkl")
FEATURES = ["Time", "V1", "V2", "V3", "V4", "Amount"]
SCHEMA = StructType([
    StructField("Time", LongType(), True),
    StructField("V1", DoubleType(), True),
    StructField("V2", DoubleType(), True),
    StructField("V3", DoubleType(), True),
    StructField("V4", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", IntegerType(), True),
])

def main():
    if not os.path.isfile(MODEL_PATH):
        print("Run train_model.py first. Missing " + MODEL_PATH, file=sys.stderr)
        sys.exit(1)
    model = joblib.load(MODEL_PATH)
    spark = SparkSession.builder.appName("StreamInference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    value_str = raw.select(F.col("value").cast(StringType()))
    parsed = value_str.select(F.from_json(F.col("value"), SCHEMA).alias("data")).select("data.*")
    for c in FEATURES:
        parsed = parsed.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    def apply_model(batch_df, batch_id):
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        X = pdf[FEATURES].fillna(0)
        pdf["prediction"] = model.predict(X)
        print("Batch", batch_id, "predictions:")
        print(pdf[FEATURES + ["prediction"]].to_string())

    q = parsed.writeStream.foreachBatch(apply_model).start()
    q.awaitTermination()

if __name__ == "__main__":
    main()
