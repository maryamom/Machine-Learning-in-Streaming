#!/usr/bin/env python3
"""Phase 2: Detect unusual events - flag events that deviate significantly from batch stats."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType, StringType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")
THRESHOLD_STD = float(os.environ.get("ANOMALY_STD_THRESHOLD", "3.0"))

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
    spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(F.from_json(F.col("value").cast(StringType()), SCHEMA).alias("data")).select("data.*")
    parsed = parsed.withColumn("Amount", F.coalesce(F.col("Amount"), F.lit(0.0)))

    def flag_anomalies(batch_df, batch_id):
        pdf = batch_df.toPandas()
        if pdf.empty:
            return
        amt = pdf["Amount"]
        mean_a = amt.mean()
        std_a = amt.std()
        if std_a and std_a > 0:
            z = (amt - mean_a) / std_a
            pdf["is_anomaly"] = (z.abs() > THRESHOLD_STD)
        else:
            pdf["is_anomaly"] = False
        anomalies = pdf[pdf["is_anomaly"]]
        if not anomalies.empty:
            print("Batch", batch_id, "anomalies (|z| > " + str(THRESHOLD_STD) + "):")
            print(anomalies[["Time", "Amount"]].to_string())

    q = parsed.writeStream.foreachBatch(flag_anomalies).start()
    q.awaitTermination()

if __name__ == "__main__":
    main()
