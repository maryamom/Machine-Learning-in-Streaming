#!/usr/bin/env python3
"""Send transaction events from data/sample_transactions.json to Kafka topic 'transactions'."""
import os
import sys
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "sample_transactions.json")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TRANSACTIONS_TOPIC", "transactions")

def main():
    if not os.path.isfile(DATA):
        print("File not found: " + DATA, file=sys.stderr)
        sys.exit(1)
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP.split(","),
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )
    except KafkaError as e:
        print("Kafka error: " + str(e), file=sys.stderr)
        sys.exit(1)
    with open(DATA, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                producer.send(TOPIC, value=line)
                time.sleep(0.1)
    producer.flush()
    producer.close()
    print("Done.")

if __name__ == "__main__":
    main()
