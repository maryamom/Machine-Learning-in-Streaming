# Lab — Machine Learning on Streams

You will build a **real-time Machine Learning pipeline** using **Kafka** and **Spark Structured Streaming**, and apply ML concepts on **continuous data streams**.

---

## Concepts Covered

- **Streaming & Systems**: Kafka topics, producers, consumers, Kafka → Spark
- **Stream Processing**: Spark Structured Streaming, micro-batch model
- **ML on Streams**: Streaming inference, anomaly detection, drift monitoring

---

## Data

Credit Card Fraud Detection (Kaggle): https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

Place `creditcard.csv` in `data/` or use the included sample. Run `spark/train_model.py` to train and save the model.

---

## Lab Structure

### Phase 1 — Streaming Inference
- Stream events from Kafka into Spark
- Load pre-trained model, apply to each event, output predictions

### Phase 2 — Anomaly Detection
- Compute statistics on the stream, flag events that deviate significantly

### Phase 3 — Drift Monitoring
- Track prediction statistics over time, detect potential concept drift

---

## Requirements

- Docker, Python 3.9+, Kafka via Docker, Spark (local or Docker)

## Quick Start

1. `docker compose up -d` (start Kafka)
2. Train model: `python spark/train_model.py`
3. Producer: `python kafka/producer.py`
4. Phase 1: `python spark/stream_inference.py`
5. Phase 2: `python spark/anomaly_detection.py`
6. Phase 3: `python spark/monitoring.py`
