#!/usr/bin/env python3
"""Train a small classifier on sample data and save for streaming inference."""
import json
import os
import sys

import pandas as pd
from sklearn.linear_model import LogisticRegression

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "sample_transactions.json")
MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model.pkl")

FEATURES = ["Time", "V1", "V2", "V3", "V4", "Amount"]
TARGET = "Class"

def main():
    if not os.path.isfile(DATA):
        print(f"Missing {DATA}", file=sys.stderr)
        sys.exit(1)
    rows = []
    with open(DATA) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    X = df[FEATURES]
    y = df[TARGET]
    model = LogisticRegression(max_iter=500, random_state=42)
    model.fit(X, y)
    import joblib
    joblib.dump(model, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

if __name__ == "__main__":
    main()
