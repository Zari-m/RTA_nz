from fastapi import FastAPI
from pydantic import BaseModel
import pickle, numpy as np
import pandas as pd

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    hour: int
    is_electronics: int
    tx_per_day: int

# TWÓJ KOD
# Endpoint POST /score
# Przyjmij Transaction, zwróć: {"is_fraud": bool, "fraud_probability": float}
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/score")
def score_transaction(tx: Transaction):
    features = pd.DataFrame([{
        "amount": tx.amount,
        "hour": tx.hour,
        "is_electronics": tx.is_electronics,
        "tx_per_day": tx.tx_per_day
    }])
    
    prediction = model.predict(features)[0]
    probability = model.predict_proba(features)[0][1]

    return {
        "is_fraud": bool(prediction),
        "fraud_probability": float(probability)
    }
