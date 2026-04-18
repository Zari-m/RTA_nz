from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

# TWÓJ KOD
# Dla każdej transakcji:
# 1. Wyciągnij cechy (amount, hour z timestamp, is_electronics, tx_per_day=5)
# 2. requests.post(API_URL, json=features)
# 3. Jeśli is_fraud: wyślij do tematu 'alerts', wypisz ALERT
for message in consumer:
    tx = message.value

    features = {
        "amount": tx["amount"],
        "hour": datetime.fromisoformat(tx["timestamp"]).hour,
        "is_electronics": 1 if tx["category"] == "elektronika" else 0,
        "tx_per_day": 5
    }

    r = requests.post(API_URL, json=features)
    result = r.json()

    if result["is_fraud"]:
        alert = {
            "alert_time": datetime.now().isoformat(),
            "transaction": tx,
            "fraud_probability": result["fraud_probability"]
        }
        alert_producer.send("alerts", value=alert)
        print("ALERT:", alert)
