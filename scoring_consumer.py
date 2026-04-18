from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []

    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1')

    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2')

    if 'hour' in tx:
        hour = tx['hour']
    else:
        hour = datetime.fromisoformat(tx['timestamp']).hour

    if hour < 6:
        score += 2
        rules.append('R3')

    return score, rules

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)

    if score >= 3:
        alert = {
            'tx_id': tx['tx_id'],
            'user_id': tx['user_id'],
            'amount': tx['amount'],
            'store': tx['store'],
            'category': tx['category'],
            'timestamp': tx['timestamp'],
            'score': score,
            'rules': rules,
            'status': 'PODEJRZANA'
        }

        if 'hour' in tx:
            alert['hour'] = tx['hour']

        alert_producer.send('alerts', value=alert)
        alert_producer.flush()

        print(f"ALERT: {alert['tx_id']} | score={score} | rules={rules} | amount={alert['amount']:.2f} PLN")
