from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value

    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1

    total_amount[store] += amount

    msg_count += 1

    if msg_count % 10 == 0:
        print("\n=== PODSUMOWANIE ===")
        print(f"Przetworzono: {msg_count} wiadomości")
        print(f"{'Sklep':<12} {'Liczba':<10} {'Suma (PLN)':<15}")
        print("-" * 40)

        for sklep in store_counts:
            print(f"{sklep:<12} {store_counts[sklep]:<10} {total_amount[sklep]:<15.2f}")
