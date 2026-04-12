%%file consumer_anomalies.py

from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję anomalii prędkości (więcej niż 3 transakcje / 60s)...")

user_history = defaultdict(list)

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')

    current_time = time.time()
    user_history[user_id].append(current_time)
    
    window_start = current_time - 60
    user_history[user_id] = [t for t in user_history[user_id] if t > window_start]
    
    tx_count = len(user_history[user_id])
    
    if tx_count > 3:
        print(f"[ALERT] Użytkownik {user_id} wykonał {tx_count} transakcji w ciągu minuty!")
