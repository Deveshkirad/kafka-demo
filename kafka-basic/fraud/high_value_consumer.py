from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'fraud-detection',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for transactions above 5000...")

for message in consumer:
    tx = message.value

    if tx.get("amount", 0) > 5000:
        print("\n=== HIGH VALUE TRANSACTION ===")
        print(f"User ID : {tx['userId']}")
        print(f"Tx ID   : {tx['tx_id']}")
        print(f"Amount  : ${tx['amount']}")
        print(f"Time    : {tx['timestamp']}")
        print("============================")