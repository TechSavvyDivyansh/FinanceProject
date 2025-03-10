from kafka import KafkaConsumer

TOPIC_NAME = "transaction-stream"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode("utf-8")
)

print("Listening for messages...")

for message in consumer:
    print(f"Received: {message.value}")
