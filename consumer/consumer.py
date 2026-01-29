import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "kafka:29092"
TOPIC = "plc-data"

def make_consumer():
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="demo-consumer",
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
        except NoBrokersAvailable:
            print("Kafka no disponible aún, reintentando en 2s...")
            time.sleep(2)

consumer = make_consumer()

for msg in consumer:
    print("CONSUMED:", msg.value)
