import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "kafka:29092"
TOPIC = "plc-data"

def make_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=10,
                linger_ms=10,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
        except NoBrokersAvailable:
            print("Kafka no disponible aún, reintentando en 2s...")
            time.sleep(2)

producer = make_producer()

while True:
    data = {
        "plc_id": "PLC-01",
        "temperature": round(random.uniform(20, 90), 2),
        "pressure": round(random.uniform(1, 10), 2),
        "timestamp": time.time(),
    }
    producer.send(TOPIC, data)
    producer.flush()
    print("PLC sent:", data)
    time.sleep(2)
