import json
import time
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

app = FastAPI()

BOOTSTRAP = "kafka:29092"
TOPIC = "api-data"
producer = None

def make_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=10,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
        except NoBrokersAvailable:
            print("Kafka no disponible aún (API), reintentando en 2s...")
            time.sleep(2)

producer = make_producer()

@app.post("/ingest")
def ingest(data: dict):
    try:
        producer.send(TOPIC, data)
        producer.flush()
        return {"status": "ok", "sent": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
