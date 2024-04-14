import json
import os
import time
from typing import Any, Dict, Optional

import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "station_status")
API_URL = os.getenv(
    "API_URL",
    "https://gbfs.nextbike.net/maps/gbfs/v2/nextbike_vw/en/station_status.json",
)


def fetch_data() -> Optional[Dict[str, Any]]:
    response = requests.get(API_URL, timeout=10)
    if response.status_code == 200:
        return response.json()
    print("Failed to fetch data:", response.status_code)
    return None


def publish_to_kafka(json_body: Dict[str, Any]):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(KAFKA_TOPIC, json_body)
        producer.flush()
    except Exception as e:
        raise RuntimeError("Failed to publish data to Kafka: " + str(e)) from e


if __name__ == "__main__":
    while True:
        data = fetch_data()
        if data:
            publish_to_kafka(data)
            print(f"Data published to topic {KAFKA_TOPIC}")
        else:
            print("No data fetched")
        time.sleep(60)
