import os
import json
import logging
from kafka import KafkaConsumer
from scripts import setup_logging

OUTPUT_FOLDER = "/app/data/kafka_output"
TOPIC_NAME = "air_alerts_files"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
setup_logging("kafka.log")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="air-alerts-file-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

logging.info("Kafka consumer started...")

for message in consumer:
    data = message.value

    filename = data["filename"]
    content = data["content"]

    output_path = os.path.join(OUTPUT_FOLDER, filename)

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(content)

    print(f"File saved from Kafka: {filename}")
