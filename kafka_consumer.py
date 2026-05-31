import os
import json
import logging
from kafka import KafkaConsumer

from logging_config import setup_logging
from db import create_table
from repository import upsert_alert

OUTPUT_FOLDER = "/app/data/kafka_output"
TOPIC_NAME = "air_alerts_files"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

setup_logging("kafka.log")
logging.getLogger("kafka").setLevel(logging.WARNING)

create_table()

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

    logging.info("File saved from Kafka: %s", filename)

    try:
        alerts_data = json.loads(content)

        if isinstance(alerts_data, dict) and "alerts" in alerts_data:
            alerts = alerts_data["alerts"]
        elif isinstance(alerts_data, list):
            alerts = alerts_data
        else:
            alerts = [alerts_data]

        for alert in alerts:
            upsert_alert(alert)
            logging.info("Alert saved to PostgreSQL: %s", alert.get("alert_id"))

    except Exception as error:
        logging.error("Failed to save alerts to PostgreSQL: %s", error)
