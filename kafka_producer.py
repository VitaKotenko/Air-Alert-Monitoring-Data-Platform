import os
import json
import time
import logging
from kafka import KafkaProducer
from scripts import setup_logging

setup_logging("kafka.log")

INPUT_FOLDER = "/app/data/processed"
TOPIC_NAME = "air_alerts_files"

logging.info("Kafka producer started...")
logging.getLogger("kafka").setLevel(logging.WARNING)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
        "utf-8"
    ),
)

while True:
    files = os.listdir(INPUT_FOLDER)

    for filename in files:
        file_path = os.path.join(INPUT_FOLDER, filename)

        if os.path.isfile(file_path) and filename.endswith(".json"):
            logging.info("New JSON file detected: %s", filename)

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    content = file.read()

                message = {
                    "filename": filename,
                    "content": content,
                }

                producer.send(TOPIC_NAME, value=message)
                producer.flush()

                logging.info("File sent to Kafka topic '%s': %s", TOPIC_NAME, filename)

                sent_path = file_path + ".sent"
                os.rename(file_path, sent_path)

                logging.info("File marked as sent: %s", filename + ".sent")

            except Exception as error:
                logging.error("Error while sending file to Kafka: %s", error)

    time.sleep(5)
