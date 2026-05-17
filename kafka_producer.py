import os
import json
import time
from kafka import KafkaProducer

INPUT_FOLDER = "/app/data/processed"
TOPIC_NAME = "air_alerts_files"

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
        "utf-8"
    ),
)

print("Kafka producer started...")

while True:
    files = os.listdir(INPUT_FOLDER)

    for filename in files:
        file_path = os.path.join(INPUT_FOLDER, filename)

        if os.path.isfile(file_path) and filename.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()

            message = {"filename": filename, "content": content}

            producer.send(TOPIC_NAME, value=message)
            producer.flush()

            print(f"File sent to Kafka: {filename}")

            processed_path = file_path + ".sent"
            os.rename(file_path, processed_path)

    time.sleep(5)
