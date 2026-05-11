import os
import logging

from datetime import datetime
from dotenv import load_dotenv
from scripts import get_data, transform_alerts, save_processed_data, setup_logging


def main():
    try:
        setup_logging()
        logging.info("Pipeline started")

        load_dotenv(".secrets")
        token = os.getenv("API_TOKEN")
        if not token:
            raise ValueError("API_TOKEN not found. Check .secrets file.")
        logging.info("API token found in environment")

        url = "https://api.alerts.in.ua/v1/alerts/active.json"
        headers = {"Authorization": f"Bearer {token}"}

        raw_file_name = "active_alerts_raw.json"
        raw_dir = os.path.join("data", "raw")
        raw_file_path = os.path.join(raw_dir, raw_file_name)

        processed_file_name_json = "active_alerts.json"
        processed_file_name_csv = "active_alerts.csv"
        processed_dir = os.path.join("data", "processed")
        processed_file_path_json = os.path.join(processed_dir, processed_file_name_json)
        processed_file_path_csv = os.path.join(processed_dir, processed_file_name_csv)

        collected_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        raw_data = get_data(url, headers, raw_file_path)

        logging.info("Extracting alerts from API response")
        alerts = raw_data["alerts"]
        logging.info(f"Raw alerts count: {len(alerts)}")

        logging.info("Normalizing active alerts")
        processed_data = transform_alerts(alerts, collected_at)

        logging.info(f"Processed records count: {len(processed_data)}")

        save_processed_data(
            processed_data, processed_file_path_json, processed_file_path_csv
        )

        logging.info("Pipeline finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
