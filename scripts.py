import os
import json
import csv
import requests
import logging


def save_json(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def save_csv(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    headers = [
        "alert_id",
        "location_title",
        "oblast",
        "alert_type",
        "started_at",
        "finished_at",
        "collected_at",
    ]
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)


def get_data(url, headers, file_path):
    try:
        logging.info("Fetching data from API")

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            raw_data = response.json()

            logging.info(f"Saving raw data to {file_path}")
            save_json(raw_data, file_path)

            return raw_data

        elif response.status_code == 401:
            raise ValueError("Invalid API token. Check your .secrets file.")

        elif response.status_code == 429:
            raise ValueError("Rate limit exceeded. Try again later.")

        else:
            raise ValueError(f"Unexpected status code: {response.status_code}")

    except requests.exceptions.Timeout:
        logging.exception("The request timed out")
        raise

    except requests.exceptions.RequestException as e:
        logging.exception(f"An error occurred during the request: {e}")
        raise


def validate_alert(processed_alert):
    alert_id = processed_alert.get("alert_id")

    if not alert_id:
        raise ValueError("Missing required field: alert_id")

    if not processed_alert.get("location_title"):
        raise ValueError(
            f"Missing required field: location_title for alert_id={alert_id}"
        )

    if not processed_alert.get("started_at"):
        raise ValueError(f"Missing required field: started_at for alert_id={alert_id}")

    return True


def transform_alerts(alerts, collected_at):
    processed_alerts = []

    for alert in alerts:
        processed_alert = {
            "alert_id": alert.get("id"),
            "location_title": alert.get("location_title"),
            "oblast": alert.get("location_oblast"),
            "alert_type": alert.get("alert_type"),
            "started_at": alert.get("started_at"),
            "finished_at": alert.get("finished_at"),
            "collected_at": collected_at,
        }

        validate_alert(processed_alert)
        processed_alerts.append(processed_alert)

    return processed_alerts


def save_processed_data(processed_data, json_file_path, csv_file_path):
    if not processed_data:
        raise ValueError(f"Processed data is empty")
    logging.info(f"Saving processed JSON to {json_file_path}")
    save_json(processed_data, json_file_path)

    logging.info(f"Saving processed CSV to {csv_file_path}")
    save_csv(processed_data, csv_file_path)


def setup_logging(log_file_name="pipeline.log"):
    log_file_path = os.path.join("logs", log_file_name)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8",
        filemode="w",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
