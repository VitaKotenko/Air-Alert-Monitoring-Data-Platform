import os
import json
import csv
import requests


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
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            raw_data = response.json()
            save_json(raw_data, file_path)
            return raw_data
        elif response.status_code == 401:
            raise ValueError("Invalid API token. Check your .secrets file.")
        elif response.status_code == 429:
            raise ValueError("Rate limit exceeded. Try again later.")
        else:
            raise ValueError(f"Unexpected status code: {response.status_code}")
    except requests.exceptions.Timeout:
        print("The request timed out")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def validate_alert(processed_alert):
    if not processed_alert["alert_id"]:
        raise ValueError(f"Missing required field: alert_id")
    elif not processed_alert["location_title"]:
        raise ValueError(f"Missing required field: location_title in alert_id")
    elif not processed_alert["started_at"]:
        raise ValueError(f"Missing required field: sterted_at in alert_id")
    return True


def transform_alerts(alerts, collected_at):
    processed_alerts = []
    for alert in alerts:
        processed_alert = {}

        processed_alert["alert_id"] = alert.get("id")
        processed_alert["location_title"] = alert.get("location_title")
        processed_alert["oblast"] = alert.get("location_oblast")
        processed_alert["alert_type"] = alert.get("alert_type")
        processed_alert["started_at"] = alert.get("started_at")
        processed_alert["finished_at"] = alert.get("finished_at")
        processed_alert["collected_at"] = collected_at

        validation = validate_alert(processed_alert)
        if validation:
            processed_alerts.append(processed_alert)

    return processed_alerts


def save_processed_data(processed_data, file_name_json, file_name_csv):
    if not processed_data:
        raise ValueError(f"Processed data is empty")
    save_json(processed_data, file_name_json)
    save_csv(processed_data, file_name_csv)
