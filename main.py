import os

from datetime import datetime
from dotenv import load_dotenv
from scripts import get_data, transform_alerts, save_processed_data


def main():

    load_dotenv(".secrets")
    token = os.getenv("API_TOKEN")
    if not token:
        raise ValueError("API_TOKEN not found. Check .secrets file.")

    url = "https://api.alerts.in.ua/v1/alerts/active.json"
    headers = {"Authorization": f"Bearer {token}"}

    collected_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    raw_file_name = "active_alerts_raw.json"
    raw_dir = os.path.join("data", "raw")
    raw_file_path = os.path.join(raw_dir, raw_file_name)

    processed_file_name_json = "active_alerts.json"
    processed_file_name_csv = "active_alerts.csv"
    processed_dir = os.path.join("data", "processed")
    processed_file_path_json = os.path.join(processed_dir, processed_file_name_json)
    processed_file_path_csv = os.path.join(processed_dir, processed_file_name_csv)

    raw_data = get_data(url, headers, raw_file_path)
    alerts = raw_data["alerts"]
    processed_data = transform_alerts(alerts, collected_at)
    save_processed_data(
        processed_data, processed_file_path_json, processed_file_path_csv
    )


if __name__ == "__main__":
    main()
