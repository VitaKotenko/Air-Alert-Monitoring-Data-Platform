import os
import logging

from datetime import datetime
from dotenv import load_dotenv

from scripts import (
    get_data,
    get_history_data,
    transform_alerts,
    transform_history_alerts,
    save_processed_data,
    setup_logging,
    print_query_result,
)

from repository import upsert_alert

from queries import (
    get_active_alerts,
    was_alert_active,
    count_alerts_by_region,
    get_longest_alert,
    get_top_5_regions,
)


def main():
    try:
        setup_logging()
        logging.info("Pipeline started")

        load_dotenv(".secrets")
        token = os.getenv("API_TOKEN")

        if not token:
            raise ValueError("API_TOKEN not found. Check .secrets file.")

        logging.info("API token found in environment")

        raw_dir = os.path.join("data", "raw")
        processed_dir = os.path.join("data", "processed")

        # Active alerts
        url = "https://api.alerts.in.ua/v1/alerts/active.json"
        headers = {"Authorization": f"Bearer {token}"}

        raw_file_name = "active_alerts_raw.json"
        raw_file_path = os.path.join(raw_dir, raw_file_name)

        processed_file_name_json = "active_alerts.json"
        processed_file_name_csv = "active_alerts.csv"

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
            processed_data,
            processed_file_path_json,
            processed_file_path_csv,
        )

        logging.info("Active alerts processed successfully")

        # Historical alerts
        region_uid = 14
        period = "month_ago"

        history_raw_file_name = f"history_alerts_region_{region_uid}_{period}_raw.json"
        history_processed_file_name_json = (
            f"history_alerts_region_{region_uid}_{period}.json"
        )
        history_processed_file_name_csv = (
            f"history_alerts_region_{region_uid}_{period}.csv"
        )

        history_raw_file_path = os.path.join(raw_dir, history_raw_file_name)
        history_processed_file_path_json = os.path.join(
            processed_dir,
            history_processed_file_name_json,
        )
        history_processed_file_path_csv = os.path.join(
            processed_dir,
            history_processed_file_name_csv,
        )

        logging.info("Fetching historical alerts")

        history_raw_data = get_history_data(
            region_uid=region_uid,
            period=period,
            api_token=token,
            file_path=history_raw_file_path,
        )

        logging.info("Extracting historical alerts from API response")
        history_alerts = history_raw_data["alerts"]
        logging.info(f"Historical alerts count: {len(history_alerts)}")

        logging.info("Normalizing historical alerts")
        history_processed_data = transform_history_alerts(history_alerts, collected_at)

        logging.info(
            f"Processed historical records count: {len(history_processed_data)}"
        )

        save_processed_data(
            history_processed_data,
            history_processed_file_path_json,
            history_processed_file_path_csv,
        )

        logging.info("Loading historical alerts to PostgreSQL")

        for alert in history_processed_data:
            upsert_alert(alert)

        logging.info("Historical alerts loaded to PostgreSQL successfully")

        logging.info("Running SQL analytics queries")

        active_alerts = get_active_alerts()
        print_query_result("Currently active alerts:", active_alerts)

        region = "Луганська область"
        check_time = "2026-05-24 13:00:00"

        alert_status = was_alert_active(region, check_time)
        print_query_result(
            f"Was alert active in {region} at {check_time}?",
            alert_status,
        )

        alerts_by_region = count_alerts_by_region()
        print_query_result("Alerts count by region:", alerts_by_region)

        longest_alert = get_longest_alert()
        print_query_result("Longest finished alert:", longest_alert)

        top_regions = get_top_5_regions()
        print_query_result("Top 5 regions by alerts count:", top_regions)

        logging.info("SQL analytics queries finished successfully")

        logging.info("Pipeline finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
