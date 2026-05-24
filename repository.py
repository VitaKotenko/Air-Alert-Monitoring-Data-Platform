import logging
from datetime import datetime

import psycopg2

from db import get_connection


def parse_timestamp(value):
    """
    Convert timestamp string from API to Python datetime.
    Supports None values and ISO format with Z.
    """

    if value is None:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        logging.error(f"Invalid timestamp format: {value}")
        raise error


def validate_alert(alert):
    """
    Validate required alert fields before inserting into database.
    """

    if not alert.get("alert_id"):
        raise ValueError("alert_id is required")

    if not alert.get("oblast"):
        raise ValueError("oblast is required")

    if not alert.get("started_at"):
        raise ValueError("started_at is required")


def upsert_alert(alert):
    """
    Insert alert into PostgreSQL or update it if alert already exists.
    """

    validate_alert(alert)

    query = """
    INSERT INTO air_alerts (
        alert_id,
        location_title,
        oblast,
        alert_type,
        started_at,
        finished_at,
        collected_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (alert_id)
    DO UPDATE SET
        location_title = EXCLUDED.location_title,
        oblast = EXCLUDED.oblast,
        alert_type = EXCLUDED.alert_type,
        started_at = EXCLUDED.started_at,
        finished_at = COALESCE(EXCLUDED.finished_at, air_alerts.finished_at),
        collected_at = EXCLUDED.collected_at;
    """

    connection = None

    try:
        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    alert.get("alert_id"),
                    alert.get("location_title"),
                    alert.get("oblast"),
                    alert.get("alert_type"),
                    parse_timestamp(alert.get("started_at")),
                    parse_timestamp(alert.get("finished_at")),
                    parse_timestamp(alert.get("collected_at")),
                ),
            )

        connection.commit()
        logging.info(f"Alert inserted or updated: {alert.get('alert_id')}")

    except (psycopg2.Error, ValueError) as error:
        logging.error(f"Failed to upsert alert {alert.get('alert_id')}: {error}")
        raise

    finally:
        if connection:
            connection.close()
