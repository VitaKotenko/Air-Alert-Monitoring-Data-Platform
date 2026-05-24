import logging

import psycopg2

from db import get_connection


def get_active_alerts():
    """
    Get currently active alerts.
    Active alerts are records where finished_at is NULL.
    """

    query = """
    SELECT
        alert_id,
        location_title,
        oblast AS region,
        alert_type,
        TO_CHAR(started_at, 'DD.MM.YYYY HH24:MI') AS started_at,
        COALESCE(
            TO_CHAR(finished_at, 'DD.MM.YYYY HH24:MI'),
            'не завершена'
        ) AS finished_at,
        TO_CHAR(collected_at, 'DD.MM.YYYY HH24:MI') AS collected_at
    FROM air_alerts
    WHERE finished_at IS NULL
    ORDER BY started_at DESC;
    """

    connection = None

    try:
        logging.info("Getting currently active alerts")

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return result

    except psycopg2.Error as error:
        logging.error("Failed to get active alerts: %s", error)
        raise

    finally:
        if connection:
            connection.close()


def was_alert_active(region, check_time):
    """
    Check whether an alert was active in a specific region at a specific time.

    Example:
    region = "Київська область"
    check_time = "2026-05-06 13:00:00"
    """

    query = """
    SELECT
        alert_id,
        oblast AS region,
        TO_CHAR(started_at, 'DD.MM.YYYY HH24:MI') AS started_at,
        COALESCE(
            TO_CHAR(finished_at, 'DD.MM.YYYY HH24:MI'),
            'не завершена'
        ) AS finished_at
    FROM air_alerts
    WHERE oblast = %s
      AND started_at <= %s
      AND (finished_at IS NULL OR finished_at >= %s);
    """

    connection = None

    try:
        logging.info(
            "Checking if alert was active in %s at %s",
            region,
            check_time,
        )

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(query, (region, check_time, check_time))
            result = cursor.fetchall()

        return result

    except psycopg2.Error as error:
        logging.error("Failed to check active alert: %s", error)
        raise

    finally:
        if connection:
            connection.close()


def count_alerts_by_region():
    """
    Count alerts grouped by region.
    """

    query = """
    SELECT
        oblast AS region,
        COUNT(*) AS alerts_count
    FROM air_alerts
    GROUP BY oblast
    ORDER BY alerts_count DESC;
    """

    connection = None

    try:
        logging.info("Counting alerts by region")

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return result

    except psycopg2.Error as error:
        logging.error("Failed to count alerts by region: %s", error)
        raise

    finally:
        if connection:
            connection.close()


def get_longest_alert():
    """
    Find the longest finished alert.
    Only alerts with finished_at are included.
    """

    query = """
    SELECT
        alert_id,
        oblast AS region,
        TO_CHAR(started_at, 'DD.MM.YYYY HH24:MI') AS started_at,
        TO_CHAR(finished_at, 'DD.MM.YYYY HH24:MI') AS finished_at,
        CONCAT(
            FLOOR(EXTRACT(EPOCH FROM (finished_at - started_at)) / 3600),
            ' год ',
            FLOOR(MOD(EXTRACT(EPOCH FROM (finished_at - started_at)), 3600) / 60),
            ' хв'
        ) AS duration
    FROM air_alerts
    WHERE finished_at IS NOT NULL
    ORDER BY finished_at - started_at DESC
    LIMIT 1;
    """

    connection = None

    try:
        logging.info("Finding the longest alert")

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

        return result

    except psycopg2.Error as error:
        logging.error("Failed to get longest alert: %s", error)
        raise

    finally:
        if connection:
            connection.close()


def get_top_5_regions():
    """
    Get top 5 regions by number of alerts.
    """

    query = """
    SELECT
        oblast AS region,
        COUNT(*) AS alerts_count
    FROM air_alerts
    GROUP BY oblast
    ORDER BY alerts_count DESC
    LIMIT 5;
    """

    connection = None

    try:
        logging.info("Getting top 5 regions by alerts count")

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return result

    except psycopg2.Error as error:
        logging.error("Failed to get top 5 regions: %s", error)
        raise

    finally:
        if connection:
            connection.close()
