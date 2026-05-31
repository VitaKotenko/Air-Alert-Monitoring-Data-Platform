import os
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv(".secrets")


def get_connection():
    """
    Create connection to PostgreSQL database using environment variables.
    """

    try:
        logging.info("Connecting to PostgreSQL")

        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

        logging.info("Connected to PostgreSQL successfully")
        return connection

    except psycopg2.Error as error:
        logging.error(f"PostgreSQL connection failed: {error}")
        raise


def create_table():
    """
    Create air_alerts table if it does not exist.
    """

    create_table_query = """
    CREATE TABLE IF NOT EXISTS air_alerts (
        alert_id TEXT PRIMARY KEY,
        location_title TEXT,
        oblast TEXT NOT NULL,
        alert_type TEXT,
        started_at TIMESTAMP NOT NULL,
        finished_at TIMESTAMP,
        collected_at TIMESTAMP
    );
    """

    connection = None

    try:
        logging.info("Creating table air_alerts")

        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()

        logging.info("Table air_alerts created successfully")

    except psycopg2.Error as error:
        logging.error(f"Failed to create table air_alerts: {error}")
        raise

    finally:
        if connection:
            connection.close()
