import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import logging
from logging_config import setup_logging

setup_logging("spark_processing.log")

logging.info("Starting Spark processing...")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
db_table = "public.air_alerts"
spark = None

try:
    spark = (
        SparkSession.builder.appName("Air Alerts Spark Processing")
        .master("local[*]")
        .getOrCreate()
    )

    logging.info("Spark session created")
    logging.info("Reading data from PostgreSQL table: %s", db_table)

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", db_table)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    rows_count = df.count()
    logging.info("Data was read from PostgreSQL successfully")
    logging.info("Rows loaded from PostgreSQL: %s", rows_count)

    logging.info("Showing sample records")
    df.show(20, truncate=False)

    logging.info("Spark processing finished successfully")

except Exception as error:
    logging.error("Spark processing failed: %s", error)
    raise

finally:
    if spark is not None:
        spark.stop()
        logging.info("Spark session stopped")
