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

output_root_path = "/app/data/processed/parquet/"

active_output_path = f"{output_root_path}active_alerts.parquet"
inactive_output_path = f"{output_root_path}/inactive_alerts_with_duration.parquet"
report_output_path = f"{output_root_path}/region_alerts_report.parquet"

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

    # Filter records that contain the fields required for analytical processing
    clean_df = df.filter(
        (F.col("oblast").isNotNull()) & (F.col("alert_type").isNotNull())
    )

    rows_count = df.count()
    logging.info("Data was read from PostgreSQL successfully")
    logging.info("Rows loaded from PostgreSQL: %s", rows_count)

    logging.info("Showing sample records")
    df.show(20, truncate=False)

    # Filter alerts into active and inactive datasets based on the finished_at field
    active_alerts_df = clean_df.filter(F.col("finished_at").isNull())
    inactive_alerts_df = clean_df.filter(F.col("finished_at").isNotNull())

    # Calculate duration only for inactive alerts
    inactive_alerts_df = inactive_alerts_df.withColumn(
        "duration_minutes",
        F.round(
            (F.unix_timestamp("finished_at") - F.unix_timestamp("started_at")) / 60,
            2,
        ),
    )

    # Group inactive alerts by region (oblast) and alert type, then calculate duration-based metrics
    region_report_df = inactive_alerts_df.groupBy("oblast", "alert_type").agg(
        F.count("alert_id").alias("inactive_alerts_count"),
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.min("duration_minutes"), 2).alias("min_duration_minutes"),
        F.round(F.max("duration_minutes"), 2).alias("max_duration_minutes"),
        F.max("finished_at").alias("last_finished_at"),
    )

    active_alerts_df.show(10, truncate=False)
    inactive_alerts_df.show(10, truncate=False)
    region_report_df.show(10, truncate=False)

    # Save alerts datasets to Parquet
    active_alerts_df.write.mode("overwrite").parquet(active_output_path)
    logging.info("Active alerts saved to: %s", active_output_path)

    inactive_alerts_df.write.mode("overwrite").parquet(inactive_output_path)
    logging.info("Inactive alerts with duration saved to: %s", inactive_output_path)

    region_report_df.write.mode("overwrite").parquet(report_output_path)
    logging.info("Region alerts report saved to: %s", report_output_path)

    logging.info("Spark processing finished successfully")

except Exception as error:
    logging.error("Spark processing failed: %s", error)
    raise

finally:
    if spark is not None:
        spark.stop()
        logging.info("Spark session stopped")
