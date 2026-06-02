# Air Alert Monitoring Data Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.2-black)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.1.2-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Parquet](https://img.shields.io/badge/Parquet-Output-lightgrey)
![CI](https://github.com/VitaKotenko/Air-Alert-Monitoring-Data-Platform/actions/workflows/python-ci.yml/badge.svg)

<img width="1672" height="941" alt="" src="https://github.com/user-attachments/assets/1f2253b7-ef83-4bf2-a6e1-77c3fc16d5c5" />

## Project Description

Air Alert Monitoring Data Platform is a Python-based data engineering project for collecting, processing, streaming, storing, and analyzing air alert data in Ukraine.

The project uses the alerts.in.ua API as a data source. It collects current active alerts, saves raw API responses, transforms the data into a structured format, and stores processed results as JSON and CSV files. Processed active alerts are sent to Apache Kafka and then consumed by a separate consumer service, which writes the data into PostgreSQL.

The project also supports loading historical alert data for selected Ukrainian regions. Historical alerts are loaded from a separate API endpoint and merged into the PostgreSQL table using upsert logic. This allows the system to update existing alert records, especially when an alert receives a final finished_at timestamp.

The project includes a Spark processing layer. Spark reads alert records from PostgreSQL, separates active and inactive alerts, calculates alert duration for completed alerts, creates a regional summary report, and saves analytical outputs as Parquet datasets.

The main goal of the project is to build a small end-to-end data pipeline that covers data ingestion, file processing, Kafka streaming, PostgreSQL persistence, Spark-based processing, Parquet output, and SQL-based analytics.

## Main Features

- Fetch active air alerts from the alerts.in.ua API.
- Save raw API responses to JSON files.
- Transform alert data into a structured format.
- Save processed data as JSON and CSV.
- Send processed alert files to Kafka.
- Consume Kafka messages and load alerts into PostgreSQL.
- Load historical alert data for selected regions.
- Merge active and historical data using PostgreSQL upsert.
- Run SQL analytics over stored alert records.
- Process PostgreSQL data using Apache Spark.
- Create a regional report based on completed alerts.
- Save Spark outputs as Parquet datasets.
- Use Docker Compose for PostgreSQL, Kafka, producer, consumer, and Spark services.
- Use logging for pipeline monitoring and debugging.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.12 | Main programming language |
| PostgreSQL 16 | Data storage and SQL analytics |
| Apache Kafka 4.1.2 | Streaming processed alert files |
| Apache Spark / PySpark 4.1.2 | Data processing and Parquet output |
| Docker | Containerization |
| Docker Compose | Running multi-container services |
| psycopg2 | PostgreSQL connection from Python |
| kafka-python | Kafka producer and consumer implementation |
| requests | API requests |
| python-dotenv | Loading environment variables from `.secrets` |
| logging | Logging pipeline, producer, consumer, and Spark processing events |
| Parquet | Columnar storage format for analytical outputs |
| GitHub Actions | CI pipeline on push to `main` |


## Current Architecture

The project consists of two data ingestion flows: one for currently active alerts and one for historical alerts.

### Active Alerts Flow

Active alerts are collected from the alerts.in.ua API, saved locally as raw and processed files, streamed through Kafka, and then loaded into PostgreSQL by the Kafka consumer.

```text
alerts.in.ua Active Alerts API
        ↓
main.py
        ↓
Raw JSON file
data/raw/active_alerts_raw.json
        ↓
Data transformation
        ↓
Processed JSON / CSV files
data/processed/active_alerts.json
data/processed/active_alerts.csv
        ↓
kafka_producer.py
        ↓
Kafka topic: air_alerts_files
        ↓
kafka_consumer.py
        ↓
PostgreSQL table: air_alerts
        ↓
SQL analytics
```

### Historical Alerts Flow

Historical alerts are loaded from a separate alerts.in.ua history endpoint for selected regions. These records are transformed into the same structure as active alerts and written directly to PostgreSQL using upsert logic.

```text
alerts.in.ua History API
/v1/regions/{uid}/alerts/{period}.json
        ↓
main.py
        ↓
Raw historical JSON file
data/raw/history_alerts_region_{uid}_{period}_raw.json
        ↓
Data transformation
        ↓
Processed historical JSON / CSV files
data/processed/history_alerts_region_{uid}_{period}.json
data/processed/history_alerts_region_{uid}_{period}.csv
        ↓
PostgreSQL upsert
        ↓
PostgreSQL table: air_alerts
        ↓
SQL analytics
```

### Data Merge Logic

Both active and historical alerts are stored in the same PostgreSQL table: `air_alerts`.

The `alert_id` field is used as a unique key. If a record with the same `alert_id` already exists, PostgreSQL updates the existing row instead of inserting a duplicate.

This is especially important because active alerts may have `finished_at = NULL`, while historical alerts can later provide the final `finished_at` value.

```text
Active alert:
alert_id exists, finished_at = NULL
        ↓
Historical alert with the same alert_id arrives
        ↓
PostgreSQL ON CONFLICT updates the existing record
        ↓
finished_at is completed if available
```

### Spark Processing Flow

Spark reads data from the PostgreSQL air_alerts table and creates analytical Parquet outputs.
```text
PostgreSQL table: air_alerts
        ↓
spark_processing.py
        ↓
PySpark DataFrame
        ↓
Filter analytical records
        ↓
Split into active and inactive alerts
        ↓
Calculate duration for inactive alerts
        ↓
Group inactive alerts by oblast and alert_type
        ↓
Save Parquet outputs
```

