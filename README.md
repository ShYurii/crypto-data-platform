# Crypto Data Platform — End-to-End Data Engineering Project

## Overview

This project is a complete end-to-end **Data Engineering pipeline** for processing cryptocurrency market data (Bitcoin).  
It demonstrates a modern data stack including:

- Data ingestion from external API
- Batch ETL processing with Airflow
- Stream/batch analytics with Spark
- Data storage in PostgreSQL (DWH layer)
- BI visualization in Metabase

The system is fully containerized using Docker and designed 
as a production-like data platform.

---

## Architecture

**Data Flow:**

1. Extract BTC price from CoinGecko API
2. Transform and validate data in Airflow
3. Load raw data into PostgreSQL (OLTP → staging layer)
4. Aggregate metrics using Apache Spark
5. Store aggregated results in DWH schema (`warehouse`)
6. Visualize insights in Metabase dashboards

---

## Tech Stack

- **Apache Airflow** — orchestration of ETL pipelines
- **Apache Spark** — distributed data processing & aggregation
- **PostgreSQL** — data warehouse storage
- **Metabase** — BI dashboards & visualization
- **Docker / Docker Compose** — containerized environment
- **Python** — ETL logic and API integration
- **CoinGecko API** — real-time crypto market data

---

## Project Structure
crypto-data-platform/
│
├── dags/ # Airflow DAGs (ETL pipelines)
├── spark/
│ └── jobs/ # Spark aggregation jobs
├── docker/ # Docker configuration
├── sql/ # Database schemas & DDL
├── metabase/ # Dashboard configs (optional export)
├── logs/ # Airflow logs
├── docker-compose.yml
└── README.md

---

## Pipeline Description

### 1. Extract Layer
- Fetches real-time Bitcoin price from CoinGecko API
- Ensures request validation and error handling

### 2. Transform Layer
- Data validation (null checks, type casting)
- Standardization of schema:
  - symbol
  - price_usd
  - source
  - pipeline metadata

### 3. Load Layer
- Inserts raw data into PostgreSQL
- Uses Airflow `PostgresHook`
- Ensures idempotency with `run_id`

---

## Spark Aggregation Layer

Spark job processes raw data and calculates:

- Average BTC price
- Maximum price
- Minimum price
- Number of records

Output is stored in:
warehouse.crypto_statistics

---

## Scheduling

- Airflow DAG runs every **1 minute** (can be adjusted to 5 minutes in production)
- Spark job is triggered after data load step

---

## Metabase Dashboard

Metabase is used for visualization of aggregated crypto metrics.

### Key Visualizations:
- BTC price trend over time
- Average price evolution
- Min / Max price comparison
- Number of records processed

### Setup:
1. Connect Metabase to PostgreSQL (`airflow_db`)
2. Use schema: `warehouse`
3. Create a saved query:
   ```sql
   SELECT * FROM warehouse.crypto_statistics ORDER BY snapshot_time;
   
### Build visualizations:
Line chart → price trends
Bar chart → min/max comparison
Time series → snapshot analytics

### Key Features:
Fully automated ETL pipeline
Real-time API ingestion
Distributed processing with Spark
Data warehouse design (star-like aggregation layer)
BI dashboards with Metabase
Containerized architecture

### Future Improvements:
Add Kafka streaming ingestion layer
Partitioning in PostgreSQL / move to ClickHouse
Add data quality checks (Great Expectations)
Build dbt transformation layer
Deploy on Kubernetes

### Author
Data Engineering Project by Yurii Shevchenko