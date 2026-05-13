# AWS Batch Data Pipeline using PySpark

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Enabled-orange?logo=apache-spark)
![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apache-airflow)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-yellow?logo=amazon-aws)
![CI](https://github.com/Subhajoy4831/spark-batch-pipeline/actions/workflows/ci.yml/badge.svg)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

**Status:** Fully functional, orchestrated with Airflow, and verified end-to-end via GitHub Actions CI.

---

## Overview

This project implements a production-style batch data pipeline on AWS.  
It ingests transactional CSV data, validates and transforms it using PySpark, writes optimized partitioned Parquet files to Amazon S3, and exposes the dataset for analytics using AWS Glue Data Catalog and Amazon Athena.

The pipeline is orchestrated by Apache Airflow running in Docker, with a full CI/CD pipeline that lints, unit tests, builds, and runs an end-to-end integration test on every push.

---

## Architecture

![Architecture](docs/architecture.png)

---

## AWS Services Used

- Amazon S3 – Data lake storage
- AWS Glue Data Catalog – Metadata management
- Amazon Athena – Serverless SQL querying

---

## Tech Stack

- Python
- PySpark
- Apache Airflow
- Docker + Docker Compose
- Parquet
- YAML Configuration
- Structured Logging
- GitHub Actions CI/CD

---

## Project Structure

```bash
spark-batch-pipeline
├── .github
│   └── workflows
│       └── ci.yml
├── config
│   └── config.yaml
├── dags
│   └── batch_pipeline_dag.py
├── data
│   └── input
│       └── orders.csv
├── docs
│   └── architecture.png
├── spark
│   └── spark_session.py
├── src
│   ├── ingest.py
│   ├── load.py
│   ├── transform.py
│   ├── validate.py
│   └── utils
│       ├── logger.py
│       └── retry.py
├── sql
│   └── athena_queries.sql
├── tests
│   ├── test_validate.py
│   └── test_transform.py
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yaml
├── main.py
├── Makefile
├── pyproject.toml
├── requirements.txt
└── requirements-dev.txt
```

---

## Key Engineering Decisions

- **Parquet format** for columnar storage and improved Athena performance
- **Partitioning by `order_date`** to reduce Athena scan cost
- **Explicit schema casting** to prevent Spark–Athena type mismatches
- **Glue Catalog integration** for external table management via `MSCK REPAIR TABLE`
- **Idempotent partition overwrite** for safe pipeline re-runs
- **Invalid row filtering** — bad records are dropped with a warning, pipeline always continues with valid rows

---

## Data Flow

1. Read raw CSV data from S3
2. Apply validation rules and type casting — invalid rows filtered out
3. Transform and select final columns
4. Write partitioned Parquet files to S3
5. Sync partitions with Glue Catalog
6. Query data using Athena

---

## How to Run

### Option 1: Airflow (recommended)

Requires AWS credentials in a `.env` file:

```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-south-1
```

Start the stack:

```bash
docker compose up --build
```

Open Airflow UI at [http://localhost:8085](http://localhost:8085) (admin / admin), trigger the `batch_data_pipeline` DAG.

### Option 2: Standalone Docker

```bash
docker build -t spark-batch-pipeline .
docker run -v ~/.aws:/root/.aws spark-batch-pipeline
```

### Option 3: Run Without Docker

```bash
python main.py
```

---

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

---

## Sample Query (Athena)

```sql
SELECT country, SUM(amount) AS total_amount
FROM orders_cleaned
GROUP BY country;
```

---

## Production Considerations

- Handles schema mismatches between Spark and Athena
- Prevents malformed Parquet writes
- Optimized for Athena scan efficiency
- Designed for scalable batch ingestion
- Invalid records logged and skipped — pipeline never hard-fails on bad data

---

## What This Project Demonstrates

- Real-world batch data engineering architecture
- Spark + S3 cloud integration
- Pipeline orchestration with Apache Airflow
- Containerised deployment with Docker Compose
- CI/CD with GitHub Actions (lint, unit tests, docker build, integration test)
- Metadata-driven analytics via Glue
- Athena-ready dataset optimisation
- Production-style pipeline structuring

---

## Roadmap

- [x] Airflow DAG for end-to-end pipeline orchestration
- [x] CI/CD integration with GitHub Actions
- [ ] Data quality checks using Great Expectations
- [ ] CloudWatch monitoring and alerting
- [ ] Deploy to EC2 / ECS Fargate for scheduled production runs

---

## Author

- **Subhajoy Ghosh**
- AWS Certified Developer – Associate
- Associate @ PwC

---
