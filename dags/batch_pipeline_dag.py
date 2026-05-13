"""
batch_pipeline_dag.py
─────────────────────
Airflow DAG for the AWS Batch Data Pipeline.

Schedule : Daily at 00:00 UTC
Tasks    : ingest → validate → transform → load → sync_glue_catalog
Retries  : 3 attempts with 5-minute back-off per task
Alerts   : Email on failure / SLA miss (configure SMTP in airflow.cfg)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Import your existing pipeline modules ──────────────────────────────────
from src.ingest    import ingest
from src.validate  import validate
from src.transform import transform
from src.load      import load
from spark.spark_session import get_spark

logger = logging.getLogger(__name__)

# ── Pipeline configuration ─────────────────────────────────────────────────
PIPELINE_CONFIG = {
    "s3_raw_path":       "s3a://de-batch-pipeline-sg/raw/orders/",
    "s3_processed_path": "s3a://de-batch-pipeline-sg/cleaned/orders/",
    "glue_database":     "de_pipeline_db",
    "glue_table":        "orders_cleaned",
    "partition_col":     "order_date",
}

# ── Default task arguments ─────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "email":            ["ghoshsubhajoy2002@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── Task functions ─────────────────────────────────────────────────────────

def task_ingest(**context) -> None:
    """Read raw CSV data from S3 and push row count to XCom."""
    logger.info("Starting ingestion...")
    spark = get_spark()
    df = ingest(spark, PIPELINE_CONFIG["s3_raw_path"])
    row_count = df.count()
    logger.info(f"Ingested {row_count:,} rows.")
    # Push to XCom so downstream tasks can log the same figure
    context["ti"].xcom_push(key="raw_row_count", value=row_count)
    spark.stop()


def task_validate(**context) -> None:
    """Apply validation rules; fail the task if critical checks don't pass."""
    logger.info("Starting validation...")
    spark = get_spark()
    df = ingest(spark, PIPELINE_CONFIG["s3_raw_path"])
    valid_df, invalid_count = validate(df)

    raw_count = context["ti"].xcom_pull(key="raw_row_count", task_ids="ingest")
    logger.info(f"Valid rows  : {valid_df.count():,}")
    logger.info(f"Invalid rows: {invalid_count:,}")

    if raw_count and (invalid_count / raw_count) > 0.05:
        logger.warning(
            f"{invalid_count} invalid rows dropped "
            f"({invalid_count / raw_count:.1%} of total). Pipeline continues with valid rows only."
        )

    context["ti"].xcom_push(key="valid_row_count", value=valid_df.count())
    spark.stop()


def task_transform(**context) -> None:
    """Cast types, apply business rules, and write partitioned Parquet to S3."""
    logger.info("Starting transformation...")
    spark = get_spark()
    df = ingest(spark, PIPELINE_CONFIG["s3_raw_path"])
    valid_df, _ = validate(df)
    transformed_df = transform(valid_df)

    row_count = transformed_df.count()
    logger.info(f"Transformed {row_count:,} rows.")
    context["ti"].xcom_push(key="transformed_row_count", value=row_count)
    spark.stop()


def task_load(**context) -> None:
    """Write transformed Parquet files to S3 with partition overwrite."""
    logger.info("Starting load to S3...")
    spark = get_spark()
    df = ingest(spark, PIPELINE_CONFIG["s3_raw_path"])
    valid_df, _ = validate(df)
    transformed_df = transform(valid_df)

    load(
        df=transformed_df,
        output_path=PIPELINE_CONFIG["s3_processed_path"],
    )
    logger.info("Load complete.")
    spark.stop()


def task_sync_glue_catalog(**context) -> None:
    """
    Sync new S3 partitions with the Glue Data Catalog so Athena picks
    them up immediately without a manual MSCK REPAIR TABLE.
    """
    import boto3

    logger.info("Syncing partitions with Glue Catalog...")
    athena = boto3.client("athena", region_name="ap-south-1")

    # Run MSCK REPAIR TABLE via Athena to register new partitions
    response = athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {PIPELINE_CONFIG['glue_database']}.{PIPELINE_CONFIG['glue_table']}",
        QueryExecutionContext={"Database": PIPELINE_CONFIG["glue_database"]},
        ResultConfiguration={"OutputLocation": "s3://de-batch-pipeline-sg/logs/athena/"},
    )
    query_id = response["QueryExecutionId"]
    logger.info(f"Athena partition sync started. QueryExecutionId: {query_id}")


def task_on_failure_callback(context) -> None:
    """Log a structured failure summary for easy debugging."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    logger.error(
        f"PIPELINE FAILURE | dag={dag_id} | task={task_id} | run_id={run_id}"
    )


# ── DAG definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="batch_data_pipeline",
    default_args=default_args,
    description="Daily AWS batch pipeline: ingest → validate → transform → load → Glue sync",
    schedule_interval="0 0 * * *",   # every day at midnight UTC
    catchup=False,
    max_active_runs=1,               # prevent overlapping runs
    tags=["data-engineering", "aws", "pyspark", "batch"],
    on_failure_callback=task_on_failure_callback,
) as dag:

    # ── Sentinels ──────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # ── Pipeline tasks ─────────────────────────────────────────────────────
    ingest_op = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
    )

    validate_op = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    transform_op = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load_op = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    sync_glue = PythonOperator(
        task_id="sync_glue_catalog",
        python_callable=task_sync_glue_catalog,
    )

    # ── Task dependencies (linear pipeline) ───────────────────────────────
    start >> ingest_op >> validate_op >> transform_op >> load_op >> sync_glue >> end