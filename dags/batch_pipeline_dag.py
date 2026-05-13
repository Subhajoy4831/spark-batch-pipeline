"""
batch_pipeline_dag.py
─────────────────────
Airflow DAG for the AWS Batch Data Pipeline.

Schedule : Daily at 00:00 UTC
Tasks    : ingest → validate → transform → load → sync_glue_catalog
Retries  : 3 attempts with 5-minute back-off per task
Alerts   : Email on failure / SLA miss (configure SMTP in airflow.cfg)

Staging pattern
───────────────
Each task writes its output to an intermediate S3 path so the next task
reads exactly where the previous one left off. Data is read from source
only once (in ingest) and processed exactly once per stage.

  ingest    → s3://…/staging/raw/
  validate  → s3://…/staging/validated/
  transform → s3://…/staging/transformed/
  load      → s3://…/cleaned/orders/   (final, partitioned Parquet)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from src.ingest    import ingest
from src.validate  import validate
from src.transform import transform
from src.load      import load
from spark.spark_session import get_spark

logger = logging.getLogger(__name__)

# ── Pipeline configuration ─────────────────────────────────────────────────
PIPELINE_CONFIG = {
    "s3_raw_path":          "s3a://de-batch-pipeline-sg/raw/orders/",
    "s3_staging_raw":       "s3a://de-batch-pipeline-sg/staging/raw/",
    "s3_staging_validated": "s3a://de-batch-pipeline-sg/staging/validated/",
    "s3_staging_transformed":"s3a://de-batch-pipeline-sg/staging/transformed/",
    "s3_processed_path":    "s3a://de-batch-pipeline-sg/cleaned/orders/",
    "glue_database":        "de_pipeline_db",
    "glue_table":           "orders_cleaned",
    "partition_col":        "order_date",
}

# ── Default task arguments ─────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "start_date":        datetime(2026, 1, 1),
    "email":             ["ghoshsubhajoy2002@gmail.com"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── Task functions ─────────────────────────────────────────────────────────

def task_ingest(**context) -> None:
    """Read raw CSV from S3, write staging Parquet, push row count to XCom."""
    logger.info("Starting ingestion...")
    spark = get_spark()
    df = ingest(spark, PIPELINE_CONFIG["s3_raw_path"])
    row_count = df.count()
    df.write.mode("overwrite").parquet(PIPELINE_CONFIG["s3_staging_raw"])
    logger.info(f"Ingested {row_count:,} rows → staging/raw/")
    context["ti"].xcom_push(key="raw_row_count", value=row_count)
    spark.stop()


def task_validate(**context) -> None:
    """Read staging/raw, filter invalid rows, write staging/validated."""
    logger.info("Starting validation...")
    spark = get_spark()
    df = spark.read.parquet(PIPELINE_CONFIG["s3_staging_raw"])
    valid_df, invalid_count = validate(df)

    raw_count = context["ti"].xcom_pull(key="raw_row_count", task_ids="ingest")
    logger.info(f"Valid rows  : {valid_df.count():,}")
    logger.info(f"Invalid rows: {invalid_count:,}")

    if raw_count and (invalid_count / raw_count) > 0.05:
        logger.warning(
            f"{invalid_count} invalid rows dropped "
            f"({invalid_count / raw_count:.1%} of total). Pipeline continues with valid rows only."
        )

    valid_df.write.mode("overwrite").parquet(PIPELINE_CONFIG["s3_staging_validated"])
    logger.info("Validated data written → staging/validated/")
    context["ti"].xcom_push(key="valid_row_count", value=valid_df.count())
    spark.stop()


def task_transform(**context) -> None:
    """Read staging/validated, apply transformations, write staging/transformed."""
    logger.info("Starting transformation...")
    spark = get_spark()
    df = spark.read.parquet(PIPELINE_CONFIG["s3_staging_validated"])
    transformed_df = transform(df)
    row_count = transformed_df.count()
    transformed_df.write.mode("overwrite").parquet(PIPELINE_CONFIG["s3_staging_transformed"])
    logger.info(f"Transformed {row_count:,} rows → staging/transformed/")
    context["ti"].xcom_push(key="transformed_row_count", value=row_count)
    spark.stop()


def task_load(**context) -> None:
    """Read staging/transformed, write final partitioned Parquet to cleaned/orders/."""
    logger.info("Starting load to S3...")
    spark = get_spark()
    df = spark.read.parquet(PIPELINE_CONFIG["s3_staging_transformed"])
    load(df, PIPELINE_CONFIG["s3_processed_path"])
    logger.info("Load complete → cleaned/orders/")
    spark.stop()


def task_sync_glue_catalog(**context) -> None:
    """Sync new S3 partitions with Glue Catalog so Athena picks them up immediately."""
    import boto3

    logger.info("Syncing partitions with Glue Catalog...")
    athena = boto3.client("athena", region_name="ap-south-1")

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
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "aws", "pyspark", "batch"],
    on_failure_callback=task_on_failure_callback,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

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

    start >> ingest_op >> validate_op >> transform_op >> load_op >> sync_glue >> end
