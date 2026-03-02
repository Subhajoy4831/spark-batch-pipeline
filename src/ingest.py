from pyspark.sql.functions import current_date
from src.utils.logger import get_logger

logger = get_logger(__name__)

def ingest(spark, input_path):
    logger.info("Ingest started")

    df = (
        spark.read
        .option("header", True)
        .csv(input_path)
        .withColumn("ingestion_date", current_date())
    )

    logger.info(f"Ingested {df.count()} records")
    return df