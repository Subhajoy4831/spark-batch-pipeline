from src.utils.logger import get_logger

logger = get_logger(__name__)

def load(df, output_path):
    logger.info("Load started")

    (
        df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(output_path)
    )

    logger.info("Load completed")