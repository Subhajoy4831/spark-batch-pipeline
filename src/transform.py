from src.utils.logger import get_logger

logger = get_logger(__name__)

def transform(df):
    logger.info("Transform started")
    return df.select(
        "order_id",
        "user_id",
        "amount",
        "country",
        "order_date"
    )