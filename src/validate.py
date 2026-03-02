from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType

def validate(df):
    df = (
        df.withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("user_id", col("user_id").cast(IntegerType()))
          .withColumn("amount", col("amount").cast(DoubleType()))
          .withColumn("order_date", to_date("order_date"))
    )

    valid_df = df.filter(
        col("order_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0) &
        col("order_date").isNotNull()
    )

    return valid_df