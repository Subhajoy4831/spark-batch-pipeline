import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.validate import validate


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("test_validate")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("order_id",   StringType()),
    StructField("user_id",    StringType()),
    StructField("amount",     StringType()),
    StructField("order_date", StringType()),
    StructField("country",    StringType()),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_valid_row_passes(spark):
    df = make_df(spark, [("1", "101", "250.0", "2024-12-01", "IN")])
    valid_df, invalid_count = validate(df)
    assert valid_df.count() == 1
    assert invalid_count == 0


def test_null_amount_filtered(spark):
    df = make_df(spark, [("2", "102", None, "2024-12-02", "US")])
    valid_df, invalid_count = validate(df)
    assert valid_df.count() == 0
    assert invalid_count == 1


def test_zero_amount_filtered(spark):
    df = make_df(spark, [("3", "103", "0", "2024-12-03", "IN")])
    valid_df, invalid_count = validate(df)
    assert valid_df.count() == 0
    assert invalid_count == 1


def test_invalid_date_filtered(spark):
    df = make_df(spark, [("4", "104", "150.0", "invalid_date", "UK")])
    valid_df, invalid_count = validate(df)
    assert valid_df.count() == 0
    assert invalid_count == 1


def test_mixed_rows(spark):
    rows = [
        ("1", "101", "250.0", "2024-12-01", "IN"),
        ("2", "102", None,    "2024-12-02", "US"),
        ("3", "103", "400.0", "bad-date",   "IN"),
        ("4", "104", "150.0", "2024-12-01", "UK"),
    ]
    df = make_df(spark, rows)
    valid_df, invalid_count = validate(df)
    assert valid_df.count() == 2
    assert invalid_count == 2
