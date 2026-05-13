import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.transform import transform


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("test_transform")
        .getOrCreate()
    )


def test_output_columns(spark):
    schema = StructType([
        StructField(c, StringType()) for c in
        ["order_id", "user_id", "amount", "country", "order_date", "ingestion_date"]
    ])
    df = spark.createDataFrame(
        [("1", "101", "250.0", "IN", "2024-12-01", "2026-05-13")],
        schema=schema,
    )
    result = transform(df)
    assert result.columns == ["order_id", "user_id", "amount", "country", "order_date"]


def test_ingestion_date_dropped(spark):
    schema = StructType([
        StructField(c, StringType()) for c in
        ["order_id", "user_id", "amount", "country", "order_date", "ingestion_date"]
    ])
    df = spark.createDataFrame(
        [("1", "101", "250.0", "IN", "2024-12-01", "2026-05-13")],
        schema=schema,
    )
    result = transform(df)
    assert "ingestion_date" not in result.columns


def test_row_count_preserved(spark):
    schema = StructType([
        StructField(c, StringType()) for c in
        ["order_id", "user_id", "amount", "country", "order_date"]
    ])
    rows = [
        ("1", "101", "250.0", "IN", "2024-12-01"),
        ("2", "102", "300.0", "US", "2024-12-02"),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    result = transform(df)
    assert result.count() == 2
