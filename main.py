import yaml
from spark.spark_session import get_spark
from src.ingest import ingest
from src.validate import validate
from src.transform import transform
from src.load import load

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

bucket = config["s3"]["bucket"]
raw_path = f"s3a://{bucket}/{config['s3']['raw_path']}"
clean_path = f"s3a://{bucket}/{config['s3']['clean_path']}"

spark = get_spark()

df_raw = ingest(spark, "data/input/orders.csv")
df_valid, _ = validate(df_raw)
df_clean = transform(df_valid)
load(df_clean, clean_path)

spark.stop()