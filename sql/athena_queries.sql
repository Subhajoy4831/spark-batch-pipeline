CREATE EXTERNAL TABLE IF NOT EXISTS orders_cleaned (
  order_id INT,
  user_id INT,
  amount DOUBLE,
  country STRING
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 's3://de-batch-pipeline-sg/cleaned/orders/';

MSCK REPAIR TABLE orders_cleaned;

SELECT country, SUM(amount)
FROM orders_cleaned
WHERE order_date = DATE '2024-12-01'
GROUP BY country;