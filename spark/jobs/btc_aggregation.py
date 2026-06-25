from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Spark session
spark = SparkSession.builder \
    .appName("BTC Aggregation Job") \
    .getOrCreate()

# 2. PostgreSQL connection
jdbc_url = "jdbc:postgresql://postgres:5432/airflow_db"
table = "warehouse.bitcoin_prices"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# 3. Read RAW data
df = spark.read.jdbc(
    url=jdbc_url,
    table=table,
    properties=properties
)

# 4. Debug schema
df.printSchema()

# 5. Transform (Spark magic)
agg_df = df.groupBy("symbol").agg(
    F.round(F.avg("price_usd"), 4).alias("avg_price"),
    F.round(F.max("price_usd"), 4).alias("max_price"),
    F.round(F.min("price_usd"), 4).alias("min_price"),
    F.count("*").alias("records_count")
)

agg_df = agg_df.withColumn(
    "snapshot_time",
    F.current_timestamp()
)

# 6. Optional sort
agg_df = agg_df.orderBy(F.desc("avg_price"))

# 7. Write to DWH
agg_df.write.jdbc(
    url=jdbc_url,
    table="warehouse.crypto_statistics",
    # mode="overwrite",
    mode="append",
    properties=properties
)

spark.stop()