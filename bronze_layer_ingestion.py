# Databricks notebook source
print("Databricks is connected and ready!")

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS portfolio")
spark.sql("CREATE SCHEMA IF NOT EXISTS portfolio.raw_data")
spark.sql("CREATE VOLUME IF NOT EXISTS portfolio.raw_data.taxi_files")

# COMMAND ----------

import urllib.request

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
target_path = "/Volumes/portfolio/raw_data/taxi_files/yellow_tripdata_2024-01.parquet"

urllib.request.urlretrieve(url, target_path)
print("Downloaded successfully!")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/Volumes/portfolio/raw_data/taxi_files/_schema")
        .load("/Volumes/portfolio/raw_data/taxi_files/")
)

df_with_meta = (
    df.withColumn("_ingested_at", current_timestamp())
      .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df_with_meta.writeStream
        .format("delta")
        .option("checkpointLocation", "/Volumes/portfolio/raw_data/taxi_files/_checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .table("portfolio.raw_data.bronze_taxi_trips")
)

# COMMAND ----------

df_bronze = spark.table("portfolio.raw_data.bronze_taxi_trips")
print("Row count:", df_bronze.count())
df_bronze.display()

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = (df_bronze
    .dropDuplicates(["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
    .filter(col("passenger_count").isNotNull())
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
)

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("portfolio.raw_data.silver_taxi_trips"))

print("Silver table created!")

# COMMAND ----------

df_silver_check = spark.table("portfolio.raw_data.silver_taxi_trips")
print("Silver row count:", df_silver_check.count())
df_silver_check.display()

# COMMAND ----------

from pyspark.sql.functions import to_date, sum as _sum, count as _count, avg as _avg

df_gold = (df_silver_check
    .withColumn("trip_date", to_date("tpep_pickup_datetime"))
    .groupBy("trip_date")
    .agg(
        _count("*").alias("total_trips"),
        _sum("fare_amount").alias("total_revenue"),
        _avg("trip_distance").alias("avg_trip_distance"),
        _avg("fare_amount").alias("avg_fare")
    )
    .orderBy("trip_date")
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("portfolio.raw_data.gold_daily_taxi_summary"))

print("Gold table created!")

# COMMAND ----------

df_gold_check = spark.table("portfolio.raw_data.gold_daily_taxi_summary")
print("Gold row count:", df_gold_check.count())
df_gold_check.display()

# COMMAND ----------

