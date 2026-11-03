# Databricks notebook source
dbutils.widgets.text("run_date", "2019-10-01")
run_date = dbutils.widgets.get("run_date")
print("run_date =", run_date)

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

SRC = f"/Volumes/ecommerce/raw/landing/events/date={run_date}/"

# Bronze = apa adanya. Semua kolom dibaca string, tipe data urusan silver.
# Schema ditulis eksplisit biar Spark nggak buang waktu nebak dari 1,2 juta baris.
SCHEMA = """
    event_time    string,
    event_type    string,
    product_id    string,
    category_id   string,
    category_code string,
    brand         string,
    price         string,
    user_id       string,
    user_session  string
"""

df = (
    spark.read.option("header", "true").schema(SCHEMA).csv(SRC)
    .withColumn("event_date", F.lit(run_date).cast("date"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{run_date}'")
    .partitionBy("event_date")
    .saveAsTable("ecommerce.bronze.events")
)

# COMMAND ----------

n = spark.read.table("ecommerce.bronze.events").where(F.col("event_date") == run_date).count()
print(f"bronze {run_date}: {n:,} baris")