# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("run_date", "2019-10-01")
run_date = dbutils.widgets.get("run_date")
print("run_date =", run_date)

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

bronze = spark.read.table("ecommerce.bronze.events").where(F.col("event_date") == run_date)

typed = (
    bronze
    # " UTC" dibuang dulu, sesi udah di-set UTC jadi nggak ada geser timezone diam-diam.
    .withColumn("event_time", F.to_timestamp(
        F.regexp_replace("event_time", " UTC$", ""), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("product_id",  F.col("product_id").cast("long"))
    .withColumn("category_id", F.col("category_id").cast("long"))
    .withColumn("user_id",     F.col("user_id").cast("long"))
    .withColumn("price",       F.col("price").cast("decimal(10,2)"))
    .withColumn("category_l1", F.split("category_code", r"\.").getItem(0))
    .withColumn("category_l2", F.split("category_code", r"\.").getItem(1))
    # Harga 0 ditandai, bukan dibuang: eventnya sah, cuma nggak boleh ikut hitung revenue.
    .withColumn("is_price_zero", F.coalesce(F.col("price"), F.lit(0)) <= 0)
)

# dipake dua kali (hitung DQ + tulis), dedup itu shuffle mahal
silver = typed.dropDuplicates(
    ["event_time", "event_type", "product_id", "user_id", "user_session"]
).cache()

# COMMAND ----------

# Angka-angka ini yang masuk README. Jangan ngarang, ambil dari sini.
dq = silver.select(
    F.count("*").alias("baris_sesudah_dedup"),
    F.sum(F.col("event_time").isNull().cast("int")).alias("event_time_gagal_parse"),
    F.sum(F.col("brand").isNull().cast("int")).alias("brand_kosong"),
    F.sum(F.col("category_code").isNull().cast("int")).alias("category_code_kosong"),
    F.sum(F.col("is_price_zero").cast("int")).alias("harga_nol"),
).collect()[0].asDict()

dq["baris_sebelum_dedup"] = bronze.count()
dq["duplikat_dibuang"] = dq["baris_sebelum_dedup"] - dq["baris_sesudah_dedup"]

for k, v in dq.items():
    print(f"{k:<24} {v:>12,}")

assert dq["event_time_gagal_parse"] == 0, "format event_time nggak sesuai dugaan"

# COMMAND ----------

(
    silver.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{run_date}'")
    .partitionBy("event_date")
    .saveAsTable("ecommerce.silver.events")
)

print(f"silver {run_date}: {dq['baris_sesudah_dedup']:,} baris")