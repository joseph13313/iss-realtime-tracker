# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp
import requests, time, json
from datetime import datetime, timezone

BRONZE_DB = "iss_tracker"
BRONZE_TBL = "iss_tracker.bronze_positions"

POLL_INTERVAL = 5 # seconds between each API call
BATCH_SIZE = 12 # write to delta every 12 records (1 minute)

print(f"Target table : {BRONZE_TBL}")
print(f"Poll every  : {POLL_INTERVAL}s")
print(f"Batch write : every {BATCH_SIZE} records ({BATCH_SIZE*POLL_INTERVAL}s)")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
spark.sql(f"USE {BRONZE_DB}")

print(f"Database '{BRONZE_DB}' ready.")


# COMMAND ----------

bronze_schema = StructType([
    StructField("raw_json", StringType(), False),
    StructField("ingested_at", StringType(), False),
    StructField("api_url", StringType(), False)
])

# COMMAND ----------

def run_bronze_producer(minutes: int = 3):
    buffer = []
    end_time = time.time() + (minutes * 60)
    write_count = 0
    record_count = 0

    print(f"Starting Bronze producer - running for {minutes} minutes")
    print(f"Writing batches of {BATCH_SIZE} records to: {BRONZE_TBL}")
    print("-"* 55)

    while time.time() < end_time:
        try:
            response = requests.get("http://api.open-notify.org/iss-now.json",timeout=15)
            buffer.append({
                "raw_json": response.text,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "api_url": "http://api.open-notify.org/iss-now.json"
            })
            record_count += 1

            if len(buffer) >= BATCH_SIZE:
                df = spark.createDataFrame(buffer, schema=bronze_schema)
                df.write.format("delta").mode("append").saveAsTable(BRONZE_TBL)

                write_count += 1
                print(f"[Batch {write_count}] wote {BATCH_SIZE} records | total so far: {record_count}")
                buffer = []

        except requests.exceptions.RequestException as e:
            print(f"API error (skipping this reading): {e}")

        time.sleep(POLL_INTERVAL)

    if buffer:
        df = spark.createDataFrame(buffer, schema=bronze_schema)
        df.write.format("delta").mode("append").saveAsTable(BRONZE_TBL)
        write_count += 1
        print(f"[Final Batch] wote {len(buffer)} remaining records")

    print("-" * 55)
    print(f"Done. {write_count} batch writes, {record_count} total records")

run_bronze_producer(minutes=3)

# COMMAND ----------

bronze_df = spark.read.table(BRONZE_TBL)

print(f"Total records in Bronze: {bronze_df.count()}")

bronze_df.orderBy("ingested_at", ascending=False).show(5, truncate=80)

# COMMAND ----------

spark.sql(f"""
          SELECT
            ingested_at,
            get_json_object(raw_json, '$.iss_position.latitude') AS lat,
            get_json_object(raw_json, '$.iss_position.longitude') AS lon,
            get_json_object(raw_json, '$.timestamp') AS event_ts,
            get_json_object(raw_json, '$.message') AS status
        FROM {BRONZE_TBL}
        ORDER BY ingested_at DESC
        LIMIT 10
          """).show()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {BRONZE_TBL}").show(10, truncate=False)

# COMMAND ----------

# Check how many orbits worth of data we have
spark.sql(f"""
          SELECT
            COUNT(*) AS total_records,
            MIN(get_json_object(raw_json, '$.timestamp')) AS first_ts,
            MAX(get_json_object(raw_json, '$.timestamp')) AS last_ts,
            ROUND((MAX(CAST(get_json_object(raw_json, '$.timestamp') AS BIGINT)) - MIN(CAST(get_json_object(raw_json, '$.timestamp') AS BIGINT)))
             / 5400.0, 2) AS orbits_covered
            FROM {BRONZE_TBL}
          """).show()

# COMMAND ----------

# Re-run Cell 1 (config) and Cell 3 (schema) to restore variables in memory
# Confirm your data survived:

spark.sql("SELECT COUNT(*) FROM iss_tracker.bronze_positions").show()

# COMMAND ----------

