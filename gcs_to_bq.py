"""
Simple PySpark job: reads Akhil.txt from GCS and writes it to BigQuery.
Runs on a Dataproc cluster triggered by GitHub Actions.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

# --- Args passed in from the GitHub Actions workflow ---
# sys.argv[1] = GCS_URI  e.g. gs://my-bucket/Akhil.txt
# sys.argv[2] = BQ_TABLE e.g. my-project.my_dataset.akhil_table

GCS_URI  = sys.argv[1]   # e.g. gs://my-bucket/Akhil.txt
BQ_TABLE = sys.argv[2]   # e.g. my_project.my_dataset.akhil_table
BQ_TEMP_BUCKET = sys.argv[3]  # GCS bucket for BQ temp files

spark = SparkSession.builder.appName("GCS-to-BQ").getOrCreate()

# Read the text file — each line becomes one row
df = spark.read.text(GCS_URI)

# Add a timestamp column so you can see when it was loaded
df = df.withColumn("loaded_at", F.current_timestamp())

# Write to BigQuery
df.write \
  .format("bigquery") \
  .option("table", BQ_TABLE) \
  .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
  .mode("overwrite") \
  .save()

print(f"Done! Written to {BQ_TABLE}")
spark.stop()
