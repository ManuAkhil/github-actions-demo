"""
Simple PySpark job: reads Akhil.txt from GCS and writes it to BigQuery.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GCS_URI        = sys.argv[1]   # gs://lumibucket-1/Akhil.txt
BQ_TABLE       = sys.argv[2]   # crested-acumen-495421-n0.demo_dataset.akhil_table
BQ_TEMP_BUCKET = sys.argv[3]   # lumibucket-1

spark = SparkSession.builder.appName("GCS-to-BQ").getOrCreate()

# Read the text file — each line becomes one row
df = spark.read.text(GCS_URI)

# Add a timestamp column
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
