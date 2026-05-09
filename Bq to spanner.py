"""
PySpark job: BigQuery -> Cloud Spanner
Reads manu_table from BQ and writes to Spanner.
"""

import sys
from pyspark.sql import SparkSession

BQ_TABLE       = sys.argv[1]   # crested-acumen-495421-n0.demo_dataset.manu_table
BQ_TEMP_BUCKET = sys.argv[2]   # lumibucket-1
SPANNER_INSTANCE = sys.argv[3] # my-spanner-instance
SPANNER_DATABASE = sys.argv[4] # my-spanner-db
SPANNER_TABLE    = sys.argv[5] # manu_table

spark = SparkSession.builder.appName("BQ-to-Spanner").getOrCreate()

# Read from BigQuery
df = spark.read \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .load()

df.show()
print(f"Rows read from BQ: {df.count()}")

# Write to Spanner using the Spark-Spanner connector
df.write \
    .format("cloud-spanner") \
    .option("projectId", "crested-acumen-495421-n0") \
    .option("instanceId", SPANNER_INSTANCE) \
    .option("databaseId", SPANNER_DATABASE) \
    .option("table", SPANNER_TABLE) \
    .mode("overwrite") \
    .save()

print(f"Done! Written to Spanner {SPANNER_INSTANCE}/{SPANNER_DATABASE}/{SPANNER_TABLE}")
spark.stop()