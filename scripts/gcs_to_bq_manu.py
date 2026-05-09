"""
PySpark job: reads Manu.txt (CSV) from GCS and writes to BigQuery with schema.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

GCS_URI        = sys.argv[1]   # gs://lumibucket-1/Manu.txt
BQ_TABLE       = sys.argv[2]   # crested-acumen-495421-n0.demo_dataset.manu_table
BQ_TEMP_BUCKET = sys.argv[3]   # lumibucket-1

# Explicit schema — maps directly to the BQ table columns
schema = StructType([
    StructField("id",         IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("age",        IntegerType(), True),
    StructField("city",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     IntegerType(), True),
])

spark = SparkSession.builder.appName("GCS-to-BQ-Manu").getOrCreate()

# Read CSV with header and schema
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(GCS_URI)

df.show()  # prints in Dataproc logs so you can verify

# Write to BigQuery
df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .mode("overwrite") \
    .save()

print(f"Done! Written to {BQ_TABLE}")
spark.stop()