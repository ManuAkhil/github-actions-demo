"""
PySpark job: BigQuery -> Cloud SQL (MySQL)
Reads manu_table from BQ and writes to MySQL via JDBC.
"""

import sys
from pyspark.sql import SparkSession
from google.cloud import secretmanager

BQ_TABLE         = sys.argv[1]  # crested-acumen-495421-n0.demo_dataset.manu_table
BQ_TEMP_BUCKET   = sys.argv[2]  # lumibucket-1
MYSQL_HOST       = sys.argv[3]  # Cloud SQL private IP or connection name
MYSQL_DB         = sys.argv[4]  # database name
MYSQL_TABLE      = sys.argv[5]  # table name
MYSQL_USER       = sys.argv[6]  # mysql user
MYSQL_PASS_SECRET = sys.argv[7] # secret manager secret name


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/crested-acumen-495421-n0/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


spark = SparkSession.builder.appName("BQ-to-MySQL").getOrCreate()

# Read from BigQuery
df = spark.read \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .load()

df.show()
print(f"Rows read from BQ: {df.count()}")

# Fetch MySQL password from Secret Manager
password = get_secret(MYSQL_PASS_SECRET)

# JDBC URL for Cloud SQL MySQL
jdbc_url = (
    f"jdbc:mysql:///{MYSQL_DB}"
    f"?cloudSqlInstance={MYSQL_HOST}"
    f"&socketFactory=com.google.cloud.sql.mysql.SocketFactory"
    f"&useSSL=false"
)

# Write to MySQL via JDBC
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", MYSQL_TABLE) \
    .option("user", MYSQL_USER) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()

print(f"Done! Written to MySQL {MYSQL_DB}.{MYSQL_TABLE}")
spark.stop()