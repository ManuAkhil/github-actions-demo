"""
Simple script: BQ table2 -> Cloud SQL MySQL
Reads data from BigQuery manu_table2 and writes to MySQL.
"""

import pymysql
from google.cloud import bigquery
from google.cloud.sql.connector import Connector

# Config
PROJECT          = "crested-acumen-495421-n0"
BQ_DATASET       = "demo_dataset"
BQ_TABLE         = "manu_table2"
MYSQL_INSTANCE   = "crested-acumen-495421-n0:us-central1:mysql-demo"
MYSQL_DATABASE   = "demo_db"
MYSQL_TABLE      = "manu_table"
MYSQL_USER       = "sparkuser"
MYSQL_PASSWORD   = "Spark@1234"


def main():
    # Step 1 - Read from BQ table2
    print("Reading from BigQuery manu_table2...")
    bq_client = bigquery.Client(project=PROJECT)

    rows = bq_client.query(
        f"SELECT id, name, age, city, department, salary "
        f"FROM `{PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    ).result()

    data = [
        (row.id, row.name, row.age, row.city, row.department, row.salary)
        for row in rows
    ]
    print(f"Read {len(data)} rows from BigQuery")

    # Step 2 - Connect to Cloud SQL MySQL via connector
    print("Connecting to MySQL...")
    connector = Connector()

    conn = connector.connect(
        MYSQL_INSTANCE,
        "pymysql",
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DATABASE,
    )

    # Step 3 - Insert rows into MySQL
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE} (
            id         INT          NOT NULL,
            name       VARCHAR(100),
            age        INT,
            city       VARCHAR(100),
            department VARCHAR(100),
            salary     INT,
            PRIMARY KEY(id)
        )
    """)

    cursor.executemany(
        f"INSERT INTO {MYSQL_TABLE} (id, name, age, city, department, salary) "
        f"VALUES (%s, %s, %s, %s, %s, %s) "
        f"ON DUPLICATE KEY UPDATE "
        f"name=VALUES(name), age=VALUES(age), city=VALUES(city), "
        f"department=VALUES(department), salary=VALUES(salary)",
        data,
    )

    conn.commit()
    cursor.close()
    conn.close()
    connector.close()

    print(f"Done! {len(data)} rows written to MySQL {MYSQL_TABLE}")


if __name__ == "__main__":
    main()
