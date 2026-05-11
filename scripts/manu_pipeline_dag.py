"""
DAG: Manu Pipeline
Runs all tasks when triggered by GitHub Actions.

Tasks:
  1. GCS -> BQ table1 via Dataflow
  2. CREATE BQ table2 LIKE table1
  3. INSERT table1 -> table2
  4. BQ table2 -> Spanner
  5. BQ table2 -> MySQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from google.cloud import spanner
from google.cloud import bigquery
from google.cloud.sql.connector import Connector
import pymysql

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT          = "crested-acumen-495421-n0"
REGION           = "us-central1"
BUCKET           = "lumibucket-1"
DATASET          = "demo_dataset"
SPANNER_INSTANCE = "manu-spanner"
SPANNER_DATABASE = "manu_db"
SPANNER_TABLE    = "manu_table"
MYSQL_INSTANCE   = "crested-acumen-495421-n0:us-central1:mysql-demo"
MYSQL_DATABASE   = "demo_db"
MYSQL_TABLE      = "manu_table"
MYSQL_USER       = "sparkuser"
MYSQL_PASSWORD   = "Spark@1234"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "manu",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Python functions for Spanner and MySQL ────────────────────────────────────

def bq_to_spanner():
    """Read from BQ table2 and write to Spanner."""
    print("Reading from BigQuery manu_table2...")
    bq_client = bigquery.Client(project=PROJECT)

    rows = bq_client.query(
        f"SELECT id, name, age, city, department, salary "
        f"FROM `{PROJECT}.{DATASET}.manu_table2`"
    ).result()

    data = [
        (row.id, row.name, row.age, row.city, row.department, row.salary)
        for row in rows
    ]
    print(f"Read {len(data)} rows from BigQuery")

    spanner_client = spanner.Client(project=PROJECT)
    instance       = spanner_client.instance(SPANNER_INSTANCE)
    database       = instance.database(SPANNER_DATABASE)

    with database.batch() as batch:
        batch.insert_or_update(
            table=SPANNER_TABLE,
            columns=["id", "name", "age", "city", "department", "salary"],
            values=data,
        )
    print(f"Done! {len(data)} rows written to Spanner")


def bq_to_mysql():
    """Read from BQ table2 and write to Cloud SQL MySQL."""
    print("Reading from BigQuery manu_table2...")
    bq_client = bigquery.Client(project=PROJECT)

    rows = bq_client.query(
        f"SELECT id, name, age, city, department, salary "
        f"FROM `{PROJECT}.{DATASET}.manu_table2`"
    ).result()

    data = [
        (row.id, row.name, row.age, row.city, row.department, row.salary)
        for row in rows
    ]
    print(f"Read {len(data)} rows from BigQuery")

    connector = Connector()
    conn = connector.connect(
        MYSQL_INSTANCE,
        "pymysql",
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DATABASE,
    )

    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE} (
            id INT NOT NULL, name VARCHAR(100), age INT,
            city VARCHAR(100), department VARCHAR(100), salary INT,
            PRIMARY KEY(id)
        )
    """)
    cursor.executemany(
        f"INSERT INTO {MYSQL_TABLE} "
        f"(id, name, age, city, department, salary) "
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
    print(f"Done! {len(data)} rows written to MySQL")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
        dag_id="manu_pipeline",
        default_args=default_args,
        description="GCS -> BQ -> Spanner -> MySQL",
        schedule_interval=None,        # triggered externally by GitHub Actions
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["manu", "gcs", "bq", "spanner", "mysql"],
) as dag:

    # Task 1 — Dataflow: GCS -> BQ table1
    gcs_to_bq = DataflowTemplatedJobStartOperator(
        task_id="gcs_to_bq_table1",
        template="gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery",
        project_id=PROJECT,
        location=REGION,
        parameters={
            "inputFilePattern":                    f"gs://{BUCKET}/Manu.txt",
            "JSONPath":                            f"gs://{BUCKET}/scripts/schema.json",
            "outputTable":                         f"{PROJECT}:demo_dataset.manu_table1",
            "bigQueryLoadingTemporaryDirectory":   f"gs://{BUCKET}/dataflow/temp",
            "javascriptTextTransformGcsPath":      f"gs://{BUCKET}/scripts/transform.js",
            "javascriptTextTransformFunctionName": "transform",
        },
        environment={
            "tempLocation":    f"gs://{BUCKET}/dataflow/temp",
            "stagingLocation": f"gs://{BUCKET}/dataflow/staging",
        },
        gcp_conn_id="google_cloud_default",
    )

    # Task 2 — BQ: CREATE table2 LIKE table1
    create_table2 = BigQueryInsertJobOperator(
        task_id="create_bq_table2",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                    `{PROJECT}.{DATASET}.manu_table2`
                    LIKE `{PROJECT}.{DATASET}.manu_table1`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Task 3 — BQ: INSERT table1 -> table2
    copy_to_table2 = BigQueryInsertJobOperator(
        task_id="copy_table1_to_table2",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT}.{DATASET}.manu_table2`
                    SELECT * FROM `{PROJECT}.{DATASET}.manu_table1`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Task 4 — BQ table2 -> Spanner
    to_spanner = PythonOperator(
        task_id="bq_to_spanner",
        python_callable=bq_to_spanner,
    )

    # Task 5 — BQ table2 -> MySQL
    to_mysql = PythonOperator(
        task_id="bq_to_mysql",
        python_callable=bq_to_mysql,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    gcs_to_bq >> create_table2 >> copy_to_table2 >> to_spanner >> to_mysql
