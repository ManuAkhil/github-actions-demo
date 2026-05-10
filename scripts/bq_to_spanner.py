"""
Simple script: BQ table2 -> Cloud Spanner
Reads data from BigQuery manu_table2 and writes to Spanner.
"""

from google.cloud import bigquery
from google.cloud import spanner

# Config
PROJECT          = "crested-acumen-495421-n0"
BQ_DATASET       = "demo_dataset"
BQ_TABLE         = "manu_table2"
SPANNER_INSTANCE = "manu-spanner"
SPANNER_DATABASE = "manu_db"
SPANNER_TABLE    = "manu_table"


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

    # Step 2 - Write to Spanner
    print("Writing to Spanner...")
    spanner_client = spanner.Client(project=PROJECT)
    instance       = spanner_client.instance(SPANNER_INSTANCE)
    database       = instance.database(SPANNER_DATABASE)

    with database.batch() as batch:
        batch.insert_or_update(
            table=SPANNER_TABLE,
            columns=["id", "name", "age", "city", "department", "salary"],
            values=data,
        )

    print(f"Done! {len(data)} rows written to Spanner {SPANNER_TABLE}")


if __name__ == "__main__":
    main()