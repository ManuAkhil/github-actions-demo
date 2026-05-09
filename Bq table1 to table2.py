"""
BQ table1 -> BQ table2
Automatically creates table2 and copies all data from table1.
Runs as a simple Python script in GitHub Actions — no Dataflow/Dataproc needed.
"""

import argparse
from google.cloud import bigquery


def copy_table(project, dataset, table1, table2):
    client = bigquery.Client(project=project)

    source      = f"{project}.{dataset}.{table1}"
    destination = f"{project}.{dataset}.{table2}"

    print(f"Copying {source} -> {destination}")

    # CREATE OR REPLACE table2 using a BQ query — auto-creates with same schema
    query = f"""
        CREATE OR REPLACE TABLE `{destination}`
        AS SELECT * FROM `{source}`
    """

    job = client.query(query)
    job.result()  # wait for completion

    # Verify row count
    result = client.query(f"SELECT COUNT(*) as cnt FROM `{destination}`").result()
    for row in result:
        print(f"table2 created with {row.cnt} rows")

    print(f"Done! {destination} is ready.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--table1",  required=True)
    parser.add_argument("--table2",  required=True)
    args = parser.parse_args()

    copy_table(args.project, args.dataset, args.table1, args.table2)