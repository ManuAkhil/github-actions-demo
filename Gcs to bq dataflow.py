"""
Dataflow job: GCS -> BigQuery (table1)
Uses Apache Beam — runs on Google Dataflow.
Reads Manu.txt from GCS and writes to BQ table1 with schema.
"""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# ── Schema — matches Manu.txt columns exactly ────────────────────────────────
BQ_SCHEMA = {
    "fields": [
        {"name": "id",         "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name",       "type": "STRING",  "mode": "NULLABLE"},
        {"name": "age",        "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "city",       "type": "STRING",  "mode": "NULLABLE"},
        {"name": "department", "type": "STRING",  "mode": "NULLABLE"},
        {"name": "salary",     "type": "INTEGER", "mode": "NULLABLE"},
    ]
}


def parse_csv_line(line):
    """Convert each CSV line into a BQ row dict. Skips the header."""
    if line.startswith("id,"):   # skip header row
        return None
    parts = line.split(",")
    if len(parts) != 6:
        return None
    return {
        "id":         int(parts[0].strip()),
        "name":       parts[1].strip(),
        "age":        int(parts[2].strip()),
        "city":       parts[3].strip(),
        "department": parts[4].strip(),
        "salary":     int(parts[5].strip()),
    }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      required=True,  help="GCS input file path")
    parser.add_argument("--bq_table",   required=True,  help="BQ table1 e.g. project:dataset.table")
    parser.add_argument("--project",    required=True,  help="GCP project ID")
    parser.add_argument("--bucket",     required=True,  help="GCS temp bucket")
    parser.add_argument("--region",     default="us-central1")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # ── Pipeline options ──────────────────────────────────────────────────────
    options = PipelineOptions(pipeline_args)

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.region  = known_args.region
    google_cloud_options.staging_location = f"gs://{known_args.bucket}/dataflow/staging"
    google_cloud_options.temp_location    = f"gs://{known_args.bucket}/dataflow/temp"
    google_cloud_options.job_name         = "manu-gcs-to-bq"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    # ── Pipeline ──────────────────────────────────────────────────────────────
    with beam.Pipeline(options=options) as p:
        (
                p
                | "Read CSV from GCS"   >> beam.io.ReadFromText(known_args.input)
                | "Parse CSV lines"     >> beam.Map(parse_csv_line)
                | "Filter None rows"    >> beam.Filter(lambda x: x is not None)
                | "Write to BQ table1"  >> WriteToBigQuery(
            table=known_args.bq_table,
            schema=BQ_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        )
        )

    print(f"Dataflow job submitted! Data written to {known_args.bq_table}")


if __name__ == "__main__":
    run()