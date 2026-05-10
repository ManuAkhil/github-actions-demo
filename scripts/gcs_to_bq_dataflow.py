"""
Dataflow job: GCS -> BigQuery (table1)
Uses Apache Beam — runs on Google Dataflow.
"""

import argparse
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions,
    StandardOptions, SetupOptions
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

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
    if line.startswith("id,"):
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
    parser.add_argument("--input",    required=True)
    parser.add_argument("--bq_table", required=True)
    parser.add_argument("--project",  required=True)
    parser.add_argument("--bucket",   required=True)
    parser.add_argument("--region",   default="us-central1")
    parser.add_argument("--sa_key",   required=False,
                        help="Path to service account key JSON")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set credentials explicitly for Apache Beam
    if known_args.sa_key and os.path.exists(known_args.sa_key):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = known_args.sa_key
        print(f"Using credentials from: {known_args.sa_key}")

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project          = known_args.project
    google_cloud_options.region           = known_args.region
    google_cloud_options.staging_location = f"gs://{known_args.bucket}/dataflow/staging"
    google_cloud_options.temp_location    = f"gs://{known_args.bucket}/dataflow/temp"
    google_cloud_options.job_name         = "manu-gcs-to-bq"

    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read CSV"        >> beam.io.ReadFromText(known_args.input)
            | "Parse CSV"       >> beam.Map(parse_csv_line)
            | "Filter None"     >> beam.Filter(lambda x: x is not None)
            | "Write to BQ"     >> WriteToBigQuery(
                table=known_args.bq_table,
                schema=BQ_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

    print(f"Done! Data written to {known_args.bq_table}")


if __name__ == "__main__":
    run()
