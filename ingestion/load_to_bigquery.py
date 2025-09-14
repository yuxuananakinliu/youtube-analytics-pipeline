import os
from datetime import date
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Read .env for local runs
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCS_BUCKET")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

EXEC_DATE = os.getenv("DATE_OVERRIDE") or date.today().isoformat()
DATASET = "youtube_raw"

# Map each GCS file to a BigQuery raw table
SOURCES = [
    (f"gs://{BUCKET}/raw/date={EXEC_DATE}/channels.json",   f"{PROJECT_ID}.{DATASET}.channels_raw"),
    (f"gs://{BUCKET}/raw/date={EXEC_DATE}/video_ids.json",  f"{PROJECT_ID}.{DATASET}.video_ids_raw"),
    (f"gs://{BUCKET}/raw/date={EXEC_DATE}/video_stats.json",f"{PROJECT_ID}.{DATASET}.video_stats_raw"),
]

def main():
    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY
        ),
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
    )

    for gcs_uri, table_id in SOURCES:
        try:
            print(f"Loading {gcs_uri} → {table_id} ...")
            job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
            job.result()
            dest = client.get_table(table_id)
            print(f"Loaded {dest.num_rows} total rows in {table_id}")
        except NotFound as e:
            print(f"[WARN] Not found: {gcs_uri} or table {table_id}. {e}")
        except Exception as e:
            print(f"[ERROR] Failed to load {gcs_uri} → {table_id}: {e}")

if __name__ == "__main__":
    main()