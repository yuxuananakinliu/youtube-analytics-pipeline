import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Use DATE_OVERRIDE if it exists in env, else Airflow {{ ds }} (YYYY-MM-DD)
def pick_date_override(**context):
    return os.environ.get("DATE_OVERRIDE") or context["ds"]

# --- Python callables that import your repo code ---
def run_ingestion_callable(exec_date: str):
    # your script supports passing exec_date; it already uploads 3 NDJSON files to GCS
    from pathlib import Path
    import sys
    sys.path.append("/opt/airflow/repo/ingestion")
    from fetch_youtube import run_ingestion  # :contentReference[oaicite:3]{index=3}
    run_ingestion(exec_date=exec_date)

def run_loader_callable(exec_date: str):
    from pathlib import Path
    import os, sys
    sys.path.append("/opt/airflow/repo/ingestion")
    # ensure loader uses DATE_OVERRIDE consistently
    os.environ["DATE_OVERRIDE"] = exec_date
    from load_to_bigquery import main as load_main  # :contentReference[oaicite:4]{index=4}
    load_main()

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_daily",
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval="0 7 * * *",  # daily at 07:00
    catchup=False,
    max_active_runs=1,
    tags=["youtube","gcp","dbt"],
) as dag:

    pick_date = PythonOperator(
        task_id="pick_date",
        python_callable=pick_date_override,
    )

    ingest = PythonOperator(
        task_id="ingest_gcs",
        python_callable=lambda ti: run_ingestion_callable(ti.xcom_pull(task_ids="pick_date")),
    )

    load_raw = PythonOperator(
        task_id="load_bq_raw",
        python_callable=lambda ti: run_loader_callable(ti.xcom_pull(task_ids="pick_date")),
    )

    # Run dbt inside the container (dbt is installed in the image)
    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd="/opt/airflow/repo",
        bash_command="dbt run && dbt test",
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/repo/dbt",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/creds/gcp_sa.json",
            # pass through your regular envs
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID",""),
            "BQ_LOCATION": os.environ.get("BQ_LOCATION","US"),
        },
    )

    pick_date >> ingest >> load_raw >> dbt_run