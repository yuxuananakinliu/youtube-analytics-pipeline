@echo off
setlocal
set "HERE=%~dp0"
set "DBT_PROFILES_DIR=%HERE%dbt"
set "GCP_PROJECT_ID=orbital-nuance-471817-n0"
set "GOOGLE_APPLICATION_CREDENTIALS=%HERE%creds\gcp_sa.json"

REM 1) Ingest YouTube -> GCS
"%HERE%.venv\Scripts\python.exe" "%HERE%ingestion\fetch_youtube.py"

REM 2) Append today’s files GCS -> BigQuery raw
"%HERE%.venv\Scripts\python.exe" "%HERE%ingestion\load_to_bigquery.py"

REM 3) Transform + tests
"%HERE%.venv\Scripts\dbt.exe" run
"%HERE%.venv\Scripts\dbt.exe" test
endlocal