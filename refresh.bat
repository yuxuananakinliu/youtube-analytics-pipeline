@echo off
setlocal
set "HERE=%~dp0"
if not "%~1"=="" set "DATE_OVERRIDE=%~1"

set "DBT_PROFILES_DIR=%HERE%dbt"
set "GCP_PROJECT_ID=orbital-nuance-471817-n0"
set "GOOGLE_APPLICATION_CREDENTIALS=%HERE%creds\gcp_sa.json"

REM 1) Ingest (GCS)
"%HERE%.venv\Scripts\python.exe" "%HERE%ingestion\fetch_youtube.py"

REM 2) Load (GCS -> BQ raw)
"%HERE%.venv\Scripts\python.exe" "%HERE%ingestion\load_to_bigquery.py"

REM 3) Transform & test (dbt)
"%HERE%.venv\Scripts\dbt.exe" run
"%HERE%.venv\Scripts\dbt.exe" test

endlocal