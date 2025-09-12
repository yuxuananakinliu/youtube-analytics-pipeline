@echo off
setlocal
set "HERE=%~dp0"
set "DBT_PROFILES_DIR=%HERE%dbt"
set "GCP_PROJECT_ID=YOUR_GCP_PROJECT_ID_HERE"
set "GOOGLE_APPLICATION_CREDENTIALS=%HERE%creds\gcp_sa.json"

"%HERE%.venv\Scripts\python.exe" "%HERE%ingestion\fetch_youtube.py"
"%HERE%.venv\Scripts\dbt.exe" run
"%HERE%.venv\Scripts\dbt.exe" test
endlocal