@echo off
setlocal
rem --- repo root of this script ---
set "HERE=%~dp0"

rem --- point dbt to repo-local profiles ---
set "DBT_PROFILES_DIR=%HERE%dbt"

rem --- inject env vars for dbt profile (EDIT project id once) ---
set "GCP_PROJECT_ID=orbital-nuance-471817-n0"
set "GOOGLE_APPLICATION_CREDENTIALS=%HERE%creds\gcp_sa.json"

rem --- call dbt inside venv, pass through all args ---
"%HERE%.venv\Scripts\dbt.exe" %*
endlocal