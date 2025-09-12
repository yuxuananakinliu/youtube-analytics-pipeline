# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]
- Airflow (Docker) orchestration DAG
- GitHub Actions nightly job (alternative)
- dbt model for titles joined into facts (`fct_video_daily_metrics_with_titles`)
- Backfill utility for demo time series

## [1.0.0] - 2025-09-12
### Added
- Python ingestion `fetch_youtube.py` pulling YouTube Data API → GCS (NDJSON)
- BigQuery raw tables (`channels_raw`, `video_ids_raw`, `video_stats_raw`)
- dbt project: staging views; marts including:
  - `fct_video_daily_metrics` (incremental, partitioned by `load_date`)
  - `agg_channel_trends_7d`, `dim_channel`, `video_latest_snapshot` (views)
- Tests for core models
- Repo-local runners: `dbt.bat`, `refresh.bat`
- Looker Studio dashboard (3 pages: Channel Overview, Top Videos, Video Details)
- README with setup, run, and dashboard instructions

### Changed
- Configured schema macro to deploy staging/marts into desired datasets (optional)

### Fixed
- Handled empty JSON structs for BigQuery autodetect (`contentDetails.contentRating`)
