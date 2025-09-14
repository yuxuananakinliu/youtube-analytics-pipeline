# Changelog
All notable changes to this project are documented here.
This project follows [Semantic Versioning](https://semver.org/) and “Keep a Changelog”.

## [Unreleased]
- Airflow DAG to orchestrate ingest → load → dbt.
- Backfill flag (DATE_OVERRIDE) end-to-end (loader + dbt).
- CI: lint + dbt compile on PRs.

## [0.1.1] - 2025-09-13
### Added
- **One-button refresh** flow: GCS ingest → BigQuery raw load → dbt run/test.
- **Ingestion**: `fetch_youtube.py` pulls channel metadata, recent video ids, and stats; prunes empty structures; uploads NDJSON to `gs://<bucket>/raw/date=YYYY-MM-DD/`. :contentReference[oaicite:4]{index=4}
- **Loader**: `load_to_bigquery.py` appends the three NDJSON files into `youtube_raw` tables with autodetect + append semantics. :contentReference[oaicite:5]{index=5}
- **Staging & marts** (dbt): staging views on `youtube_stg`, marts on `youtube_analytics`, including incremental `fct_video_daily_metrics`.
- **Utilities**:
  - `fix_video_stats_empty_struct.py` — optional cleaner that rewrites `video_stats_clean.json` with empty dict/list fields removed. :contentReference[oaicite:6]{index=6}
  - `smoke_upload.py` — sanity NDJSON upload to GCS. :contentReference[oaicite:7]{index=7}

### Changed
- **dbt schema naming fixed**: ensured `generate_schema_name` macro is loaded so models deploy to `youtube_stg` and `youtube_analytics` exactly (no suffixed datasets). :contentReference[oaicite:0]{index=0}
- **Raw tables partitioned**: `youtube_raw.{channels_raw, video_ids_raw, video_stats_raw}` are created as **ingestion-time partitioned** to support `_PARTITIONTIME` in staging queries. (Configured via `LoadJobConfig` in the loader.) :contentReference[oaicite:1]{index=1}
- **Channel set updated**: switched to two high-activity channels (e.g., MrBeast, Kurzgesagt) via `ingestion/channel_list.json` consumed by `fetch_youtube.py`. :contentReference[oaicite:2]{index=2}
- Repo-local runners (`refresh.bat`, `dbt.bat`) so venv activation isn’t required.

### Fixed
- **Staging errors**: `_PARTITIONTIME` now resolves because raw tables are partitioned; staging views (`stg_youtube_*`) build cleanly.
- **Schema drift on channels**: when YouTube adds fields (e.g., `snippet.defaultLanguage`), raw loads can be recreated or configured to allow schema evolution via `schema_update_options`. (See loader for append config.) :contentReference[oaicite:3]{index=3}

### Notes
- Keep secrets out of git: `.env`, `creds/*.json` are ignored; scripts read from env and local files at runtime.


## [0.1.0] - 2025-09-12
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
