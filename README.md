# YouTube Analytics Pipeline 

> GCP + BigQuery + dbt + Looker Studio

Batch pipeline that ingests YouTube Data API → stores raw in GCS → lands in BigQuery → transforms with dbt into analytics marts → visualized in Looker Studio.

## Architecture
Ingestion (Python) → GCS (NDJSON) → BigQuery (raw) → dbt (staging + marts, tests) → Looker Studio (BI)
```text
/ingestion/fetch_youtube.py
└─ calls YouTube Data API, uploads NDJSON to gs://<bucket>/raw/date=YYYY-MM-DD/.json
GCS ─► BigQuery raw (JSON load)
dbt ─► stg_ views → fct_video_daily_metrics (incremental, partitioned) → agg_* / dim_* views
BI ─► Looker Studio report (Channel overview, Top videos, Video details)
```

## Stack
- **GCP**: Cloud Storage, BigQuery
- **Python**: `google-api-python-client`, `google-cloud-storage`, `google-cloud-bigquery`
- **dbt**: BigQuery adapter, tests, incremental models
- **BI**: Looker Studio

## Repo Layout
```text
ingestion/ # Python ingestion (YouTube → GCS)
dbt/ # dbt profiles + models + macros
models/
staging/ # stg_youtube_channels / video_ids / video_stats (views)
marts/ # fct_video_daily_metrics (incremental), agg_channel_trends_7d, dim_channel, video_latest_snapshot
macros/ # generate_schema_name (optional)
dbt_project.yml
dbt.bat / refresh.bat # repo-local runners (no global env needed)
```

## BigQuery Datasets & Models

- Raw: youtube_raw (NDJSON → BigQuery)
- Staging (views):
  - `stg_youtube_channels`
  - `stg_youtube_video_ids`
  - `stg_youtube_video_stats`
- Fact (incremental table): `fct_video_daily_metrics`
  - Partitioned by `load_date`, clustered by (`channel_id`, `video_id`)
- Aggregates / dims (views):
  - `agg_channel_trends_7d`, `dim_channel`, `video_latest_snapshot`
 
## Dashboard (*Looker Studio*)
#### Connect to BigQuery and add these tables as sources:
- `fct_video_daily_metrics` (trends)
- `agg_channel_trends_7d` (7d KPIs)
- `video_latest_snapshot` (latest per video)
- `dim_channel` (channel names, metadata)
#### Pages:
1. **Channel Overview** — scorecards for 7d KPIs + time series (daily_views)
2. **Top Videos** — table/bar of last 7 days
3. **Video Details** — selectors + trend lines
