# YouTube Analytics Pipeline 

> GCP + BigQuery + dbt + Looker Studio

Batch pipeline that ingests YouTube Data API → stores raw in GCS → lands in BigQuery → transforms with dbt into analytics marts → visualized in Looker Studio.

## Architecture
Ingestion (Python) → GCS (NDJSON) → BigQuery (raw) → dbt (staging + marts, tests) → Looker Studio (BI)
```text
YouTube Data API ─► ingestion/fetch_youtube.py
└─ uploads NDJSON → GCS (gs://<bucket>/raw/date=YYYY-MM-DD/*.json)

GCS ─► BigQuery (youtube_raw.)
dbt ─► youtube_stg. (staging views)
└─ youtube_analytics.fct_video_daily_metrics (incremental, partitioned)
└─ youtube_analytics.agg_* / dim_* / snapshot views
Airflow ─► Schedules and runs daily ingestion + dbt
BI ─► Looker Studio dashboards
```

## Stack
- **GCP**: Cloud Storage, BigQuery  
- **Python**: `google-api-python-client`, `google-cloud-storage`, `google-cloud-bigquery`  
- **dbt**: BigQuery adapter, macros, schema tests  
- **Airflow**: Dockerized orchestrator (`youtube_daily` DAG)  
- **BI**: Looker Studio  

## Repo Layout
```text
ingestion/            # Python ingestion (YouTube → GCS)
dbt/                  # dbt project (profiles + models + macros)
  ├─ models/
  │   ├─ staging/     # stg_youtube_channels / video_ids / video_stats
  │   └─ marts/       # fct_video_daily_metrics, agg_channel_trends_7d, dim_channel
  ├─ macros/          # custom schema macro
  └─ dbt_project.yml
airflow/              # Airflow Docker setup, DAGs, configs
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

#### Live Dashboard Link: https://lookerstudio.google.com/s/i4UfhnBNXck

#### Connect to BigQuery and add these tables as sources:
- `fct_video_daily_metrics` (trends)
- `agg_channel_trends_7d` (7d KPIs)
- `video_latest_snapshot` (latest per video)
- `dim_channel` (channel names, metadata)

#### Pages:
1. **Channel Overview** — scorecards for 7d KPIs + time series (daily_views)
2. **Top Videos** — table/bar of last 7 days
3. **Video Details** — selectors + trend lines

## Airflow Orchestration

- Dockerized Airflow (docker-compose.yaml)
- `.env` manages secrets:
  - `GCP_PROJECT_ID`, `GCS_BUCKET`, `BQ_LOCATION`
  - `YOUTUBE_API_KEY`
  - `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`
- DAG: `youtube_daily`
  - Tasks: ingestion → BigQuery load → dbt run → dbt test
