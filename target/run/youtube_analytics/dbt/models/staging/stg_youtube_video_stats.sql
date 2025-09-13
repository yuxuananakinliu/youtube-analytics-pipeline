

  create or replace view `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_video_stats`
  OPTIONS()
  as with src as (
  select
    id                                     as video_id,
    snippet.channelId                      as channel_id,
    snippet.title                          as video_title,
    snippet.publishedAt                    as video_published_at,
    cast(statistics.viewCount    as int64) as views,
    cast(statistics.likeCount    as int64) as likes,
    cast(statistics.commentCount as int64) as comments,
    contentDetails.duration                as duration_iso,
    current_date()                         as load_date
  from `orbital-nuance-471817-n0.youtube_raw.video_stats_raw`
),
parsed as (
  select
    *,
    (
      coalesce(SAFE_CAST(REGEXP_EXTRACT(duration_iso, r'(\d+)H') AS INT64), 0) * 3600
      + coalesce(SAFE_CAST(REGEXP_EXTRACT(duration_iso, r'(\d+)M') AS INT64), 0) * 60
      + coalesce(SAFE_CAST(REGEXP_EXTRACT(duration_iso, r'(\d+)S') AS INT64), 0)
    ) as duration_seconds
  from src
)
select * from parsed;

