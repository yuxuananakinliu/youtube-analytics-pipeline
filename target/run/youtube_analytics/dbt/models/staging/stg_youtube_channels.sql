

  create or replace view `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_channels`
  OPTIONS()
  as with src as (
  select
    id                                        as channel_id,
    snippet.title                             as channel_title,
    snippet.country                           as channel_country,
    snippet.publishedAt                       as channel_published_at,
    cast(statistics.viewCount       as int64) as channel_view_count,
    cast(statistics.subscriberCount as int64) as channel_subscribers,
    cast(statistics.videoCount      as int64) as channel_video_count,
    date(_partitiontime)                      as load_date
  from `orbital-nuance-471817-n0.youtube_raw.channels_raw`
)
select * from src;

