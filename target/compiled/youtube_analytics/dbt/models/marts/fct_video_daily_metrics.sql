

-- Pull from the staging model's flattened columns
with src as (
  select
    video_id,
    channel_id,
    load_date,
    views,
    likes,
    comments
  from `orbital-nuance-471817-n0`.`youtube_stg_youtube_stg`.`stg_youtube_video_stats`
  
    -- Reprocess yesterday + today so lag() boundaries stay correct
    where load_date >= (
      select date_sub(coalesce(max(load_date), date '1900-01-01'), interval 1 day)
      from `orbital-nuance-471817-n0`.`youtube_stg_youtube_analytics`.`fct_video_daily_metrics`
    )
  
),

with_lag as (
  select
    *,
    lag(views)    over (partition by video_id order by load_date) as prev_views,
    lag(likes)    over (partition by video_id order by load_date) as prev_likes,
    lag(comments) over (partition by video_id order by load_date) as prev_comments
  from src
)

select
  video_id,
  channel_id,
  load_date,
  views,
  likes,
  comments,
  views    - coalesce(prev_views, 0)    as daily_views,
  likes    - coalesce(prev_likes, 0)    as daily_likes,
  comments - coalesce(prev_comments, 0) as daily_comments
from with_lag