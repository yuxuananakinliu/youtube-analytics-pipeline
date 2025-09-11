
  
    

    create or replace table `orbital-nuance-471817-n0`.`youtube_stg_youtube_analytics`.`fct_video_daily_metrics`
      
    
    

    
    OPTIONS()
    as (
      with daily as (
  select
    video_id,
    channel_id,
    load_date,
    views,
    likes,
    comments
  from `orbital-nuance-471817-n0`.`youtube_stg_youtube_stg`.`stg_youtube_video_stats`
),
with_lag as (
  select
    *,
    lag(views)    over (partition by video_id order by load_date) as prev_views,
    lag(likes)    over (partition by video_id order by load_date) as prev_likes,
    lag(comments) over (partition by video_id order by load_date) as prev_comments
  from daily
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
    );
  