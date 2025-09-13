

with daily as (
  select
    channel_id,
    video_id,
    load_date,
    daily_views,
    daily_likes,
    daily_comments
  from `orbital-nuance-471817-n0`.`youtube_analytics`.`fct_video_daily_metrics`
),
rollup_cte as (
  select
    channel_id,
    load_date,
    sum(daily_views)    over (
      partition by channel_id
      order by load_date
      rows between 6 preceding and current row
    ) as views_7d,
    sum(daily_likes)    over (
      partition by channel_id
      order by load_date
      rows between 6 preceding and current row
    ) as likes_7d,
    sum(daily_comments) over (
      partition by channel_id
      order by load_date
      rows between 6 preceding and current row
    ) as comments_7d
  from daily
)
select distinct channel_id, load_date, views_7d, likes_7d, comments_7d
from rollup_cte