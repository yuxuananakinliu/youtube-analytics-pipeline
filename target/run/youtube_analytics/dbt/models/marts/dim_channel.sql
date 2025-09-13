

  create or replace view `orbital-nuance-471817-n0`.`youtube_analytics`.`dim_channel`
  OPTIONS()
  as 

with ranked as (
  select
    channel_id,
    channel_title,
    channel_country,
    channel_published_at,
    channel_view_count,
    channel_subscribers,
    channel_video_count,
    load_date,
    row_number() over (partition by channel_id order by load_date desc) as rn
  from `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_channels`
)
select
  channel_id,
  channel_title,
  channel_country,
  channel_published_at,
  channel_view_count,
  channel_subscribers,
  channel_video_count,
  load_date as snapshot_date
from ranked
where rn = 1;

