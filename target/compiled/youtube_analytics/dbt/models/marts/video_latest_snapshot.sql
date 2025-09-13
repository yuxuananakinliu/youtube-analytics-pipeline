

with ranked as (
  select
    s.video_id,
    s.channel_id,
    s.video_title,
    s.video_published_at,
    s.views,
    s.likes,
    s.comments,
    s.duration_seconds,
    s.load_date,
    row_number() over (partition by s.video_id order by s.load_date desc) as rn
  from `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_video_stats` s
)
select
  r.video_id,
  r.channel_id,
  r.video_title,
  r.video_published_at,
  r.views,
  r.likes,
  r.comments,
  r.duration_seconds,
  r.load_date as snapshot_date
from ranked r
where rn = 1