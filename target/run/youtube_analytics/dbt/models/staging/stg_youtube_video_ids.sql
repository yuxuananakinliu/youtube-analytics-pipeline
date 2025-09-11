

  create or replace view `orbital-nuance-471817-n0`.`youtube_stg_youtube_stg`.`stg_youtube_video_ids`
  OPTIONS()
  as select
  videoId              as video_id,
  channelId            as channel_id,
  date(_partitiontime) as load_date
from `orbital-nuance-471817-n0.youtube_raw.video_ids_raw`;

