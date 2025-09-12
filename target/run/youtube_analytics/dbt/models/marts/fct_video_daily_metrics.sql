-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `orbital-nuance-471817-n0`.`youtube_stg_youtube_analytics`.`fct_video_daily_metrics` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.video_id = DBT_INTERNAL_DEST.video_id
                ) and (
                    DBT_INTERNAL_SOURCE.load_date = DBT_INTERNAL_DEST.load_date
                )

    
    when matched then update set
        `video_id` = DBT_INTERNAL_SOURCE.`video_id`,`channel_id` = DBT_INTERNAL_SOURCE.`channel_id`,`load_date` = DBT_INTERNAL_SOURCE.`load_date`,`views` = DBT_INTERNAL_SOURCE.`views`,`likes` = DBT_INTERNAL_SOURCE.`likes`,`comments` = DBT_INTERNAL_SOURCE.`comments`,`daily_views` = DBT_INTERNAL_SOURCE.`daily_views`,`daily_likes` = DBT_INTERNAL_SOURCE.`daily_likes`,`daily_comments` = DBT_INTERNAL_SOURCE.`daily_comments`
    

    when not matched then insert
        (`video_id`, `channel_id`, `load_date`, `views`, `likes`, `comments`, `daily_views`, `daily_likes`, `daily_comments`)
    values
        (`video_id`, `channel_id`, `load_date`, `views`, `likes`, `comments`, `daily_views`, `daily_likes`, `daily_comments`)


    