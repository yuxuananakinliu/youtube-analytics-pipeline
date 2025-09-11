
    
    

with dbt_test__target as (

  select video_id || '-' || cast(load_date as string) as unique_field
  from `orbital-nuance-471817-n0`.`youtube_stg_youtube_analytics`.`fct_video_daily_metrics`
  where video_id || '-' || cast(load_date as string) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


