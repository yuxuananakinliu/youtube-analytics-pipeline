
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_date
from `orbital-nuance-471817-n0`.`youtube_stg_youtube_analytics`.`fct_video_daily_metrics`
where load_date is null



  
  
      
    ) dbt_internal_test