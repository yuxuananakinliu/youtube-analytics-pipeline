
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_date
from `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_video_stats`
where load_date is null



  
  
      
    ) dbt_internal_test