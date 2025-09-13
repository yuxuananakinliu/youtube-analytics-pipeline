
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select video_id
from `orbital-nuance-471817-n0`.`youtube_stg`.`stg_youtube_video_stats`
where video_id is null



  
  
      
    ) dbt_internal_test