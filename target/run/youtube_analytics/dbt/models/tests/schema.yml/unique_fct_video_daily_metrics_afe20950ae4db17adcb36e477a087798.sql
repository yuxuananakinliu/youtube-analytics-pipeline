
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select video_id || '-' || cast(load_date as string) as unique_field
  from `orbital-nuance-471817-n0`.`youtube_analytics`.`fct_video_daily_metrics`
  where video_id || '-' || cast(load_date as string) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test