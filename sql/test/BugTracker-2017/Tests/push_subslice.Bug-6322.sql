select
  subq_0.c3 as c2
from 
  (select  
        sample_0.proj4text as c3,
        sample_0.auth_name as c7
      from 
        sys.spatial_ref_sys as sample_0
      where true
      limit 134) as subq_0
where (true)
  or ((select pc from sys.tracelog)
       is not NULL);

select
      (select active from sys.sessions) as c3,
       subq_0.c0 as c9
from
  (select sample_0.proj4text as c0, 7 as c1
      from sys.spatial_ref_sys as sample_0
              where sample_0.srtext is not NULL limit 95) as subq_0
where cast(coalesce(17, subq_0.c1) as int) is not NULL;

