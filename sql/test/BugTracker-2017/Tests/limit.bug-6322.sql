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
