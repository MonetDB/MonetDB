select  
  ref_2.name as c0, 
  case when true then ref_3.dimpos else ref_3.dimpos end
     as c1
from 
  sys.tables as ref_0
      inner join sys.dependencies as ref_1
          right join sys.tables as ref_2
          on ((ref_2.temporary is NULL) 
              or (ref_2.system is not NULL))
        inner join bam.pg as sample_1
        on (false)
      on (ref_0.system = ref_2.system )
    inner join sys.netcdf_vardim as ref_3
    on (true)
where ref_2.commit_action is NULL;
