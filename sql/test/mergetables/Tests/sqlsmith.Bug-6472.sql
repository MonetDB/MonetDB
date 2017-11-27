select  
  subq_0.c1 as c0, 
  subq_0.c3 as c1, 
  subq_0.c1 as c2
from 
  (select  
        ref_0.rrsmb as c0, 
        ref_0.ticks as c1, 
        ref_0.thread as c2, 
        92 as c3, 
        ref_0.majflt as c4, 
        ref_0.stmt as c5, 
        ref_0.minflt as c6, 
        ref_0.majflt as c7, 
        ref_0.majflt as c8, 
        36 as c9, 
        ref_0.writes as c10, 
        ref_0.writes as c11, 
        ref_0.thread as c12, 
        ref_0.thread as c13
      from 
        sys.tracelog as ref_0
      where ref_0.rrsmb is NULL
      limit 90) as subq_0
where EXISTS (
  select  
      subq_0.c9 as c0, 
      subq_0.c13 as c1, 
      ref_3.name as c2, 
      ref_3.length as c3, 
      ref_5.rkey as c4
    from 
      sys.netcdf_dims as ref_3
          inner join tmp._columns as ref_4
            left join tmp.keys as ref_5
            on (true)
          on (ref_3.name = ref_5.name )
        left join (select  
              subq_0.c1 as c0
            from 
              sys._tables as ref_6
            where ref_6.access is not NULL) as subq_1
        on ((true) 
            or (true))
    where true);
