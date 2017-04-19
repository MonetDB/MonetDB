select  
  subq_0.c1 as c0, 
  subq_0.c2 as c1
from 
  (select  
        ref_25.name as c0, 
        (select sessiontimeout from sys.sessions)
           as c1, 
        63 as c2, 
        ref_24.var_id as c3, 
        ref_25.name as c4, 
        (select sorted from sys.storage)
           as c5
      from 
        sys.idxs as ref_23
            inner join sys.netcdf_vardim as ref_24
              left join sys.db_user_info as ref_25
              on (ref_24.dimpos = ref_25.default_schema )
            on (ref_23.id = ref_24.var_id )
          right join tmp.objects as ref_26
          on (ref_25.name = ref_26.name )
      where EXISTS (
        select distinct 
            ref_27.table_id as c0
          from 
            tmp.keys as ref_27
              right join tmp.keys as ref_28
              on (ref_27.type = ref_28.id )
          where ref_27.type is not NULL)) as subq_0
where subq_0.c5 is NULL;
