select  
  64 as c0
from 
  (select distinct 
        68 as c0
      from 
        tmp.keys as ref_0
          inner join sys.args as ref_1
              right join sys.geometry_columns as ref_2
              on (ref_1.id = ref_2.srid )
            left join sys.netcdf_dims as ref_12
              right join sys.args as ref_13
              on (ref_12.name = ref_13.name )
            on (ref_1.id = ref_13.id )
          on (ref_0.action = ref_13.id )
      where (ref_2.f_geometry_column is not NULL) 
        and (false)) as subq_0
where subq_0.c0 is not NULL;
