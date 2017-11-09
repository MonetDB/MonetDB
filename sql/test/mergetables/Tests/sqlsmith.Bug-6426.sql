select  
  sample_4.x as c0, 
  sample_0.location as c1
from 
  sys.netcdf_files as sample_0
      right join tmp.idxs as sample_1
          right join sys.types as sample_2
          on (sample_1.table_id = sample_2.id )
        inner join sys.s as sample_4
          inner join tmp.idxs as ref_0
          on (false)
        on (sample_2.digits = ref_0.id )
      on (sample_0.file_id = sample_4.x )
    left join sys.netcdf_files as sample_6
          inner join sys.netcdf_dims as ref_1
            inner join sys.s1 as ref_2
            on (false)
          on (sample_6.file_id = ref_1.dim_id )
        right join bam.rg as sample_7
        on (ref_2.x = sample_7.pi )
      left join sys.netcdf_dims as ref_3
            right join tmp._columns as ref_4
            on (ref_3.length is NULL)
          right join sys.triggers as sample_8
          on (ref_3.name = sample_8.name )
        left join sys.systemfunctions as ref_5
        on (ref_3.dim_id is not NULL)
      on ((false) 
          or (true))
    on (sample_0.file_id = sample_8.id )
where case when true then ref_0.name else ref_0.name end
     is NULL;
