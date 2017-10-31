select  
  sample_8.dimpos as c0, 
  ref_4.x as c1, 
  ref_2.y as c2
from 
  sys.r3 as ref_2
        left join tmp._tables as sample_6
        on (ref_2.y = sample_6.id )
      left join sys._tables as sample_7
          right join sys.netcdf_vardim as sample_8
            left join sys.querylog_catalog as ref_3
            on (sample_8.dimpos = ref_3.mal )
          on (true)
        left join sys.s1 as ref_4
        on (sample_8.dimpos = ref_4.x )
      on ((sample_7.id is NULL) 
          or (sample_8.var_id is not NULL))
    left join sys.optimizers as ref_12
        inner join bam.sq as ref_13
        on (ref_12.def = ref_13.sn )
      inner join sys.keywords as ref_14
      on (ref_13.sp is not NULL)
    on ((sample_6.commit_action is NULL) 
        or (ref_14.keyword is not NULL))
where ref_3.optimize is NULL
limit 86;

select
  ref_7.length as c0,
  sample_1.type as c1,
  ref_7.length as c2,
  sample_8.function_type_id as c3
from
  sys.columns as sample_1
    inner join bam.rg as ref_4
        left join sys.keys as ref_6
        on (ref_4.pi = ref_6.id )
      right join sys.table_types as sample_7
            left join sys.function_types as sample_8
              left join sys.r as sample_9
              on (sample_9.y is not NULL)
            on (sample_7.table_type_name = sample_8.function_type_name )
          left join sys.r as sample_10
            right join sys.netcdf_dims as ref_7
            on ((false)
                or (true))
          on (sample_10.z is not NULL)
        right join sys._tables as sample_12
          left join bam.rg as ref_8
          on (sample_12.id = ref_8.pi )
        on (sample_10.y = ref_8.pi )
      on (ref_4.cn = sample_9.z )
    on (sample_1.type_scale = ref_4.pi )
where sample_10.y is NULL
limit 14;
