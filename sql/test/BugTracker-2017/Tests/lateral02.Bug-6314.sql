select
  (select id from sys.schemas) as c0
from 
  (select  subq_0.c3 as c4
          from 
            lateral (select
                  ref_0.ndim as c3
                from 
                  tmp._tables as sample_1
                where ((select index_type_id from sys.index_types) is not NULL)
                  or (true)) as subq_0
          where (select n_regionkey from sys.nation) is not NULL) as subq_1
      inner join sys.idxs as ref_1
      on (subq_1.c2 = ref_1.id )
    inner join sys.netcdf_attrs as sample_15
        inner join sys.dependency_types as ref_20
        on (sample_15.att_name = ref_20.dependency_type_name )
      right join sys.privilege_codes as sample_16
      on (sample_15.att_type = sample_16.privilege_code_name )
    on (ref_1.type = sample_16.privilege_code_id )
where sample_16.privilege_code_id is NULL;
