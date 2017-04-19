-- And a new run after 234K queries
-- !42S02!SELECT: no such table 'querylog_calls'


select
  8 as c0
from
  tmp.keys as ref_68
      left join sys.auths as ref_70
        inner join sys._columns as ref_86
            inner join tmp.keys as ref_101
                inner join sys.querylog_calls as ref_102
                on (ref_101.type = ref_102.cpu )
              inner join sys.idxs as ref_103
              on (ref_102.io = ref_103.id )
            on (ref_86.table_id = ref_102.cpu )
          right join sys.privileges as ref_111
              left join sys.systemfunctions as ref_112
                left join sys.querylog_calls as ref_113
                on (ref_112.function_id = ref_113.cpu )
              on (ref_111.privileges = ref_112.function_id )
            right join sys.schemas as ref_114
              inner join sys.geometry_columns as ref_115
                right join sys.users as ref_116
                on (ref_115.f_table_name = ref_116.name )
              on (ref_114.owner = ref_115.srid )
            on (ref_111.obj_id = ref_116.default_schema )
          on (ref_103.id = ref_113.cpu )
        on (ref_70.id = ref_111.obj_id )
      on (ref_68.rkey = ref_114.id )
    left join sys.columns as ref_121
        inner join sys.types as ref_122
        on (ref_121.type_digits = ref_122.id )
      inner join tmp._tables as ref_123
      on (ref_122.schema_id = ref_123.id )
    on (ref_116.default_schema = ref_123.id )
where cast(coalesce(cast(coalesce(ref_101.table_id,
      ref_121.number) as int),
    ref_68.table_id) as int) is NULL
limit 164; 
