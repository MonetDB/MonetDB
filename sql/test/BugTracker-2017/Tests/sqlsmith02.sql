--Another round. After 116K queries

--Seems geom related:
--!42S02!SELECT: no such table 'spatial_ref_sys'

select
  ref_78.mod as c0,
  ref_79.name as c1
from
  sys.columns as ref_38
        right join sys.args as ref_72
            right join sys.spatial_ref_sys as ref_75
                right join sys.geometry_columns as ref_76
                on (ref_75.proj4text = ref_76.f_table_schema )
              right join sys.spatial_ref_sys as ref_77
                inner join sys.functions as ref_78
                on (ref_77.proj4text = ref_78.name )
              on (ref_76.srid = ref_78.id )
            on (ref_72.id = ref_75.srid )
          left join tmp.objects as ref_79
          on (ref_75.srid = ref_79.id )
        on (ref_38.type_digits = ref_75.srid )
      left join sys.schemas as ref_80
          inner join sys.schemas as ref_81
            left join sys.netcdf_vardim as ref_95
              inner join sys._columns as ref_96
              on (ref_95.dim_id = ref_96.id )
            on (ref_81.owner = ref_96.id )
          on (ref_80.authorization = ref_81.id )
        right join sys.dependencies as ref_125
          left join tmp.idxs as ref_126
            inner join sys.sequences as ref_155
              inner join tmp.idxs as ref_156
                inner join sys.types as ref_157
                on (ref_156.type = ref_157.id )
              on (ref_155.name = ref_157.systemname )
            on (ref_126.id = ref_156.id )
          on (ref_125.id = ref_155.id )
        on (ref_96.type = ref_156.name )
      on (ref_79.name = ref_157.systemname )
    left join sys.queue as ref_158
    on (ref_125.depend_id = ref_158.progress )
where ref_38.table_id is NULL
limit 63; 
