-- After 154K queries.

select
  ref_696.id as c0,
  ref_699.tag as c1
from
  sys.functions as ref_678
        left join sys._columns as ref_695
          right join tmp._columns as ref_696
            inner join sys.geometry_columns as ref_697
              left join sys.users as ref_698
              on (ref_697.f_geometry_column = ref_698.name )
            on (ref_696.storage = ref_698.name )
          on (ref_695.storage = ref_698.name )
        on (ref_678.language = ref_698.default_schema )
      left join sys.queue as ref_699
      on (ref_678.language = ref_699.progress )
    left join sys.columns as ref_700
    on (ref_698.fullname = ref_700.name )
where ref_700.id is not NULL
limit 77; 
