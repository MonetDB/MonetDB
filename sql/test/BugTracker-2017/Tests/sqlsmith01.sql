--after 17K queries starting from empty db

select
  ref_20.sm as c0,
  cast(coalesce(ref_20.file_id,
    ref_20.file_id) as bigint) as c1
from
  bam.rg as ref_20
where ref_20.fo is not NULL;
select
  cast(coalesce(subq_0.c0,
    subq_0.c0) as int) as c0,
  subq_0.c0 as c1,
  subq_0.c0 as c2,
  subq_0.c0 as c3
from
  (select
        ref_20.id as c0
      from
        sys.querylog_history as ref_11
          inner join sys.users as ref_18
                inner join sys.geometry_columns as ref_19
                on (ref_18.default_schema = ref_19.srid )
              right join tmp.keys as ref_20
              on (ref_18.fullname = ref_20.name )
            left join sys._columns as ref_21
              inner join sys.columns as ref_22
              on (ref_21.type_scale = ref_22.id )
            on (ref_20.rkey = ref_21.id )
          on (ref_11.arguments = ref_19.f_table_catalog )
      where ref_22.type is not NULL) as subq_0
where false
limit 132;

