select
   cast(coalesce(ref_8.name,
     cast(nullif(ref_7.column,
       cast(null as clob)) as clob)) as clob) as c0,
   ref_10.function_id as c1,
   cast(coalesce(ref_6.action,
     ref_6.id) as int) as c2
from
   sys.types as ref_0
     left join sys.netcdf_attrs as ref_1
       inner join sys.keys as ref_6
         inner join sys.storagemodelinput as ref_7
             left join sys.optimizers as ref_8
               inner join sys.users as ref_9
                 right join sys.systemfunctions as ref_10
                 on (ref_9.default_schema = ref_10.function_id )
               on (true)
             on (ref_7.typewidth = ref_9.default_schema )
           inner join sys.systemfunctions as ref_11
           on (ref_9.default_schema = ref_11.function_id )
         on (ref_6.action is NULL)
       on (ref_6.action is not NULL)
     on (ref_0.radix = ref_11.function_id )
where EXISTS (
   select
       case when ref_8.name is NULL then ref_7.orderidx else ref_7.orderidx end
          as c0
     from
       (select
                 ref_8.def as c0,
                 ref_9.fullname as c1
               from
                 sys.r2 as ref_12
               where ref_11.function_id is NULL
               limit 79) as subq_0
           left join tmp._tables as ref_13
             inner join sys.netcdf_vardim as ref_14
             on (ref_14.dimpos is NULL)
           on ((ref_1.att_type is not NULL)
               or (ref_0.id is NULL))
         inner join sys.netcdf_vardim as ref_15
         on (ref_10.function_id is not NULL)
     where ref_0.systemname is NULL)
limit 77;
