select
  cast(coalesce(ref_0.stop,
    ref_0.stop) as timestamp) as c0,
  ref_0.id as c1,
  cast(coalesce(ref_0.cpu,
    ref_0.io) as int) as c2,
  ref_0.run as c3,
  ref_0.stop as c4,
  ref_0.cpu as c5,
  case when cast(nullif(ref_0.tuples,
        ref_0.tuples) as bigint) is not NULL then ref_0.io else ref_0.io end
     as c6
from
  sys.querylog_calls as ref_0
where cast(coalesce(ref_0.tuples,
    case when EXISTS (
        select
            ref_1.id as c0
          from
            sys.args as ref_1
              left join tmp.objects as ref_2
                right join sys.spatial_ref_sys as ref_3
                on (true)
              on (ref_1.type_scale = ref_2.id )
          where true) then ref_0.run else ref_0.run end
      ) as bigint) is not NULL
limit 101;
