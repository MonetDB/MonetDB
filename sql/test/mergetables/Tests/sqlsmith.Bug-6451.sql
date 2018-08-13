select  
  ref_2.y as c0, 
  cast(coalesce(ref_2.z,
    ref_2.z) as clob) as c1, 
  ref_2.z as c2, 
  case when ref_2.y is NULL then subq_0.c0 else subq_0.c0 end
     as c3, 
  ref_2.y as c4, 
  cast(coalesce(subq_0.c0,
    ref_2.z) as clob) as c5, 
  ref_2.x as c6
from 
  sys.r2 as ref_2,
  lateral (select  
        ref_2.z as c0
      from 
        sys.netcdf_vardim as ref_3
      where ref_3.dim_id is NULL) as subq_0
where EXISTS (
  select  
      case when false then ref_9.z else ref_9.z end
         as c0, 
      subq_2.c5 as c1, 
      subq_0.c0 as c2, 
      ref_2.z as c3, 
      subq_2.c5 as c4
    from 
      (select  
                ref_2.y as c0, 
                ref_4.type as c1, 
                subq_0.c0 as c2, 
                ref_2.y as c3
              from 
                sys.keys as ref_4
              where ref_2.z is NULL) as subq_1
          left join (select  
                ref_5.gr_name as c0, 
                ref_5.att_name as c1, 
                ref_2.z as c2, 
                subq_0.c0 as c3, 
                44 as c4, 
                subq_0.c0 as c5, 
                ref_2.y as c6, 
                ref_2.x as c7, 
                ref_2.y as c8
              from 
                sys.netcdf_attrs as ref_5
              where false) as subq_2
          on (subq_1.c2 = subq_2.c2 )
        right join sys.r as ref_9
        on (subq_2.c4 = ref_9.x )
    where ref_9.x is not NULL)
limit 64;
