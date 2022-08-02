select  
  64 as c0, 
  ref_12.y as c1, 
  ref_12.y as c2, 
  case when true then ref_12.z else ref_12.z end
     as c3, 
  cast(coalesce(ref_12.b,
    ref_12.b) as boolean) as c4, 
  ref_12.x as c5
from 
  sys.s as ref_12
where EXISTS (
  select  
      ref_17.keyword as c0, 
      subq_0.c5 as c1, 
      ref_12.x as c2, 
      subq_0.c1 as c3, 
      ref_17.keyword as c4, 
      subq_0.c6 as c5, 
      ref_17.keyword as c6, 
      subq_0.c9 as c7, 
      ref_12.z as c8, 
      ref_12.y as c9, 
      23 as c10, 
      subq_0.c1 as c11
    from 
      sys.keywords as ref_17
        left join (select  
              ref_19.y as c0, 
              ref_12.x as c1, 
              ref_19.z as c2, 
              ref_19.y as c3, 
              ref_12.z as c4, 
              ref_12.y as c5, 
              ref_19.y as c6, 
              24 as c7, 
              ref_12.b as c8, 
              ref_12.z as c9
            from 
              sys.r2 as ref_19
            where false
            limit 79) as subq_0
        on ((subq_0.c4 is NULL) 
            or (subq_0.c2 is NULL))
    where false)
limit 123;
