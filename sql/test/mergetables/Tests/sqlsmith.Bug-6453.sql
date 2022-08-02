select  
  cast(nullif(subq_0.c3,
    subq_0.c3) as int) as c0
from 
  (select  
        ref_0.x as c0, 
        ref_0.y as c1, 
        ref_0.z as c2, 
        ref_0.x as c3, 
        ref_0.y as c4
      from 
        sys.s2 as ref_0
      where (ref_0.z is NULL) 
        and (EXISTS (
          select  
              ref_1.role_id as c0, 
              ref_0.z as c1, 
              ref_1.role_id as c2, 
              ref_1.role_id as c3, 
              ref_0.z as c4, 
              ref_1.login_id as c5, 
              ref_0.z as c6, 
              ref_0.y as c7
            from 
              sys.user_role as ref_1
            where (EXISTS (
                select  
                    ref_0.y as c0, 
                    ref_1.login_id as c1, 
                    ref_0.x as c2, 
                    ref_0.x as c3, 
                    ref_0.x as c4, 
                    77 as c5, 
                    ref_1.role_id as c6, 
                    ref_1.login_id as c7, 
                    ref_0.x as c8, 
                    ref_0.x as c9, 
                    ref_2.name as c10, 
                    ref_0.y as c11, 
                    ref_2.file_id as c12, 
                    ref_2.vartype as c13
                  from 
                    sys.netcdf_vars as ref_2
                  where ref_0.x is not NULL)) 
              or (ref_0.y is NULL)))
      limit 109) as subq_0
where (EXISTS (
    select  
        ref_3.z as c0, 
        subq_0.c2 as c1, 
        ref_3.z as c2, 
        ref_3.x as c3, 
        100 as c4, 
        ref_3.x as c5, 
        ref_3.z as c6
      from 
        sys.r as ref_3
      where ref_3.x is not NULL)) 
  or (subq_0.c2 is NULL)
limit 141;

-- simplified version 
select  ref_0.x from sys.s2 as ref_0 
 where 	EXISTS (
          select ref_1.role_id as c0 from sys.user_role as ref_1 
	   where (EXISTS ( select ref_0.y as c0 from sys.netcdf_vars)) or (ref_0.y is NULL)
	)
;
