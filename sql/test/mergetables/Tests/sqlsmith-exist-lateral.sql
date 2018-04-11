select 
  ref_11.access as c0,
  ref_12.revsorted as c1
from
  sys._tables as ref_11
    inner join sys.statistics as ref_12
    on (((false)
          or (ref_11.id is NULL))
        or (EXISTS (
          select 
              ref_13.dim_id as c0,
              ref_11.query as c1
            from
              sys.netcdf_vardim as ref_13,
              lateral (select 
                    ref_11.access as c0,
                    ref_13.var_id as c1,
                    ref_11.commit_action as c2,
                    ref_11.system as c3
                  from
                    sys.r2 as ref_14
                  where true) as subq_0
            where ref_13.var_id is not NULL)))
where true;

