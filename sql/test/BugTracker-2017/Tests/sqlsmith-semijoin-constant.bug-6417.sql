select  
  ref_0.sessionid as c0
from 
  sys.sessions as ref_0
    right join sys.netcdf_dims as sample_5
    on ((true) 
        or (ref_0.sessionid is NULL))
where ref_0.querytimeout is NULL
limit 106;
