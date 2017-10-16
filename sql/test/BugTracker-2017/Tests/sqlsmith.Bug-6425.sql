select  
  sample_2.keyword as c0
from 
  sys.optimizers as sample_0
      right join sys.netcdf_vardim as sample_1
      on (((true) 
            or (false)) 
          or (true))
    right join sys.keywords as sample_2
      right join sys.privilege_codes as sample_3
      on (sample_2.keyword = sample_3.privilege_code_name )
    on (sample_1.var_id = sample_3.privilege_code_id )
where false
limit 106;
