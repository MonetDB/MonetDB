select  
  ref_136.ship as c0, 
  36 as c1, 
  50 as c2
from 
  sys.columns as ref_133
      inner join sys._columns as ref_134
      on (ref_133.table_id = ref_134.id )
    inner join sys.querylog_calls as ref_136
      left join sys.environment as ref_138
      on (ref_136.arguments = ref_138.name )
    on (ref_133.number = ref_136.cpu )
where (ref_138.name is not NULL) 
  and (EXISTS (
		    select  
		        ref_139.cycle as c0
			      from 
			        sys.sequences as ref_139
				      where ref_139.cycle is NULL))
		limit 168;

