
select  
  ref_116.id as c0, 
    35 as c1, 
      ref_164.sorting_order as c2, 
        ref_1.name as c3, 
	  ref_165.table_id as c4, 
	    ref_152.function_id as c5, 
	      77 as c6, 
	        ref_1.value as c7, 
		  ref_165.type as c8
		  from 
		    sys.environment as ref_1
		        inner join sys.columns as ref_116
			      inner join sys.systemfunctions as ref_152
			              inner join bam.files as ref_164
				                  right join sys.keys as ref_165
						              on (ref_164.sorting_order = ref_165.name )
							                left join sys.statistics as ref_168
									          on (ref_165.table_id = ref_168.column_id )
										          on (ref_152.function_id = ref_165.id )
											        on (ref_116.storage = ref_165.name )
												    on (ref_1.value = ref_164.file_location )
												    where (cast(coalesce(83,
												          ref_168.width) as int) is not NULL) 
												      and (ref_165.action is not NULL)
												      limit 62;
