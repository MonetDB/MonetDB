select auxiliary 
from sys.tablestoragemodel 
where ((select r_regionkey from sys.region) is NULL)
  or ((select index_type_name from sys.index_types) is not NULL);

select
          sample_69.f_table_catalog as c2
from    
  sys.storagemodel as sample_65
	left join sys.geometry_columns as sample_69
	on (sample_65.type = sample_69.f_table_catalog )
        where 
        ((select default_schema from sys.users) is NULL)
          or (cast(coalesce((select side_effect from sys.functions) ,
              sample_65.revsorted) as boolean) is not NULL);
