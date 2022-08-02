call sys.storagemodelinit();
create table estimated_storage
as (
	  select "table" as tblname,
	        max("count") as count
		  from sys.storage()
		  where "schema" = 'sys'
		  group by "table")
	with data;

