select * from 
	columns 
		inner join 
	tables 
		on (columns.table_id = tables.id) 
		inner join 
	schemas on (tables.schema_id = schemas.id);

select * from 
	columns c
		inner join 
	tables t
		on (c.table_id = t.id) 
		inner join 
	schemas s on (t.schema_id = s.id);
