select schemas.name, tables.name, columns.name from 
	columns 
		inner join 
	tables 
		on (columns.table_id = tables.id) 
		inner join 
	schemas on (tables.schema_id = schemas.id)
 where tables."system" = true 
 order by schemas.name, tables.name, columns.name;


select s.name, t.name, c.name from 
	columns c
		inner join 
	tables t
		on (c.table_id = t.id) 
		inner join 
	schemas s on (t.schema_id = s.id)
 where t."system" = true
 order by s.name, t.name, c.name;
