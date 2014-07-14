select * from schemas;
select * from tables;
select * from columns;
select schemas.name, tables.name, columns.name 
from schemas, tables, columns
where 
	schemas.id = tables.schema_id 
AND 	tables.id = columns.table_id;

select s.name, t.name
from (schemas as s FULL OUTER JOIN tables as t ON s.id = t.schema_id);

select s.name, t.name, c.name
from ((schemas as s FULL OUTER JOIN tables as t ON s.id = t.schema_id)
	FULL OUTER JOIN columns as c ON t.id = c.table_id );

commit;
