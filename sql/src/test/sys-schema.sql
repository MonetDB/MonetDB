select * from sys.schemas;
select * from sys.schemas;
select * from sys.tables;
select * from sys.columns;
select s.name, t.name, c.name 
from sys.schemas as s, sys.tables as t, sys.columns as c
where 
	s.id = t.schema_id 
AND 	t.id = c.table_id;

select s.name, t.name
from (sys.schemas as s FULL OUTER JOIN sys.tables as t ON s.id = t.schema_id);

select s.name, t.name, c.name
from ((sys.schemas as s FULL OUTER JOIN sys.tables as t ON s.id = t.schema_id)
	FULL OUTER JOIN sys.columns as c ON t.id = c.table_id );
