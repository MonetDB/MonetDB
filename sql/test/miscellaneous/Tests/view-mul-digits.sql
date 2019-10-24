START TRANSACTION;

create temp table mytable(b bigint);
insert into mytable values (1), (2), (3);

create view sys.myview as select cast(2 * "b" as bigint) as mycol from mytable;

select t.name, c.name, c.type, c.type_digits, c.type_scale
from sys.tables t join sys.columns c on c.table_id = t.id where t.name = 'myview';

ROLLBACK;
