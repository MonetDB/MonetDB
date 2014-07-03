create table aggrtest00 ( col1 varchar(10), col2 integer );

select json.tojsonarray(col1) from aggrtest00;

insert into aggrtest00 values ('hallo', 1);

select json.tojsonarray(col1) from aggrtest00;

insert into aggrtest00 values ('world', 1);

select json.tojsonarray(col1) from aggrtest00;

select json.tojsonarray(col1) from aggrtest00 group by col2;

insert into aggrtest00 values ('foobar', 2);

select json.tojsonarray(col1) from aggrtest00;

select json.tojsonarray(col1) from aggrtest00 group by col2;

delete from aggrtest00;

insert into aggrtest00 values (NULL, 1);

select json.tojsonarray(col1) from aggrtest00;

insert into aggrtest00 values ('hello', 1);

select json.tojsonarray(col1) from aggrtest00;

insert into aggrtest00 values ('world', 2);

select json.tojsonarray(col1) from aggrtest00;

select json.tojsonarray(col1) from aggrtest00 group by col2;

drop table aggrtest00;
