create table aggrtest ( col1 varchar(10), col2 integer );

select json.tojsonarray(col1) from aggrtest;

insert into aggrtest values ('hallo', 1);

select json.tojsonarray(col1) from aggrtest;

insert into aggrtest values ('world', 1);

select json.tojsonarray(col1) from aggrtest;

select json.tojsonarray(col1) from aggrtest group by col2;

insert into aggrtest values ('foobar', 2);

select json.tojsonarray(col1) from aggrtest;

select json.tojsonarray(col1) from aggrtest group by col2;
