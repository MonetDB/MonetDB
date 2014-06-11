create table aggrtest01 ( col1 double, col2 integer );

select json.tojsonarray(col1) from aggrtest01;

insert into aggrtest01 values (0.1234, 1);

select json.tojsonarray(col1) from aggrtest01;

insert into aggrtest01 values (5.6789, 1);

select json.tojsonarray(col1) from aggrtest01;

select json.tojsonarray(col1) from aggrtest01 group by col2;

insert into aggrtest01 values (0.516273849, 2);

select json.tojsonarray(col1) from aggrtest01;

select json.tojsonarray(col1) from aggrtest01 group by col2;

delete from aggrtest01;

insert into aggrtest01 values (NULL, 1);

select json.tojsonarray(col1) from aggrtest01;

insert into aggrtest01 values (NULL, 1);

--select * from aggrtest01;

select json.tojsonarray(col1) from aggrtest01;

select json.tojsonarray(col1) from aggrtest01 group by col2;

insert into aggrtest01 values (0.1234, 1);

select json.tojsonarray(col1) from aggrtest01;

insert into aggrtest01 values (0.516273849, 2);

select * from aggrtest01;

select json.tojsonarray(col1) from aggrtest01;

select json.tojsonarray(col1) from aggrtest01 group by col2;

delete from aggrtest01 where col1 is null;

--select * from aggrtest01;

select json.tojsonarray(col1) from aggrtest01;

select json.tojsonarray(col1) from aggrtest01 group by col2;

drop table aggrtest01;
