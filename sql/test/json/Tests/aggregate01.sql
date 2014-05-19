create table aggrtest1 ( col1 double, col2 integer );

select json.tojsonarray(col1) from aggrtest1;

insert into aggrtest1 values (0.1234, 1);

select json.tojsonarray(col1) from aggrtest1;

insert into aggrtest1 values (5.6789, 1);

select json.tojsonarray(col1) from aggrtest1;

select json.tojsonarray(col1) from aggrtest1 group by col2;

insert into aggrtest1 values (0.516273849, 2);

select json.tojsonarray(col1) from aggrtest1;

select json.tojsonarray(col1) from aggrtest1 group by col2;

drop table aggrtest1;
