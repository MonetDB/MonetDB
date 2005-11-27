select 'test' like 'test';

create table sfbug_1076605 ( str1 varchar(10) );
insert into sfbug_1076605 values ( 'tset');
insert into sfbug_1076605 values ( 'tste');
insert into sfbug_1076605 values ( 'test');
insert into sfbug_1076605 values ( 'ttse');
insert into sfbug_1076605 values ( 'tets');

select * from sfbug_1076605 where str1 like 'test';

