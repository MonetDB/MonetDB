select 'test' like 'test';
rollback;

create table sfbug_1076605 ( str1 varchar(10) );
commit;
insert into sfbug_1076605 values ( 'tset');
insert into sfbug_1076605 values ( 'tste');
insert into sfbug_1076605 values ( 'test');
insert into sfbug_1076605 values ( 'ttse');
insert into sfbug_1076605 values ( 'tets');
commit;

select * from sfbug_1076605 where str1 like 'test';

