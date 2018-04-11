start transaction;

create table testme (a int, b clob);
insert into testme values (1, 'another'), (1, 'testing'), (1, 'todo');
select a, group_concat(b) from testme group by a;

insert into testme values (2, 'lets'), (2, 'get'), (2, 'harder');
select a, group_concat(b) from testme group by a;

insert into testme values (3, 'even'), (2, 'more'), (1, 'serious');
select a, group_concat(b) from testme group by a;

insert into testme values (3, ''), (3, 'more'), (3, ''), (3, 'stress'), (4, NULL);
select a, group_concat(b) from testme group by a;

insert into testme values (3, NULL), (4, NULL);
select a, group_concat(b) from testme group by a;

insert into testme values (5, ''), (4, 'nothing'), (5, ''), (3, '');
select a, group_concat(b) from testme group by a;

rollback;
