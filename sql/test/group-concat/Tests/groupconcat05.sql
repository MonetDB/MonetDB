start transaction;

create table testmore (a int, b clob);

insert into testmore values (1, 'another'), (1, 'testing'), (1, 'todo');
select a, group_concat(b, '!') from testmore group by a;
select group_concat(b, '!') from testmore;

insert into testmore values (2, 'lets'), (3, 'get'), (2, 'harder');
select a, group_concat(b, '---') from testmore group by a;
select group_concat(b, '---') from testmore;

insert into testmore values (3, 'even'), (2, 'more'), (1, '');
select a, group_concat(b, '') from testmore group by a;
select group_concat(b, '') from testmore;

select a, group_concat(b, '-') from testmore group by a;
select a, group_concat(b) from testmore group by a;

insert into testmore values (3, 'even'), (2, NULL), (1, '');
select a, group_concat(b, 'XyZ\n') from testmore group by a;

select a, group_concat(b, NULL) from testmore group by a;
select group_concat(b, NULL) from testmore;

rollback;
