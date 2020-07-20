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
select a, group_concat(b, E'XyZ\n') from testmore group by a;

select a, group_concat(b, NULL) from testmore group by a;
select group_concat(b, NULL) from testmore;
select group_concat(a, NULL) from testmore;
select group_concat(a, a) from testmore;
select group_concat(a, 8) from testmore;
select group_concat(a, b) from testmore;
select group_concat(b, a) from testmore;

select group_concat('😀', '😁') as "😃" from (values (1),(2),(3), (NULL)) v;
select group_concat('😀', '😁') over () as "😃" from (values (1),(2),(3), (NULL)) v;

select group_concat(null) || 'a';
select group_concat(null) || 'a' from testmore;
select group_concat(null) over () || 'a';
select group_concat(null) over () || 'a' from testmore;

select group_concat('') || 'a' where false;
select group_concat('') over () || 'a' where false;

select group_concat('');
select group_concat('') from testmore;
select group_concat('') over () from testmore;
select group_concat('', '') over () from testmore;

/* listagg is the SQL standard name of group_concat */
select listagg(a) from testmore;
select listagg(b) from testmore;

select listagg(a, a) from testmore;
select listagg(b, b) from testmore;
select listagg(a, b) from testmore;
select listagg(b, a) from testmore;

rollback;
