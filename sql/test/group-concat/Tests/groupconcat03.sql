start transaction;

create table testme (b char(8));

insert into testme values ('');
select group_concat(b) from testme;

insert into testme values ('one'), ('two'), ('three');
select group_concat(b) from testme;

insert into testme values ('');
select group_concat(b) from testme;

create table othertest (a int, b clob);
insert into othertest values (1, 'test'), (1, ''), (1, 'me');
select a, group_concat(b) from othertest group by a;

insert into othertest values (2, 'other'), (2, 'test'), (2, '');
select a, group_concat(b) from othertest group by a;

insert into othertest values (3, ''), (2, 'i want to see the commas'), (3, ''), (4, '');
select a, group_concat(b) from othertest group by a;

select a, group_concat(b) as compacted from othertest group by a having count(*) > 2;

rollback;
