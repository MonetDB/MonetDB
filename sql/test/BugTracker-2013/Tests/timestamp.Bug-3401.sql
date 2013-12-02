create table datebug (time timestamp, foo INT);
insert into datebug values ('2013-01-01 00:00:00', 1);
insert into datebug values ('2013-02-01 00:00:00', 2);
insert into datebug values ('2013-03-01 00:00:00', 3);
insert into datebug values ('2013-04-01 00:00:00', 4);
insert into datebug values ('2013-05-01 00:00:00', 5);
insert into datebug values ('2013-06-01 00:00:00', 6);
select * from datebug where time < '2013-02-28';
select * from datebug where time < '2013-02-29';
select * from datebug where time > '2013-01-01' and time < '2013-02-30';
select * from datebug where time > '2013-01-01' and time < 'foobar';
drop table datebug;

