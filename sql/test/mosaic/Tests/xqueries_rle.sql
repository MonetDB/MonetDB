set optimizer='mosaic_pipe';

create table xtmp3( i integer, b boolean, f real,t timestamp);
insert into xtmp3 values
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(0, false, 0.316,'2014-08-23 11:34:54.000000'),
(5, false, 0.317,'2014-08-23 11:34:54.000000'),
(6, false, 0.317,'2014-08-23 11:34:54.000000'),
(7, false, 0.317,'2014-08-23 11:34:54.000000'),
(8, false, 0.317,'2014-08-23 11:34:54.000000'),
(9, false, 0.317,'2014-08-23 11:34:54.000000'),
(10, false, 0.317,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(1, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(2, false, 0.314,'2014-08-23 11:34:54.000000'),
(3, true, 0.314,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(4, true, 0.316,'2014-08-23 11:34:54.000000'),
(0, false, 0.316,'2014-08-23 11:34:54.000000'),
(5, false, 0.317,'2014-08-23 11:34:54.000000'),
(6, false, 0.317,'2014-08-23 11:34:54.000000'),
(7, false, 0.317,'2014-08-23 11:34:54.000000'),
(8, false, 0.317,'2014-08-23 11:34:54.000000'),
(9, false, 0.317,'2014-08-23 11:34:54.000000'),
(10, false, 0.317,'2014-08-23 11:34:54.000000');

alter table xtmp3 set read only;

select cast(sum(i) as bigint) from xtmp3;
select sum(f) from xtmp3;
explain select count(*) from xtmp3 where i <4;
select count(*) from xtmp3 where i <4;
explain select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where f <0.316;
select count(*) from xtmp3 where b = true;

alter table xtmp3 alter column i set storage 'runlength';
alter table xtmp3 alter column b set storage 'runlength';
alter table xtmp3 alter column f set storage 'runlength';

select compressed from storage where "table"='xtmp3';

select cast(sum(i) as bigint) from xtmp3;
select sum(f) from xtmp3;

explain select count(*) from xtmp3 where i <4;
select count(*) from xtmp3 where i <4;
explain select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where f <0.316;
select count(*) from xtmp3 where b = true;

--select * from storage where "table" = 'xtmp3';
alter table xtmp3 alter column i set storage NULL;
alter table xtmp3 alter column b set storage NULL;
alter table xtmp3 alter column f set storage NULL;

select compressed from storage where "table"='xtmp3';

select cast(sum(i) as bigint) from xtmp3;
select sum(f) from xtmp3;

explain select count(*) from xtmp3 where i <4;
select count(*) from xtmp3 where i <4;
explain select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where i = 7;
select count(*) from xtmp3 where f <0.316;
select count(*) from xtmp3 where b = true;

alter table xtmp3 set read write;
--select * from xtmp3;

drop table xtmp3;
