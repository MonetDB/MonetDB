drop table tmp3;
create table tmp3( i integer, b boolean, f real);
insert into tmp3 values
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(0, false, 0.316),
(5, false, 0.317),
(6, false, 0.317),
(7, false, 0.317),
(8, false, 0.317),
(9, false, 0.317),
(10, false, 0.317),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(1, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(2, false, 0.314),
(3, true, 0.314),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(4, true, 0.316),
(0, false, 0.316),
(5, false, 0.317),
(6, false, 0.317),
(7, false, 0.317),
(8, false, 0.317),
(9, false, 0.317),
(10, false, 0.317);

alter table tmp3 set read only;

explain select * from tmp3;
select * from tmp3;
--select * from storage where "table" = 'tmp3';

--call sys.compress('sys','tmp3');
alter table tmp3 alter column i set storage 'rle';
alter table tmp3 alter column b set storage 'rle';
alter table tmp3 alter column f set storage 'rle';
explain select * from tmp3;
select * from tmp3;
--select * from storage where "table" = 'tmp3';

--call sys.decompress('sys','tmp3');
alter table tmp3 alter column i set storage NULL;
alter table tmp3 alter column b set storage NULL;
alter table tmp3 alter column f set storage NULL;
explain select * from tmp3;
select * from tmp3;

alter table tmp3 set read write;
select * from tmp3;

--drop table tmp3;
