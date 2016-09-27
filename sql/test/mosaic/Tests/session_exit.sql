set optimizer='sequential_pipe';

explain select * from tmp3;
select * from tmp3;

alter table tmp3 alter column i set storage 'raw';
alter table tmp3 alter column b set storage 'raw';
alter table tmp3 alter column f set storage 'raw';
explain select * from tmp3;
select * from tmp3;
select sum(i) from tmp3;
select sum(f) from tmp3;

--select * from storage where "table" = 'tmp3';
select count(*) from tmp3;

alter table tmp3 alter column i set storage NULL;
alter table tmp3 alter column b set storage NULL;
alter table tmp3 alter column f set storage NULL;
explain select * from tmp3;
select * from tmp3;
select sum(i) from tmp3;
select sum(f) from tmp3;

alter table tmp3 set read write;
select * from tmp3;

drop table tmp3;
