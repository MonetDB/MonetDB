set optimizer='sequential_pipe';

select * from tmp3RLE;

--select * from storage where "table" = 'tmp3RLE';
select count(*) from tmp3RLE;

alter table tmp3RLE alter column i set storage NULL;
alter table tmp3RLE alter column b set storage NULL;
alter table tmp3RLE alter column f set storage NULL;
alter table tmp3RLE alter column t set storage NULL;
explain select * from tmp3RLE;
select * from tmp3RLE;

alter table tmp3RLE set read write;
--select * from tmp3RLE;

drop table tmp3RLE;
