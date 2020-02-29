set optimizer='mosaic_pipe';

select * from tmp3rle;

--select * from storage where "table" = 'tmp3rle';
select count(*) from tmp3rle;

alter table tmp3rle alter column i set storage NULL;
alter table tmp3rle alter column b set storage NULL;
alter table tmp3rle alter column f set storage NULL;
alter table tmp3rle alter column t set storage NULL;

select compressed from storage where "table"='tmp3rle’;

explain select * from tmp3rle;
select * from tmp3rle;

alter table tmp3rle set read write;
--select * from tmp3rle;

drop table tmp3rle;
