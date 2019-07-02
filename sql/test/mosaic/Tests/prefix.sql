set optimizer='mosaic_pipe';

create table tmpPrefix( i integer);
insert into tmpPrefix values (1), (1), (1), (1), (1), (1), (2), (2);

select * from tmpPrefix;

alter table tmpPrefix alter column i set storage 'prefix';

select * from tmpPrefix;
select count(*) from tmpPrefix;

alter table tmpPrefix alter column i set storage NULL;

select * from tmpPrefix;
select cast(sum(i) as bigint) from tmpPrefix;

drop table tmpPrefix;
