-- test robustness against small tables
create table xtmp2( i integer);
select schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp2';
alter table xtmp2 set read only;

--create ordered index sys_xtmp2_i_oidx on xtmp2(i);
call createorderindex('sys','xtmp2','i');
select schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp2';
select * from xtmp2 where i>=0 and i <8;

create table xtmp3( i integer);
insert into xtmp3 values(3);
select schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp3';

alter table xtmp3 set read only;
--create ordered index sys_xtmp3_i_oidx on xtmp3(i);
call createorderindex('sys','xtmp3','i');
select schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp3';
select * from xtmp3 where i>=0 and i <8;

call droporderindex('sys','xtmp2','i');
call droporderindex('sys','xtmp3','i');
--drop index sys_xtmp2_i_oidx;
--drop index sys_xtmp3_i_oidx;

drop table xtmp2;
drop table xtmp3;

