-- test robustness against small tables
create table xtmp2( i integer);
select * from storage where "table"= 'xtmp2';
alter table xtmp2 set read only;
create ordered index sys_xtmp2_i_oidx on xtmp2(i);
select * from storage where "table"= 'xtmp2';
select * from xtmp2 where i>=0 and i <8;

create table xtmp3( i integer);
insert into xtmp3 values(3);
select * from storage where "table"= 'xtmp3';
alter table xtmp3 set read only;
create ordered index sys_xtmp3_i_oidx on xtmp3(i);
select * from storage where "table"= 'xtmp3';
select * from xtmp3 where i>=0 and i <8;

drop table xtmp2;
drop table xtmp3;

