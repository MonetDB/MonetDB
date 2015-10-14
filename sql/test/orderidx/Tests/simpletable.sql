create table xtmp1( i integer);
insert into xtmp1 values (1),(2),(4),(0);
select * from xtmp1;

select * from storage where "table"= 'xtmp1';
alter table xtmp1 set READ ONLY;
create ordered index sys_xtmp1_i_oidx on xtmp1 (i);

select * from storage where "table"= 'xtmp1';

select * from xtmp1 where i <0;
select * from xtmp1 where i <1;
select * from xtmp1 where i <2;
select * from xtmp1 where i <5;
select * from xtmp1 where i <8;

select * from xtmp1 where i>=0 and i <8;
select * from xtmp1 where i>=2 and i <=2;

drop table xtmp1;
