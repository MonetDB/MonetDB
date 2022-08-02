create table tmp1(i int);
insert into tmp1 values(1);
create table tmp2(i int);
insert into tmp2 values(2);

alter table tmp1 add table tmp2;

select * from tmp1;

drop table tmp1;
drop table tmp2;
