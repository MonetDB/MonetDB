create table btest(links integer, rechts integer);
insert into btest values (1, 4);
select * from btest where 3 between links and rechts;

insert into btest values (127,128);

select * from btest where 3 between links and rechts;

drop table btest;
