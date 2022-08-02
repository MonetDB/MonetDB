create table count_crash (i int);
insert into count_crash values(2);
insert into count_crash values(7);
insert into count_crash values(4);
select count(1000) from count_crash;
drop table count_crash;
