START TRANSACTION;
create table foo3633 (a int, b int);
insert into foo3633 values (1,1);
select a from foo3633 group by a having sum(b)>count(*)*.5;
select a,sum(b)>count(*)*.5 from foo3633 group by a;
