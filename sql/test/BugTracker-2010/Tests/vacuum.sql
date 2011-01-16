-- simple sanity test for vacuum

create table vac1(i int, s string);
insert into vac1 values(1,'the'), (2,'quick'),(3,'brown'),(5,'runs'),(4,'fox'),(6,'over'),(7,'the'),(8,'lazy'),(9,'dog');
select * from vac1 order by i;
select * from vac1;

call shrink('sys','vac1');
select * from vac1;

delete from vac1 where i = 8;
call shrink('sys','vac1');
select * from vac1;

delete from vac1 where i = 2;
call shrink('sys','vac1');
select * from vac1;

delete from vac1 where i > 6;
call vacuum('sys','vac1');
select * from vac1;

drop table vac1;


