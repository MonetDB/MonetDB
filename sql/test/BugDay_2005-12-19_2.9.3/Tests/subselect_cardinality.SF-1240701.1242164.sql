create table tab (i integer);
insert into tab values (1),(2),(3);

select * from tab where i = (select 0 from tab where i < 1) order by i;
select * from tab where i = (select 0 from tab where i < 2) order by i;
select * from tab where i = (select 0 from tab where i < 3) order by i;
select * from tab where i in (select 0 from tab where i < 1) order by i;
select * from tab where i in (select 0 from tab where i < 2) order by i;
select * from tab where i in (select 0 from tab where i < 3) order by i;
select * from tab where i = (select i from tab where i < 1) order by i;
select * from tab where i = (select i from tab where i < 2) order by i;
select * from tab where i = (select i from tab where i < 3) order by i;
select * from tab where i in (select i from tab where i < 1) order by i;
select * from tab where i in (select i from tab where i < 2) order by i;
select * from tab where i in (select i from tab where i < 3) order by i;

drop table tab;
