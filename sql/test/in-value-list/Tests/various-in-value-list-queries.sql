create table foo (i int);
insert into foo values (null), (10), (null), (20), (10), (30), (30), (30), (50), (40), (50);

select * from foo where i in (10, 20, 20, 30, 30) order by i;

select * from foo where i not in (10, 20, 20, 30, 30) order by i;

select * from foo where i = 40 or i in (10, 20, 20, 30, 30) order by i;

select * from foo where i = 40 or i not in (10, 20, 20, 30, 30) order by i;

select * from foo where i = 40 and i in (10, 20, 20, 30, 30) order by i;

select * from foo where i = 40 and i not in (10, 20, 20, 30, 30) order by i;

select * from foo where i in (10, 20, 20, 30, 30) and i in (20, 30, 40) order by i;

select * from foo where i not in (10, 20, 20, 30, 30) and i not in (20, 30, 40) order by i;

select * from foo where i in (10, 20, 20, 30, 30) and i not in (20, 30, 40) order by i;

select * from foo where i not in (10, 20, 20, 30, 30) and i in (20, 30, 40) order by i;

select * from foo where i in (10, 20, 20, 30, 30) or i in (20, 30, 40) order by i;

select * from foo where i not in (10, 20, 20, 30, 30) or i not in (20, 30, 40) order by i;

select * from foo where i in (10, 20, 20, 30, 30) or i not in (20, 30, 40) order by i;

select * from foo where i not in (10, 20, 20, 30, 30) or i in (20, 30, 40) order by i;

select * from foo where i = 40 and i not in (10, 20 + 20, 30) order by i;

select * from foo where i = 40 and i in (2*10 + 10, null, i - 1, i, i + 1, null) order by i;

select * from foo where i = 40 and i not in (2*10 + 10, null, i - 1, i, i + 1, null) order by i;

select * from foo where i = 40 and (i + 10) in (2*10 + 10, null, i - 1, i, i + 10, null) order by i;

select * from foo where i = 40 and (i + 10) not in (2*10 + 10, null, i - 1, i, i + 10, null) order by i;

select * from foo where i = 40 or i not in (10, 20 + 20, 30) order by i;

select * from foo where i = 40 or i in (2*10 + 10, null, i - 1, i, i + 1, null) order by i;

select * from foo where i = 40 or i not in (2*10 + 10, null, i - 1, i, i + 1, null) order by i;

select * from foo where i = 40 or (i + 10) in (2*10 + 10, null, i - 1, i, i + 10, null) order by i;

select * from foo where i = 40 or (i + 10) not in (2*10 + 10, null, i - 1, i, i + 10, null) order by i;

select * from foo where i in (10, null, 20, null) order by i;

select * from foo where i not in (10, null, 20, null) order by i;

select * from foo where i = 40 and i not in (null) order by i;

select * from foo where i = 40 and i in (null) order by i;

select * from foo where i = 40 and i in (select bar.i from foo as bar where i in (10, 20)) order by i;

select * from foo where i = 40 or i in (select bar.i from foo as bar where i in (10, 20)) order by i;

select * from foo where i = 40 and i not in (select bar.i from foo as bar where i in (10, 20)) order by i;

select * from foo where i = 40 or i not in (select bar.i from foo as bar where i in (10, 20)) order by i;
