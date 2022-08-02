create table foo (a int);
select b from(select 1,1=1,1,1,1 from foo union all select 1,null,1,1,1)as t(a,b,c,d,e);
drop table foo;
