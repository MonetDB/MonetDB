create table foo (z int);
select aaa from (select 0,0,0,0,0,foo.z from foo)as t(aaa,b,c,d,e,f);
drop table foo;
