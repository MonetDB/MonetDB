create table foo (a int, b int);
prepare select count(b) from (select a,b from foo union all select null,null from foo) t;
drop table foo;
