create table type_test ( dval double, nval numeric(5,3));
insert into type_test values (5.4, 5.4),(1.3,1.3),(8.252, 8.252);
select * from type_test;
select count(dval), count(nval) from type_test;
select sum(dval), sum(nval) from type_test;
drop table type_test;
