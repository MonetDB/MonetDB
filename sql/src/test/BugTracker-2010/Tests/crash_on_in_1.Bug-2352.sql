create table test8 (test integer);
select test from test8 where test in (test + 1);
drop table test8;
