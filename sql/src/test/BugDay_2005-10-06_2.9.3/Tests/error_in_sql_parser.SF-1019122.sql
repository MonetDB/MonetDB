create table test (m interval month);
commit;
select * from test;
drop table test;
commit;
