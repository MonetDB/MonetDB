create table test ( t1 dec(5,2), t2 numeric(7,3));
insert into test values (100.01, 100.021);
insert into test values (90.91, 980.21);
insert into test values (100.901, 10.021);
insert into test values (9990.91, 99980.021);
select * from test;
