create table test (c1 int);
insert into test values(1);
SELECT * FROM test WHERE (true OR c1 = 3) AND c1 = 2; 
SELECT * FROM test WHERE (true OR c1 = 3) AND c1 = 1; 
SELECT * FROM test WHERE (false OR c1 = 3) AND c1 = 2; 
SELECT * FROM test WHERE (false OR c1 = 3) AND c1 = 1; 
drop table test;

