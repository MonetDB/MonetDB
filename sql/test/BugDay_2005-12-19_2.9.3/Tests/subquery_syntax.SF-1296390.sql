create table test1296390 (id int);
insert into test1296390 values (1);
insert into test1296390 (select max(id) + 1 from test1296390);
insert into test1296390 ((select max(id) + 1 from test1296390));
