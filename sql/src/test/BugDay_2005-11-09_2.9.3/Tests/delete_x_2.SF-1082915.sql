create table test(a integer, b int);
insert into test(a) values(NULL);
insert into test(b) values(NULL);
commit;
delete from test;
delete from test;
