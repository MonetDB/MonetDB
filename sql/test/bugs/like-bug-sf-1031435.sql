start transaction;
create table like_test (str varchar(10));
insert into like_test values('');
insert into like_test values('t');
insert into like_test values('ts');
insert into like_test values('tsz');
select * from like_test;
select * from like_test where str like 't_';
rollback;
