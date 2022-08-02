create table test_oidx (c1 int, c2 string);
insert into test_oidx (c1, c2) values (1, 'ccc'), (2, 'bbb'), (3, 'eee'), (4, 'aaa'), (5, 'ddd');
select * from test_oidx order by c2, c1;
create ordered index test_oidx1 on test_oidx (c2);
select * from test_oidx order by c2, c1;
drop index test_oidx1;
drop table test_oidx;

create table test_oidx (c1 int, c2 char(5));
insert into test_oidx (c1, c2) values (1, 'ccc'), (2, 'bbb'), (3, 'eee'), (4, 'aaa'), (5, 'ddd');
select * from test_oidx order by c2, c1;
create ordered index test_oidx1 on test_oidx (c2);
select * from test_oidx order by c2, c1;
drop index test_oidx1;
drop table test_oidx;

create table test_oidx (c1 int, c2 varchar(6));
insert into test_oidx (c1, c2) values (1, 'ccc'), (2, 'bbb'), (3, 'eee'), (4, 'aaa'), (5, 'ddd');
select * from test_oidx order by c2, c1;
create ordered index test_oidx1 on test_oidx (c2);
select * from test_oidx order by c2, c1;
drop index test_oidx1;
drop table test_oidx;

create table test_oidx (c1 int, c2 clob);
insert into test_oidx (c1, c2) values (1, 'ccc'), (2, 'bbb'), (3, 'eee'), (4, 'aaa'), (5, 'ddd');
select * from test_oidx order by c2, c1;
create ordered index test_oidx1 on test_oidx (c2);
select * from test_oidx order by c2, c1;
drop index test_oidx1;
drop table test_oidx;


-- tests for all other datatypes, including blob, url, json, uuid, inet and hugeint are done in sql/test/orderidx/Tests/ oidx_all_types.sql and oidx_hge_type.sql.

