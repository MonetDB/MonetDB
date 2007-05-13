create table t( i decimal(10,4) default null);
insert into t values('1.12345');
insert into t values('1.1234');
insert into t values('1.123');
insert into t values('1.12');
insert into t values('1.1');
insert into t values('1.');
select * from t;
drop table t;
