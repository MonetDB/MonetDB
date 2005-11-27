create table t11 (str varchar(60));
insert into t11 values ('bla');
insert into t11 values ('blabla bla');
insert into t11 values ('5');
insert into t11 values ('bla bla bla bla bla');
insert into t11 values ('bladibaldibla');

select cast(str as varchar(4)), length(cast(str as varchar(4))) from t11;
select cast(str as char(4)), length(cast(str as char(4))) from t11;
drop table t11;
