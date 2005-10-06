create table t (str varchar(60));
insert into t values ('bla');
insert into t values ('blabla bla');
insert into t values ('5');
insert into t values ('bla bla bla bla bla');
insert into t values ('bladibaldibla');

select cast(str as varchar(4)), length(cast(str as varchar(4))) from t;
select cast(str as char(4)), length(cast(str as char(4))) from t;
