create table t978045 (name varchar(1024));
insert into t978045 values ('niels'),('fabian'),('martin');

select name from t978045 union select name from t978045;
select name from t978045 union all select name from t978045;

drop table t978045;
