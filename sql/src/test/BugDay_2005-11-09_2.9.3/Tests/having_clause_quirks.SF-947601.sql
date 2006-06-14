create table t947601 (name varchar(1024));
insert into t947601 values ('niels'),('fabian'),('martin');

select name from t947601 having 1=1;
select name from t947601 group by name having 1=1;
