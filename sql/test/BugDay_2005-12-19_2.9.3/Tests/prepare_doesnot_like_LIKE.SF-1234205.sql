create table t1234205 (name varchar(1024));
insert into t1234205 values ('niels'),('fabian'),('martin');

prepare select name from t1234205 where name like ?;
exec ** ('%');
prepare select name from t1234205 where name like 'n%';
exec ** ();
