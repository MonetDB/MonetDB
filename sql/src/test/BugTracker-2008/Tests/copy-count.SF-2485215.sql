start transaction;
create table t(i int);
insert into t values(1123);
copy select * from t into stdout using delimiters ',';
copy select count(*) from t into stdout using delimiters ',';
abort;
