start transaction;
create table t(i int);
insert into t values(1123);
copy select * from t into stdout using delimiters ',', '\n';
copy select count(*) from t into stdout using delimiters ',', '\n';
rollback;
