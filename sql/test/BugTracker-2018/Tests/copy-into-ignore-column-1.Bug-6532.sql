start transaction;
create table t (id int auto_increment, a clob);
copy 1 records into t (a) from stdin (id, a) using delimiters ',','\n','"' null as '';
100,
select count(a) from t;
rollback;
