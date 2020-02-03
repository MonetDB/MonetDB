start transaction;
create table t6815 (type_digits int);
select ifthenelse((type_digits > 0), '('||type_digits||')', '') as opt_len from t6815;
rollback;
