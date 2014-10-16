start transaction;

create table float8_tbl(f1 double);
insert into float8_tbl(f1) values ('   -34.84');
select sign(f1) as sign_f1 from float8_tbl f;
insert into float8_tbl(f1) values ('1.2345678901234e+200');
select sign(f1) as sign_f1 from float8_tbl f;

rollback;
