create table test_dbl (
val double
);

insert into test_dbl values (1e+308);

select * from test_dbl;
