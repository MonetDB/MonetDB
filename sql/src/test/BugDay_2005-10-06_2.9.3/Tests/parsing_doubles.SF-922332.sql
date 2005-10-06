create table test (
val double
);

insert into test values (1e+308);

select * from test;
