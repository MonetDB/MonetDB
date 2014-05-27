-- the SQL interface to R
create temporary table hannes( i integer);
insert into hannes values (1804289383), (846930886), (1681692777), (1714636915),
(1957747793), (424238335), (719885386), (1649760492), (596516649), (1189641421);

create schema rapi;
create function eval(expr string) returns double external name rapi.sql_eval;

select rapi.eval('return(1234.0);');

create function eval(expr string) returns table(ret1 double) external name rapi.sql_eval;

select * from rapi.eval('return(1234.0);');

create aggregate function eval(exp string, arg1 int) returns table(ret1 double) external name rapi.sql_eval;

select rapi.eval('ret1 <- Re(fft(arg1)); return(ret1)',i) from hannes;

drop table hannes;
