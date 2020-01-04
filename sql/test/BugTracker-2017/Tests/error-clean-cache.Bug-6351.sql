prepare select 1;
declare a int;
set a = 2;
select a;
iamerror; --just an error
select a; --error, variable cache was cleaned
