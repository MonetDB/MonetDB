prepare select 1;
declare a int;
set a = 2;
select a;
iamerror; --just an error
select a; --a is still there
