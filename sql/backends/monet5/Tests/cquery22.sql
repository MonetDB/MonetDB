create stream table cqinput22(input integer) set window 2;
create function calculateaverage() returns real begin return select AVG(input) from cqinput22; end;
create function calculateaverage(dev int) returns real begin return select AVG(dev + input) from cqinput22; end;

start continuous function calculateaverage() with cycles 3 as calcavg;
start continuous function calculateaverage() with cycles 2 as calcavg; --error
start continuous function calculateaverage() with cycles 1 as calc;

start continuous function calculateaverage(12) with cycles 4 as calcavg; --error
start continuous function calculateaverage(10) as calc; --error
start continuous function calculateaverage(9) with cycles 1 as othercalc;
start continuous function calculateaverage() as othercalc; --error

call cquery.wait(1000);

stop all continuous;

drop function calculateaverage();
drop function calculateaverage(int);
drop table cqinput22;
