create schema xyz;
create procedure xyz.p() begin declare x integer; set x = 1; end;
call xyz.p();
call p();

drop procedure xyz.p;
drop schema xyz;
