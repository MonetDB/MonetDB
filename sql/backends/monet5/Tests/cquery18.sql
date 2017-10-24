-- test errors in CQ, it should pause automatically if that happens
-- trigger an zero division
create table testing18(aaa real);

create procedure cq_query18(ppp integer)
begin
	insert into testing18 values (1 / ppp);
end;

start continuous procedure cq_query18(1) with heartbeat 1000 cycles 1;

call cquery.wait(2000);

select aaa from testing18;

start continuous procedure cq_query18(0) with heartbeat 1000 cycles 2;

call cquery.wait(3000);

select aaa from testing18;

start continuous procedure cq_query18(1) with heartbeat 1000 cycles 2; --error

pause continuous cq_query18; --error

select alias, errors from cquery.status();

resume continuous cq_query18; --ok

stop continuous cq_query18; --ok

start continuous procedure cq_query18(2) with heartbeat 1000 cycles 2; --ok

call cquery.wait(3000);

select aaa from testing18;

select alias, "errors" from cquery.status();

drop procedure cq_query18;
drop table testing18;
