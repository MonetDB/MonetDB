--test the server behavior of reproducing the same calls consecutively
create table cqresult07(i integer);

create procedure cq_basic07()
begin
	insert into cqresult07 (select count(*) from cqresult07);
end;

stop continuous cq_basic07(); --error

start continuous cq_basic07() WITH HEARTBEAT 1000 CYCLES 300; --1 second

call cquery.wait(2100);

pause continuous cq_basic07(); --ok

pause continuous cq_basic07(); --error

resume continuous cq_basic07() WITH HEARTBEAT 2000 CYCLES 300; --ok

resume continuous cq_basic07() WITH HEARTBEAT 2000 CYCLES 300; --error

stop continuous cq_basic07(); --ok

stop continuous cq_basic07(); --error

pause continuous cq_basic07(); --error

resume continuous cq_basic07() WITH HEARTBEAT 2000 CYCLES 300; --error

drop procedure cq_basic07;
drop table cqresult07;
