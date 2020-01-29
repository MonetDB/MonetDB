--stop a continuous query within another continuous query
create table testing13 (a int);

create procedure cq_query13a()
begin
	insert into testing13 values (1);
end;

create procedure cq_query13b()
begin
	stop continuous cq_query13a;
end;

start continuous sys.cq_query13a() with heartbeat 3000;
start continuous sys.cq_query13b() with heartbeat 1000 cycles 1;

call sleep(2500);

drop procedure sys.cq_query13a;
drop procedure sys.cq_query13b;

drop table testing13;
