--stop a continuous query within another continuous query
create table testing13 (a int);

create procedure cq_query13a()
begin
	insert into testing13 values (1);
end;

create procedure cq_query13b()
begin
	stop continuous sys.cq_query13a();
end;

start continuous sys.cq_query13a() with heartbeat 3000;
start continuous sys.cq_query13b() with cycles 1;

call cquery.wait(1000);

stop continuous sys.cq_query13b();

drop procedure sys.cq_query13a;
drop procedure sys.cq_query13b;

drop table testing13;
