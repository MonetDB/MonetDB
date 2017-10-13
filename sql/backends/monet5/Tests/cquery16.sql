--cannot start a CQ from a procedure that does not exist
create stream table testing16 (a int) set window 1;
create table results16 (a int);

start continuous sys.cq_query16a(); --error

create procedure cq_query16b() --error
begin
	start continuous sys.cq_query16a();
end;

create procedure cq_query16a()
begin
	insert into results16 (select * from testing16);
end;

insert into testing16 values (1);
start continuous sys.cq_query16a() with cycles 1;

call cquery.wait(1000);

drop procedure sys.cq_query16a;

drop table testing16;
drop table results16;
