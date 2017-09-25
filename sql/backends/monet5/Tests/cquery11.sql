--start and stop the same continuous query continuously
create stream table testing12 (a int) set window 1 stride 1;

create table results12 (a int);

create procedure cq_query12()
begin
	insert into results12 (select count(*) from testing12);
end;

start continuous sys.cq_query12() with cycles 1;
stop continuous sys.cq_query12();

start continuous sys.cq_query12() with cycles 12;
stop continuous sys.cq_query12();

start continuous sys.cq_query12();
stop continuous sys.cq_query12();

select count(*) from results12;

drop procedure sys.cq_query12;

drop table testing12;
drop table results12;
