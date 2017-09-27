--start a continuous query from the body of another one
create stream table testing14 (a int) set window 1 stride 1;
create table results14 (a int);

create procedure cq_query14a()
begin
	insert into results14 (select * from testing14);
end;

create procedure cq_query14b()
begin
	insert into testing14 values (1), (2), (3);
	start continuous sys.cq_query14a() with cycles 2;
end;

start continuous sys.cq_query14b() with cycles 1;

call cquery.wait(2500);

select count(*) from results14; --should be 2

drop procedure sys.cq_query14a;
drop procedure sys.cq_query14b;

drop table testing14;
drop table results14;
