--only the monetdb user can invoke the all statements
create table cqresult081(i integer);

create procedure cq_basic081()
begin
	insert into cqresult081 values (1);
end;

create table cqresult082(i integer);

create procedure cq_basic082()
begin
	insert into cqresult082 values (2);
end;

start continuous cq_basic081();

start continuous cq_basic082();

stop all continuous; --ok

stop all continuous; --nothing happens

drop procedure cq_basic081;
drop procedure cq_basic082;
drop table cqresult081;
drop table cqresult082;
