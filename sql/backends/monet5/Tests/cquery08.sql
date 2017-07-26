--only the monetdb user can invoke the all statements
create table cqresult081(i integer);

create procedure cq_basic081()
begin
	insert into cqresult081 VALUES (1);
end;

create table cqresult082(i integer);

create procedure cq_basic082()
begin
	insert into cqresult082 VALUES (2);
end;

start continuous cq_basic081();

start continuous cq_basic082();

create user dummyme with password 'weak' name 'Just an user' schema sys;

set session authorization dummyme;

stop all continuous; --error

reset session authorization;

stop all continuous; --ok

drop user dummyme;
drop procedure cq_basic081;
drop procedure cq_basic082;
drop table cqresult081;
drop table cqresult082;
