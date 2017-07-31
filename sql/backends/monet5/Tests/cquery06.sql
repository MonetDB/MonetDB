--test proper continuous query handling via the SQL parser
create table cqresult06(i integer);

create procedure cq_basic06()
begin
	insert into cqresult06 (select count(*) from cqresult06);
end;

pause continuous cq_basic06(); --error

start continuous cq_basic06() with heartbeat 1000; --1 second

call cquery.wait(2100);

pause continuous cq_basic06(); --it's ok

stop continuous cq_basic06();

drop procedure cq_basic06;
drop table cqresult06;
