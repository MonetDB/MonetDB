-- create a continuous query and find it

create stream table cqtbl(i integer);

-- the hello example
create continuous procedure cqfoo(v integer)
begin
	insert into cqtbl values(v);
end;
select * from functions where name = 'cqfoo';

-- continuous procedures can always be called directly
-- this simplifies debugging as well
-- Main limitation 
call cqfoo(123);
select * from cqtbl;

start continuous cqfoo(321);
select * from cqtbl;

stop continuous cqfoo(321);
--stop continuous cqfoo;
select * from cqtbl;

drop procedure cqfoo;
drop table cqtbl;

