--test alias definitions for continuous queries
create table cqresult15(i integer);

create procedure cq_basic15(val int)
begin
	insert into cqresult15 values (val);
end;

start continuous cq_basic15(2) as one_alias;

start continuous cq_basic15(2) as one_aliasother;

start continuous cq_basic15(3) as one_alias; --error

stop continuous cq_basic15; --error

stop continuous one_alias;

stop continuous one_aliasother;

stop continuous one_aliasother; --error

drop procedure cq_basic15;
drop table cqresult15;
