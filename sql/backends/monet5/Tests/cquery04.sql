-- Test strides on a stream table
create stream table cqinput06(aaa integer) set window 2 stride all;

create table cqresult06(aaa integer);

create procedure cq_basic06()
begin
	insert into cqresult06 (select count(*) from cqinput06);
end;

start continuous procedure sys.cq_basic06();

insert into cqinput06 values (1), (2);

insert into cqinput06 values (3), (4);

call cquery.wait(1000);

select aaa from cqresult06; --output 2 tuples with value 2

insert into cqinput06 values (5);

call cquery.wait(1000);

select aaa from cqresult06; --output 2 tuples with value 2

insert into cqinput06 values (6);

call cquery.wait(1000);

select aaa from cqresult06; --output 3 tuples with value 2

stop continuous procedure sys.cq_basic06();

drop procedure cq_basic06;
drop table cqinput06;
drop table cqresult06;
