-- Test strides on a stream table
create stream table cqinput04(aaa integer) set window 2 stride all;

create table cqresult04(aaa integer);

create procedure cq_basic04()
begin
	insert into cqresult04 (select count(*) from cqinput04);
end;

start continuous procedure sys.cq_basic04();

insert into cqinput04 values (1), (2);

insert into cqinput04 values (3), (4);

call cquery.wait(1000);

select aaa from cqresult04; --output 2 tuples with value 2

insert into cqinput04 values (5);

call cquery.wait(1000);

select aaa from cqresult04; --output 2 tuples with value 2

insert into cqinput04 values (6);

call cquery.wait(1000);

select aaa from cqresult04; --output 3 tuples with value 2

stop continuous procedure cq_basic04;

drop procedure cq_basic04;
drop table cqinput04;
drop table cqresult04;
