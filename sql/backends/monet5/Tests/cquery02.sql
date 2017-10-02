-- Test strides on a stream table
create stream table cqinput02(aaa integer) set window 4 stride 3;

create table cqresult02(aaa integer);

create procedure cq_basic02()
begin
	insert into cqresult02 (select sum(aaa) from cqinput02);
end;

start continuous procedure sys.cq_basic02();

insert into cqinput02 values (1), (2), (3);

call cquery.wait(1000);

select aaa from cqresult02; --output no tuples

insert into cqinput02 values (4);

call cquery.wait(1000);

select aaa from cqresult02; --output 1 tuple with value 10

insert into cqinput02 values (5), (6);

call cquery.wait(1000);

select aaa from cqresult02; --output 1 tuple with value 10

insert into cqinput02 values (7);

call cquery.wait(1000);

select aaa from cqresult02; --output 2 tuples with value 10 and 22

insert into cqinput02 values (1000);

call cquery.wait(1000);

select aaa from cqresult02; --output 2 tuples with value 10 and 22

stop continuous procedure cq_basic02;

drop procedure cq_basic02;
drop table cqinput02;
drop table cqresult02;
