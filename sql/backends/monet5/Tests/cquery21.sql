create stream table cqinput21(input integer) set window 2 stride 1;

create table cqresult21(output real);

create procedure cq_basic21()
begin
	insert into cqresult21 (select avg(input) from cqinput21);
end;

start continuous procedure cq_basic21() with cycles 2 as myname;

insert into cqinput21 values (10);

call cquery.wait(1000);

select input from cqinput21; --should not tumble

select output from cqresult21; --nothing

call cquery.wait(1000);

insert into cqinput21 values (20);

call cquery.wait(1000);

select input from cqinput21;

select output from cqresult21; --15

call cquery.wait(1000);

insert into cqinput21 values (30);

call cquery.wait(1000);

select input from cqinput21;

select output from cqresult21; --25

call cquery.wait(1000);

insert into cqinput21 values (40);

select input from cqinput21;

select output from cqresult21; --15, 25

call cquery.wait(1000);

drop procedure cq_basic21;
drop table cqinput21;
drop table cqresult21;
