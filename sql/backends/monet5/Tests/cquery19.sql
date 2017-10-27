--test CQs with temporary stream tables
create temporary stream table testing19 (aa int, b clob) set window 1;
create table results19(aaa int);

create procedure cq_query19a()
begin
	insert into results19 (select count(aa) from testing19);
end;

create procedure cq_query19b()
begin
	insert into results19 (select sum(aa) from testing19);
end;

start continuous procedure cq_query19a() with cycles 1;

insert into testing19 values (1, '1');

call cquery.wait(1000);

select aaa from results19; --output 1

alter stream table testing19 set window 2;

insert into testing19 values (2, '2'), (3, '3'), (4, '4');

call cquery.wait(1000);

select aaa from results19; --output 1

start continuous procedure cq_query19b() with cycles 2;

call cquery.wait(1000);

select aaa from results19; --output 1, 5

insert into testing19 values (5, '5');

call cquery.wait(1000);

select aaa from results19; --output 1, 5, 9

drop procedure cq_query19a;
drop procedure cq_query19b;
drop table testing19;
drop table results19;
