--trigger other continuous queries inside a continuous query
create stream table testing11a (a int) set window 1 stride 1;
create stream table testing11b (a int) set window 1 stride 1;
create stream table testing11c (a int) set window 1 stride 1;

create table results11a (a int);
create table results11b (a int);

create procedure cq_query11a()
begin
	insert into results11a (select count(*) from testing11a);
end;

create procedure cq_query11b()
begin
	insert into results11b (select count(*) from testing11b);
end;

create procedure cq_query11c()
begin
	insert into testing11a (select count(*) from testing11c);
	insert into testing11b (select count(*) from testing11c);
end;

start continuous sys.cq_query11a() with cycles 1;
start continuous sys.cq_query11b();
start continuous sys.cq_query11c() with cycles 2;

insert into testing11c values (1);
insert into testing11c values (2);
insert into testing11c values (3);

call cquery.wait(1000);

stop continuous sys.cq_query11b();

select count(*) from results11a; --should be 1
select count(*) from results11b; --should be 2

drop procedure sys.cq_query11a;
drop procedure sys.cq_query11b;
drop procedure sys.cq_query11c;

drop table testing11a;
drop table testing11b;
drop table testing11c;
drop table results11a;
drop table results11b;
