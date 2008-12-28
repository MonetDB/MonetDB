-- The squid example is meant to analyse the building blocks
-- needed to get an octopus application in the air quickly.
-- The squid is built around a database comprising 2 tables
-- each with two attributes only.

create table squidA(
	Bid int, Cval int);
create table squidD(
	Eid int, Fval int);

-- let's fill the tables
create procedure fill(lim int) 
begin
	declare i int;
	set i = 0;
	while i < lim do
		insert into squidA values(i,rand());
		insert into squidD values(i,rand());
		set i = i+1;
	end while;
end;

declare size int;
set size=100;
call fill(size);

-- over this database we want to run the following
-- queries

select Bid from squidA;
select * from squidA;
select count(*) from squidA;
select count(*) from squidA where Cval <10000;
select count(*) from squidA where Bid < size/2;
select count(*) from squidA where Bid mod 2 = 0;

select Bid,Eid from squidA, squidD
where squidA.Bid = squidD.Eid;
select Bid,Eid from squidA, squidD
where squidA.Bid = squidD.Eid
and squidA.Cval <1000 and squidD.Fval<1000;
