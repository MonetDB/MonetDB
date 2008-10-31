-- The query history mechanism of MonetDB/SQL relies on a few hooks 
-- inside the kernel. The most important one is the SQL global 
-- variable 'history', which is used by all sessions.

set history=true;

-- Whenever a query is compiled and added to the cache, it is also entered
-- into the 'queryDefinition' table using a hardwired call to 'keepQuery'.

create table queryDefinitions(
	id int primary key,
	defined timestamp,
	name string,
	query string,
	parse int,
	optimize int
);

-- Each query call is stored in the table queryCall using 'keepCall'.
-- At regular intervals the query history table should be cleaned.
create table queryCalls(
	id int references sys.queryDefinitions(id),
	called timestamp,
	arguments string,
	elapsed int,
	inblock int,
	oublock int
);

create view queryLog as
select * from queryDefinitions qd, queryCalls ql
where qd.id= ql.id;

-- the signature is used in the kernel, don't change it
create procedure keepQuery(
	id int,
	query string,
	parse int,
	optimize int) 
begin
	insert into queryDefinitions
	values(id, now(),user, query, parse, optimize);
end;

-- the signature is used in the kernel, don't change it
create procedure keepCall(
	id int,
	called timestamp,
	arguments string,
	elaps int,
	inblock int,
	oublock int) 
begin
	insert into queryCalls
	values(id, called, arguments, elaps, inblock,oublock);
end;

-- the remainder are samples to test in isolation.
call keepQuery(1,'select 1;',100,20);
call keepCall(1,now(),'user.s0_0(1)', 912,0,0);
call keepCall(1,now(),'user.s0_0(2)', 899,0,0);
select * from queryLog;
