-- The query history mechanism of MonetDB/SQL relies on a few hooks 
-- inside the kernel. The most important one is the SQL global 
-- variable 'history', which is used by all sessions.
-- it is set automatically at the end of this script.

-- Whenever a query is compiled and added to the cache, it is also entered
-- into the 'queryHistory' table using a hardwired call to 'keepQuery'.

create table queryHistory(
	id int primary key,
	defined timestamp,
	name string,
	query string,
	parse bigint,
	optimize bigint
);

-- Each query call is stored in the table callHistory using 'keepCall'.
-- At regular intervals the query history table should be cleaned.
-- This can be done manually on the SQL console, or be integrated
-- in the keepQuery and keepCall.
create table callHistory(
	id int references queryHistory(id),
	called timestamp,
	arguments string,
	elapsed bigint,
	inblock bigint,
	oublock bigint
);

create view queryLog as
select * from queryHistory qd, callHistory ql
where qd.id= ql.id;

-- the signature is used in the kernel, don't change it
create procedure keepQuery(
	i int,
	query string,
	parse bigint,
	optimize bigint) 
begin
	declare cnt int;
	set cnt = (select count(*) from queryHistory where id= i);
	if cnt = 0
	then
		insert into queryHistory
		values(i, now(),user, query, parse, optimize);
	end if;
end;

-- the signature is used in the kernel, don't change it
create procedure keepCall(
	id int,
	called timestamp,
	arguments string,
	elapsed bigint,
	inblock bigint,
	oublock bigint) 
begin
	insert into callHistory
	values(id, called, arguments, elapsed, inblock,oublock);
end;

-- The remainder are initialization calls to make sure
-- the functions are compiled into MAL and stored away.
-- If they are not ran, then the upcalls will fail.
call keepQuery(0,'--init history',0,0);
call keepCall(0,now(),'--init history',0,0,0);

set history=true;
