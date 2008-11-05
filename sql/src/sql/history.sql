-- The query history mechanism of MonetDB/SQL relies on a few hooks 
-- inside the kernel. The most important one is the SQL global 
-- variable 'history', which is used by all sessions.
-- it is set automatically at the end of this script.

-- Whenever a query is compiled and added to the cache, it is also entered
-- into the 'queryHistory' table using a hardwired call to 'keepQuery'.

create table queryHistory(
	id wrd primary key,
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
	id wrd references queryHistory(id),
	called timestamp,
	arguments string,
	elapsed bigint,		-- total time from sending the query and get the complete results back
	exectime bigint,	-- execution time from the start until results export
	restime  bigint,	-- execution time until from the start until the query plan's end
	inblock bigint,
	oublock bigint,
	numthreads int,		-- number of threads used
	rscload int,		-- resources load
	tmpfootprint bigint, 	-- footprint for tmp bats of the plan
	glbfootprnnt bigint 	-- footprint for bats in the plan
);

create view queryLog as
select * from queryHistory qd, callHistory ql
where qd.id= ql.id;

-- the signature is used in the kernel, don't change it
create procedure keepQuery(
	i wrd,
	query string,
	parse bigint,
	optimize bigint) 
begin
	insert into queryHistory
	values(i, now(), user, query, parse, optimize);
end;

-- the signature is used in the kernel, don't change it
create procedure keepCall(
	id wrd,
	called timestamp,
	arguments string,
	elapsed bigint,
	exectime bigint,
	restime bigint,
	inblock bigint,
	oublock bigint,
	numthreads int,
	rscload int,
	tmpfootprint bigint,
	glbfootprint bigint)
begin
	insert into callHistory
	values(id, called, arguments, elapsed, exectime, restime, inblock, oublock, numthreads, rscload, tmpfootprint, glbfootprint);
end;

set history=true;
