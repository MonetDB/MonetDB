create table slice_test (x int, y int, val int);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 0, 1, 12985);
insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

insert into slice_test values ( 1, 1, 28323);
insert into slice_test values ( 3, 5, 89439);

set trace = 'none'; -- non-documented feature to not get any trace output
create function GetTrace()
	returns table (
		event integer,		-- event counter
		clk varchar(20), 	-- wallclock, no mtime in kernel
		pc varchar(50), 	-- module.function[nr]
		thread int, 		-- thread identifier
		"user" int, 		-- user identifier
		ticks bigint, 		-- time in microseconds
		reads bigint, 		-- number of blocks read
		writes bigint, 	-- number of blocks written
		rbytes bigint,		-- amount of bytes touched
		wbytes bigint,		-- amount of bytes written
		type string,		-- return types
		stmt string			-- actual statement executed
	)
	external name sql.dump_trace;

TRACE select x,y from slice_test limit 1;
-- When mitosis was activated (i.e., the MAL plan contains mat.*() statements,
-- then there sould also be at least one mat.slice() (or mat.pack()) statement.
WITH TheTrace AS ( SELECT * FROM GetTrace() )
SELECT count(*) FROM
( SELECT count(*) AS mat       FROM TheTrace WHERE stmt LIKE '% := mat.%'       ) as m,
( SELECT count(*) AS mat_slice FROM TheTrace WHERE stmt LIKE '% := mat.slice(%' ) as ms,
( SELECT count(*) AS mat_pack  FROM TheTrace WHERE stmt LIKE '% := mat.pack(%'  ) as mp
WHERE ( mat = 0 AND mat_slice+mat_pack = 0 ) OR ( mat > 0 AND mat_slice+mat_pack > 0 );

TRACE select cast(x as string)||'-bla-'||cast(y as string) from slice_test limit 1;
-- When mitosis was activated (i.e., the MAL plan contains mat.*() statements,
-- then there sould also be at least one mat.slice() (or mat.pack()) statement.
WITH TheTrace AS ( SELECT * FROM GetTrace() )
SELECT count(*) FROM
( SELECT count(*) AS mat       FROM TheTrace WHERE stmt LIKE '% := mat.%'       ) as m,
( SELECT count(*) AS mat_slice FROM TheTrace WHERE stmt LIKE '% := mat.slice(%' ) as ms,
( SELECT count(*) AS mat_pack  FROM TheTrace WHERE stmt LIKE '% := mat.pack(%'  ) as mp
WHERE ( mat = 0 AND mat_slice+mat_pack = 0 ) OR ( mat > 0 AND mat_slice+mat_pack > 0 );

drop function GetTrace;

drop table slice_test;

