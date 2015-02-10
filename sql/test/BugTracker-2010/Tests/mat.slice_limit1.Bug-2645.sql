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


TRACE select x,y from slice_test limit 1;

-- When mitosis was activated (i.e., the MAL plan contains mat.*() statements,
-- then there sould also be at least one mat.slice() (or mat.pack*()) statement.
WITH TheTrace AS ( SELECT * FROM tracelog() )
SELECT count(*) FROM
( SELECT count(*) AS mat       FROM TheTrace WHERE stmt LIKE '% := mat.%'       ) as m,
( SELECT count(*) AS mat_slice FROM TheTrace WHERE stmt LIKE '% := mat.slice(%' ) as ms,
( SELECT count(*) AS mat_pack  FROM TheTrace WHERE stmt LIKE '% := mat.pack%(%' ) as mp
WHERE ( mat = 0 AND mat_slice+mat_pack = 0 ) OR ( mat > 0 AND mat_slice+mat_pack > 0 );

TRACE select cast(x as string)||'-bla-'||cast(y as string) from slice_test limit 1;
-- When mitosis was activated (i.e., the MAL plan contains mat.*() statements,
-- then there sould also be at least one mat.slice() (or mat.pack()) statement.
WITH TheTrace AS ( SELECT * FROM tracelog() )
SELECT count(*) FROM
( SELECT count(*) AS mat       FROM TheTrace WHERE stmt LIKE '% := mat.%'       ) as m,
( SELECT count(*) AS mat_slice FROM TheTrace WHERE stmt LIKE '% := mat.slice(%' ) as ms,
( SELECT count(*) AS mat_pack  FROM TheTrace WHERE stmt LIKE '% := mat.pack%(%' ) as mp
WHERE ( mat = 0 AND mat_slice+mat_pack = 0 ) OR ( mat > 0 AND mat_slice+mat_pack > 0 );

drop function GetTrace;

drop table slice_test;

