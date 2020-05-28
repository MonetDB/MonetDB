CREATE SCHEMA "kagami_dump";
CREATE TABLE "kagami_dump"."test_task" ("sys_id" CHAR(32) DEFAULT '', "number" VARCHAR(40), "parent" VARCHAR(32));
INSERT INTO "kagami_dump".test_task(sys_id, number, parent) VALUES ('aaa', 'T0001', null),('bbb','T0002','aaa');

SELECT parent."sys_id" FROM "kagami_dump"."test_task" parent INNER JOIN "kagami_dump"."test_task" child ON child."parent" = parent."sys_id" GROUP BY parent."sys_id" HAVING count(child."sys_id") >= 1 ORDER BY parent."number"; --error, parent."number" requires an aggregate function

DROP SCHEMA "kagami_dump" CASCADE;

CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES(97,1,99), (15,81,47), (87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES(64,77,40), (75,67,58), (46,51,23);

SELECT CAST(+ col1 * - col1 AS BIGINT) AS col2 FROM tab0 GROUP BY col2, col0, col1 HAVING + - col0 / - AVG ( ALL + col2 ) - - - AVG ( DISTINCT + col0 ) + col0 IS NULL;
SELECT DISTINCT + 40 / + + col0 AS col2 FROM tab0 GROUP BY col0, col0, col2 HAVING NOT ( NOT + - 80 BETWEEN NULL AND + - 73 ) OR NOT ( + col0 >= - COUNT ( * ) + - COUNT ( DISTINCT - col0 ) );
SELECT ALL * FROM tab0 AS cor0 WHERE col2 NOT IN ( 22, 18, CAST ( NULL AS INTEGER ) + - 77 );
SELECT CAST(58 + + 78 + - COALESCE ( ( + CASE 68 WHEN - 77 - - 38 THEN NULL ELSE COUNT ( * ) END ), + 81 + - COUNT ( * ) + + CAST ( NULL AS INTEGER ), + + 34 * - 30 * + COUNT ( * ) ) * 15 * 38 AS BIGINT) AS col1;
	--434

SELECT * FROM tab0 AS cor0 WHERE NOT - 39 <> 11; --empty
SELECT DISTINCT * FROM tab0 WHERE NOT - - 12 <> + + 96; --empty
SELECT * FROM tab0 AS cor0 WHERE - 52 = + 32; --empty
SELECT ALL * FROM tab0 WHERE 68 = - + 83; --empty

SELECT * FROM tab2 AS cor0 WHERE NOT - ( - + 57 ) + - ( ( - - col2 ) ) BETWEEN + - col2 AND + col2;
	-- 46 51 23

SELECT col0 FROM tab2 WHERE - - col2;
	-- 64
	-- 75
	-- 46

SELECT 1 FROM tab0 WHERE NOT (NOT NULL IN (1));
	--empty

SELECT NOT (NOT NULL IN (1)) FROM tab0;
	-- NULL
	-- NULL
	-- NULL

SELECT - col0 + + CAST ( NULL AS INTEGER ) AS col2 FROM tab0 AS cor0 WHERE NOT ( NOT + - CAST ( NULL AS INTEGER ) NOT IN ( col0 / CAST ( col2 AS INTEGER ) - + col1 ) );
	--empty

SELECT NOT ( NOT + - CAST ( NULL AS INTEGER ) NOT IN ( col0 / CAST ( col2 AS INTEGER ) - + col1 ) ) FROM tab0 AS cor0;
	-- NULL
	-- NULL
	-- NULL

PLAN SELECT DISTINCT col0, col1, col2, col0 FROM tab0;

SELECT DISTINCT col0, col1, col2, col0 FROM tab0;
	-- 97  1 99 97
	-- 15 81 47 15
	-- 87 21 10 87

PLAN SELECT col0 FROM tab2 WHERE CAST(col2 AS BIGINT) = 40;

SELECT col0 FROM tab2 WHERE CAST(col2 AS BIGINT) = 40;
	-- 64

SELECT 11 FROM tab1 AS cor0 LEFT JOIN tab0 ON 80 = 70;
	-- 11
	-- 11
	-- 11

SELECT col0 FROM tab0 ORDER BY tab0.col0;
	-- 15
	-- 87
	-- 97

SELECT DISTINCT 99 col2 FROM tab1 WHERE NOT - ( 43 ) + + 98 = + col2;
	-- 99

SELECT * FROM tab2 AS cor0 WHERE NOT - 59 + + 47 <> + ( + col0 );
	-- empty

SELECT CAST(+ col2 * col2 AS BIGINT) FROM tab2 AS cor0 WHERE NOT - CAST ( NULL AS INTEGER ) <> - - col1 AND NOT NULL NOT BETWEEN ( NULL ) AND - 91 - - + 27 * + col2;
	-- empty

SELECT + 2 FROM tab0 AS cor0 WHERE NOT - 29 IS NULL OR NOT NULL IS NULL AND NOT NULL BETWEEN + col1 - + 60 AND + 37 * + col1 + + col0;
	-- 2
	-- 2
	-- 2

SELECT * FROM tab0 WHERE NOT - col0 - col1 * col2 <= ( + col0 ) AND NOT ( + col2 + col1 - col1 ) NOT BETWEEN - col0 AND - col1 + - col2 / col1;
	-- empty

SELECT * FROM tab0 AS cor0 WHERE NOT col1 BETWEEN - col0 AND col0 + col1 * col1 AND - col1 BETWEEN col0 AND ( NULL ) OR NOT col0 * col0 + col0 <= NULL;
	-- empty

SELECT DISTINCT * FROM tab2 WHERE NOT ( - + 50 + ( 70 ) ) = + col2;
	-- 64 77 40
	-- 46 51 23
	-- 75 67 58

SELECT col0 FROM tab0 ORDER BY sys.tab0.col0; --error, TODO

prepare select col0 from tab0 where (?) in (select col0 from tab0);
prepare select col0 from tab0 where (?,?) in (select col0,col1 from tab0);
prepare select col0 from tab0 where (col1,col1) in (select col0,? from tab0);
prepare select col0 from tab0 where (col1,col1) in (select ?,? from tab0);
prepare select col0 from tab0 where (col0) in (?);
prepare select col0 from tab0 where (col0) in (?,?);

prepare select ? < ANY (select max(col0) from tab0) from tab0 t1;
prepare select col0 = ALL (select ? from tab0) from tab0 t1;

prepare select 1 from tab0 where 1 between ? and ?;
prepare select 1 from tab0 where ? between 1 and ?;
prepare select 1 from tab0 where ? between ? and 1;

prepare select EXISTS (SELECT ? FROM tab0) from tab0;
prepare select EXISTS (SELECT ?,? FROM tab0) from tab0;

prepare select col0 from tab0 where (?) in (?); --error
prepare select ? = ALL (select ? from tab0) from tab0 t1; --error
prepare select 1 from tab0 where ? between ? and ?; --error

prepare select case when col0 = 0 then ? else 1 end from tab0;
prepare select case when col0 = 0 then 1 else ? end from tab0;
prepare select case when col0 = 0 then ? else ? end from tab0; --error

prepare select case when col0 = 0 then ? when col0 = 1 then ? else 1 end from tab0;
prepare select case when col0 = 0 then ? when col0 = 1 then ? else ? end from tab0; --error

prepare select ? is null from tab0; --error
prepare select max(?); --error
prepare select max(?) over (); --error

drop table tab1;
CREATE TABLE tab1(col0 INTEGER, col1 STRING);
prepare select 1 from tab1 where (col0,col1) in (select ?,? from tab1);

SELECT 1 FROM tab0 where CASE WHEN 64 IN ( col0 ) THEN true END;
	--empty

SELECT 1 FROM tab0 GROUP BY col0 HAVING CASE WHEN 64 IN ( col0 ) THEN TRUE END;
	--empty

SELECT + col2 + + col0 AS col0 FROM tab0 AS cor0 GROUP BY col1, col2, col0 HAVING NULL IN ( + ( - - ( CASE WHEN 64 IN ( col0 * - col2 + + col1 ) THEN - 98 END ) ) * - 13 );
	--empty

drop table tab0;
drop table tab1;
drop table tab2;

SELECT DISTINCT CAST(+ 77 - - - CASE - CAST ( NULL AS INTEGER ) WHEN - 11 THEN NULL WHEN - 34 THEN 81 ELSE - 15 + 20 * - ( + CAST ( + ( 96 ) AS INTEGER ) ) END AS BIGINT) AS col0, 35 AS col0;
	-- 2012 35

SELECT CAST(- CASE - ( + 0 ) WHEN 18 + - 60 THEN NULL WHEN - CASE - 67 WHEN + - 79 * + COUNT ( * ) / 30 - + 32 * + 69 THEN MAX ( DISTINCT + 34 ) / + 0 ELSE NULL END * + ( + 45 ) + + 19 * 17 THEN 
20 WHEN + SUM ( ALL + 78 ) + + 69 THEN 29 * 74 ELSE + NULLIF ( 82, + 72 + 26 ) * 7 END * - 92 + MIN ( 88 + 57 ) AS BIGINT) AS col0;
	-- 52953

SELECT ALL CAST(19 * - CASE + - COUNT ( * ) WHEN 40 THEN NULL WHEN - - CAST ( NULL AS INTEGER ) * + - 1 THEN + 39 ELSE 27 + - MIN ( DISTINCT 13 ) END + 36 + 70 - - 69 * + COUNT ( * ) * 20 + + 83 AS BIGINT) AS col0;
	-- 1303

SELECT CAST(- 4 * + COUNT ( * ) + 22 + 69 AS BIGINT) AS col2, CAST(- ( + CASE + 85 WHEN - 77 / - CAST ( + CASE - + 51 WHEN 79 THEN + 95 + 13 * ( 60 * 77 ) END AS INTEGER ) + + 82 - COUNT ( * ) * COUNT ( * ) THEN 
NULL WHEN SUM ( ALL 99 ) THEN COUNT ( * ) ELSE COUNT ( * ) * - ( COUNT ( * ) ) END ) * 46 - 83 AS BIGINT);
	--  87 -37

SELECT DISTINCT CAST(- CAST ( CASE 81 WHEN 48 - - 3 / - - 47 - - CAST ( NULL AS INTEGER ) THEN NULL WHEN COUNT ( * ) THEN + - 91 + + 43 ELSE + - 0 END AS INTEGER ) AS BIGINT) AS col0, CAST(- 27 * + 43 AS BIGINT);
	-- 0 -1161

SELECT DISTINCT CAST(66 * - + CAST ( - - COUNT ( * ) AS INTEGER ) * 54 * + CASE 68 WHEN - 56 * + CAST ( NULL AS INTEGER ) + + + 47 THEN 
- 83 ELSE + 59 + - COUNT ( * ) - 26 * ( 59 ) * 11 - - + COUNT ( * ) * - 73 END + + COUNT ( * ) / + ( 86 + 19 * 90 ) AS BIGINT);
	-- 60192396

SELECT DISTINCT CAST(+ - CASE + 66 WHEN - CAST ( NULL AS INTEGER ) THEN NULL ELSE COUNT ( * ) + - CASE + COUNT ( * ) WHEN - 69 - + - 28 THEN - + MIN ( + 92 ) WHEN - COUNT ( 32 ) THEN 
+ ( + 23 ) * + 17 * - 37 WHEN 63 THEN NULL ELSE + 67 * - 34 - 64 END - 31 END * - 94 AS BIGINT) AS col2;
	-- 217328

SELECT DISTINCT CAST(( + CASE WHEN NOT NULL BETWEEN NULL AND NULL THEN NULL ELSE + COUNT ( * ) END ) AS BIGINT) AS col1;
	-- 1
