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

drop table tab0;
drop table tab1;
drop table tab2;
