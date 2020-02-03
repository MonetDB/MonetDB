CREATE SCHEMA "kagami_dump";
CREATE TABLE "kagami_dump"."test_task" ("sys_id" CHAR(32) DEFAULT '', "number" VARCHAR(40), "parent" VARCHAR(32));
INSERT INTO "kagami_dump".test_task(sys_id, number, parent) VALUES ('aaa', 'T0001', null),('bbb','T0002','aaa');

SELECT parent."sys_id" FROM "kagami_dump"."test_task" parent INNER JOIN "kagami_dump"."test_task" child ON child."parent" = parent."sys_id" GROUP BY parent."sys_id" HAVING count(child."sys_id") >= 1 ORDER BY parent."number"; --error, parent."number" requires an aggregate function

DROP SCHEMA "kagami_dump" CASCADE;


CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES(97,1,99), (15,81,47), (87,21,10);

SELECT CAST(+ col1 * - col1 AS BIGINT) AS col2 FROM tab0 GROUP BY col2, col0, col1 HAVING + - col0 / - AVG ( ALL + col2 ) - - - AVG ( DISTINCT + col0 ) + col0 IS NULL;
SELECT DISTINCT + 40 / + + col0 AS col2 FROM tab0 GROUP BY col0, col0, col2 HAVING NOT ( NOT + - 80 BETWEEN NULL AND + - 73 ) OR NOT ( + col0 >= - COUNT ( * ) + - COUNT ( DISTINCT - col0 ) );
SELECT ALL * FROM tab0 AS cor0 WHERE col2 NOT IN ( 22, 18, CAST ( NULL AS INTEGER ) + - 77 );

prepare select col0 from tab0 where (?) in (select col0 from tab0);
prepare select col0 from tab0 where (?,?) in (select col0,col1 from tab0);
prepare select col0 from tab0 where (col1,col1) in (select col0,? from tab0);
prepare select col0 from tab0 where (col1,col1) in (select ?,? from tab0);
prepare select col0 from tab0 where (col0) in (?);
prepare select col0 from tab0 where (col0) in (?,?);

prepare select ? < ANY (select max(col0) from tab0) from tab0 t1;
prepare select col0 = ALL (select ? from tab0) from tab0 t1;

prepare select col0 from tab0 where (?) in (?); --error

CREATE TABLE tab1(col0 INTEGER, col1 STRING);
prepare select 1 from tab1 where (col0,col1) in (select ?,? from tab1);

drop table tab0;
drop table tab1;
