START TRANSACTION;
CREATE TABLE "t1" ("c0" DECIMAL(18,3),"c1" BINARY LARGE OBJECT NOT NULL,"c2" DECIMAL(18,3),CONSTRAINT "t1_c1_unique" UNIQUE ("c1"));
PREPARE SELECT DISTINCT (SELECT DISTINCT r'|m<v' FROM t1 WHERE ((t1.c0)<(?)) GROUP BY t1.c2, ?), ?, t1.c2 FROM t1 WHERE CAST(? AS BOOLEAN) LIMIT 2103332269785059850;
	-- Could not determine type for argument number 2
ROLLBACK;

START TRANSACTION;
CREATE TABLE "t0" ("c0" BOOLEAN NOT NULL);
CREATE TABLE "t1" ("c0" DECIMAL(18,3));
CREATE TABLE "t2" ("c0" DECIMAL(18,3),"c2" DATE);
PREPARE (SELECT DISTINCT t0.c0, INTERVAL '1734780053' SECOND FROM t0, t1) UNION ALL (SELECT ?, ? FROM t2);
ROLLBACK;

START TRANSACTION;
CREATE TABLE "t0" ("c0" DATE,"c2" INTEGER);
CREATE TABLE "t1" ("c1" TIMESTAMP,"c2" INTEGER);
CREATE TABLE "t2" ("c0" DATE,"c1" TIMESTAMP,"c2" INTEGER);
PREPARE (SELECT ?, t1.c2 FROM t1, t0 WHERE (SELECT DISTINCT (t1.c2) BETWEEN ASYMMETRIC (?) AND (t1.c2) FROM t1 CROSS JOIN 
((SELECT DISTINCT 6.9089063E7, TRUE FROM t2 WHERE TRUE) EXCEPT (SELECT ALL 0.4, FALSE FROM t2, t1 INNER JOIN t0 ON FALSE)) AS sub0 WHERE FALSE)) INTERSECT DISTINCT (SELECT DISTINCT 0.2, ? FROM t0, t2 WHERE ?);
ROLLBACK;

PREPARE SELECT 1 WHERE greatest(true, ?);
	-- ? should be set to boolean

PREPARE SELECT (SELECT ? FROM (select 1) as v1(c0));
	-- cannot determine parameter type

PREPARE SELECT ?, CASE 'weHtU' WHEN (values (?)) THEN 'G' END;
	-- cannot determine parameter type

PREPARE SELECT DISTINCT ?, CAST(CASE least(?, r'weHtU') WHEN ? THEN ? WHEN ? THEN ? WHEN (VALUES (?)) THEN r'G' ELSE ? END AS DATE) WHERE (?) IS NOT NULL LIMIT 519007555986016405;
	-- cannot have a parameter for IS NOT NULL operator

PREPARE SELECT (1 + CAST(l0t0.c0 AS BIGINT)) * scale_up(?, 2) FROM (select 1) AS l0t0(c0);

PREPARE SELECT DISTINCT ((((CAST(l0t0.c0 AS INT))-(CAST(? AS BIGINT))))*(scale_up(?, ((-438139776)*(-813129345))))) FROM (select 1) AS l0t0(c0);

PREPARE SELECT round(-'b', ?);
PREPARE SELECT sql_max(+ (0.29353363282850464), round(- (sql_min('-Infinity', ?)), ?)) LIMIT 8535194086169274474;
PREPARE VALUES (CAST(? >> 1.2 AS INTERVAL SECOND)), (interval '1' second); -- error, types decimal(2,1) and sec_interval(13,0) are not equal

PREPARE (SELECT DISTINCT ((CAST(- (CASE r'' WHEN r'tU1' THEN 1739172851 WHEN ? THEN -1313600539 WHEN r'X(' THEN NULL WHEN r')''CD' THEN 95 END) AS BIGINT))&(least(- (-235253756), 64)))
WHERE ((rtrim(r'Z'))LIKE(r'rK'))) UNION ALL (SELECT ALL ? WHERE (scale_down(ifthenelse(TRUE, 18, ?), r'4')) IS NULL);

PREPARE VALUES (CASE WHEN true THEN 5 BETWEEN 4 AND 2 END);

START TRANSACTION;
create view v10(vc0) as (select l0v0.vc0 from (values (1, 2)) as l0v0(vc0, vc1));
prepare select case when true then false when ? then not exists (select ?, ?, 6) end from v10 as l0v10;
ROLLBACK;

prepare values (0.34, (values ((select ? from (select 1) as t1(x))))), (3, 2);

prepare with cte0(c0) as (select 1), cte1(c0,c1,c2) as (select distinct 1, 2, false)
select distinct least('y', (values (''), (''))), 1, (select ? from (select 1) as l1v0(x))
from cte0 as l0cte0, cte1 as l0cte1 order by l0cte0.c0 desc nulls last, l0cte1.c2 desc nulls first; -- ? can't be defined, so error

prepare with cte0(c0) as (select 2) select 1 <> all(select 2 from (values (1),(2)) as t1) from cte0 as l0cte0 group by ?; --error, cannot have a parameter for group by column

prepare with cte0(c0) as (select 2) select 1 <> all(select 2 from (values (1),(2)) as t1) from cte0 as l0cte0 order by ?; --error, cannot have a parameter for order by column

START TRANSACTION;
CREATE FUNCTION myintudf(a INT) RETURNS INT RETURN a + 1;
PREPARE SELECT myintudf(?);
EXEC **(1);
ROLLBACK;
