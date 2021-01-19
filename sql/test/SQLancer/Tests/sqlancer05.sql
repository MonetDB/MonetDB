CREATE TABLE "sys"."t1" (
	"c0" INTERVAL SECOND NOT NULL,
	CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"),
	CONSTRAINT "t1_c0_unique" UNIQUE ("c0")
);
INSERT INTO t1(c0) VALUES(INTERVAL '1474617942' SECOND), (NULL);
INSERT INTO t1(c0) VALUES(INTERVAL '1427502482' SECOND), (INTERVAL '-1598854979' SECOND); 
DELETE FROM t1 WHERE CASE r'FALSE' WHEN r'879628043' THEN TRUE ELSE ((((t1.c0)>=(t1.c0)))OR(((TRUE)OR(TRUE)))) END;
INSERT INTO t1(c0) VALUES(INTERVAL '236620278' SECOND);
INSERT INTO t1(c0) VALUES(INTERVAL '1448897349' SECOND);
DELETE FROM t1 WHERE CAST(TRUE AS BOOLEAN);
	-- all rows deleted
DROP TABLE sys.t1;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" TIMESTAMP,"c1" INTERVAL MONTH,CONSTRAINT "t0_c1_unique" UNIQUE ("c1"));
CREATE TABLE "sys"."t1" ("c0" BOOLEAN, "c1" DECIMAL(18,3) NOT NULL);
COPY 9 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	0.526
true	0.259
true	0.382
NULL	0.994
NULL	0.101
NULL	0.713
NULL	0.433
NULL	0.899
NULL	0.239

CREATE TABLE "sys"."t2" ("c0" BOOLEAN, "c1" DECIMAL(18,3));
COPY 3 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	0.692
NULL	0.860
NULL	0.230

SELECT max(t2.c1) FROM t1 LEFT OUTER JOIN t2 ON CASE WHEN t2.c0 < t1.c0 OR t1.c0 THEN t1.c0 WHEN t1.c0 THEN t1.c0 END;
	-- 0.860
create view v0(c0) as (select all 7.948668E7 from t1) with check option;
SELECT max(t2.c1) FROM t0, t1 CROSS JOIN v0 LEFT OUTER JOIN t2 
ON CASE WHEN ((((((((v0.c0)=(v0.c0)))OR(((t1.c0)>=(t1.c0)))))AND(((t2.c0)<(t1.c0)))))OR(((((t1.c0)AND(t1.c0)))AND(t1.c0)))) 
THEN t1.c0 WHEN COALESCE(COALESCE(TRUE, t2.c0, t1.c0), CASE t1.c1 WHEN t2.c1 THEN t2.c0 WHEN t1.c1 THEN t2.c0 ELSE t1.c0 END) 
THEN t2.c0 WHEN t1.c0 THEN t1.c0 END;
rollback;

START TRANSACTION;
CREATE TABLE t0("c1" INTEGER NOT NULL,CONSTRAINT "t0_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "t0_c1_unique" UNIQUE ("c1"));
COPY 8 RECORDS INTO t0 FROM stdin USING DELIMITERS E'\t',E'\n','"';
262015489
-1667887296
1410307212
1073218199
-167204307
1394786866
1112194034
2140251980

SELECT 1 FROM (select time '12:43:09' from t0) as v0(c0) RIGHT OUTER JOIN (SELECT INTERVAL '2' SECOND FROM t0) AS sub0 ON TIME '07:04:19' BETWEEN CASE 'b' WHEN 'a' THEN v0.c0 ELSE v0.c0 END AND v0.c0;
	--8 rows of 1
create view v0(c0, c1) as (select all time '12:43:09', interval '1251003346' second from t0) with check option;
SELECT count(ALL - (CAST(NULL AS INT))) FROM v0 RIGHT OUTER JOIN (SELECT INTERVAL '1380374779' SECOND FROM t0) AS sub0 ON 
COALESCE(TRUE, (TIME '07:04:19') BETWEEN SYMMETRIC (CASE r'漈' WHEN r'T㊆ßwU.H' THEN v0.c0 ELSE v0.c0 END) AND (v0.c0));
rollback;

START TRANSACTION;
CREATE TABLE "sys"."t2" (
	"c0" BOOLEAN,
	"c1" DOUBLE NOT NULL,
	CONSTRAINT "t2_c1_pkey" PRIMARY KEY ("c1"),
	CONSTRAINT "t2_c0_c1_unique" UNIQUE ("c0", "c1")
);
COPY 3 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	0.385788843525793
true	0.6696036757274413
false	0.3589261452091549

INSERT INTO t2 VALUES (CASE WHEN TRUE THEN TIMESTAMP '1970-01-11 13:04:02' END BETWEEN TIMESTAMP '1970-01-05 05:04:47' AND TIMESTAMP '1970-01-01 20:00:35', 1);
INSERT INTO t2 VALUES(NOT ((CASE WHEN TRUE THEN TIMESTAMP '1970-01-11 13:04:02' ELSE TIMESTAMP '1970-01-02 23:25:05' END) 
NOT BETWEEN SYMMETRIC (COALESCE(TIMESTAMP '1969-12-30 12:07:22', TIMESTAMP '1970-01-05 05:04:47')) AND (CASE WHEN TRUE THEN TIMESTAMP '1970-01-01 20:00:35' END)), 2);
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t1" ("c0" TIMESTAMP NOT NULL,CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"));
CREATE TABLE "sys"."t2" ("c0" TIMESTAMP);
COPY 3 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"1970-01-25 08:23:04.000000"
"1970-01-06 08:19:06.000000"
"1970-01-19 15:18:44.000000"

create view v0(c0) as (select all -1454749390 from t2, t1 where not 
((interval '990585801' month) in (interval '1558064353' month, interval '1877885111' month, interval '1286819945' month))) 
with check option;

SELECT CAST(SUM(agg0) AS BIGINT) FROM (
SELECT count(ALL + (((v0.c0)/(((v0.c0)^(v0.c0)))))) as agg0 FROM v0 WHERE CAST(CAST(v0.c0 AS STRING) AS BOOLEAN)
UNION ALL
SELECT count(ALL + (((v0.c0)/(((v0.c0)^(v0.c0)))))) as agg0 FROM v0 WHERE NOT (CAST(CAST(v0.c0 AS STRING) AS BOOLEAN))
UNION ALL
SELECT count(ALL + (((v0.c0)/(((v0.c0)^(v0.c0)))))) as agg0 FROM v0 WHERE (CAST(CAST(v0.c0 AS STRING) AS BOOLEAN)) IS NULL)
as asdf;
	-- 0
SELECT count(ALL + (((v0.c0)/(((v0.c0)^(v0.c0)))))) FROM v0;
	-- 0
SELECT count(ALL + (((v0.c0)/(((v0.c0)^(v0.c0)))))) FROM v0 ORDER BY v0.c0 ASC NULLS LAST, v0.c0 DESC NULLS FIRST, v0.c0 DESC NULLS LAST;
	--error, v0.c0 cannot be used without an aggregate function
ROLLBACK;

-- This crash only happens on auto-commit mode
CREATE TABLE "sys"."t1" ("c0" DECIMAL(18,3) NOT NULL,CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"));
COPY 2 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
0.403
0.008

DELETE FROM t1 WHERE (((0.86983466) IS NOT NULL) = TRUE) = TRUE;
INSERT INTO t1(c0) VALUES(0.40), (0.75);
UPDATE t1 SET c0 = 0.28 WHERE (CAST(INTERVAL '31' MONTH AS STRING)) NOT IN (COALESCE('靟', 'P먌+}I*CpQ'));
SELECT c0 FROM t1;
DROP TABLE t1;

SELECT 1 WHERE '0';
	--empty
SELECT 1 WHERE NOT '0';
	-- 1
SELECT 1 WHERE '0' IS NULL;
	--empty

CREATE TABLE "sys"."t0" ("c2" DATE NOT NULL DEFAULT DATE '1970-01-03',
	CONSTRAINT "t0_c2_pkey" PRIMARY KEY ("c2"),
	CONSTRAINT "t0_c2_unique" UNIQUE ("c2"),
	CONSTRAINT "t0_c2_fkey" FOREIGN KEY ("c2") REFERENCES "sys"."t0" ("c2")
);
CREATE TABLE "sys"."t1" ("c0" CHARACTER LARGE OBJECT NOT NULL,CONSTRAINT "t1_c0_unique" UNIQUE ("c0"));
COPY 5 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"t8rMম<b\015"
"}鸺s"
""
"!5Ef"
">l\tigL쓵9Q"

CREATE TABLE "sys"."t2" ("c0" TIME NOT NULL, "c1" DECIMAL(18,3) NOT NULL DEFAULT 0.428153, "c2" TIME,
	CONSTRAINT "t2_c2_c0_c1_unique" UNIQUE ("c2", "c0", "c1")
);
COPY 1 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
05:17:55	0.450	02:23:09

create view v0(c0, c1) as (select t2.c2, cast(timestamp '1970-01-01 17:26:21' as timestamp) from t0, t2);
create view v1(c0) as (select distinct 0.73 from t2, t1, t0, v0) with check option;

SELECT 1 FROM v0 NATURAL JOIN (SELECT t1.c0, t2.c0 FROM t2, t1) AS sub0;
	-- error, common column name "c0" appears more than once in right table
SELECT count(ALL + (((((18)%(-16)))/(CAST(TRUE AS INT))))) FROM t1, v1, t0 FULL OUTER JOIN v0 
ON (CAST(+ (-116) AS BOOLEAN)) = FALSE RIGHT OUTER JOIN t2 
ON (COALESCE(v0.c1, v0.c1, v0.c1, v0.c1)) NOT IN (TIMESTAMP '1970-01-15 03:05:56', TIMESTAMP '1970-01-14 22:30:41') 
NATURAL JOIN (SELECT t1.c0, CAST(48 AS INT), t2.c0 FROM t2, v0, v1, t0, t1) AS sub0;
	-- error, common column name "c0" appears more than once in right table
DROP TABLE t0 cascade;
DROP TABLE t1 cascade;
DROP TABLE t2 cascade;

select sub0.c0 from (select 1 as c0, 2 as c0) as sub0;
	--error, column reference "c0" is ambiguous

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" VARCHAR(31) NOT NULL,"c1" BOOLEAN,"c2" TIMESTAMP NOT NULL,
	CONSTRAINT "t0_c0_c2_c1_unique" UNIQUE ("c0", "c2", "c1"),
	CONSTRAINT "t0_c2_unique" UNIQUE ("c2"),
	CONSTRAINT "t0_c1_c0_unique" UNIQUE ("c1", "c0"),
	CONSTRAINT "t0_c0_unique" UNIQUE ("c0")
);
CREATE TABLE "sys"."t1" ("c1" INTERVAL MONTH NOT NULL,CONSTRAINT "t1_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "t1_c1_unique" UNIQUE ("c1"));
COPY 2 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
311141409
1247645829

CREATE TABLE "sys"."t2" ("c1" BIGINT NOT NULL,CONSTRAINT "t2_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "t2_c1_unique" UNIQUE ("c1"));
COPY 4 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1909667273
720758463
24545308
-1804793562

create view v0(c0, c1) as (select distinct t0.c0, t0.c2 from t2, t0 where t0.c1);

SELECT 1 FROM v0, t0 JOIN t1 ON t0.c1 WHERE (CASE CASE t0.c0 WHEN t0.c0 THEN t1.c1 END WHEN t1.c1 THEN v0.c1 END) BETWEEN (v0.c1) AND (t0.c2);
	-- empty
SELECT count(agg0) FROM (
SELECT ALL sum(ALL t1.c1) as agg0 FROM v0, t0 JOIN t1 ON t0.c1 CROSS JOIN t2 WHERE (CASE CASE t0.c0 WHEN t0.c0 THEN t1.c1 END WHEN t1.c1 THEN v0.c1 END) NOT  BETWEEN ASYMMETRIC (v0.c1) AND (t0.c2)
UNION ALL
SELECT ALL sum(ALL t1.c1) as agg0 FROM v0, t0 JOIN t1 ON t0.c1 CROSS JOIN t2 WHERE NOT ((CASE CASE t0.c0 WHEN t0.c0 THEN t1.c1 END WHEN t1.c1 THEN v0.c1 END) NOT  BETWEEN ASYMMETRIC (v0.c1) AND (t0.c2))
UNION ALL
SELECT ALL sum(ALL t1.c1) as agg0 FROM v0, t0 JOIN t1 ON t0.c1 CROSS JOIN t2 WHERE ((CASE CASE t0.c0 WHEN t0.c0 THEN t1.c1 END WHEN t1.c1 THEN v0.c1 END) NOT  BETWEEN ASYMMETRIC (v0.c1) AND (t0.c2)) IS NULL
) as asdf;
	-- 0
ROLLBACK;

START TRANSACTION;
CREATE TABLE "t1" ("c0" INTERVAL MONTH NOT NULL,CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"));
COPY 5 RECORDS INTO "t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
141227804
1365727954
498933006
1181578353
651226237

SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CASE INTERVAL '-1' MONTH WHEN INTERVAL '2' MONTH 
THEN NOT t1.c0 BETWEEN t1.c0 AND t1.c0 ELSE t1.c0 < CASE FALSE WHEN TRUE THEN t1.c0 ELSE t1.c0 END END as count FROM t1) 
as res;
	-- 0
SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CAST(CASE INTERVAL '-803989404' MONTH WHEN INTERVAL '795326851' MONTH 
THEN NOT ((t1.c0) BETWEEN ASYMMETRIC (t1.c0) AND (t1.c0)) ELSE ((t1.c0)<(CASE FALSE WHEN TRUE THEN t1.c0 ELSE t1.c0 END)) END AS INT) as count FROM t1) 
as res;
	-- 0
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" DOUBLE, "c1" TIMESTAMP,
	CONSTRAINT "t0_c0_unique" UNIQUE ("c0"),
	CONSTRAINT "t0_c1_unique" UNIQUE ("c1"),
	CONSTRAINT "t0_c0_c1_unique" UNIQUE ("c0", "c1"),
	CONSTRAINT "t0_c0_fkey" FOREIGN KEY ("c0") REFERENCES "sys"."t0" ("c0")
);
COPY 4 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	"1970-01-19 09:37:48.000000"
NULL	"1970-01-25 15:39:05.000000"
NULL	"1970-01-25 02:16:22.000000"
NULL	"1970-01-20 18:57:37.000000"

CREATE TABLE "sys"."t1" ("c0" DOUBLE,"c1" TIMESTAMP);
COPY 5 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	"1970-01-17 22:34:50.000000"
NULL	"1970-01-21 20:17:49.000000"
0.5197361696675626	NULL
0.46979060080234214	NULL
0.0047394257892724445	NULL

SELECT t0.c1 FROM t0 WHERE (((((t0.c0) NOT IN (-1003666733, t0.c0))OR((t0.c0) NOT BETWEEN SYMMETRIC (((14)*(10))) AND (- (75)))))OR('TRUE'));
	-- 1970-01-19 09:37:48
	-- 1970-01-25 15:39:05
	-- 1970-01-25 02:16:22
	-- 1970-01-20 18:57:37

SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CAST((((((t0.c0) NOT IN (-10, t0.c0))OR((t0.c0) NOT  BETWEEN SYMMETRIC (((14)*(10))) AND (- (756050096)))))OR('TRUE')) AS INT) as count FROM t0) as res;
	-- 4
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t1" (
	"c0" CHAR(420)     NOT NULL,
	CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0")
);
COPY 14 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"-1284014837"
"R/<"
""
"s2"
")L"
"-1637493938"
"0.7778392099491236"
"w"
"-\\h"
"d"
"mfvgds&o"
"449949101"
"f8i8c"
"?"

SELECT t1.c0 FROM t1 WHERE t1.c0 NOT ILIKE t1.c0;
	--empty
SELECT t1.c0 NOT ILIKE t1.c0 FROM t1;
	--14 * false
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" INTERVAL MONTH,"c1" INTERVAL SECOND);
create view v0(c0) as (select all t0.c1 from t0);
CALL sys.reuse('sys', 'v0'); --error not persistent
ROLLBACK;

CREATE TABLE "sys"."t1" ("c1" BOOLEAN);
INSERT INTO t1(c1) VALUES(TRUE), (FALSE), (FALSE);
INSERT INTO t1(c1) VALUES((- (2127809083)) NOT IN (CASE TIME '22:10:54' WHEN TIME '19:30:46' THEN -1396572561 WHEN TIME '15:58:42' THEN -1164282298 END, + (-767637633), COALESCE(1638931666, 596854699))), (TRUE);
INSERT INTO t1(c1) VALUES(FALSE), (TRUE), (FALSE), (FALSE), (FALSE), (FALSE), (TRUE), (TRUE), (FALSE), (TRUE);
DELETE FROM t1 WHERE t1.c1;
CALL sys.reuse('sys', 't1');
DROP TABLE "sys"."t1";

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" DATE NOT NULL,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),CONSTRAINT "t0_c0_unique" UNIQUE ("c0"));
COPY 4 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1970-01-10
1970-01-08
1970-01-11
1970-01-04

CREATE TABLE "sys"."t1" ("c0" BIGINT NOT NULL,"c1" BOOLEAN,CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"));
COPY 36 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
23	false
24	false
25	false
229738198	NULL
1819118647	NULL
26	false
2058744872	NULL
-168288931	NULL
166463833	NULL
-255018368	NULL
27	false
-233859923	false
77211519	NULL
28	false
159399657	NULL
29	false
2026785942	NULL
-418264598	NULL
1594290070	NULL
2030525887	NULL
-841582890	NULL
1837616768	NULL
-6844300	NULL
-950410411	false
30	false
-487156028	false
2127505141	NULL
-3355985	false
31	false
32	false
-20002400	NULL
658423972	NULL
33	false
34	false
-721417633	NULL
132893186	NULL

CREATE TABLE "sys"."t2" ("c0" DATE NOT NULL);
COPY 26 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1970-01-25
1970-01-04
1970-01-07
1970-01-02
1970-01-21
1970-01-22
1970-01-01
1970-01-12
1970-01-21
1970-01-04
1970-01-12
1970-01-21
1970-01-10
1970-01-18
1970-01-24
1970-01-03
1970-01-10
1970-01-11
1970-01-01
1970-01-08
1970-01-01
1970-01-11
1970-01-23
1970-01-07
1970-01-02
1970-01-23

SELECT t1.c1 FROM t1, t2 NATURAL JOIN t0 WHERE (CAST((TIME '15:03:17') NOT IN (TIME '11:36:59', TIME '21:14:10', 
TIME '01:03:49') AS INT)) BETWEEN ASYMMETRIC (t1.c0) AND (t1.c0);
	-- empty
SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CAST((CAST((TIME '15:03:17') NOT IN (TIME '11:36:59', TIME '21:14:10', 
TIME '01:03:49') AS INT)) BETWEEN ASYMMETRIC (t1.c0) AND (t1.c0) AS INT) as count FROM t1, t2 NATURAL JOIN t0) as res;
	-- 0
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" (
	"c0" INTERVAL SECOND NOT NULL,
	"c1" DOUBLE        NOT NULL,
	CONSTRAINT "t0_c1_c0_pkey" PRIMARY KEY ("c1", "c0"),
	CONSTRAINT "t0_c0_c1_unique" UNIQUE ("c0", "c1")
);
COPY 2 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
-1209789190.000	879292012
1924429801.000	0.6708880135477207

CREATE TABLE "sys"."t1" (
	"c0" TIME,
	"c1" DECIMAL(18,3),
	CONSTRAINT "t1_c0_c1_unique" UNIQUE ("c0", "c1")
);
COPY 8 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
04:31:04	0.075
22:31:37	NULL
03:24:48	0.588
NULL	0.808
05:31:38	0.351
NULL	0.024
NULL	0.794
23:37:32	0.907

select cast(sum(count) as bigint) from (select cast(not (not (true)) as int) as count from t1 natural join t0) as res;
	-- NULL
select cast(interval '-1' second as time);
	--23:59:59
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t2" ("c0" INTERVAL MONTH,"c1" BINARY LARGE OBJECT);
COPY 11 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	E0
NULL	A0B0
NULL	00
NULL	NULL
1655951380	NULL
1624060581	NULL
503097283	NULL
1181487537	NULL
NULL	
NULL	4D3ABFB0
1233829956	0C

create view v0(vc0, vc1, vc2) as (values (length(r'-165227345'), greatest(interval '773370894' second, interval '-320486280' second),
sql_add(cast(date '1970-01-06' as timestamp), case 0.34182486343556306440660819134791381657123565673828125 when 20419
then interval '-359816023' second else interval '413001394' second end))) with check option;

select t2.c1 from (values (10, 773370894.000, timestamp '1983-02-07 02:36:34')) v0, t2
right outer join (values (47940718, null)) as sub0 on nullif(true, (t2.c1) between (t2.c1) and (t2.c1));

select t2.c1 from v0, t2 right outer join (values (least(ifthenelse(false, r'', r'47940718'), sql_max(r'栩', r'U*>Kz0')),
case cast(interval '-165227345' month as string) when "insert"(r'f3', 0.57506007, -1550051844, r'0.06527867754732408')
then r'u' when r'2033856974' then case when true then r'' when false then r'-1543387917' when true then r'0.4055556886118663'
when false then r'c' else r'E\n' end end)) as sub0 on nullif(not (false), (t2.c1) not between asymmetric (t2.c1) and (t2.c1))
where case ((50)*(1836147184)) when least(5110, 1303739364) then (false) = false when scale_down(0.07871449894764926,
0.26471586054989171277185278086108155548572540283203125) then least(true, true) when
cast(0.92609287116711858089956876938231289386749267578125 as double) then greatest(true, false) end;
ROLLBACK;

START TRANSACTION;
create view v5(vc0, vc1, vc2) as (values (2090916872, -1.850350369280157e-11, 566614814)) with check option;

create view v29(vc0, vc1) as (values (0.2, 0.2), (0.4, 0.5)) with check option;

create view v57(vc0, vc1) as (values (0.6, 0.7), (0.8, 0.9)) with check option;

create view v76(vc0) as (with cte0(c0,c1,c2,c3) as (select 0.32, 1, true, 1), cte1(c0,c1) as (select -0.50347245, 23.2)
select distinct true from (values(1),(2)) l0v54,cte0 as l0cte0,cte1 as l0cte1) with check option;

create view v82(vc0, vc1) as (with cte1(c0,c1,c2) as (select 1, case when false
then true when false then false end, 1 from v29 as l1v29)
select case l0cte1.c1 when true then least(null, l0cte1.c0) end, 1 from cte1 as l0cte1
join (select 1, '1', cast(l1v57.vc1 as bigint) from v57 as l1v57,
v5 as l1v5, v76 as l1v76) as sub1 on false) with check option;

WITH cte0(c0,c1,c2) AS (SELECT CAST('1' AS INT), 1, 0.82 FROM v82 AS l1v82) SELECT 1 FROM cte0 AS l0cte0;
	--rel_push_project_down optimizer issue
ROLLBACK;
