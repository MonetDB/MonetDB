START TRANSACTION;
CREATE TABLE "t1" ("c0" BIGINT NOT NULL,CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"));
INSERT INTO t1(c0) VALUES(2), (+ ((VALUES (sql_min(3, 4)))));
SELECT * from t1;
ROLLBACK;

CREATE TABLE "sys"."t0" ("c0" TIME NOT NULL, "c1" VARCHAR(143),
	CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"), CONSTRAINT "t0_c0_unique" UNIQUE ("c0"), CONSTRAINT "t0_c1_unique" UNIQUE ("c1"));
COPY 7 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
21:19:08	""
13:02:49	NULL
01:02:11	NULL
16:34:25	NULL
12:11:43	NULL
10:35:38	NULL
04:26:50	NULL

CREATE TABLE "sys"."t1" ("c0" CHAR(375) NOT NULL, CONSTRAINT "t1_c0_pkey" PRIMARY KEY ("c0"), CONSTRAINT "t1_c0_fkey" FOREIGN KEY ("c0") REFERENCES "sys"."t0" ("c1"));
insert into t1 values ('');
insert into t1(c0) values ((select 'a')), ('b');
insert into t1(c0) values(r']BW扗}FUp'), (cast((values (greatest(r'Aᨐ', r'_'))) as string(616))), (r'');
DROP TABLE t1;
DROP TABLE t0;

CREATE TABLE "sys"."t0" ("c0" BOOLEAN,"c1" DECIMAL(14,3));
COPY 7 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
false	0.458
true	4.112
false	0.201
false	0.347
true	0.420
false	0.127
false	0.502

CREATE TABLE "sys"."t1" ("c0" BOOLEAN,"c1" DECIMAL(14,3));
COPY 10 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	0.000
false	0.187
false	0.000
false	NULL
false	NULL
true	NULL
NULL	0.325
NULL	0.374
true	NULL
true	NULL

select 1 from t1, t0 where cast(t1.c1 as clob) not like ((select 'A' from t0, t1) except all (select 'B' from t0)); --error, more than one row returned
select 1 from t1, t0 where (select 1 from t1) like cast(t1.c1 as clob); --error, more than one row returned
select 1 from t1, t0 where cast(t1.c1 as clob) between 'b' and ((select 'A' from t0)); --error, more than one row returned
select 1 from t1, t0 where ((select 'A' from t0)) between cast(t1.c1 as clob) and 'a'; --error, more than one row returned
select 1 from t1, t0 where cast(t1.c1 as clob) between ((select 1 from t0)) and 'c'; --error, more than one row returned

drop table t0;
drop table t1;

START TRANSACTION;
CREATE TABLE "t0" ("c0" DOUBLE PRECISION,"c2" BIGINT);
INSERT INTO "t0" VALUES (NULL, 4), (NULL, 6), (NULL, 0), (NULL, 2), (NULL, 1);

CREATE TABLE "t2" ("c0" DOUBLE PRECISION,"c1" bigint,"c2" BIGINT,"c4" REAL);
INSERT INTO "t2" VALUES (4, 0, 6, NULL),(0.692789052132086, -1, 9, NULL),(2, 0, 6, NULL),(0.9469594820593024, 1, NULL, NULL),(NULL, 0, 6, NULL),
(0.39272912837466945, 8, NULL, NULL),(NULL, NULL, 4, NULL),(2, 0, 6, NULL),(-1596101049, 0, 6, NULL),(-1951243968, 0, 6, NULL),(NULL, 0, 6, NULL),
(NULL, 0, 6, NULL),(NULL, 0, 6, NULL),(NULL, 0, 6, NULL);

SELECT 4 = ANY(SELECT t2.c2 FROM t2) FROM t0;
	-- True
	-- True
	-- True
	-- True
	-- True
ROLLBACK;

START TRANSACTION;
CREATE TABLE "t0" ("c0" REAL,"c1" BOOLEAN,"c3" DOUBLE PRECISION);
INSERT INTO "t0" VALUES (NULL, false, NULL);
CREATE TABLE "t1" ("c0" REAL);
INSERT INTO "t1" VALUES (2),(2),(2),(2),(2),(2),(2),(0.27167553),(0.67248166),(0.7818908),(-9.1086214e+08),(-0.9899925);
CREATE TABLE "t2" ("c0" REAL,"c1" BOOLEAN);
INSERT INTO "t2" VALUES (-1.2357439e+08, false), (0.16160075, false), (NULL, true);

SELECT t0.c3 FROM t2, t0 WHERE FALSE BETWEEN t0.c3 = ANY(SELECT t0.c0 FROM t0 WHERE t0.c1) AND TRUE;
	-- NULL
	-- NULL
	-- NULL
SELECT FALSE BETWEEN t0.c3 = ANY(SELECT t0.c0 FROM t0 WHERE t0.c1) AND TRUE FROM t2, t0;
	-- True
	-- True
	-- True
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" BINARY LARGE OBJECT NOT NULL,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),CONSTRAINT "t0_c0_unique" UNIQUE ("c0"));
INSERT INTO "sys"."t0" VALUES (BINARY LARGE OBJECT 'F4BECB7E'),(BINARY LARGE OBJECT ''),(BINARY LARGE OBJECT 'E0'),(BINARY LARGE OBJECT 'B0'),(BINARY LARGE OBJECT 'A0FA'),
(BINARY LARGE OBJECT 'E67D3FC0'),(BINARY LARGE OBJECT 'A324'),(BINARY LARGE OBJECT '49E0');
CREATE TABLE "sys"."t1" ("c0" BINARY LARGE OBJECT);
INSERT INTO "sys"."t1" VALUES (BINARY LARGE OBJECT ''), (BINARY LARGE OBJECT 'A0'), (BINARY LARGE OBJECT '');

SELECT t0.c0 FROM t1, t0 WHERE 4 NOT IN (6, 1.7976931348623157E308, 0);
	-- 24 blobs
SELECT 4 NOT IN (6, 1.7976931348623157E308, 0) FROM t1, t0;
	-- 24 true
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t1" ("c1" TINYINT,"c2" CHAR(22),"c4" INTEGER);
INSERT INTO "sys"."t1" VALUES (NULL, NULL, 0),(NULL, NULL, -391221783),(NULL, NULL, -1107653660),(NULL, NULL, 0),(7, 'cTR'' Abp', 0),(7, 'cTR'' Abp', 1),(7, 'cTR'' Abp', -1);

SELECT t1.c2 FROM t1 WHERE CASE 3 WHEN ((SELECT 0.48) INTERSECT DISTINCT (SELECT -1.2)) THEN FALSE ELSE TRUE END;
	-- 7 values
SELECT CASE 3 WHEN ((SELECT 0.48) INTERSECT DISTINCT (SELECT -1.2)) THEN FALSE ELSE TRUE END FROM t1;
	-- 7 values
ROLLBACK;

START TRANSACTION;
CREATE TABLE "t0" ("c0" SMALLINT NOT NULL,"c2" INTEGER);
CREATE TABLE "t1" ("c0" SMALLINT NOT NULL,"c2" SMALLINT);
INSERT INTO "t1" VALUES (4, NULL),(3, 4),(3, 5),(2, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(6, NULL),(3, NULL),(5, NULL),
(0, 8),(3, NULL),(8, 7),(-1, 6),(3, -3),(7, 7),(0, 1),(8, NULL),(9, NULL),(9, NULL),(-1, NULL),(2, NULL),(7, 8),(1, 2);
CREATE TABLE "t2" ("c0" SMALLINT);
INSERT INTO "t2" VALUES (NULL), (NULL), (0);

SELECT t1.c0 FROM t1 FULL OUTER JOIN (SELECT 3.3 FROM t2) AS sub0 ON true WHERE (EXISTS (SELECT 1)) OR greatest(TRUE, FALSE);
	-- 90 rows
SELECT (EXISTS (SELECT 1)) OR greatest(TRUE, FALSE) FROM t1 FULL OUTER JOIN (SELECT 3.3 FROM t2) AS sub0 ON true;
	-- 90 rows
ROLLBACK;
