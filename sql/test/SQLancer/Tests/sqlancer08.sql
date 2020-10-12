START TRANSACTION;
CREATE TABLE "t0" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t0_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t0_tc0_unique" UNIQUE ("tc0"));
INSERT INTO "t0" VALUES ('1048409847'), ('ph'), ('CV'), ('T\t'), ('!iG&');
CREATE TABLE "t1" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t1_tc0_unique" UNIQUE ("tc0"),CONSTRAINT "t1_tc0_fkey" FOREIGN KEY ("tc0") REFERENCES "t1" ("tc0"));
select 1 from t0 join t1 on sql_min(true, t1.tc0 between rtrim(t0.tc0) and 'a');
	-- empty
select cast("isauuid"(t1.tc0) as int) from t0 full outer join t1 on
not (sql_min(not ((interval '505207731' day) in (interval '1621733891' day)), (nullif(t0.tc0, t1.tc0)) between asymmetric (rtrim(t0.tc0)) and (cast((r'_7') in (r'', t0.tc0) as string(891)))));
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("tc0" UUID NOT NULL,CONSTRAINT "t0_tc0_pkey" PRIMARY KEY ("tc0"), CONSTRAINT "t0_tc0_unique" UNIQUE ("tc0"));
COPY 8 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
c3fc2aee-1e03-50cf-f4c7-e6bbbb3e31a3
1efaa28b-1e44-0b5b-517b-5790d23acf5f
32cf1b57-bccb-9e00-80a2-e5af23e5cccc
5a9fe00d-b21e-6fba-efba-33ceefdebfb5
68714cba-2af2-3de1-ebd0-eba5d8da68ce
a40776ba-5e2d-02bd-1b59-0b1ad9b5d311
b5a5abcd-bb90-56a2-ffd3-f321403b6e9e
0b2d9fdb-8bfb-5fec-bebb-c658aecb013c

CREATE TABLE "sys"."t1" ("tc1" VARCHAR(486));
COPY 3 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"0.9918446996922964"
NULL
"{t鏷>9縣+B"

SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT ALL CAST(t0.tc0 <> ANY(VALUES (UUID 'EaFBB6AC-6Eb9-00d3-7cb0-8EC8b5ad59D8'), (UUID 'bA3ca114-Cb42-7CA8-dCdF-1fB6F2dFF704'), (UUID 'dbcea1AC-60dB-8DdA-ae8C-4FC400321eD6')) AS INT) as count FROM t0, t1) as res;
	--24
ROLLBACK;

CREATE TABLE t0(tc0 INTERVAL MONTH DEFAULT (INTERVAL '1997904243' MONTH), tc1 TIME UNIQUE);
INSERT INTO t0(tc0) VALUES(INTERVAL '444375026' MONTH);
DELETE FROM t0 WHERE TRUE;
ALTER TABLE t0 ALTER tc0 SET NOT NULL;
INSERT INTO t0(tc0) VALUES(INTERVAL '-625288924' MONTH);
UPDATE t0 SET tc0 = (t0.tc0) WHERE TRUE;
DROP TABLE t0;

START TRANSACTION;
CREATE TABLE "t1" ("tc0" DOUBLE NOT NULL,"tc1" CHARACTER LARGE OBJECT);
COPY 7 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
-1823648899	""
929994438	"0.0"
1388143804	""
-1060683114	NULL
0.6102056577219861	NULL
0.5788611308131733	NULL
0.36061345372160747	NULL

SELECT t1.tc0 FROM t1 WHERE "isauuid"(lower(lower("truncate"(t1.tc1, NULL))));
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("tc0" CHARACTER LARGE OBJECT NOT NULL);
CREATE TABLE "sys"."t1" ("tc0" CHARACTER LARGE OBJECT NOT NULL);

select t0.tc0 from t0 cross join t1 where "isauuid"(cast(trim(t1.tc0) between t0.tc0 and 'a' as clob));
	-- empty
select t0.tc0 from t0 cross join t1 where "isauuid"(cast((substr(rtrim(t1.tc0, t1.tc0), abs(-32767), 0.27)) between asymmetric (t0.tc0) and (cast(time '01:09:03' as string)) as string(19)));
	-- empty
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t2" ("tc0" BIGINT NOT NULL,CONSTRAINT "t2_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t2_tc0_unique" UNIQUE ("tc0"));
COPY 4 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1611702516
0
-803413833
921740890

select t2.tc0, scale_down(0.87735366430185102171179778451914899051189422607421875, t2.tc0) from t2;
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("tc0" BIGINT NOT NULL,CONSTRAINT "t0_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t0_tc0_unique" UNIQUE ("tc0"));
COPY 3 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
34818777
-2089543687
0

CREATE TABLE "sys"."t1" ("tc0" TIMESTAMP NOT NULL,CONSTRAINT "t1_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t1_tc0_unique" UNIQUE ("tc0"));
CREATE TABLE "sys"."t2" ("tc1" INTERVAL DAY  NOT NULL,CONSTRAINT "t2_tc1_pkey" PRIMARY KEY ("tc1"),CONSTRAINT "t2_tc1_unique" UNIQUE ("tc1"),CONSTRAINT "t2_tc1_unique" UNIQUE ("tc1"));
COPY 3 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
133611486249600.000
48909174537600.000
55100204380800.000

SELECT ALL t1.tc0 FROM t2, t1 FULL OUTER JOIN t0 ON TRUE WHERE (ascii(ltrim(replace(r'', r'l/', r'(')))) IS NOT NULL;
	-- empty
SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CAST((ascii(ltrim(replace(r'', r'l/', r'(')))) IS NOT NULL AS INT) as count FROM t2, t1 FULL OUTER JOIN t0 ON TRUE) as res;
	-- 0
ROLLBACK;

select cast('0.2.3' as decimal(10,2)); -- error, invalid decimal
select cast('+0..2' as decimal(10,2)); -- error, invalid decimal

START TRANSACTION;
create view v0(vc0) as (values (0.6686823));
create view v5(vc0) as (values ("concat"(r'-730017219', r'0.232551533113189')));

SELECT 1 FROM v0 RIGHT OUTER JOIN v5 ON 'pBU' <= ifthenelse(NOT TRUE, v5.vc0, v5.vc0);
	-- 1
SELECT CAST(SUM(count) AS BIGINT) FROM (SELECT CAST("isauuid"(splitpart(CAST(((-1206869754)|(-1610043466)) AS STRING(528)), r'0.7805510128618084', 985907011)) AS INT) as count 
FROM v0 RIGHT OUTER JOIN v5 ON ((r'pBU')<=(ifthenelse(NOT (sql_min(TRUE, TRUE)), lower(v5.vc0), "concat"(v5.vc0, v5.vc0))))) as res;
	-- 0
ROLLBACK;
