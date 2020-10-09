START TRANSACTION;
CREATE TABLE "t0" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t0_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t0_tc0_unique" UNIQUE ("tc0"));
INSERT INTO "t0" VALUES ('1048409847'), ('ph'), ('CV'), ('T\t'), ('!iG&');
CREATE TABLE "t1" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t1_tc0_unique" UNIQUE ("tc0"),CONSTRAINT "t1_tc0_fkey" FOREIGN KEY ("tc0") REFERENCES "t1" ("tc0"));
select 1 from t0 join t1 on sql_min(true, t1.tc0 between rtrim(t0.tc0) and 'a');
	-- empty
select cast("isauuid"(t1.tc0) as int) from t0 full outer join t1 on
not (sql_min(not ((interval '505207731' day) in (interval '1621733891' day)), (nullif(t0.tc0, t1.tc0)) between asymmetric (rtrim(t0.tc0)) and (cast((r'_7') in (r'', t0.tc0) as string(891)))));
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
