START TRANSACTION;
CREATE TABLE "t0" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t0_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t0_tc0_unique" UNIQUE ("tc0"));
INSERT INTO "t0" VALUES ('1048409847'), ('ph'), ('CV'), ('T\t'), ('!iG&');
CREATE TABLE "t1" ("tc0" VARCHAR(32) NOT NULL,CONSTRAINT "t1_tc0_unique" UNIQUE ("tc0"),CONSTRAINT "t1_tc0_fkey" FOREIGN KEY ("tc0") REFERENCES "t1" ("tc0"));
select 1 from t0 join t1 on sql_min(true, t1.tc0 between rtrim(t0.tc0) and 'a');
	-- empty
select cast("isauuid"(t1.tc0) as int) from t0 full outer join t1 on
not (sql_min(not ((interval '505207731' day) in (interval '1621733891' day)), (nullif(t0.tc0, t1.tc0)) between asymmetric (rtrim(t0.tc0)) and (cast((r'_7') in (r'', t0.tc0) as string(891)))));
ROLLBACK;
