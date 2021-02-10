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
