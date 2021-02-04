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
