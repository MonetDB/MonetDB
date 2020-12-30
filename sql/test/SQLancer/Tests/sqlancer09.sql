CREATE TABLE t0(tc0 INTERVAL MONTH DEFAULT (INTERVAL '1997904243' MONTH), tc1 TIME UNIQUE);
INSERT INTO t0(tc0) VALUES(INTERVAL '444375026' MONTH);
DELETE FROM t0 WHERE TRUE;
ALTER TABLE t0 ALTER tc0 SET NOT NULL;
INSERT INTO t0(tc0) VALUES(INTERVAL '-625288924' MONTH);
UPDATE t0 SET tc0 = (t0.tc0) WHERE TRUE;
DROP TABLE t0;

CREATE TABLE t2(tc0 TINYINT);
ALTER TABLE t2 ADD PRIMARY KEY(tc0);
INSERT INTO t2(tc0) VALUES(44), (126), (117);
ALTER TABLE t2 ADD FOREIGN KEY (tc0) REFERENCES t2(tc0) MATCH FULL;
DROP TABLE t2;

START TRANSACTION;
CREATE TABLE "t2" ("tc0" BIGINT NOT NULL,CONSTRAINT "t2_tc0_pkey" PRIMARY KEY ("tc0"),CONSTRAINT "t2_tc0_unique" UNIQUE ("tc0"), CONSTRAINT "t2_tc0_fkey" FOREIGN KEY ("tc0") REFERENCES "t2" ("tc0"));
CREATE TABLE "t0" ("tc0" TINYINT NOT NULL,"tc2" TINYINT NOT NULL,CONSTRAINT "t0_tc2_tc0_unique" UNIQUE ("tc2", "tc0"),CONSTRAINT "t0_tc2_fkey" FOREIGN KEY ("tc2") REFERENCES "t2" ("tc0"));

update t0 set tc2 = 119, tc0 = cast(t0.tc0 as bigint);
update t0 set tc2 = 119, tc0 = (least(+ (cast(least(0, t0.tc0) as bigint)), sign(scale_down(100, 1)))) where true;
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" TIMESTAMP NOT NULL,"c1" DOUBLE,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),CONSTRAINT "t0_c0_unique" UNIQUE ("c0"));
CREATE TABLE "sys"."t1" ("c0" DECIMAL(12,3));
COPY 8 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
19522599.000
0.638
0.071
12.000
0.156
0.902
-546.000
0.603

CREATE TABLE "sys"."t2" ("c0" TIMESTAMP,"c1" DOUBLE);
COPY 2 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
NULL	-869912003
NULL	0.9641209077369987

create view v0(vc0) as (with cte0(c0) as (with cte0(c0,c1) as (values (interval '2' day, ((null)%(0.3)))),
cte1(c0,c1,c2,c3,c4) as (select all least(r'', r'2'), date '1970-01-13', ((r'')ilike(r'OF먉_')),
((-2)+(-3)), cast(true as string(105)) where false) select distinct cast(case 1.1 when 0.2 then
l1cte1.c1 when 0.4 then l1cte1.c1 when 1.03728474E9 then l1cte1.c1 when 0.2 then l1cte1.c1 else l1cte1.c1 end as string)
from t0 as l1t0, t1 as l1t1,cte0 as l1cte0,cte1 as l1cte1 where not (l1cte1.c2)) select distinct least(-1, l0t0.c1)
from t1 as l0t1, t0 as l0t0,cte0 as l0cte0 where least(cast(l0cte0.c0 as boolean), true));

merge into t0 using (select * from v0) as v0 on true when not matched then insert (c1, c0) values ((select 1 from t1), timestamp '1970-01-20 08:57:27');

merge into t0 using (select * from v0) as v0 on ((r'>\nAH')not like(cast(scale_up(99, 0.1) as string(278))))
when not matched then insert (c1, c0) values (((((abs(-5))%((select -3 from t1 as l3t1, t2 as l3t2 where true))))
>>((((values (1), (1)))>>((select distinct 2 from t1 as l3t1 where false))))), ifthenelse(abs(0.3) = 
all(values ((select all 0.1 where true)), (case -1 when -3 then 0.1 
when -2 then -5 end), (((1)>>(1)))), case when least(true, false) then greatest(timestamp '1970-01-15 21:14:28', timestamp '1970-01-02 15:11:23') end,
nullif(timestamp '1970-01-20 08:57:27', sql_add(timestamp '1970-01-07 21:19:48', interval '-3' day))));
ROLLBACK;

START TRANSACTION;
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
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t1" ("c0" BIGINT);
COPY 4 RECORDS INTO "sys"."t1" FROM stdin USING DELIMITERS E'\t',E'\n','"';
1096730569
-655229050
1040813052
-1340211666

create view v0(vc0, vc1, vc2) as (values (uuid '39FcCcEE-5033-0d81-42Eb-Ac6fFaA9EF2d', ((case when true then lower(r'e') end)ilike(cast(sql_sub(interval '1600798007' month, interval '854525416' month) as string(583)))),
cast((greatest(time '12:29:42', time '00:13:46')) not between asymmetric (sql_min(time '01:00:00', time '08:31:00')) and (greatest(time '00:12:32', time '11:40:56')) as bigint)));

MERGE INTO t1 USING (SELECT * FROM v0) AS v0 ON (((COALESCE(24656, 0.42848459531531180033425698638893663883209228515625, 1153747454, 0.04253046482486677604128999519161880016326904296875, 417897684))%(((-4.65033856E8)/(98)))))
NOT BETWEEN SYMMETRIC (+ (NULLIF(-1338511329, 12))) AND (CASE WHEN CASE TIME '06:02:29' WHEN TIME '22:17:20' THEN TRUE ELSE TRUE END THEN "second"(INTERVAL '1243665924' DAY) WHEN (FALSE) = TRUE THEN CASE WHEN FALSE THEN -116446524
WHEN TRUE THEN 1702709680 WHEN r'TRUE' THEN 1255285064 END
WHEN (UUID 'baF49A5B-1862-19aa-E6F8-b3C5A7F4b1FF') BETWEEN SYMMETRIC (UUID '63A9aBBe-87b1-683a-2c68-eCd5cC7FE7E9') 
AND (UUID '82eb84EF-dF3D-a45e-f92b-E42BdfFEB1B9') THEN - (1129823324) END) WHEN MATCHED THEN DELETE;

SELECT 1 FROM (SELECT 1 FROM v0) AS v0(v0) inner join t1 ON 1 BETWEEN 2 AND 1;
-- Disable rel_simplify_ifthenelse optimizer
SELECT 1 FROM (SELECT 1 FROM v0) AS v0(v0) inner join t1 ON 1 BETWEEN 2 AND (CASE WHEN 1 BETWEEN 2 AND 3 THEN 2 END);
ROLLBACK;
