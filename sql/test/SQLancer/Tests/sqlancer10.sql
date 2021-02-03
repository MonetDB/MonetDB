START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" BOOLEAN NOT NULL,"c2" INTEGER,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"));
INSERT INTO "sys"."t0" VALUES (true, 0);

CREATE TABLE "sys"."t2" ("c0" DOUBLE NOT NULL,CONSTRAINT "t2_c0_pkey" PRIMARY KEY ("c0"),CONSTRAINT "t2_c0_unique" UNIQUE ("c0"));
COPY 6 RECORDS INTO "sys"."t2" FROM stdin USING DELIMITERS E'\t',E'\n','"';
8
1
-139590671
542699836
0.852979835289385
0.9886505493437159

create view v1(vc0, vc1, vc2, vc3) as ((select 10, 7, 'n', 2 where false)
union (select 2, -0.18, 'a', 2 from t2 as l0t2 join (values (0.23), (-0.24)) as sub0 on false)) with check option;

select 1 from v1, t2, t0 join (select false) as sub0 on true where cast(t0.c0 as clob) between lower(v1.vc0) and v1.vc2;
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t2" ("c0" BOOLEAN,"c2" INTEGER);
INSERT INTO "sys"."t2" VALUES (true, NULL), (NULL, 4), (NULL, 1);

UPDATE t2 SET c0 = TRUE WHERE COALESCE(t2.c0, (t2.c0) IN (FALSE));
UPDATE t2 SET c0 = TRUE WHERE COALESCE(t2.c0, (t2.c0) NOT IN (FALSE), t2.c0, least(t2.c0, t2.c0), (t2.c0) = FALSE, t2.c0, CASE t2.c2
WHEN t2.c2 THEN t2.c0 ELSE t2.c0 END, ((r'n')LIKE(r'')), ((r'PQ Q<!')LIKE(r'왋di5Xf%N')), (r'cZ') IN (r'0.49842616303390397'));
ROLLBACK;

START TRANSACTION;
CREATE TABLE "sys"."t0" ("c0" CHAR(89) NOT NULL,"c1" BOOLEAN,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),
	CONSTRAINT "t0_c0_unique" UNIQUE ("c0"),CONSTRAINT "t0_c1_c0_unique" UNIQUE ("c1", "c0"));
COPY 11 RECORDS INTO "sys"."t0" FROM stdin USING DELIMITERS E'\t',E'\n','"';
"熡U"	false
"3"	NULL
"6"	NULL
"0.6714721480805466"	NULL
"true"	true
"OD6N綥"	false
"흷)%^Ae+c蝢"	true
"9"	false
"']iq"	true
"E"	true
"0.5036928534407451"	false

update t0 set c1 = true where t0.c0 = t0.c0 and t0.c1 = t0.c1;
update t0 set c1 = true, c0 = r'.+' where (((("isauuid"(t0.c0))and(((t0.c0)=(t0.c0)))))and(((t0.c1)=(t0.c1))));
ROLLBACK;

START TRANSACTION;
create view v2(vc0, vc1) as (values (NULL, 1),(true, 2),(true, 3));
create view v6(vc2) as ((select '') except (select 'a'));
select 1 from v6, v2 join (values (0.54), (3), (0)) as sub0 on true where (v2.vc0 > v2.vc0) between (v2.vc0 = v2.vc0) and "isauuid"(v6.vc2);
ROLLBACK;

START TRANSACTION;
CREATE TABLE t1(c0 int);
CREATE VIEW v4(vc0) AS ((SELECT NULL FROM t1 AS l0t1) UNION ALL (SELECT true));
INSERT INTO t1(c0) VALUES(12), (2), (6), (3), (1321), (10), (8), (1), (2), (3), (9);
DELETE FROM t1 WHERE true;
INSERT INTO t1(c0) VALUES(9), (1), (3), (2), (5);
SELECT 1 FROM v4 JOIN (SELECT 2) AS sub0 ON COALESCE(v4.vc0, v4.vc0 BETWEEN v4.vc0 AND v4.vc0);
ROLLBACK;
