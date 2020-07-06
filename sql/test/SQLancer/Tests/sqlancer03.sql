select splitpart(r'%Fz晟2墁', '', 1), splitpart(r'%Fz晟2墁', r'', 2), splitpart(r'%Fz晟2墁', r'', 1271186887);

START TRANSACTION; -- Bug 6916
CREATE TABLE "t0" ("c0" BOOLEAN NOT NULL,"c1" SMALLINT NOT NULL,CONSTRAINT "t0_c0_c1_pkey" PRIMARY KEY ("c0", "c1"));
INSERT INTO "t0" VALUES (false, -1);
create view v0(c0, c1, c2, c3) as (select ((t0.c1)<<(cast(0.09114074486978418487836961503489874303340911865234375 as int))), 0.4088967652609865, 0.3848869389602949109274732109042815864086151123046875, t0.c0 from t0 where t0.c0);

SELECT v0.c0 FROM t0 FULL OUTER JOIN v0 ON t0.c0;
	-- NULL
SELECT v0.c0 FROM t0 FULL OUTER JOIN v0 ON t0.c0 WHERE (rtrim(((upper(''))||(v0.c1)))) IS NULL;
	-- NULL
SELECT v0.c0 FROM t0 FULL OUTER JOIN v0 ON t0.c0 WHERE NOT ((rtrim(((upper(''))||(v0.c1)))) IS NULL);
	-- empty
SELECT v0.c0 FROM t0 FULL OUTER JOIN v0 ON t0.c0 WHERE ((rtrim(((upper(''))||(v0.c1)))) IS NULL) IS NULL;
	-- empty

ROLLBACK;

START TRANSACTION; -- Bug 6918
CREATE TABLE "sys"."t0" ("c0" BOOLEAN NOT NULL,"c1" BIGINT,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"),CONSTRAINT "t0_c0_unique" UNIQUE ("c0"));
create view v0(c0, c1, c2) as (select all 2.020551048E9, 0.16688174, 0.3732000026221729 from t0 where t0.c0) with check option;
SELECT sql_min(sql_max(NULL, ''), '') FROM v0 LEFT OUTER JOIN t0 ON true;
SELECT sql_min(sql_max(NULL, ''), '');
SELECT ALL length(upper(MIN(ALL CAST(((trim(CAST(r'' AS STRING(659)), CAST(r'o3%+i]抔DCöf▟nßOpNbybಜ7' AS STRING)))||(sql_min(sql_max(NULL, r''), splitpart(r'x', r',7+.', t0.c1)))) AS STRING(151))))), 0.4179268710155164 
FROM v0 LEFT OUTER JOIN t0 ON NOT (t0.c0) WHERE t0.c0 GROUP BY 0.3584962, CAST(t0.c1 AS STRING(601)), t0.c1;
ROLLBACK;
