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
