START TRANSACTION;

CREATE TABLE "a" (
	"property" int	NOT NULL,
	"class"    int	NOT NULL,
	CONSTRAINT "a_property_class_pkey" PRIMARY KEY ("class", "property")
);

INSERT INTO "a" VALUES (1, 17);
INSERT INTO "a" VALUES (21, 17);
INSERT INTO "a" VALUES (22, 17);
INSERT INTO "a" VALUES (20, 2);
INSERT INTO "a" VALUES (19, 17);
INSERT INTO "a" VALUES (4, 16);
INSERT INTO "a" VALUES (5, 16);
INSERT INTO "a" VALUES (6, 16);
INSERT INTO "a" VALUES (29, 16);
INSERT INTO "a" VALUES (12, 16);
INSERT INTO "a" VALUES (13, 11);
INSERT INTO "a" VALUES (26, 16);
INSERT INTO "a" VALUES (25, 16);
INSERT INTO "a" VALUES (23, 18);
INSERT INTO "a" VALUES (24, 18);
INSERT INTO "a" VALUES (15, 16);


-- table b differs from a that 50% of the values are decreased by 1 (last part)
CREATE TABLE "b" (
	"property" int	NOT NULL,
	"class"    int	NOT NULL,
	CONSTRAINT "b_property_class_pkey" PRIMARY KEY ("class", "property")
);

INSERT INTO "b" VALUES (1, 17);
INSERT INTO "b" VALUES (21, 17);
INSERT INTO "b" VALUES (22, 17);
INSERT INTO "b" VALUES (20, 2);
INSERT INTO "b" VALUES (19, 17);
INSERT INTO "b" VALUES (4, 16);
INSERT INTO "b" VALUES (5, 16);
INSERT INTO "b" VALUES (6, 16);
INSERT INTO "b" VALUES (28, 15);
INSERT INTO "b" VALUES (11, 15);
INSERT INTO "b" VALUES (12, 10);
INSERT INTO "b" VALUES (25, 15);
INSERT INTO "b" VALUES (24, 15);
INSERT INTO "b" VALUES (23, 17);
INSERT INTO "b" VALUES (14, 15);

-- make it permanent
COMMIT;

-- simple check whether everything is there
START TRANSACTION;
SELECT * FROM a;
SELECT * FROM b;
ROLLBACK;

-- require an upcast from sht to int
START TRANSACTION;
SELECT class FROM a EXCEPT SELECT 16 ORDER BY class; -- all but 16
SELECT class FROM a UNION SELECT 16 ORDER BY class; -- all with 16 (distinct so invisible)
SELECT class FROM a INTERSECT SELECT 16 ORDER BY class; -- just 16
ROLLBACK;

-- do the same with a real table
START TRANSACTION;
SELECT * FROM a EXCEPT SELECT * FROM b ORDER BY class, property; -- should be last 50% of a
SELECT * FROM a UNION SELECT * FROM b ORDER BY class, property; -- should be a + last 50% of b
SELECT * FROM a INTERSECT SELECT * FROM b ORDER BY class, property; -- should be first 50% of a
ROLLBACK;

-- now check the non-duplicate removing versions
-- (they are very tricky, so be on your marks!)
START TRANSACTION;
SELECT class FROM a EXCEPT ALL SELECT 16 ORDER BY class; -- all but one 16 (will have 16 in output!)
SELECT class FROM a UNION ALL SELECT 16 ORDER BY class; -- all plus 16
SELECT class FROM a INTERSECT ALL SELECT 16 ORDER BY class; -- just one 16 (!)
ROLLBACK;

-- do the same with a real table (because our tables have a key on both
-- columns we also use only the first column here, as otherwise the ALL
-- would never be tested for tables)
START TRANSACTION;
SELECT * FROM a EXCEPT ALL SELECT * FROM b ORDER BY class, property; -- last 50% of a
SELECT class FROM a EXCEPT ALL SELECT class FROM b ORDER BY class; -- a minus the elements from b that are in a (if count(x) in a > count(x) in b, x will appear in output)
SELECT * FROM a UNION ALL SELECT * FROM b ORDER BY class, property; -- a + b
SELECT * FROM a INTERSECT ALL SELECT * FROM b ORDER BY class, property; -- first 50% of a
SELECT class FROM a INTERSECT ALL SELECT class FROM b ORDER BY class; -- only those that are both in a and b (min(count(a, x), count(b, x)) !!!
ROLLBACK;

-- cleanup! (also should cascade into dropping the index)
START TRANSACTION;
DROP TABLE "a";
DROP TABLE "b";
COMMIT;
