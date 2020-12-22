START TRANSACTION;

CREATE TABLE foo (i INT);
INSERT INTO foo VALUES (10), (40), (20), (5);

CREATE TABLE bar (i INT, j INT);
INSERT INTO bar VALUES (30, 300), (20, 200), (50, 500), (40, 400);

PLAN SELECT foo.i, bar.i FROM foo LEFT JOIN bar ON foo.i = bar.i WHERE bar.i IS NOT NULL;

PLAN SELECT foo.i, bar.i FROM foo LEFT JOIN bar ON foo.i = bar.i WHERE bar.j IS NOT NULL;

PLAN SELECT foo.i, bar.i FROM foo RIGHT JOIN bar ON foo.i = bar.i WHERE (2*foo.i > 20 OR (400 < foo.i*2 AND foo.i+foo.i = foo.i));

PLAN SELECT foo.i, bar.i FROM foo FULL OUTER JOIN bar ON foo.i = bar.i WHERE (2*foo.i > 20 OR (400 < foo.i*2 AND foo.i+foo.i = foo.i));

PLAN SELECT foo.i, bar.i FROM foo FULL OUTER JOIN bar ON foo.i = bar.i WHERE bar.j IS NOT NULL;

PLAN SELECT foo.i, bar.i FROM foo FULL OUTER JOIN bar ON foo.i = bar.i WHERE (2*foo.i > 20 OR (400 < foo.i*2 AND foo.i+foo.i = foo.i)) AND bar.j IS NOT NULL;

ROLLBACK;
