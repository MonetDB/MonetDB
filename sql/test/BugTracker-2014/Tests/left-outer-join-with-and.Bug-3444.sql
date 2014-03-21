START TRANSACTION;

CREATE TABLE "sys"."table1" ("taskid" INTEGER, "cid" INTEGER);
CREATE TABLE "sys"."table2" ("taskid" INTEGER, "cname" VARCHAR(255));
SELECT * FROM table1 t1 LEFT OUTER JOIN table2 t2 ON (t1.taskid = t2.taskid) WHERE (t1.cid IS NULL OR t1.cid = t2.taskid OR t2.cname = 'test');
SELECT * FROM table1 t1 LEFT OUTER JOIN table2 t2 ON (t1.taskid = t2.taskid) AND (t1.cid IS NULL OR t1.cid = t2.taskid);
SELECT * FROM table1 t1 LEFT OUTER JOIN table2 t2 ON (t1.taskid = t2.taskid) AND (t1.cid IS NULL OR t1.cid = t2.taskid OR t2.cname = 'test');

ROLLBACK;
