START TRANSACTION;

CREATE TABLE "TestBulkDataInsert" (c1 BIGINT NOT NULL PRIMARY KEY, c2 VARCHAR(50) NOT NULL, c3 CLOB);

INSERT INTO "TestBulkDataInsert" SELECT * FROM (VALUES(1,'1a','1b'),(2,'2a','2b'),(3,'3a','3b')) vt3(c1,c2,c3);

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM (VALUES(?,?,?)) vt3(c1,c2,c3);
exec **(4, '4a', '4b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM (VALUES(?,?,?),(?,?,?)) vt3(c1,c2,c3);
exec **(5, '5a', '5b', 6, '6b', '6b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM (VALUES(?,?,?),(?,?,?),(?,?,?)) vt3(c1,c2,c3);
exec **(7, '7a', '7b', 8, '8b', '8b', 9, '9b', '9b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM (VALUES(10,'10a','10b'),(11,'11a','11b'),(?,?,?)) vt3(c1,c2,c3);
exec **(12, '12a', '12b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM (SELECT ?,?,?) vt3(c1,c2,c3);
exec **(13, '13a', '13b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM ((SELECT 14,?,'14b') UNION ALL (SELECT ?,'15a',?)) vt3(c1,c2,c3);
exec **('14a', 15, '15b');

PREPARE INSERT INTO "TestBulkDataInsert" SELECT * FROM ((VALUES (16,?,'16b')) UNION ALL (VALUES (?,'17a',?))) vt3(c1,c2,c3);
exec **('16a', 17, '17b');

SELECT * FROM "TestBulkDataInsert";

DROP TABLE "TestBulkDataInsert";

ROLLBACK;
