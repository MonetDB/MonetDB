START TRANSACTION;

CREATE TABLE simple_table("id" INTEGER);
INSERT INTO simple_table VALUES (1),(2),(3),(4),(5);
SELECT COUNT(*) AS val FROM simple_table ORDER BY val DESC;
SELECT COUNT(*) AS val FROM simple_table ORDER BY val DESC LIMIT 5;
SELECT COUNT(*) AS val FROM simple_table HAVING 1 > 0 ORDER BY val DESC LIMIT 5;

ROLLBACK;
