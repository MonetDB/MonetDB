CREATE TABLE foo (col CHAR(8));
INSERT INTO foo VALUES ('bee');
 
-- This query triggers the error
SELECT * FROM foo WHERE NOT col LIKE 'b%';

-- This query works:
SELECT * FROM foo WHERE (col NOT LIKE 'b%');

DROP TABLE foo;
