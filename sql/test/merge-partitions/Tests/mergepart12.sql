CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));

ALTER TABLE testme ADD TABLE sublimits1 AS PARTITION BETWEEN '-200' AND '300';
ALTER TABLE testme ADD TABLE sublimits2 AS PARTITION BETWEEN '301' AND '500';

INSERT INTO testme VALUES (-150, 'first'), (103, 'second'), (450, 'third'), (301, 'fourth');

UPDATE testme SET b = 'b' || b || 'e';
UPDATE testme SET a = a + 1;

UPDATE sublimits1 SET b = 'a' || b || 'f';
UPDATE sublimits1 SET a = a * 3;

UPDATE sublimits2 SET b = 'a' || b || 'f';
UPDATE sublimits2 SET a = a * 3;

SELECT a, b FROM testme;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;

ALTER TABLE testme DROP TABLE sublimits1;
ALTER TABLE testme DROP TABLE sublimits2;

DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE testme;
