CREATE MERGE TABLE testupdates (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));

INSERT INTO sublimits1 VALUES (1000, 'ups');
ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100; --error
TRUNCATE sublimits1;
ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100;

INSERT INTO sublimits1 VALUES (100, 'ok');
UPDATE sublimits1 SET a = a + 1; --error
UPDATE sublimits1 SET b = 'p' || b WHERE a = 100;

SELECT a, b FROM testupdates;
SELECT a, b FROM sublimits1;

ALTER TABLE testupdates DROP TABLE sublimits1;

UPDATE sublimits1 SET a = a + 1;
UPDATE sublimits1 SET b = b || 's' WHERE a = 101;

ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100; --error
UPDATE sublimits1 SET a = a - 1;
ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100;

SELECT a, b FROM testupdates;
SELECT a, b FROM sublimits1;

ALTER TABLE testupdates DROP TABLE sublimits1;

START TRANSACTION;
ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100;
ROLLBACK;

UPDATE sublimits1 SET a = a + 1;

ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 101;

SELECT a, b FROM sublimits1;
SELECT a, b FROM testupdates;

ALTER TABLE testupdates DROP TABLE sublimits1;
ALTER TABLE testupdates ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100; --error

DROP TABLE sublimits1;
DROP TABLE testupdates;
