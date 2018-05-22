CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));

INSERT INTO sublimits1 VALUES (0, 'dummy');

ALTER TABLE testme ADD TABLE sublimits1 AS PARTITION WITH NULL; --error
TRUNCATE sublimits1;
ALTER TABLE testme ADD TABLE sublimits1 AS PARTITION WITH NULL;

INSERT INTO testme VALUES (NULL, 'first'), (NULL, NULL);
INSERT INTO sublimits1 VALUES (NULL, 'second'), (NULL, NULL);

INSERT INTO testme VALUES (2, 'third'); --error
INSERT INTO sublimits1 VALUES (2, 'third'); --error

SELECT a, b FROM testme;
SELECT a, b FROM sublimits1;

ALTER TABLE testme ADD TABLE sublimits2 AS PARTITION WITH NULL; --error
ALTER TABLE testme ADD TABLE sublimits2 AS PARTITION BETWEEN '301' AND '500' WITH NULL; --error
ALTER TABLE testme ADD TABLE sublimits2 AS PARTITION BETWEEN '301' AND '500';

INSERT INTO testme VALUES (NULL, 'fourth'), (303, 'null'), (NULL, 'fifth');

SELECT a, b FROM testme;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;

ALTER TABLE testme DROP TABLE sublimits1;
ALTER TABLE testme DROP TABLE sublimits2;

DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE testme;
