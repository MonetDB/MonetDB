CREATE TABLE testing9 (a1 int, b2 text);

CREATE VIEW viewtest AS SELECT a1, b2 FROM testing9 WHERE a1 = 1;

INSERT INTO testing9 VALUES (1, 'one'), (2, 'two');

SELECT a1, b2 FROM viewtest;

CREATE OR REPLACE VIEW viewtest AS SELECT a1, b2 FROM testing9 WHERE b2 = 'two';

SELECT a1, b2 FROM viewtest;

CREATE OR REPLACE VIEW failureview AS SELECT b2 FROM testingnothing WHERE b2 = 'two'; --error

DROP VIEW viewtest;
DROP TABLE testing9;
