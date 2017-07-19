CREATE TABLE testing (a1 int, b2 text);

CREATE VIEW viet AS SELECT a1, b2 FROM testing WHERE a1 = 1;

INSERT INTO testing VALUES (1, 'one'), (2, 'two');

SELECT a1, b2 FROM viet;

CREATE OR REPLACE VIEW viet AS SELECT a1, b2 FROM testing WHERE b2 = 'two';

SELECT a1, b2 FROM viet;

CREATE OR REPLACE VIEW failureview AS SELECT b2 FROM testing2 WHERE b2 = 'two'; --error
