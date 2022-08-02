CREATE TABLE testing7 (ab INT);
CREATE TABLE testing8 (abc INT);

CREATE TRIGGER nanani AFTER INSERT ON testing7 FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing8 VALUES (1); END;

INSERT INTO testing7 values (1);

SELECT abc FROM testing8; --should get a single row with value 1

CREATE OR REPLACE TRIGGER nanani AFTER INSERT ON testing7 FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing8 VALUES (2); END;

INSERT INTO testing7 values (1);

SELECT abc FROM testing8; --the previous row plus 2

CREATE OR REPLACE TRIGGER failedtrigger AFTER INSERT ON testing3 FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing8 VALUES (3); END; --error

DROP TRIGGER nanani;
DROP TABLE testing7;
DROP TABLE testing8;
