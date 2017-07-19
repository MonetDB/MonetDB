CREATE TABLE testing (ab INT);
CREATE TABLE testing2 (abc INT);

CREATE TRIGGER nanani AFTER INSERT ON testing FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing2 VALUES (1); END;

INSERT INTO testing values (1);

SELECT abc FROM testing2; --should get a single row with value 1

CREATE OR REPLACE TRIGGER nanani AFTER INSERT ON testing FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing2 VALUES (2); END;

INSERT INTO testing values (1);

SELECT abc FROM testing2; --the previous row plus 2

CREATE OR REPLACE TRIGGER failedtrigger AFTER INSERT ON testing3 FOR EACH STATEMENT BEGIN ATOMIC INSERT INTO testing2 VALUES (3); END; --error
