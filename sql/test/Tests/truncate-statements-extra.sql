CREATE TABLE testing4  (a INT PRIMARY KEY);
CREATE TABLE testing5 (abc INT PRIMARY KEY);

ALTER TABLE testing5 ADD CONSTRAINT "a_fkey" FOREIGN KEY ("abc") REFERENCES testing4  ("a");

INSERT INTO testing4  VALUES (1);
INSERT INTO testing5 VALUES (1);

TRUNCATE testing4; --error
SELECT a FROM testing4;
SELECT abc FROM testing5;

TRUNCATE testing4  RESTRICT; --error
SELECT a FROM testing4;
SELECT abc FROM testing5;

TRUNCATE testing4  CASCADE;
SELECT a FROM testing4;
SELECT abc FROM testing5;

-- test sequences restarting
CREATE TABLE testing6 (a INT AUTO_INCREMENT, b INT);

INSERT INTO testing6 (b) VALUES (1);
INSERT INTO testing6 (b) VALUES (1);
SELECT a, b FROM testing6;
TRUNCATE testing6;

INSERT INTO testing6 (b) VALUES (3);
INSERT INTO testing6 (b) VALUES (4);
SELECT a, b FROM testing6;
TRUNCATE testing6 CONTINUE IDENTITY;

INSERT INTO testing6 (b) VALUES (5);
INSERT INTO testing6 (b) VALUES (6);
SELECT a, b FROM testing6;
TRUNCATE testing6 RESTART IDENTITY;

INSERT INTO testing6 (b) VALUES (7);
INSERT INTO testing6 (b) VALUES (8);
SELECT a, b FROM testing6;

DROP TABLE testing5;
DROP TABLE testing4;
DROP TABLE testing6;
