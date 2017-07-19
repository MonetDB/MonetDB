CREATE TABLE testing (a INT PRIMARY KEY);
CREATE TABLE testing2 (abc INT PRIMARY KEY);

ALTER TABLE testing2 ADD CONSTRAINT "a_fkey" FOREIGN KEY ("abc") REFERENCES testing ("a");

INSERT INTO testing VALUES (1);
INSERT INTO testing2 VALUES (1);

TRUNCATE testing; --error
SELECT a FROM testing;
SELECT abc FROM testing2;

TRUNCATE testing RESTRICT; --error
SELECT a FROM testing;
SELECT abc FROM testing2;

TRUNCATE testing CASCADE;
SELECT a FROM testing;
SELECT abc FROM testing2;

-- test sequences restarting
CREATE TABLE testing3 (a INT AUTO_INCREMENT, b INT);

INSERT INTO testing3 (b) VALUES (1);
INSERT INTO testing3 (b) VALUES (1);
SELECT a, b FROM testing3;
TRUNCATE testing3;

INSERT INTO testing3 (b) VALUES (3);
INSERT INTO testing3 (b) VALUES (4);
SELECT a, b FROM testing3;
TRUNCATE testing3 CONTINUE IDENTITY;

INSERT INTO testing3 (b) VALUES (5);
INSERT INTO testing3 (b) VALUES (6);
SELECT a, b FROM testing3;
TRUNCATE testing3 RESTART IDENTITY;

INSERT INTO testing3 (b) VALUES (7);
INSERT INTO testing3 (b) VALUES (8);
SELECT a, b FROM testing3;
