
CREATE TABLE a(n integer, x varchar(255));
INSERT INTO a VALUES(1, 'ONE');
INSERT INTO a VALUES(2, 'TWO');
INSERT INTO a VALUES(2, 'TWO');
INSERT INTO a VALUES(3, 'THREE');

CREATE VIEW va AS
SELECT DISTINCT n, x
FROM a;


CREATE TABLE b(n integer, x varchar(255));
INSERT INTO b VALUES(1, 'ONE');
INSERT INTO b VALUES(2, 'TWO');
INSERT INTO b VALUES(3, 'THREE');

CREATE VIEW v AS
SELECT va.x as a, b.x as b
FROM   va, b
WHERE  va.n = b.n;
select * from va;

select * from v;

DROP VIEW v;
DROP VIEW va;
DROP TABLE b;
DROP TABLE a;

CREATE TABLE a(n integer, x varchar(255));
INSERT INTO a VALUES(1, 'ONE');
INSERT INTO a VALUES(2, 'TWO');
INSERT INTO a VALUES(2, 'TWO');
INSERT INTO a VALUES(3, 'THREE');


CREATE TABLE b(n integer, x varchar(255));
INSERT INTO b SELECT DISTINCT n,x from a;

CREATE TABLE c(n serial, x varchar(255));
INSERT INTO c (x) SELECT DISTINCT x from a;

SELECT * FROM a;   -- the initial duplicates are here
SELECT * FROM b;   -- here they get removed, correct
SELECT * FROM c;   -- here they don't get removed, wrong

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
