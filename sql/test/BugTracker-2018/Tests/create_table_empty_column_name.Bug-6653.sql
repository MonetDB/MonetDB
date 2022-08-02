CREATE TABLE tbl ("" INT); --error

CREATE TABLE "" (i INT); --error

CREATE TABLE selectme (a STRING);

INSERT INTO selectme VALUES (""); --error
INSERT INTO selectme VALUES ('');

SELECT * FROM (SELECT a FROM selectme) AS ""; --error
SELECT a AS "" FROM (SELECT a FROM selectme) AS "other"; --error
SELECT a AS "othera" FROM (SELECT a FROM selectme) AS "other";

DROP TABLE selectme;
