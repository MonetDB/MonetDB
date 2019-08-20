START TRANSACTION;

CREATE TABLE trimtest (
	str varchar(20)
);

SAVEPOINT monetsp1;

INSERT INTO trimtest VALUES ('string1');
INSERT INTO trimtest VALUES ('  string2');
INSERT INTO trimtest VALUES ('string3  ');
INSERT INTO trimtest VALUES ('  string4  ');

SELECT * FROM trimtest;

UPDATE trimtest SET str = trim(str);

SELECT * FROM trimtest;

ROLLBACK TO SAVEPOINT monetsp1;

SELECT * FROM trimtest;

ROLLBACK;
