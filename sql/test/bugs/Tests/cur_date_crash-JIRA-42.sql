CREATE TABLE foo (bar int);
CREATE TABLE foo1 (bar int);

SELECT bar FROM foo WHERE bar NOT IN (SELECT bar FROM foo1);

SELECT CURDATE() FROM foo;

SELECT CURDATE() FROM foo WHERE bar NOT IN (SELECT bar FROM foo1);
drop table foo1;
drop table foo;
