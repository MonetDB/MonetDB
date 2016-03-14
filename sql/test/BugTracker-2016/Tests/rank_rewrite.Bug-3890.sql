
CREATE TABLE x(a1 string);
INSERT INTO x VALUES ('aa'),('bb'),('cc'),('aa');
CREATE VIEW y AS SELECT row_number() OVER(ORDER BY a1) AS a1, a1 AS a2 FROM (SELECT a1 FROM x GROUP BY a1) AS t;

drop view y;
drop table x;
