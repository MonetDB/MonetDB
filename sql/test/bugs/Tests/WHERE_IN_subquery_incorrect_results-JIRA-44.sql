CREATE TABLE foo (fooid INTEGER);
CREATE TABLE bar (fooid INTEGER, barval VARCHAR(8));
INSERT INTO foo VALUES (1);
SELECT * FROM foo AS f LEFT JOIN bar AS B ON (f.fooid = b.fooid) WHERE (f.fooid) IN (SELECT 1);

DROP TABLE foo;
DROP TABLE bar;
