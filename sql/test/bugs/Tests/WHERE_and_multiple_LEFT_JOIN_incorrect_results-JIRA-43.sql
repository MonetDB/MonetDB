CREATE TABLE foo (fooid INTEGER, fooval VARCHAR(8));
CREATE TABLE bar (fooid INTEGER, barint INTEGER);
INSERT INTO foo VALUES (1, 'A');
INSERT INTO bar VALUES (1, 111), (1, 222);

SELECT * FROM foo AS f LEFT JOIN bar AS b1 ON f.fooid = b1.fooid LEFT JOIN bar AS b2 ON f.fooid = b2.fooid WHERE b1.barint > b2.barint;

DROP TABLE foo;
DROP TABLE bar;
