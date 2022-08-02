CREATE TABLE foo (id INTEGER, bar1 INTEGER, bar2 INTEGER);
CREATE TABLE bar (barid INTEGER, value CHAR(10));
INSERT INTO bar VALUES (1, 'aaa');
INSERT INTO bar VALUES (2, 'bbb');
INSERT INTO bar VALUES (3, 'ccc');
INSERT INTO foo VALUES (100, 1, 2);
INSERT INTO foo VALUES (101, 2, 3);
SELECT B.value, F.id FROM bar B LEFT JOIN foo F ON (F.bar1 = B.barid OR F.bar2 = B.id);

DROP TABLE bar;
DROP TABLE foo;
