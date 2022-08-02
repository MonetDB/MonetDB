
CREATE TABLE tableA(x integer, y integer);
INSERT INTO tableA values(1,10);
INSERT INTO tableA values(2,20);
CREATE TABLE tableB(x integer, y integer);
INSERT INTO tableB values(1,-10);
INSERT INTO tableB values(2,-20);

SELECT A.x, A.y, B.x, B.y
FROM tableA A
LEFT JOIN tableB B ON ( B.y < -10 ) ;

SELECT A.x, A.y, B.x, B.y
FROM tableA A
LEFT JOIN tableB B ON ( A.y < 20 ) ;

SELECT A.x, A.y, B.x, B.y
FROM tableA A
RIGHT JOIN tableB B ON ( B.y < -10 ) ;

SELECT A.x, A.y, B.x, B.y
FROM tableA A
RIGHT JOIN tableB B ON ( A.y < 20 ) ;

SELECT A.x, A.y, B.x, B.y
FROM tableA A
FULL JOIN tableB B ON ( B.y < -10 ) ;

SELECT A.x, A.y, B.x, B.y
FROM tableA A
FULL JOIN tableB B ON ( A.y < 20 ) ;

DROP TABLE tableA;
DROP TABLE tableB;
