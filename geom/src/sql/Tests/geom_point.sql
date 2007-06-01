
CREATE TABLE points (p POINT);
INSERT INTO points values ('POINT(10 10)');
INSERT INTO points values ('POINT(10)');
INSERT INTO points values ('POINT()');
INSERT INTO points values ('POINT');
INSERT INTO points values ('');
SELECT * FROM points;
DROP TABLE points;
