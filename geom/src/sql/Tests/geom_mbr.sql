
CREATE TABLE mbrs (b MBR);
INSERT INTO mbrs values ('POINT(10 10)');
INSERT INTO mbrs values (mbr(POINT 'POINT(10 10)'));
INSERT INTO mbrs values ('LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO mbrs values (mbr(LINESTRING 'LINESTRING(10 10, 20 20, 30 40)'));
INSERT INTO mbrs values ('LINESTRING(10 10, 20 20)');
INSERT INTO mbrs values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO mbrs values (mbr(POLYGON 'POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))'));
INSERT INTO mbrs values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');
INSERT INTO mbrs values (mbr('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))'));
INSERT INTO mbrs values (mbr('POINT(10)'));
INSERT INTO mbrs values (mbr('POINT()'));
INSERT INTO mbrs values (mbr('POINT'));
INSERT INTO mbrs values (mbr(''));
INSERT INTO mbrs values (NULL);
SELECT * FROM mbrs;
DROP TABLE mbrs;
