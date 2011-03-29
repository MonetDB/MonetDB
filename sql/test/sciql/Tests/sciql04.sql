INSERT INTO grid VALUES(1,1,25);

DELETE FROM matrix WHERE x = 2;
INSERT INTO matrix SELECT x-1, y, v FROM matrix WHERE x > 2;
INSERT INTO matrix SELECT x, y, 0 FROM matrix WHERE x = 3;
INSERT INTO matrix(v) SELECT v FROM stripes;
