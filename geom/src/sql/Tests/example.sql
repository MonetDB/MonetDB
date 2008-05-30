
CREATE TABLE forests(id INT,name TEXT,shape MULTIPOLYGON);
CREATE TABLE buildings(id INT,name TEXT,location POINT,outline POLYGON);

INSERT INTO forests VALUES(109, 'Green Forest',
'MULTIPOLYGON( ((28 26,28 0,84 0,84 42,28 26), (52 18,66 23,73 9,48 6,52 18)), ((59 18,67 18,67 13,59 13,59 18)))');

INSERT INTO buildings VALUES(113, '123 Main Street',
	'POINT( 52 30 )',
	'POLYGON( ( 50 31, 54 31, 54 29, 50 29, 50 31) )');
INSERT INTO buildings VALUES(114, '215 Main Street',
	'POINT( 64 33 )',
	'POLYGON( ( 66 34, 62 34, 62 32, 66 32, 66 34) )');

SELECT forests.name,buildings.name
FROM forests,buildings
WHERE forests.name = 'Green Forest' and
    Overlaps(forests.shape, buildings.outline) = true;

ALTER TABLE forests ADD bbox mbr;
UPDATE forests SET bbox = mbr(shape);
ALTER TABLE buildings ADD bbox mbr;
UPDATE buildings SET bbox = mbr(outline);

SELECT forests.name,buildings.name
FROM forests,buildings
WHERE forests.name = 'Green Forest' AND
    mbroverlaps(forests.bbox,buildings.bbox) = TRUE AND
    Overlaps(forests.shape, buildings.outline) = TRUE;

drop table buildings;
drop table forests;
