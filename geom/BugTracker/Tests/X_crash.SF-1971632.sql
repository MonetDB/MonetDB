set optimizer = 'sequential_pipe'; -- to get predictable errors

CREATE TABLE geoms (id INTEGER, g GEOMETRY);
INSERT INTO geoms values (1, 'POINT(10 10)');
INSERT INTO geoms values (2, 'LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO geoms values (3, 'POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO geoms values (4, 'POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');

select count(*) from geoms;
select ST_X(g) from geoms order by id;

DROP TABLE geoms;
