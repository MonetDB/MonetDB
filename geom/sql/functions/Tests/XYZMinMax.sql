select id, ST_XMin(geom) AS "Xmin", ST_YMin(geom) AS "Ymin", ST_XMax(geom) AS "Xmax", ST_YMax(geom) AS "Ymax" from geometries;
select id, ST_XMin(mbr(geom)) AS "Xmin", ST_YMin(mbr(geom)) AS "Ymin", ST_XMax(mbr(geom)) AS "Xmax", ST_YMax(mbr(geom)) AS "Ymax" from geometries;
