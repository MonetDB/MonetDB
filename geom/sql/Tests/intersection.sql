select ST_Intersection(st_pointfromtext('POINT(0 0)'), st_linefromtext('LINESTRING ( 2 0, 0 2 )'));
select ST_Intersection(st_pointfromtext('POINT(0 0)'), st_linefromtext('LINESTRING ( 0 0, 0 2 )'));
select ST_Intersection(st_pointfromtext('POINT(0 0)', 4326), st_linefromtext('LINESTRING ( 0 0, 0 2 )', 2318));
select st_intersection(st_polygonfromtext('polygon((0 0, 0 10, 10 10, 10 0, 0 0))', 4326), st_polygonfromtext('polygon((5 5, 5 15, 15 15, 15 5, 5 5))', 4326));
