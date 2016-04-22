select ST_Centroid(st_mpointfromtext('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));
select ST_Centroid(st_polygonfromtext('polygon((0 0, 0 4, 3 4, 3 0, 0 0))'));
select ST_Centroid(st_polygonfromtext('polygon((0 0, 0 40, 40 40, 40 0, 0 0))'));
select ST_Centroid(st_polygonfromtext('polygon((0 0 10, 0 40 10, 40 40 10, 40 0 10, 0 0 10))')); --does not recognise the 3rd coordinate
