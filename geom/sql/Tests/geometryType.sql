select geometrytype(st_pointfromtext('point(10 10)'));
select geometrytype(st_pointfromtext('point(20 20)', 4326));
select geometrytype(st_pointfromtext('point(10 10 10)'));
select geometrytype(st_makepoint(10, 10));
select geometrytype(st_point(20, 20));
select geometrytype(st_makepoint(10, 10, 10));
select geometrytype(st_linefromtext('linestring(10 10, 20 20, 30 30)'));
select geometrytype(st_linefromtext('linestring(20 20, 30 30, 40 40)', 4326));
select geometrytype(st_linefromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
select geometrytype(st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select geometrytype(st_polygonfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
select geometrytype(st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
select geometrytype(st_mpointfromtext('multipoint(10 10, 20 20)'));
select geometrytype(st_mpointfromtext('multipoint(20 20, 30 30)', 4326));
select geometrytype(st_mpointfromtext('multipoint(20 20 20, 30 30 30)', 4326));
select geometrytype(st_mlinefromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select geometrytype(st_mlinefromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
select geometrytype(st_mlinefromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
select geometrytype(st_mpolyfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
select geometrytype(st_mpolyfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
select geometrytype(st_mpolyfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));

select st_geometrytype(st_pointfromtext('point(10 10)'));
select st_geometrytype(st_pointfromtext('point(20 20)', 4326));
select st_geometrytype(st_pointfromtext('point(10 10 10)'));
select st_geometrytype(st_makepoint(10, 10));
select st_geometrytype(st_point(20, 20));
select st_geometrytype(st_makepoint(10, 10, 10));
select st_geometrytype(st_linefromtext('linestring(10 10, 20 20, 30 30)'));
select st_geometrytype(st_linefromtext('linestring(20 20, 30 30, 40 40)', 4326));
select st_geometrytype(st_linefromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
select st_geometrytype(st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select st_geometrytype(st_polygonfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
select st_geometrytype(st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
select st_geometrytype(st_mpointfromtext('multipoint(10 10, 20 20)'));
select st_geometrytype(st_mpointfromtext('multipoint(20 20, 30 30)', 4326));
select st_geometrytype(st_mpointfromtext('multipoint(20 20 20, 30 30 30)', 4326));
select st_geometrytype(st_mlinefromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select st_geometrytype(st_mlinefromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
select st_geometrytype(st_mlinefromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
select st_geometrytype(st_mpolyfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
select st_geometrytype(st_mpolyfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
select st_geometrytype(st_mpolyfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));

select geometrytype(st_geomfromtext('point(10 10)'));
select geometrytype(st_geomfromtext('linestring(10 10, 20 20, 30 30)'));
select geometrytype(st_geomfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select geometrytype(st_geomfromtext('multipoint(10 10, 20 20)'));
select geometrytype(st_geomfromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select geometrytype(st_geomfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));

select st_geometrytype(st_geomfromtext('point(10 10)'));
select st_geometrytype(st_geomfromtext('linestring(10 10, 20 20, 30 30)'));
select st_geometrytype(st_geomfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select st_geometrytype(st_geomfromtext('multipoint(10 10, 20 20)'));
select st_geometrytype(st_geomfromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select st_geometrytype(st_geomfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));



