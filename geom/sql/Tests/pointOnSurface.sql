select ST_PointOnSurface(st_wkttosql('POINT(0 5)'));
select ST_PointOnSurface(st_wkttosql('LINESTRING(0 5, 0 10)'));
select ST_PointOnSurface(st_wkttosql('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'));
select ST_PointOnSurface(st_wkttosql('LINESTRING(0 5 1, 0 0 1, 0 10 2)'));

