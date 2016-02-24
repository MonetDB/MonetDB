select mbr(st_geomfromtext('LINESTRING(0 0, 1 1)')) ~= mbr(st_geomfromtext('LINESTRING(0 1, 1 0)')) as equality;
select st_geomfromtext('LINESTRING(0 0, 1 1)') ~= st_geomfromtext('LINESTRING(0 1, 1 0)') as equality;
