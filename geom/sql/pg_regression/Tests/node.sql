-- Node two crossing lines
select 't1', st_asewkt(st_node(ST_GeomFromText('MULTILINESTRING((0 0, 10 0),(5 -5, 5 5))', 10)));

-- Node two overlapping lines
select 't2', st_asewkt(st_node(ST_GeomFromText('MULTILINESTRING((0 0, 10 0, 20 0),(25 0, 15 0, 8 0))', 10)));

-- Node a self-intersecting line
select st_node(ST_GeomFromText('LINESTRING(0 0, 10 10, 0 10, 10 0)', 10));
