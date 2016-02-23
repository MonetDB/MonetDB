-- SRIDs are checked!
select 't1', st_asewkt(st_snap(
  ST_GeomFromText('LINESTRING(0 0, 10 0)', 10), 
  ST_GeomFromText('LINESTRING(0 0, 100 0)', 5), 0));

-- SRIDs are retained
select 't2', st_asewkt(st_snap(
  ST_GeomFromText('LINESTRING(0 0, 10 0)', 10), 
  ST_GeomFromText('LINESTRING(0 0, 9 0)', 10), 1));

-- Segment snapping
select 't3', st_asewkt(st_snap(
  ST_GeomFromText('LINESTRING(0 0, 10 0)'), 
  ST_GeomFromText('LINESTRING(5 -1, 5 -10)'), 2));

-- Vertex snapping
select 't4', st_asewkt(st_snap(
  ST_GeomFromText('LINESTRING(0 0, 10 0)'), 
  ST_GeomFromText('POINT(11 0)'), 2));

-- http://sourceforge.net/mailarchive/forum.php?thread_name=4CE841B8.4090702@cgf.upv.es&forum_name=jts-topo-suite-user
select 't5', st_asewkt(st_snap(
  ST_GeomFromText('LINESTRING (70 250, 190 340)'),
  ST_GeomFromText('POLYGON ((230 340, 300 340, 300 240, 230 240, 230 340))'),50));
