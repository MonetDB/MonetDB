-- Conformance Item T43
SELECT ST_Intersects(road_segments.centerline, divided_routes.centerlines) FROM road_segments, divided_routes WHERE road_segments.fid = 102 AND divided_routes.name = 'Route 75';
