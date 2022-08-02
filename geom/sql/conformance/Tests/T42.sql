-- Conformance Item T42
SELECT ST_Crosses(road_segments.centerline, divided_routes.centerlines) FROM road_segments, divided_routes WHERE road_segments.fid = 102 AND divided_routes.name = 'Route 75';
