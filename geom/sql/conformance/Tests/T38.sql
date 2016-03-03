-- Conformance Item T38
SELECT ST_Disjoint(centerlines, boundary) FROM divided_routes, named_places WHERE divided_routes.name = 'Route 75' AND named_places.name = 'Ashton';
