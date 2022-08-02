-- Conformance Item T37
SELECT ST_Equals(boundary, ST_PolygonFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )',101)) FROM named_places WHERE name = 'Goose Island';
