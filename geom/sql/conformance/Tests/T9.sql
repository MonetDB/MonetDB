-- Conformance Item T9
SELECT ST_AsText(ST_PolygonFromWKB(ST_AsBinary(ST_WKBToSQL(boundary)),101)) FROM named_places WHERE name = 'Goose Island';
