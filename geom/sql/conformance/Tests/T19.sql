-- Conformance Item T19
SELECT ST_IsClosed(ST_LineFromWKB(ST_AsBinary(ST_Boundary(ST_WKBToSQL(boundary))),ST_SRID(boundary))) FROM named_places WHERE name = 'Goose Island';
