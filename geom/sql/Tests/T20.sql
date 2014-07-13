-- Conformance Item T20
SELECT IsRing(LineFromWKB(AsBinary(Boundary(boundary)),SRID(boundary))) FROM named_places WHERE name = 'Goose Island';
