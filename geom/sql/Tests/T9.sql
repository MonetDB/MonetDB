-- Conformance Item T9
SELECT AsText(PolyFromWKB(AsBinary(boundary),101)) FROM named_places WHERE name = 'Goose Island';
