SELECT count(*)
FROM buildings, bridges
WHERE Contains(Buffer(bridges."position", 15.0), buildings.footprint) = 1;
-- Conformance Item T52
