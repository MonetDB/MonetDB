-- Conformance Item T51
SELECT count(*) FROM buildings, bridges WHERE ST_Contains(ST_Buffer(bridges."position", 15.0), buildings.footprint) = 1;
