DROP TABLE map_neatlines;
DROP TABLE named_places;
DROP TABLE ponds;
DROP TABLE buildings;
DROP TABLE streams;
DROP TABLE bridges;
DROP TABLE forests;
DROP TABLE divided_routes;
DROP TABLE road_segments;
DROP TABLE lakes;

DELETE FROM spatial_ref_sys WHERE srid=101;
