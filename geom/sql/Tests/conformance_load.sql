-- C.3.3.1 Geometry types and functions schema construction

-- POINT (1)
-- Bridges 2D
CREATE TABLE bridges (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	"position" POINT);
INSERT INTO geometry_columns VALUES ('schema', 'bridges', 'position', 'schema', 'bridge_geom',NULL, 1, 2, 0, 101);
INSERT INTO bridges VALUES(110, 'Cam Bridge', PointFromText('POINT( 44 31 )', 101));
-- Bridges 3D
CREATE TABLE bridges3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	"position" POINT);
INSERT INTO geometry_columns VALUES ('schema', 'bridges3d', 'position', 'schema', 'bridge3d_geom',NULL, 1, 3, 0, 101);
INSERT INTO bridges3d VALUES(110, 'Cam Bridge', PointFromText('POINT( 44 31 49)', 101));

-- LINESTRING (2)
-- Road Segments 2D
CREATE TABLE road_segments (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	aliases CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerline LINESTRING);
INSERT INTO geometry_columns VALUES ('schema', 'road_segments', 'centerline', 'schema', 'road_segment_geom',NULL, 2, 2, 0, 101);
INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2, LineFromText('LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )' ,101));
INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4, LineFromText('LINESTRING( 44 31, 56 34, 70 38 )' ,101));
INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2, LineFromText('LINESTRING( 70 38, 72 48 )' ,101));
INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4, LineFromText('LINESTRING( 70 38, 84 42 )' ,101));
INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL, 1, LineFromText('LINESTRING( 28 26, 28 0 )',101));
-- Road Segments 3D
CREATE TABLE road_segments3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	aliases CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerline LINESTRING);
INSERT INTO geometry_columns VALUES ('schema', 'road_segments3d', 'centerline', 'schema', 'road_segment3d_geom',NULL, 2, 3, 0, 101);
INSERT INTO road_segments3d VALUES(102, 'Route 5', NULL, 2, LineFromText('LINESTRING( 0 18 10, 21 16 23, 28 26 44 )' ,101));
INSERT INTO road_segments3d VALUES(103, 'Route 5', 'Main Street', 4, LineFromText('LINESTRING( 44 31 56, 34 70 38 )' ,101));
INSERT INTO road_segments3d VALUES(104, 'Route 5', NULL, 2, LineFromText('LINESTRING( 70 38 72, 48 12 56 )' ,101));
INSERT INTO road_segments3d VALUES(105, 'Main Street', NULL, 4, LineFromText('LINESTRING( 70 38 56, 84 42 12)' ,101));
INSERT INTO road_segments3d VALUES(106, 'Dirt Road by Green Forest', NULL, 1, LineFromText('LINESTRING( 28 26 28, 45 13 75 )',101));

-- LINEARRING

-- POLYGON (3)
-- Lakes 2D
CREATE TABLE lakes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	shore POLYGON);
INSERT INTO geometry_columns VALUES ('schema', 'lakes', 'shore', 'schema', 'lake_geom',NULL, 3, 2, 0, 101);
INSERT INTO lakes VALUES (101, 'Blue Lake', PolyFromText('POLYGON((52 18,66 23,73 9,48 6,52 18),(59 18,67 18,67 13,59 13,59 18))', 101));
-- Lakes 3D
CREATE TABLE lakes3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	shore POLYGON);
INSERT INTO geometry_columns VALUES ('schema', 'lakes3d', 'shore', 'schema', 'lake3d_geom',NULL, 3, 3, 0, 101);
INSERT INTO lakes3d VALUES (101, 'Blue Lake', PolyFromText('POLYGON((52 18 66, 23 73 9,48 6 52, 52 18 66),(59 18 67, 18 67 13,59 13 59, 59 18 67))', 101)); 

-- MULTIPOINT (4)
-- Forest Trees 2D
CREATE TABLE forest_trees (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	positions MULTIPOINT);
INSERT INTO geometry_columns VALUES ('schema', 'forest_trees', 'positions', 'schema', 'forest_tree_geom',NULL, 4, 2, 0, 101);
INSERT INTO forest_trees VALUES (109, 'Green Forest', MPointFromText('MULTIPOINT ((12 45), (32 28), (17 36), (32 8))', 101));
-- Forest Trees 3D
CREATE TABLE forest_trees3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	positions MULTIPOINT);
INSERT INTO geometry_columns VALUES ('schema', 'forest_trees3d', 'positions', 'schema', 'forest_tree3d_geom',NULL, 4, 3, 0, 101);
INSERT INTO forest_trees3d VALUES (109, 'Green Forest', MPointFromText('MULTIPOINT ((12 45 37), (32 28 42), (17 36 12), (32 8 0))', 101));

-- MULTILINESTRING (5)
-- Divided Routes 2D
CREATE TABLE divided_routes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerlines MULTILINESTRING);
INSERT INTO geometry_columns VALUES ('schema', 'divided_routes', 'centerlines', 'schema', 'divided_route_geom',NULL, 5, 2, 0, 101);
INSERT INTO divided_routes VALUES(119, 'Route 75', 4, MLineFromText('MULTILINESTRING((10 48,10 21,10 0),(16 0,16 23,16 48))', 101));
-- Divided Routes 3D
CREATE TABLE divided_routes3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerlines MULTILINESTRING);
INSERT INTO geometry_columns VALUES ('schema', 'divided_routes3d', 'centerlines', 'schema', 'divided_route3d_geom',NULL, 5, 3, 0, 101);
INSERT INTO divided_routes3d VALUES(119, 'Route 75', 4, MLineFromText('MULTILINESTRING((10 48 10, 21 10 0),(16 0 16, 23 16 48))', 101));

-- MULTIPOLYGON (6)
-- forests 2D
CREATE TABLE forests (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	boundary MULTIPOLYGON);
INSERT INTO geometry_columns VALUES ('schema', 'forests', 'boundary', 'schema', 'forest_geom',NULL, 6, 2, 0, 101);
INSERT INTO forests VALUES(109, 'Green Forest', 
	MPolyFromText('MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))', 101));
-- forests 3D
CREATE TABLE forests3d (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	boundary MULTIPOLYGON);
INSERT INTO geometry_columns VALUES ('schema', 'forests3d', 'boundary', 'schema', 'forest3d_geom',NULL, 6, 2, 0, 101);
INSERT INTO forests3d VALUES(109, 'Green Forest', 
	MPolyFromText('MULTIPOLYGON(((28 26 28, 0 84 0,84 42 28, 28 26 28),(52 18 66, 23 73 9,48 6 52, 52 18 66)),((59 18 67, 18 67 13, 59 13 59, 59 18 67)))', 101));


-- GEOMETRYCOLLECTION (7)





-- Ponds
CREATE TABLE ponds (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	type CHARACTER VARYING(64),
	shores MULTIPOLYGON);
-- Streams
CREATE TABLE streams (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	centerline LINESTRING);
-- Buildings
CREATE TABLE buildings (
	fid INTEGER NOT NULL PRIMARY KEY,
	address CHARACTER VARYING(64),
	"position" POINT,
	footprint POLYGON);
-- Named Places
CREATE TABLE named_places (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	boundary POLYGON);
-- Map Neatline
CREATE TABLE map_neatlines (
	fid INTEGER NOT NULL PRIMARY KEY,
	neatline POLYGON);


-- C.3.3.2 Geometry types and functions schema data loading


-- Streams
INSERT INTO streams VALUES(111, 'Cam Stream',
LineFromText('LINESTRING( 38 48, 44 41, 41 36, 44 31, 52 18 )', 101));
INSERT INTO streams VALUES(112, NULL,
LineFromText('LINESTRING( 76 0, 78 4, 73 9 )', 101));
-- Buildings
INSERT INTO buildings VALUES(113, '123 Main Street',
PointFromText('POINT( 52 30 )', 101),
PolyFromText('POLYGON( ( 50 31, 54 31, 54 29, 50 29, 50 31) )', 101));
INSERT INTO buildings VALUES(114, '215 Main Street',
PointFromText('POINT( 64 33 )', 101),
PolyFromText('POLYGON( ( 66 34, 62 34, 62 32, 66 32, 66 34) )', 101));
-- Ponds
INSERT INTO ponds VALUES(120, NULL, 'Stock Pond',
MPolyFromText(
'MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ),
( ( 26 44, 26 40, 28 42, 26 44) ) )', 101));
-- Named Places
INSERT INTO named_places VALUES(117, 'Ashton',
PolyFromText('POLYGON( ( 62 48, 84 48, 84 30, 56 30, 56 34, 62 48) )', 101));
INSERT INTO named_places VALUES(118, 'Goose Island',
PolyFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )', 101));
-- Map Neatlines
INSERT INTO map_neatlines VALUES(115,
PolyFromText('POLYGON( ( 0 0, 0 48, 84 48, 84 0, 0 0 ) )', 101));
--Geometry Columns
INSERT INTO geometry_columns VALUES ('schema', 'ponds', 'shores', 'schema', 'pond_geom',NULL, 11, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'streams', 'centerline', 'schema', 'stream_geom',NULL, 3, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'buildings', 'position', 'schema', 'building_pt_geom',NULL, 1, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'buildings', 'footprint', 'schema', 'building_area_geom',NULL, 5, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'named_places', 'boundary', 'schema', 'named_place_geom',NULL, 5, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'map_neatlines', 'neatline', 'schema', 'map_neatline_geom',NULL, 5, 2, 0, 101);
