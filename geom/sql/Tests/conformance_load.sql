-- C.3.3.1 Geometry types and functions schema construction

CREATE TABLE spatial_ref_sys (
	srid INTEGER NOT NULL PRIMARY KEY,
	auth_name CHARACTER LARGE OBJECT,
	auth_srid INTEGER,
	srtext CHARACTER VARYING(2048));
-- Geometry Columns
CREATE TABLE geometry_columns (
	f_table_schema CHARACTER LARGE OBJECT,
	f_table_name CHARACTER LARGE OBJECT,
	f_geometry_column CHARACTER LARGE OBJECT,
	g_table_schema CHARACTER LARGE OBJECT,
	g_table_name CHARACTER LARGE OBJECT,
	storage_type INTEGER,
	geometry_type INTEGER,
	coord_dimension INTEGER,
	max_ppr INTEGER,
	srid INTEGER REFERENCES spatial_ref_sys,
	CONSTRAINT gc_pk PRIMARY KEY (f_table_schema, f_table_name, f_geometry_column));
-- Lakes
CREATE TABLE lakes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	shore POLYGON);
-- Road Segments
CREATE TABLE road_segments (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	aliases CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerline LINESTRING);
-- Divided Routes
CREATE TABLE divided_routes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	num_lanes INTEGER,
	centerlines MULTILINESTRING);
-- Forests
CREATE TABLE forests (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	boundary MULTIPOLYGON);
-- Bridges
CREATE TABLE bridges (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	"position" POINT);
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
-- Ponds
CREATE TABLE ponds (
	fid INTEGER NOT NULL PRIMARY KEY,
	name CHARACTER VARYING(64),
	type CHARACTER VARYING(64),
	shores MULTIPOLYGON);
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

-- Spatial Reference System
INSERT INTO spatial_ref_sys VALUES
(101, 'POSC', 32214, 'PROJCS["UTM_ZONE_14N",
GEOGCS["World Geodetic System 72",
DATUM["WGS_72",
ELLIPSOID["NWL_10D", 6378135, 298.26]],
PRIMEM["Greenwich", 0],
UNIT["Meter", 1.0]],
PROJECTION["Transverse_Mercator"],
PARAMETER["False_Easting", 500000.0],
PARAMETER["False_Northing", 0.0],
PARAMETER["Central_Meridian", -99.0],
PARAMETER["Scale_Factor", 0.9996],
PARAMETER["Latitude_of_origin", 0.0],
UNIT["Meter", 1.0]]');
-- Lakes
INSERT INTO lakes VALUES (
101, 'Blue Lake',
PolyFromText(
'POLYGON(
(52 18,66 23,73 9,48 6,52 18),
(59 18,67 18,67 13,59 13,59 18)
)', 101));
-- Road segments
INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2,
LineFromText('LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )' ,101));
INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4,
LineFromText('LINESTRING( 44 31, 56 34, 70 38 )' ,101));
INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2,
LineFromText('LINESTRING( 70 38, 72 48 )' ,101));
INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4,
LineFromText('LINESTRING( 70 38, 84 42 )' ,101));
INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL, 1,
LineFromText('LINESTRING( 28 26, 28 0 )',101));
-- DividedRoutes
INSERT INTO divided_routes VALUES(119, 'Route 75', 4,
MLineFromText(
'MULTILINESTRING((10 48,10 21,10 0),
(16 0,16 23,16 48))', 101));
-- Forests
INSERT INTO forests VALUES(109, 'Green Forest',
MPolyFromText(
'MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),
(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))', 101));
-- Bridges
INSERT INTO bridges VALUES(110, 'Cam Bridge',
PointFromText('POINT( 44 31 )', 101));
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
INSERT INTO geometry_columns VALUES ('schema', 'lakes', 'shore', 'schema', 'lake_geom',NULL, 5, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'road_segments', 'centerline', 'schema', 'road_segment_geom',NULL, 3, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'divided_routes', 'centerlines', 'schema', 'divided_route_geom',NULL, 9, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'forests', 'boundary', 'schema', 'forest_geom',NULL, 11, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'bridges', 'position', 'schema', 'bridge_geom',NULL, 1, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'streams', 'centerline', 'schema', 'stream_geom',NULL, 3, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'buildings', 'position', 'schema', 'building_pt_geom',NULL, 1, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'buildings', 'footprint', 'schema', 'building_area_geom',NULL, 5, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'ponds', 'shores', 'schema', 'pond_geom',NULL, 11, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'named_places', 'boundary', 'schema', 'named_place_geom',NULL, 5, 2, 0, 101);
INSERT INTO geometry_columns VALUES ('schema', 'map_neatlines', 'neatline', 'schema', 'map_neatline_geom',NULL, 5, 2, 0, 101);
