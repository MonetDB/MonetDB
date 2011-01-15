-- The 2004 spec example
CREATE TABLE spatial_ref_sys (
	srid INTEGER NOT NULL PRIMARY KEY,
	auth_name VARCHAR(256),
	auth_srid INTEGER,
	srtext VARCHAR(2048));
-- Lakes
CREATE TABLE lakes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	shore POLYGON);
-- Road Segments
CREATE TABLE road_segments (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	aliases VARCHAR(64),
	num_lanes INTEGER,
	centerline LINESTRING);
-- Divided Routes
CREATE TABLE divided_routes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	num_lanes INTEGER,
	centerlines MULTILINESTRING);
-- Forests
CREATE TABLE forests (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	boundary MULTIPOLYGON);
-- Bridges
CREATE TABLE bridges (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	position POINT);
-- Streams
	CREATE TABLE streams (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	centerline LINESTRING);
-- Buildings
CREATE TABLE buildings (
	fid INTEGER NOT NULL PRIMARY KEY,
	address VARCHAR(64),
	position POINT,
	footprint POLYGON);
-- Ponds
CREATE TABLE ponds (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	type VARCHAR(64),
	shores MULTIPOYLGON);
-- Named Places
CREATE TABLE named_places (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	boundary POLYGON);
-- Map Neatline
CREATE TABLE map_neatlines (
	fid INTEGER NOT NULL PRIMARY KEY,
	neatline POLYGON);

-- B.3.3.2 Geometry types and functions schema data loading
-- Spatial Reference System
INSERT INTO spatial_ref_sys VALUES(101, 'POSC', 32214, 'PROJCS["UTM_ZONE_14N", GEOGCS["World
Geodetic System 72",DATUM["WGS_72", ELLIPSOID["NWL_10D", 6378135, 298.26]],PRIMEM["Greenwich",
0], UNIT["Meter", 1.0]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",
500000.0],PARAMETER["False_Northing", 0.0],PARAMETER["Central_Meridian", -
99.0],PARAMETER["Scale_Factor", 0.9996],PARAMETER["Latitude_of_origin", 0.0],UNIT["Meter",
1.0]]');
-- Lakes
INSERT INTO lakes VALUES (101, 'BLUE LAKE',
PolyFromText('POLYGON((52 18,66 23,73 9,48 6,52 18),
(59 18,67 18,67 13,59 13,59 18))', 101));
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
MLineFromText('MULTILINESTRING((10 48,10 21,10 0),
(16 0,16 23,16 48))', 101));
-- Forests
INSERT INTO forests VALUES(109, 'Green Forest',
MPolyFromText('MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),
(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))', 101));
-- Bridges
INSERT INTO bridges VALUES(110, 'Cam Bridge', PointFromText('POINT( 44 31 )', 101));
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
MPolyFromText('MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ),
( ( 26 44, 26 40, 28 42, 26 44) ) )', 101));
-- Named Places
INSERT INTO named_places VALUES(117, 'Ashton',
PolyFromText('POLYGON( ( 62 48, 84 48, 84 30, 56 30, 56 34, 62 48) )', 101));
INSERT INTO named_places VALUES(118, 'Goose Island',
PolyFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )', 101));
-- Map Neatlines
INSERT INTO map_neatlines VALUES(115,
PolyFromText('POLYGON( ( 0 0, 0 48, 84 48, 84 0, 0 0 ) )', 101));
B.3.3.3 Geometry types and functions schema test queries
-- Conformance Item T1
SELECT f_table_name
	FROM geometry_columns;
-- Conformance Item T2
SELECT f_geometry_column
	FROM geometry_columns
	WHERE f_table_name = 'streams';
-- Conformance Item T3
SELECT coord_dimension
	FROM geometry_columns
	WHERE f_table_name = 'streams';
-- Conformance Item T4
SELECT srid
	FROM geometry_columns
	WHERE f_table_name = 'streams';
-- Conformance Item T5
SELECT srtext
	FROM SPATIAL_REF_SYS
	WHERE SRID = 101;
-- Conformance Item T6
SELECT Dimension(shore)
	FROM lakes
	WHERE name = 'Blue Lake';
-- Conformance Item T7
SELECT GeometryType(centerlines)
	FROM lakes
	WHERE name = 'Route 75';
-- Conformance Item T8
SELECT AsText(boundary)
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T9
SELECT AsText(PolyFromWKB(AsBinary(boundary),101))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T10
SELECT SRID(boundary)
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T11
SELECT IsEmpty(centerline)
	FROM road_segments
	WHERE name = 'Route 5' AND aliases = 'Main Street';
-- Conformance Item T12
SELECT IsSimple(shore)
	FROM lakes
	WHERE name = 'Blue Lake';
-- Conformance Item T13
SELECT AsText(Boundary((boundary),101)
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T14
SELECT AsText(Envelope((boundary),101)
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T15
SELECT X(position)
	FROM bridges
	WHERE name = .Cam Bridge.;
-- Conformance Item T16
SELECT Y(position)
	FROM bridges
	WHERE name = 'Cam Bridge';
-- Conformance Item T17
SELECT AsText(StartPoint(centerline))
	FROM road_segments
	WHERE fid = 102;
-- Conformance Item T18
SELECT AsText(EndPoint(centerline))
	FROM road_segments
	WHERE fid = 102;
-- Conformance Item T19
SELECT IsClosed(LineFromWKB(AsBinary(Boundary(boundary)),SRID(boundary)))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T20
SELECT IsRing(LineFromWKB(AsBinary(Boundary(boundary)),SRID(boundary)))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T21
SELECT Length(centerline)
	FROM road_segments
	WHERE fid = 106;
-- Conformance Item T22
SELECT NumPoints(centerline)
	FROM road_segments
	WHERE fid = 102;
-- Conformance Item T23
SELECT AsText(PointN(centerline, 1))
	FROM road_segments
	WHERE fid = 102;
-- Conformance Item T24
SELECT AsText(Centroid(boundary))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T25
SELECT Contains(boundary, PointOnSurface(boundary))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T26
SELECT Area(boundary)
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T27
SELECT AsText(ExteriorRing(shore))
	FROM lakes
	WHERE name = 'Blue Lake';
-- Conformance Item T28
SELECT NumInteriorRing(shore)
	FROM lakes
	WHERE name = 'Blue Lake';
-- Conformance Item T29
SELECT AsText(InteriorRingN(shore, 1))
	FROM lakes
	WHERE name = 'Blue Lake';
-- Conformance Item T30
SELECT NumGeometries(centerlines)
	FROM divided_routes
	WHERE name = 'Route 75';
-- Conformance Item T31
SELECT AsText(GeometryN(centerlines, 2))
	FROM divided_routes
	WHERE name = 'Route 75';
-- Conformance Item T32
SELECT IsClosed(centerlines)
	FROM divided_routes
	WHERE name = 'Route 75';
-- Conformance Item T33
SELECT Length(centerlines)
	FROM divided_routes
	WHERE name = 'Route 75';
-- Conformance Item T34
SELECT AsText(Centroid(shores))
	FROM ponds
	WHERE fid = 120;
-- Conformance Item T35
SELECT Contains(shores, PointOnSurface(shores))
	FROM ponds
	WHERE fid = 120;
-- Conformance Item T36
SELECT Area(shores)
	FROM ponds
	WHERE fid = 120;
-- Conformance Item T37
SELECT Equals(boundary,
PolyFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )',1))
	FROM named_places
	WHERE name = 'Goose Island';
-- Conformance Item T38
SELECT Disjoint(centerlines, boundary)
	FROM divided_routes, named_places
	WHERE divided_routes.name = 'Route 75' AND named_places.name = 'Ashton';
-- Conformance Item T39
SELECT Touches(centerline, shore)
	FROM streams, lakes
	WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- Conformance Item T40
SELECT Within(boundary, footprint)
	FROM named_places, buildings
	WHERE named_places.name = 'Ashton' AND buildings.address = '215 Main Street';
-- Conformance Item T41
SELECT Overlaps(forests.boundary, named_places.boundary)
	FROM forests, named_places
	WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- Conformance Item T42
SELECT Crosses(road_segments.centerline, divided_routes.centerlines)
	FROM road_segments, divided_routes
	WHERE road_segment.fid = 102 AND divided_routes.name = 'Route 75';
-- Conformance Item T43
SELECT Intersects(road_segments.centerline, divided_routes.centerlines)
	FROM road_segments, divided_routes
	WHERE road_segments.fid = 102 AND divided_routes.name = 'Route 75';
-- Conformance Item T44
SELECT Contains(forests.boundary, named_places.boundary)
	FROM forests, named_places
	WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- Conformance Item T45
SELECT Relate(forests.boundary, named_places.boundary, 'TTTTTTTTT')
	FROM forests, named_places
	WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- Conformance Item T46
SELECT Distance(position, boundary)
	FROM bridges, named_places
	WHERE bridges.name = 'Cam Bridge' AND named_places.name = 'Ashton';
-- Conformance Item T47
SELECT AsText(Intersection(centerline, shore))
	FROM streams, lakes
	WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- Conformance Item T48
SELECT AsText(Difference(named_places.boundary, forests.boundary))
	FROM named_places, forests
	WHERE named_places.name = 'Ashton' AND forests.name = 'Green Forest';
-- Conformance Item T49
SELECT AsText(Union(shore, boundary))
	FROM lakes, named_places
	WHERE lakes.name = 'Blue Lake' AND named_places.name = .Goose Island.;
-- Conformance Item T50
SELECT AsText(SymDifference(shore, boundary))
	FROM lakes, named_places
	WHERE lakes.name = 'Blue Lake' AND named_places.name = 'Ashton';
-- Conformance Item T51
SELECT count(*)
	FROM buildings, bridges
	WHERE Contains(Buffer(bridges.position, 15.0), buildings.footprint) = 1;
-- Conformance Item T52
SELECT AsText(ConvexHull(shore))
	FROM lakes
	WHERE lakes.name = 'Blue Lake';
