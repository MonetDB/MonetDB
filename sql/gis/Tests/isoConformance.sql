-- Appendix B.3.1.2
-- CREATE SPATIAL_REF_SYS METADATA TABLE
CREATE TABLE spatial_ref_sys (
	srid INTEGER NOT NULL PRIMARY KEY,
	auth_name VARCHAR(256),
	auth_srid INTEGER,
	srtext VARCHAR(2048));

-- CREATE GEOMETRY_COLUMNS METADATA TABLE
CREATE TABLE geometry_columns (
	f_catalog_name VARCHAR(256),
	f_table_schema VARCHAR(256),
	f_table_name VARCHAR(256),
	f_geometry_column VARCHAR(256),
	g_catalog_name VARCHAR(256),
	g_table_schema VARCHAR(256),
	g_table_name VARCHAR(256),
	storage_type INTEGER,
	geometry_type INTEGER,
	coord_dimension INTEGER,
	max_ppr INTEGER,
	srid INTEGER REFERENCES spatial_ref_sys,
	CONSTRAINT gc_pk PRIMARY KEY (f_catalog_name, f_table_schema,
	f_table_name, f_geometry_column));

-- Create geometry tables
-- Lake Geometry
CREATE TABLE lake_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	x5 INTEGER,
	y5 INTEGER,
	CONSTRAINT l_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Road Segment Geometry
CREATE TABLE road_segment_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	CONSTRAINT rs_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Divided Route Geometry
CREATE TABLE divided_route_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	CONSTRAINT dr_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Forest Geometry
CREATE TABLE forest_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	x5 INTEGER,
	y5 INTEGER,
	CONSTRAINT f_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Bridge Geometry
CREATE TABLE bridge_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	CONSTRAINT b_gid_pk PRIMARY KEY (gid, eseq, seq));
-- Stream Geometry
CREATE TABLE mnstr_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	CONSTRAINT s_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Bulding Point Geometry
CREATE TABLE building_pt_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	CONSTRAINT bp_gid_pk PRIMARY KEY (gid, eseq, seq));
-- Bulding Area Geometry
CREATE TABLE building_area_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	x5 INTEGER,
	y5 INTEGER,
	CONSTRAINT ba_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Pond Geometry
CREATE TABLE pond_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	CONSTRAINT p_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Named Place Geometry
CREATE TABLE named_place_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	CONSTRAINT np_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Map Neatline Geometry
CREATE TABLE map_neatline_geom (
	gid INTEGER NOT NULL,
	eseq INTEGER NOT NULL,
	etype INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	x1 INTEGER,
	y1 INTEGER,
	x2 INTEGER,
	y2 INTEGER,
	x3 INTEGER,
	y3 INTEGER,
	x4 INTEGER,
	y4 INTEGER,
	x5 INTEGER,
	y5 INTEGER,
	CONSTRAINT mn_gid_pk PRIMARY KEY (gid, eseq, seq));

-- Lakes
CREATE TABLE lakes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	shore_gid INTEGER);

-- Road Segments
CREATE TABLE road_segments (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	aliases VARCHAR(64),
	num_lanes INTEGER,
	centerline_gid INTEGER);

-- Divided Routes
CREATE TABLE divided_routes (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	num_lanes INTEGER,
	centerlines_gid INTEGER);
-- Forests
CREATE TABLE forests (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	boundary_gid INTEGER);
-- Bridges
CREATE TABLE bridges (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	position_gid INTEGER);
-- Streams
CREATE TABLE streams (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	centerline_gid INTEGER);
-- Buildings
CREATE TABLE buildings (
	fid INTEGER NOT NULL PRIMARY KEY,
	address VARCHAR(64),
	position_gid INTEGER,
	footprint_gid INTEGER);
-- Ponds
CREATE TABLE ponds (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	type VARCHAR(64),
	shores_gid INTEGER);
-- Named Places
CREATE TABLE named_places (
	fid INTEGER NOT NULL PRIMARY KEY,
	name VARCHAR(64),
	boundary_gid INTEGER);
-- Map Neatline
CREATE TABLE map_neatlines (
	fid INTEGER NOT NULL PRIMARY KEY,
	neatline_gid INTEGER);

--B.3.1.3 Normalized Geometry schema data loading
--Spatial Reference System
INSERT INTO spatial_ref_sys VALUES(101, 'POSC', 32214, 'PROJCS["UTM_ZONE_14N", GEOGCS["World Geodetic
System 72",DATUM["WGS_72", ELLIPSOID["NWL_10D", 6378135, 298.26]],PRIMEM["Greenwich",
0],UNIT["Meter",1.0]],PROJECTION["Transverse_Mercator"],
PARAMETER["False_Easting", 500000.0],PARAMETER["False_Northing", 0.0],PARAMETER["Central_Meridian", -
99.0],PARAMETER["Scale_Factor", 0.9996],PARAMETER["Latitude_of_origin", 0.0],UNIT["Meter", 1.0]]');
-- Lakes
INSERT INTO lake_geom VALUES(101, 1, 5, 1, 52,18, 66,23, 73,9, 48,6, 52,18);
INSERT INTO lake_geom VALUES(101, 2, 5, 1, 59,18, 67,18, 67,13, 59,13, 59,18);
INSERT INTO lakes VALUES (101, 'BLUE LAKE', 101);
-- Road segments
INSERT INTO road_segment_geom VALUES (101, 1, 3, 1, 0,18, 10,21, 16,23);
INSERT INTO road_segment_geom VALUES (101, 1, 3, 2, 28,26, 44,31, NULL,NULL);
INSERT INTO road_segment_geom VALUES (102, 1, 3, 1, 44,31, 56,34, 70,38);
INSERT INTO road_segment_geom VALUES (103, 1, 3, 1, 70,38, 72,48, NULL,NULL);
INSERT INTO road_segment_geom VALUES (104, 1, 3, 1, 70,38, 84,42, NULL,NULL);
INSERT INTO road_segment_geom VALUES (105, 1, 3, 1, 28,26, 28,0, NULL,NULL);
INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2, 101);
INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4, 102);
INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2, 103);
INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4, 104);
INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL, 1, 105);
-- DividedRoutes
INSERT INTO divided_route_geom VALUES(101, 1, 9, 1, 10,48, 10,21, 10,0);
INSERT INTO divided_route_geom VALUES(101, 2, 9, 1, 16,0, 10,23, 16,48);
INSERT INTO divided_routes VALUES(119, 'Route 75', 4, 101);
ISO/DIS 19125-2
© ISO 2000 . All rights reserved 43
-- Forests
INSERT INTO forest_geom VALUES(101, 1, 11, 1, 28,26, 28,0, 84,0, 84,42, 28,26);
INSERT INTO forest_geom VALUES(101, 1, 11, 2, 52,18, 66,23, 73,9, 48,6, 52,18);
INSERT INTO forest_geom VALUES(101, 2, 11, 1, 59,18, 67,18, 67,13, 59,13, 59,18);
INSERT INTO forests VALUES(109, 'Green Forest', 101);
-- Bridges
INSERT INTO bridge_geom VALUES(101, 1, 1, 1, 44, 31);
INSERT INTO bridges VALUES(110, 'Cam Bridge', 101);
-- Streams
INSERT INTO mnstr_geom VALUES(101, 1, 3, 1, 38,48, 44,41, 41,36);
INSERT INTO mnstr_geom VALUES(101, 1, 3, 2, 44,31, 52,18, NULL,NULL);
INSERT INTO mnstr_geom VALUES(102, 1, 3, 1, 76,0, 78,4, 73,9 );
--
INSERT INTO streams VALUES(111, 'Cam Stream', 101);
INSERT INTO streams VALUES(112, 'Cam Stream', 102);
-- Buildings
INSERT INTO building_pt_geom VALUES(101, 1, 1, 1, 52,30);
INSERT INTO building_pt_geom VALUES(102, 1, 1, 1, 64,33);
INSERT INTO building_area_geom VALUES(101, 1, 5, 1, 50,31, 54,31,
54,29, 50,29, 50,31);
INSERT INTO building_area_geom VALUES(102, 1, 5, 1, 66,34, 62,34, 62,32,
66,32, 66,34);
INSERT INTO buildings VALUES(113, '123 Main Street', 101, 101);
INSERT INTO buildings VALUES(114, '215 Main Street', 102, 102);
-- Ponds
INSERT INTO pond_geom VALUES(101, 1, 11, 1, 24,44, 22,42, 24,40, 24,44 );
INSERT INTO pond_geom VALUES(101, 2, 11, 1, 26,44, 26,40, 28,42, 26,44 );
INSERT INTO ponds VALUES(120, NULL, 'Stock Pond', 101);
-- Named Places
INSERT INTO named_place_geom VALUES(101, 1, 5, 1, 62,48, 84,48, 84,30, 56,30);
INSERT INTO named_place_geom VALUES(101, 1, 5, 2, 56,30, 56,34, 62,48, NULL,NULL);
INSERT INTO named_place_geom VALUES(102, 1, 5, 1, 67,13, 67,18, 59,18, 59,13);
INSERT INTO named_place_geom VALUES(102, 1, 5, 2, 59,13, 67,13, NULL,NULL, NULL,NULL);
INSERT INTO named_places VALUES(117, 'Ashton', 101);
INSERT INTO named_places VALUES(118, 'Goose Island', 102);
-- Map Neatlines
INSERT INTO map_neatline_geom VALUES(101, 1, 5, 1, 0,0, 0,48, 84,48, 84,0, 0,0);
INSERT INTO map_neatlines VALUES(115, 101);
-- Geometry Columns
INSERT INTO geometry_columns VALUES ('lakes', 'shore_gid',
'lake_geom',0, 5, 2, 5, 101);
INSERT INTO geometry_columns VALUES ('road_segments', 'centerline_gid',
'road_segment_geom',0, 3, 2, 3, 101);
INSERT INTO geometry_columns VALUES ('divided_routes', 'centerlines_gid',
'divided_route_geom',0, 9, 2, 3, 101);
INSERT INTO geometry_columns VALUES ('forests', 'boundary_gid',
'forest_geom',0, 11, 2, 5, 101);
INSERT INTO geometry_columns VALUES ('bridges', 'position_gid',
'bridge_geom',0, 1, 2, 1, 101);
INSERT INTO geometry_columns VALUES ('streams', 'centerline_gid',
'mnstr_geom',0, 3, 2, 3, 101);
INSERT INTO geometry_columns VALUES ('buildings', 'position_gid',
'building_pt_geom',0, 1, 2, 1, 101);
INSERT INTO geometry_columns VALUES ('buildings', 'footprint_gid',
'building_area_geom',0, 5, 2, 5, 101);
INSERT INTO geometry_columns VALUES ('ponds', 'shores_gid',
'pond_geom',0, 11, 2, 4, 101);
INSERT INTO geometry_columns VALUES ('named_places', 'boundary_gid',
'named_place_geom',0, 5, 2, 4, 101);
INSERT INTO geometry_columns VALUES ('map_neatlines', 'neatline_gid',
'map_neatline_geom',0, 5, 2, 5, 101);
B.3.1.4 Normalized Geometry schema test queries
-- Conformance Item N1
SELECT f_table_name
FROM geometry_columns;
-- Conformance Item N2
SELECT g_table_name
FROM geometry_columns;
-- Conformance Item N3
SELECT storage_type
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N4
SELECT geometry_type
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N5
SELECT coord_dimension
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N6
SELECT max_ppr
FROM geometry_columns
WHERE f_table_name = 'streams';
