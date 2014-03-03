--Copyright © 2007 Open Geospatial Consortium, Inc. All Rights Reserved. OGC 06-104r4
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
	--g_catalog_name VARCHAR(256),
	--g_table_schema VARCHAR(256),
	--g_table_name VARCHAR(256),
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

-- Road Segment GeometrREATE TABLE road_segment_geom (
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
CREATE TABLE stream_geom (
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
