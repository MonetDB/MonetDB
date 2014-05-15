-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- make sure you load the geom module before loading this sql module
CREATE TYPE Point EXTERNAL NAME wkb;
CREATE TYPE Curve EXTERNAL NAME wkb;
CREATE TYPE LineString EXTERNAL NAME wkb;
CREATE TYPE Surface EXTERNAL NAME wkb;
CREATE TYPE Polygon EXTERNAL NAME wkb;

CREATE TYPE MultiPoint EXTERNAL NAME wkb;
CREATE TYPE MultiCurve EXTERNAL NAME wkb;
CREATE TYPE MultiLineString EXTERNAL NAME wkb;
CREATE TYPE MultiSurface EXTERNAL NAME wkb;
CREATE TYPE MultiPolygon EXTERNAL NAME wkb;

CREATE TYPE Geometry EXTERNAL NAME wkb;
CREATE TYPE GeomCollection EXTERNAL NAME wkb;

CREATE TYPE mbr EXTERNAL NAME mbr;

-- currently we only use mbr instead of
-- Envelope():Geometry
-- as that returns Geometry objects, and we prefer the explicit mbr's
-- minimum bounding rectangle (mbr)
CREATE FUNCTION mbr (g Geometry) RETURNS mbr external name geom.mbr;

CREATE FUNCTION mbroverlaps(a mbr, b mbr) RETURNS BOOLEAN external name geom."mbroverlaps";

-- Geometry Constructors
--CREATE FUNCTION ST_BdPolyFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."BdPolyFromText"; 
--CREATE FUNCTION ST_BdMPolyFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."BdMPolyFromText";
--CREATE FUNCTION ST_GeogFromText(wkt string) RETURNS Geometry external name geom."GeogFromText";
--CREATE FUNCTION ST_GeographyFromText(wkt string) RETURNS Geometry external name geom."GeographyFromText";
--CREATE FUNCTION ST_GeogFromWKB
--CREATE FUNCTION ST_GeomCollFromText
--CREATE FUNCTION ST_GeomFromWKB
--CREATE FUNCTION ST_GeomFromEWKT
--CREATE FUNCTION ST_GeometryFromText
--CREATE FUNCTION ST_GeomFromGML
--CREATE FUNCTION ST_GeomFromGeoJSON
--CREATE FUNCTION ST_GeomFromKML
--CREATE FUNCTION ST_GMLToSQL
--CREATE FUNCTION ST_GeomFromText
--CREATE FUNCTION ST_GeomFromWKB
--CREATE FUNCTION ST_LineFromMultiPoint
--CREATE FUNCTION ST_LineFromText
--CREATE FUNCTION ST_LineFromWKB
--CREATE FUNCTION ST_LinestringFromWKB
--CREATE FUNCTION ST_MakeBox2D
--CREATE FUNCTION ST_3DMakeBox
--CREATE FUNCTION ST_MakeLine
--CREATE FUNCTION ST_MakeEnvelope
--CREATE FUNCTION ST_MakePolygon
--CREATE FUNCTION ST_MakePoint
--CREATE FUNCTION ST_MakePointM
--CREATE FUNCTION ST_MLineFromText
--CREATE FUNCTION ST_MPointFromText
--CREATE FUNCTION ST_MPolyFromText
--CREATE FUNCTION ST_Point
--CREATE FUNCTION ST_PointFromText
--CREATE FUNCTION ST_PointFromWKB
--CREATE FUNCTION ST_Polygon
--CREATE FUNCTION ST_PolygonFromText
--CREATE FUNCTION ST_WKBToSQL
--CREATE FUNCTION ST_WKTToSQL
CREATE FUNCTION GeomFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."GeomFromText";
CREATE FUNCTION PointFromText(wkt string, srid SMALLINT) RETURNS Point external name geom."PointFromText";
CREATE FUNCTION LineFromText(wkt string, srid SMALLINT) RETURNS LineString external name geom."LineFromText";
CREATE FUNCTION PolyFromText(wkt string, srid SMALLINT) RETURNS Polygon external name geom."PolyFromText";
CREATE FUNCTION MPointFromText(wkt string, srid SMALLINT) RETURNS MultiPoint external name geom."MultiPointFromText";
CREATE FUNCTION MLineFromText(wkt string, srid SMALLINT) RETURNS MultiLineString external name geom."MultiLineFromText";
CREATE FUNCTION MPolyFromText(wkt string, srid SMALLINT) RETURNS MultiPolygon external name geom."MultiPolyFromText";
CREATE FUNCTION GeomCollectionFromText(wkt string, srid SMALLINT) RETURNS MultiPolygon external name geom."GeomCollectionFromText";
-- alias
CREATE FUNCTION PolygonFromText(wkt string, srid SMALLINT) RETURNS Polygon external name geom."PolyFromText";

CREATE FUNCTION AsText(g Geometry) RETURNS STRING external name geom."AsText";

CREATE FUNCTION X(g Geometry) RETURNS double external name geom."X";
CREATE FUNCTION Y(g Geometry) RETURNS double external name geom."Y";

CREATE FUNCTION Point(x double,y double) RETURNS Point external name geom.point;

-- CREATE FUNCTION Point(g Geometry) RETURNS Point external name geom.point;
-- CREATE FUNCTION Curve(g Geometry) RETURNS Curve external name geom.curve;
-- CREATE FUNCTION LineString(g Geometry) RETURNS LineString external name geom.linestring;
-- CREATE FUNCTION Surface(g Geometry) RETURNS Surface external name geom.surface;
-- CREATE FUNCTION Polygon(g Geometry) RETURNS Polygon external name geom.polygon;

-- ogc basic methods
CREATE FUNCTION Dimension(g Geometry) RETURNS integer external name geom."Dimension";
CREATE FUNCTION GeometryTypeId(g Geometry) RETURNS integer external name geom."GeometryTypeId";
CREATE FUNCTION SRID(g Geometry) RETURNS integer external name geom."SRID";
CREATE FUNCTION Envelope(g Geometry) RETURNS Geometry external name geom."Envelope";
CREATE FUNCTION IsEmpty(g Geometry) RETURNS BOOLEAN external name geom."IsEmpty";
CREATE FUNCTION IsSimple(g Geometry) RETURNS BOOLEAN external name geom."IsSimple";
CREATE FUNCTION Boundary(g Geometry) RETURNS Geometry external name geom."Boundary";

-- ogc spatial relation methods
CREATE FUNCTION Equals(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Equals";
CREATE FUNCTION Disjoint(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Disjoint";
CREATE FUNCTION "Intersect"(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Intersect";
CREATE FUNCTION Touches(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Touches";
CREATE FUNCTION Crosses(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Crosses";
CREATE FUNCTION Within(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Within";
CREATE FUNCTION Contains(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Contains";
CREATE FUNCTION Overlaps(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Overlaps";
CREATE FUNCTION Relate(a Geometry, b Geometry, pattern STRING) RETURNS BOOLEAN external name geom."Relate";

-- ogc Spatial Analysis methods
CREATE FUNCTION Area(g Geometry) RETURNS FLOAT external name geom."Area";
CREATE FUNCTION Length(g Geometry) RETURNS FLOAT external name geom."Length";
CREATE FUNCTION Distance(a Geometry, b Geometry) RETURNS FLOAT external name geom."Distance";
CREATE FUNCTION Buffer(a Geometry, distance FLOAT) RETURNS Geometry external name geom."Buffer";
CREATE FUNCTION ConvexHull(a Geometry) RETURNS Geometry external name geom."ConvexHull";
CREATE FUNCTION Intersection(a Geometry, b Geometry) RETURNS Geometry external name geom."Intersection";
CREATE FUNCTION "Union"(a Geometry, b Geometry) RETURNS Geometry external name geom."Union";
CREATE FUNCTION Difference(a Geometry, b Geometry) RETURNS Geometry external name geom."Difference";
CREATE FUNCTION SymDifference(a Geometry, b Geometry) RETURNS Geometry external name geom."SymDifference";


CREATE FUNCTION Centroid(g Geometry) RETURNS Geometry external name geom."Centroid";
CREATE FUNCTION StartPoint(g Geometry) RETURNS Geometry external name geom."StartPoint";
CREATE FUNCTION EndPoint(g Geometry) RETURNS Geometry external name geom."EndPoint";
CREATE FUNCTION NumPoints(g Geometry) RETURNS integer external name geom."NumPoints";
CREATE FUNCTION PointN(g Geometry, n SMALLINT) RETURNS Geometry external name geom."PointN";

-- create metadata tables
-- CREATE SPATIAL_REF_SYS METADATA TABLE
--CREATE TABLE spatial_ref_sys (
--        srid INTEGER NOT NULL PRIMARY KEY,
--        auth_name CHARACTER LARGE OBJECT,
--        auth_srid INTEGER,
--        srtext CHARACTER VARYING(2048));

-- CREATE GEOMETRY_COLUMNS METADATA TABLE
f_table_catalog ==> 
f_table_schema ==> 
f_table_name ==>
f_geometry_column ==> 
coord_dimension ==>
srid ==>

create view geometry_columns as
	select e.value as f_table_catalog,
		s.name as f_table_schema,
		y.f_table_name, y.f_geometry_column
	from schemas s, environment e, (
		select t.schema_id,
			t.name as f_table_name,
			x.name as f_geometry_column
		from tables t, (
			select name, table_id
			from columns
			where type in (
					select distinct sqlname
					from types
					where systemname='wkb'
					)
			) as x
		where t.id=x.table_id
		) y
	where y.schema_id=s.id and e.name='gdk_dbname';
--CREATE TABLE geometry_columns (
--	f_catalog_name CHARACTER LARGE OBJECT,
--        f_table_schema CHARACTER LARGE OBJECT,
--        f_table_name CHARACTER LARGE OBJECT,
--        f_geometry_column CHARACTER LARGE OBJECT,
--	g_catalog_name CHARACTER LARGE OBJECT,
--        g_table_schema CHARACTER LARGE OBJECT,
--        g_table_name CHARACTER LARGE OBJECT,
--        storage_type INTEGER,
--        geometry_type INTEGER,
--        coord_dimension INTEGER,
--        max_ppr INTEGER,
--        srid INTEGER REFERENCES spatial_ref_sys,
--        CONSTRAINT gc_pk PRIMARY KEY (f_table_schema, f_table_name, f_geometry_column));

--INSERT INTO spatial_ref_sys VALUES (101, 'POSC', 32214, 'PROJCS["UTM_ZONE_14N", GEOGCS["World Geodetic System 72",
--DATUM["WGS_72", ELLIPSOID["NWL_10D", 6378135, 298.26]], PRIMEM["Greenwich", 0], UNIT["Meter", 1.0]],
--PROJECTION["Transverse_Mercator"], PARAMETER["False_Easting", 500000.0], PARAMETER["False_Northing", 0.0],
--PARAMETER["Central_Meridian", -99.0], PARAMETER["Scale_Factor", 0.9996], PARAMETER["Latitude_of_origin", 0.0],
--UNIT["Meter", 1.0]]');
