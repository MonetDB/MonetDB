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

--CREATE TYPE Point EXTERNAL NAME wkb;
--CREATE TYPE Curve EXTERNAL NAME wkb;
--CREATE TYPE LineString EXTERNAL NAME wkb;
--CREATE TYPE Surface EXTERNAL NAME wkb;
--CREATE TYPE Polygon EXTERNAL NAME wkb;

--CREATE TYPE MultiPoint EXTERNAL NAME wkb;
--CREATE TYPE MultiCurve EXTERNAL NAME wkb;
--CREATE TYPE MultiLineString EXTERNAL NAME wkb;
--CREATE TYPE MultiSurface EXTERNAL NAME wkb;
--CREATE TYPE MultiPolygon EXTERNAL NAME wkb;

CREATE TYPE Geometry EXTERNAL NAME wkb;
CREATE TYPE GeometrySubtype EXTERNAL NAME wkb;

--CREATE TYPE GeomCollection EXTERNAL NAME wkb;

CREATE TYPE mbr EXTERNAL NAME mbr;


CREATE FUNCTION Has_Z(info integer) RETURNS integer EXTERNAL NAME geom."hasZ";
CREATE FUNCTION Has_M(info integer) RETURNS integer EXTERNAL NAME geom."hasM";
CREATE FUNCTION get_type(info integer) RETURNS string EXTERNAL NAME geom."getType";

-- currently we only use mbr instead of
-- Envelope():Geometry
-- as that returns Geometry objects, and we prefer the explicit mbr's
-- minimum bounding rectangle (mbr)
--//CREATE FUNCTION mbr (g Geometry) RETURNS mbr external name geom.mbr;

--//CREATE FUNCTION mbroverlaps(a mbr, b mbr) RETURNS BOOLEAN external name geom."mbroverlaps";

CREATE FUNCTION ST_AsText(g Geometry) RETURNS string external name geom."AsText";

---------------------------------------------- Geometry Constructors -----------------------------------------------------------------------
-- Create Geometry from text (wkt)
CREATE FUNCTION ST_GeomFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
CREATE FUNCTION ST_GeometryFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
CREATE FUNCTION ST_PointFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."PointFromText"; 
CREATE FUNCTION ST_LineFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."LineFromText";
CREATE FUNCTION ST_PolygonFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."PolygonFromText";
CREATE FUNCTION ST_MPointFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPointFromText";
CREATE FUNCTION ST_MLineFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MLineFromText";
CREATE FUNCTION ST_MPolyFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPolyFromText";
CREATE FUNCTION ST_GeomCollFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomCollFromText";
--CREATE FUNCTION ST_BdPolyFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."BdPolyFromText"; 
--CREATE FUNCTION ST_BdMPolyFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."BdMPolyFromText";

CREATE FUNCTION ST_GeomFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
CREATE FUNCTION ST_GeometryFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
CREATE FUNCTION ST_WKTToSQL(wkt string) RETURNS Geometry external name geom."GeomFromText";
CREATE FUNCTION ST_PointFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."PointFromText"; 
CREATE FUNCTION ST_LineFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."LineFromText";
CREATE FUNCTION ST_PolygonFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."PolygonFromText";
CREATE FUNCTION ST_MPointFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MPointFromText";
CREATE FUNCTION ST_MLineFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MLineFromText";
CREATE FUNCTION ST_MPolyFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MPolyFromText";
CREATE FUNCTION ST_GeomCollFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomCollFromText";
--CREATE FUNCTION ST_GeogFromText(wkt string) RETURNS Geography external name geom."GeographyFromText";
--CREATE FUNCTION ST_GeographyFromText(wkt string) RETURNS Geography external name geom."GeographyFromText";


-- Create Geometry from raw representation (byte array)
--CREATE FUNCTION ST_GeomFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid smallint) RETURNS Geometry EXTERNAL NAME geom."GeomFromWKB";
--CREATE FUNCTION ST_PointFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid smallint) RETURNS Geometry EXTERNAL NAME geom."PointFromWKB";
--CREATE FUNCTION ST_LineFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid smallint) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_LinestringFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid smallint) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";

--CREATE FUNCTION ST_GeomFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."GeomFromWKB";
--CREATE FUNCTION ST_WKBToSQL(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."GeomFromWKB";
--CREATE FUNCTION ST_PointFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."PointFromWKB";
--CREATE FUNCTION ST_LineFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_LinestringFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_GeogFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geography EXTERNAL NAME geom."GeogFromWKB";

-- Create Geometry from simpler geometries
CREATE FUNCTION ST_MakePoint(x double, y double) RETURNS Geometry EXTERNAL NAME geom."MakePoint"; 
CREATE FUNCTION ST_Point(x double, y double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
CREATE FUNCTION ST_MakePoint(x double, y double, z double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
--ERROR: HOW TO CREATE A 4D POINT?
--CREATE FUNCTION ST_MakePoint(x double, y double, z double, m double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
CREATE FUNCTION ST_MakePointM(x double, y double, m double) RETURNS Geometry EXTERNAL NAME geom."MakePointM";
--CREATE FUNCTION ST_MakeLine(geometry set geoms)?????
--CREATE FUNCTION ST_MakeLine(geom1 Geometry, geom2 Geometry) RETURNS Geometry external name geom."MakeLine";
--CREATE FUNCTION ST_MakeLine(geoms_arr Geometry[]) RETURNS Geometry external name geom."MakeLine";
--CREATE FUNCTION ST_LineFromMultiPoint(pointGeom Geometry) RETURNS Geometry external name geom."LineFromMultiPoint"; --gets mutlipoint returns linestring
--CREATE FUNCTION ST_MakeEnvelope(xmin double, ymin double, xmax double, ymax double, srid SMALLINT) RETURNS Geometry external name geom."MakeEnvelope";
--CREATE FUNCTION ST_MakePolygon(geom Geometry) RETURNS Geometry external name geom."MakePolygon"; --gets linestring
--CREATE FUNCTION ST_MakePolygon(outerGeom Geometry, interiorGeoms Geometry[]) RETURNS Geometry external name geom."MakePolygon"; --gets linestrings
--CREATE FUNCTION ST_Polygon(geom Geometry, srid SMALLINT) RETURNS Geometry external name geom."Polygon" --gets linestring
--CREATE FUNCTION ST_MakeBox2D(lowLeftPointGeom Geometry, upRightPointGeom Geometry) RETURNS mbr external name geom."MakeBox2D"; --gets 2d points
--CREATE FUNCTION ST_3DMakeBox(lowLeftPointGeom Geometry, upRightPointGeom Geometry) RETURNS mbr external name geom."MakeBox3D"; --gets 3d points

-- Other constructors
--CREATE FUNCTION ST_Box2dFromGeoHash() RETURNS mbr external name geom."Box2dFromGeoHash";
--CREATE FUNCTION ST_GeomFromEWKB
--CREATE FUNCTION ST_GeomFromEWKT
--CREATE FUNCTION ST_GeomFromGML
--CREATE FUNCTION ST_GeomFromGeoJSON
--CREATE FUNCTION ST_GeomFromKML
--CREATE FUNCTION ST_GMLToSQL
--CREATE FUNCTION ST_PointFromGeoHash

------------------------------------------- Geometry Accessors -----------------------------------------------------------------------------
CREATE FUNCTION GeometryType(geom Geometry) RETURNS string EXTERNAL NAME geom."GeometryType";
CREATE FUNCTION ST_Boundary(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Boundary"
CREATE FUNCTION ST_CoordDim(geom Geometry) RETURNS integer EXTERNAL NAME geom."CoordDim";
CREATE FUNCTION ST_Dimension(geom Geometry) RETURNS integer EXTERNAL NAME geom."Dimension";
CREATE FUNCTION ST_EndPoint(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."EndPoint"; --returns point gets linestring
CREATE FUNCTION ST_Envelope(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Envelope";
CREATE FUNCTION ST_ExteriorRing(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."ExteriorRing"; --returns linestring gets polygon
--CREATE FUNCTION ST_GeometryN(gem Geometry, geomNum integer) RETURNS Geometry EXTERNAL NAME
--CREATE FUNCTION ST_GeometryType(geom Geometry) RETURNS string EXTERNAL NAME
CREATE FUNCTION ST_InteriorRingN(geom Geometry, ringNum integer) RETURNS Geometry EXTERNAL NAME "InteriorRingN"; --returns linestring gets polygon
--CREATE FUNCTION ST_IsClosed(geom Geometry) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsCollection(geom Geometry) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsEmpty(geom Geometry) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsRing(geom Geometry) RETURNS boolean EXTERNAL NAME --is meaningfull only for linestrings
--CREATE FUNCTION ST_IsSimple(geom Geometry) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsValid(geom Geometry) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsValid(geom Geometry, flags integer) RETURNS boolean EXTERNAL NAME
--CREATE FUNCTION ST_IsValidReason(geom Geometry) RETURNS string EXTERNAL NAME
--CREATE FUNCTION ST_IsValidReason(geom Geometry, flags integer) RETURNS string EXTERNAL NAME
--CREATE FUNCTION ST_IsValidDetail(geom Geometry) RETURNS A_CUSTOM_ROW EXTERNAL NAME
--CREATE FUNCTION ST_IsValidDetail(geom Geometry, flags integer) RETURNS A_CUSTOM_ROW EXTERNAL NAME
--CREATE FUNCTION ST_M(geom Geometry) RETURNS double EXTERNAL NAME
--CREATE FUNCTION ST_NDims(geom Geometry) RETURNS integer EXTERNAL NAME
--CREATE FUNCTION ST_NPoints(geom Geometry) RETURNS integer EXTERNAL NAME
--CREATE FUNCTION ST_NRings(geom Geometry) RETURNS integer EXTERNAL NAME --is meaningfull for polygon and multipolygon
--CREATE FUNCTION ST_NumGeometries(geom Geometry) RETURNS integer EXTERNAL NAME
--CREATE FUNCTION ST_NumInteriorRings(geom Geometry) RETURNS integer EXTERNAL NAME --works only with polygon
--CREATE FUNCTION ST_NumInteriorRing(geom Geometry) RETURNS integer EXTERNAL NAME --alias opf the above
--CREATE FUNCTION ST_NumPatches(geom Geometry) RETURNS integer EXTERNAL NAME --works only with polyhedral surface
CREATE FUNCTION ST_NumPoints(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumPoints"; --works with linestring and circularstring
--CREATE FUNCTION ST_PatchN(geom Geometry, patchNum integer) RETURNS Geometry EXTERNAL NAME --works with polyhedral surface
CREATE FUNCTION ST_PointN(geom Geometry, pointNum n) RETURNS Geometry EXTERNAL NAME geom."PointN"; --get linestring returns point
CREATE FUNCTION ST_SRID(geom Geometry) RETURNS integer EXTERNAL NAME geom."SRID";
CREATE FUNCTION ST_StartPoint(geom Geometry) RETURNS geometry EXTERNAL NAME geom."StartPoint"; --gets linestring returns point
--CREATE FUNCTION ST_Summary(geom Geometry) RETURNS string EXTERNAL NAME
CREATE FUNCTION ST_X(geom Geometry) RETURNS double EXTERNAL NAME geom."X"; --gets point
--CREATE FUNCTION ST_XMax(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME
--CREATE FUNCTION ST_XMin(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME
CREATE FUNCTION ST_Y(geom Geometry) RETURNS double EXTERNAL NAME geom."Y"; --gets point
--CREATE FUNCTION ST_YMan(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME
--CREATE FUNCTION ST_YMin(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME
CREATE FUNCTION ST_Z(geom Geometry) RETURNS double EXTERNAL NAME geom."Z"; --gets point
--CREATE FUNCTION ST_ZMax(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME
--CREATE FUNCTION ST_Zmflag(geom Geometry) RETURNS smallint EXTERNAL NAME --0=2d, 1=3dm, 2=3dz, 4=4d
--CREATE FUNCTION ST_ZMin(box3d Geometry_OR_Box2D_OR_Box3D) RETURNS double EXTERNAL NAME


--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME
--CREATE FUNCTION RETURNS EXTERNAL NAME



-- CREATE FUNCTION Point(g Geometry) RETURNS Point external name geom.point;
-- CREATE FUNCTION Curve(g Geometry) RETURNS Curve external name geom.curve;
-- CREATE FUNCTION LineString(g Geometry) RETURNS LineString external name geom.linestring;
-- CREATE FUNCTION Surface(g Geometry) RETURNS Surface external name geom.surface;
-- CREATE FUNCTION Polygon(g Geometry) RETURNS Polygon external name geom.polygon;

-- ogc basic methods
--//CREATE FUNCTION IsEmpty(g Geometry) RETURNS BOOLEAN external name geom."IsEmpty";
--//CREATE FUNCTION IsSimple(g Geometry) RETURNS BOOLEAN external name geom."IsSimple";

-- ogc spatial relation methods
--//CREATE FUNCTION Equals(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Equals";
--//CREATE FUNCTION Disjoint(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Disjoint";
--//CREATE FUNCTION "Intersect"(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Intersect";
--//CREATE FUNCTION Touches(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Touches";
--//CREATE FUNCTION Crosses(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Crosses";
--//CREATE FUNCTION Within(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Within";
--//CREATE FUNCTION Contains(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Contains";
--//CREATE FUNCTION Overlaps(a Geometry, b Geometry) RETURNS BOOLEAN external name geom."Overlaps";
--//CREATE FUNCTION Relate(a Geometry, b Geometry, pattern STRING) RETURNS BOOLEAN external name geom."Relate";

-- ogc Spatial Analysis methods
--//CREATE FUNCTION Area(g Geometry) RETURNS FLOAT external name geom."Area";
--//CREATE FUNCTION Length(g Geometry) RETURNS FLOAT external name geom."Length";
--//CREATE FUNCTION Distance(a Geometry, b Geometry) RETURNS FLOAT external name geom."Distance";
--//CREATE FUNCTION Buffer(a Geometry, distance FLOAT) RETURNS Geometry external name geom."Buffer";
--//CREATE FUNCTION ConvexHull(a Geometry) RETURNS Geometry external name geom."ConvexHull";
--//CREATE FUNCTION Intersection(a Geometry, b Geometry) RETURNS Geometry external name geom."Intersection";
--//CREATE FUNCTION "Union"(a Geometry, b Geometry) RETURNS Geometry external name geom."Union";
--//CREATE FUNCTION Difference(a Geometry, b Geometry) RETURNS Geometry external name geom."Difference";
--//CREATE FUNCTION SymDifference(a Geometry, b Geometry) RETURNS Geometry external name geom."SymDifference";


--//CREATE FUNCTION Centroid(g Geometry) RETURNS Geometry external name geom."Centroid";


-- create spatial_ref_sys metadata table
CREATE TABLE spatial_ref_sys (
	srid INTEGER NOT NULL PRIMARY KEY,
	auth_name VARCHAR (256),
	auth_srid INTEGER,
	srtext VARCHAR (2048),
	proj4text VARCHAR (2048)
);

-- create geometry_columns metadata view
create view geometry_columns as
	select e.value as f_table_catalog,
		s.name as f_table_schema,
		y.f_table_name, y.f_geometry_column, y.coord_dimension, y.srid, y.type
	from schemas s, environment e, (
		select t.schema_id,
			t.name as f_table_name,
			x.name as f_geometry_column,
			has_z(info)+has_m(info)+2 as coord_dimension,
			srid, get_type(info) as type
		from tables t, (
			select name, table_id, type_digits AS info, type_scale AS srid
			from columns
			where type in ( select distinct sqlname from types where systemname='wkb')
			) as x
		where t.id=x.table_id
		) y
	where y.schema_id=s.id and e.name='gdk_dbname';


copy into spatial_ref_sys from '/export/scratch1/gast730/DEV/MonetDB/geom/sql/postgis_spatial_ref_sys.csv' using delimiters ',';
