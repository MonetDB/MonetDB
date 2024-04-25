-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.


-------------------------------------------------------------------------
------------------------- Geography functions ---------------------------
-------------------------------------------------------------------------
CREATE FUNCTION ST_DistanceGeographic(geom1 Geometry, geom2 Geometry) RETURNS double EXTERNAL NAME geom."DistanceGeographic";
GRANT EXECUTE ON FUNCTION ST_DistanceGeographic(Geometry, Geometry) TO PUBLIC;
CREATE FILTER FUNCTION ST_DWithinGeographic(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom."DWithinGeographic";
GRANT EXECUTE ON FILTER ST_DWithinGeographic(Geometry, Geometry, double) TO PUBLIC;
CREATE FILTER FUNCTION ST_IntersectsGeographic(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom."IntersectsGeographic";
GRANT EXECUTE ON FILTER ST_IntersectsGeographic(Geometry, Geometry) TO PUBLIC;

-------------------------------------------------------------------------
----------------------- New Geometry functions --------------------------
-------------------------------------------------------------------------
CREATE AGGREGATE ST_Collect(geom Geometry) RETURNS Geometry external name aggr."Collect";
GRANT EXECUTE ON AGGREGATE ST_Collect(Geometry) TO PUBLIC;
CREATE FILTER FUNCTION ST_Intersects(geom1 Geometry, geom2 Geometry) EXTERNAL NAME rtree."Intersects";
GRANT EXECUTE ON FILTER ST_Intersects(Geometry, Geometry) TO PUBLIC;
CREATE FILTER FUNCTION ST_Intersects_NoIndex(geom1 Geometry, geom2 Geometry) EXTERNAL NAME geom."Intersects_noindex";
GRANT EXECUTE ON FILTER ST_Intersects_NoIndex(Geometry, Geometry) TO PUBLIC;
CREATE FILTER FUNCTION ST_DWithin(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME rtree."DWithin";
GRANT EXECUTE ON FILTER ST_DWithin(Geometry, Geometry, double) TO PUBLIC;
CREATE FILTER FUNCTION ST_DWithin_NoIndex(geom1 Geometry, geom2 Geometry, distance double) EXTERNAL NAME geom."DWithin_noindex";
GRANT EXECUTE ON FILTER ST_DWithin_NoIndex(Geometry, Geometry, double) TO PUBLIC;

-------------------------------------------------------------------------
------------------------- Old Geom functions ----------------------------
-------------------------------------------------------------------------

-- make sure you load the geom module before loading this sql module

-- currently we only use mbr instead of
-- Envelope():Geometry
-- as that returns Geometry objects, and we prefer the explicit mbr's
-- minimum bounding rectangle (mbr)
CREATE FUNCTION mbr(geom Geometry) RETURNS mbr external name geom."mbr";
GRANT EXECUTE ON FUNCTION mbr(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Overlaps(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrOverlaps";
GRANT EXECUTE ON FUNCTION ST_Overlaps(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Contains(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrContains";
GRANT EXECUTE ON FUNCTION ST_Contains(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Equals(box1 mbr, box2 mbr) RETURNS boolean EXTERNAL NAME geom."mbrEqual";
GRANT EXECUTE ON FUNCTION ST_Equals(mbr, mbr) TO PUBLIC;
CREATE FUNCTION ST_Distance(box1 mbr, box2 mbr) RETURNS double EXTERNAL NAME geom."mbrDistance";
GRANT EXECUTE ON FUNCTION ST_Distance(mbr, mbr) TO PUBLIC;



--CREATE FUNCTION mbrOverlapOrLeft(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrLeft";
--CREATE FUNCTION mbrOverlapOrBelow(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrBelow";
--CREATE FUNCTION mbrOverlapOrRight(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrRight";
--CREATE FUNCTION mbrLeft(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrLeft";
--CREATE FUNCTION mbrBelow(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrBelow";
--CREATE FUNCTION mbrEqual(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrEqual";
--CREATE FUNCTION mbrRight(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrRight";
--CREATE FUNCTION mbrContained(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrContained";
--CREATE FUNCTION mbrOverlapOrAbove(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrOverlapOrAbove";
--CREATE FUNCTION mbrAbove(box1 mbr, box2 mbr) RETURNS boolean external name geom."mbrAbove";


-------------------------------------------------------------------------
------------------------- Management Functions- -------------------------
-------------------------------------------------------------------------
--CREATE PROCEDURE AddGeometryColumn(table_name string, column_name string, srid integer, geometryType string, dimension integer)
--CREATE FUNCTION AddGeometryColumn(table_name string, column_name string, srid integer, geometryType string, dimension integer) RETURNS string
--BEGIN
--	DECLARE column_type string;
--	SET column_type = concat('geometry( ', geometryType);
--	SET column_type = concat(column_type, ', ');
--	SET column_type = concat(column_type, srid);
--	SET column_type = concat(column_type, ' )');
--	ALTER TABLE table_name ADD column_name column_type; --geometry('point', 28992);
--
--	RETURN column_type;
--END;

--CREATE PROCEDURE t(table_name string, column_name string, column_type string)
--BEGIN
--	ALTER TABLE table_name ADD column_name;
--END;

--CREATE FUNCTION t(table_name string, column_name string, srid integer, type string, dimension integer) RETURNS string
--BEGIN
--	EXECUTE PROCEDURE AddGeometryColumn(table_name, column_name, srid, type, dimension);
--	RETURN '';
--END;



---------------------------------
-- OGC - Simple Feature Access --
---------------------------------

--Construct a Geometry from a WKT
CREATE FUNCTION ST_WKTToSQL(wkt string) RETURNS Geometry external name geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_WKTToSQL(string) TO PUBLIC;

--Construct a Geometry from a WKB
CREATE FUNCTION ST_WKBToSQL(geom string) RETURNS Geometry EXTERNAL NAME geom."FromBinary";
GRANT EXECUTE ON FUNCTION ST_WKBToSQL(string) TO PUBLIC;

--Obtaining WKT from Geometry
CREATE FUNCTION ST_AsText(geom Geometry) RETURNS string EXTERNAL NAME geom."AsText";
GRANT EXECUTE ON FUNCTION ST_AsText(Geometry) TO PUBLIC;

--Obtainig WKB from Geometry
CREATE FUNCTION ST_AsBinary(geom Geometry) RETURNS string EXTERNAL NAME geom."AsBinary";
GRANT EXECUTE ON FUNCTION ST_AsBinary(Geometry) TO PUBLIC;

--Functions on Geometries
CREATE FUNCTION ST_Dimension(geom Geometry) RETURNS integer EXTERNAL NAME geom."Dimension";
GRANT EXECUTE ON FUNCTION ST_Dimension(Geometry) TO PUBLIC;
CREATE FUNCTION ST_GeometryType(geom Geometry) RETURNS string EXTERNAL NAME geom."GeometryType2";
GRANT EXECUTE ON FUNCTION ST_GeometryType(Geometry) TO PUBLIC;
CREATE FUNCTION ST_SRID(geom Geometry) RETURNS integer EXTERNAL NAME geom."getSRID";
GRANT EXECUTE ON FUNCTION ST_SRID(Geometry) TO PUBLIC;
CREATE FUNCTION ST_SetSRID(geom Geometry, srid integer) RETURNS Geometry EXTERNAL NAME geom."setSRID";
GRANT EXECUTE ON FUNCTION ST_SetSRID(Geometry, integer) TO PUBLIC;
CREATE FUNCTION ST_IsEmpty(geom Geometry) RETURNS boolean EXTERNAL NAME geom."IsEmpty";
GRANT EXECUTE ON FUNCTION ST_IsEmpty(Geometry) TO PUBLIC;
CREATE FUNCTION ST_IsSimple(geom Geometry) RETURNS boolean EXTERNAL NAME geom."IsSimple";
GRANT EXECUTE ON FUNCTION ST_IsSimple(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Boundary(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Boundary";
GRANT EXECUTE ON FUNCTION ST_Boundary(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Envelope(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Envelope";
GRANT EXECUTE ON FUNCTION ST_Envelope(Geometry) TO PUBLIC;
--Functions testing spatial relations between Geometries
CREATE FUNCTION ST_Equals(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Equals";
GRANT EXECUTE ON FUNCTION ST_Equals(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Disjoint(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Disjoint";
GRANT EXECUTE ON FUNCTION ST_Disjoint(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Touches(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Touches";
GRANT EXECUTE ON FUNCTION ST_Touches(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Crosses(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Crosses";
GRANT EXECUTE ON FUNCTION ST_Crosses(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Within(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Within";
GRANT EXECUTE ON FUNCTION ST_Within(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Contains(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Contains";
GRANT EXECUTE ON FUNCTION ST_Contains(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Overlaps(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Overlaps";
GRANT EXECUTE ON FUNCTION ST_Overlaps(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Relate(geom1 Geometry, geom2 Geometry, intersection_matrix_pattern string) RETURNS boolean EXTERNAL NAME geom."Relate";
GRANT EXECUTE ON FUNCTION ST_Relate(Geometry, Geometry, string) TO PUBLIC;
--Distance between Geometries
CREATE FUNCTION ST_Distance(geom1 Geometry, geom2 Geometry) RETURNS double EXTERNAL NAME geom."Distance";
GRANT EXECUTE ON FUNCTION ST_Distance(Geometry, Geometry) TO PUBLIC;


--Functions that implement spatial operators
CREATE FUNCTION ST_Intersection(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom."Intersection";
GRANT EXECUTE ON FUNCTION ST_Intersection(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Difference(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom."Difference";
GRANT EXECUTE ON FUNCTION ST_Difference(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Union(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom."Union";
GRANT EXECUTE ON FUNCTION ST_Union(Geometry, Geometry) TO PUBLIC;
CREATE AGGREGATE ST_Union(geom Geometry) RETURNS Geometry external name geom."Union";
CREATE FUNCTION ST_SymDifference(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom."SymDifference";
GRANT EXECUTE ON FUNCTION ST_SymDifference(Geometry, Geometry) TO PUBLIC;
CREATE FUNCTION ST_Buffer(geom Geometry, radius double) RETURNS Geometry EXTERNAL NAME geom."Buffer";
GRANT EXECUTE ON FUNCTION ST_Buffer(Geometry, double) TO PUBLIC;
CREATE FUNCTION ST_ConvexHull(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."ConvexHull";
GRANT EXECUTE ON FUNCTION ST_ConvexHull(Geometry) TO PUBLIC;

--Functions on Point
CREATE FUNCTION ST_X(geom Geometry) RETURNS double EXTERNAL NAME geom."X";
GRANT EXECUTE ON FUNCTION ST_X(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Y(geom Geometry) RETURNS double EXTERNAL NAME geom."Y";
GRANT EXECUTE ON FUNCTION ST_Y(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Z(geom Geometry) RETURNS double EXTERNAL NAME geom."Z";
GRANT EXECUTE ON FUNCTION ST_Z(Geometry) TO PUBLIC;

--Functions on Curve (i.e. LineString)
CREATE FUNCTION ST_StartPoint(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."StartPoint";
GRANT EXECUTE ON FUNCTION ST_StartPoint(Geometry) TO PUBLIC;
CREATE FUNCTION ST_EndPoint(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."EndPoint";
GRANT EXECUTE ON FUNCTION ST_EndPoint(Geometry) TO PUBLIC;
CREATE FUNCTION ST_IsRing(geom Geometry) RETURNS boolean EXTERNAL NAME geom."IsRing";
GRANT EXECUTE ON FUNCTION ST_IsRing(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Length(geom Geometry) RETURNS double EXTERNAL NAME geom."Length"; --valid also for MultiCurve
GRANT EXECUTE ON FUNCTION ST_Length(Geometry) TO PUBLIC;
CREATE FUNCTION ST_IsClosed(geom Geometry) RETURNS boolean EXTERNAL NAME geom."IsClosed"; --valid also for MultiCurve
GRANT EXECUTE ON FUNCTION ST_IsClosed(Geometry) TO PUBLIC;

--Functions on LineString
CREATE FUNCTION ST_NumPoints(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumPoints";
GRANT EXECUTE ON FUNCTION ST_NumPoints(Geometry) TO PUBLIC;
CREATE FUNCTION ST_PointN(geom Geometry, positionNum integer) RETURNS Geometry EXTERNAL NAME geom."PointN";
GRANT EXECUTE ON FUNCTION ST_PointN(Geometry, integer) TO PUBLIC;

--Functions on Surface (i.e. Polygon and Polyhedral Surface) and MultiSurface
CREATE FUNCTION ST_Centroid(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Centroid";
GRANT EXECUTE ON FUNCTION ST_Centroid(Geometry) TO PUBLIC;
CREATE FUNCTION ST_PointOnSurface(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."PointOnSurface";
GRANT EXECUTE ON FUNCTION ST_PointOnSurface(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Area(geom Geometry) RETURNS double EXTERNAL NAME geom."Area";
GRANT EXECUTE ON FUNCTION ST_Area(Geometry) TO PUBLIC;

--Functions on Polygon
CREATE FUNCTION ST_ExteriorRing(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."ExteriorRing";
GRANT EXECUTE ON FUNCTION ST_ExteriorRing(Geometry) TO PUBLIC;
CREATE FUNCTION ST_SetExteriorRing(geom Geometry) RETURNS Geometry external name geom."MakePolygon"; --gets a linestring and creates a polygon (postGIS: ST_MakePolygon)
GRANT EXECUTE ON FUNCTION ST_SetExteriorRing(Geometry) TO PUBLIC;
CREATE FUNCTION ST_NumInteriorRing(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumInteriorRings";
GRANT EXECUTE ON FUNCTION ST_NumInteriorRing(Geometry) TO PUBLIC;
CREATE FUNCTION ST_InteriorRingN(geom Geometry, positionNum integer) RETURNS Geometry EXTERNAL NAME geom."InteriorRingN";
GRANT EXECUTE ON FUNCTION ST_InteriorRingN(Geometry, integer) TO PUBLIC;
CREATE FUNCTION ST_InteriorRings(geom Geometry) RETURNS GeometryA EXTERNAL NAME geom."InteriorRings";
GRANT EXECUTE ON FUNCTION ST_InteriorRings(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_SetInteriorRings(geom GeometryA) RETURNS Geometry EXTERNAL NAME geom."SetInteriorRings"; --what is this function supposed to do????

--Functions on GeomCollection
CREATE FUNCTION ST_NumGeometries(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumGeometries";
GRANT EXECUTE ON FUNCTION ST_NumGeometries(Geometry) TO PUBLIC;
CREATE FUNCTION ST_GeometryN(geom Geometry, positionNum integer) RETURNS Geometry EXTERNAL NAME geom."GeometryN";
GRANT EXECUTE ON FUNCTION ST_GeometryN(Geometry, integer) TO PUBLIC;

--Functions on Polyhedral Surfaces (a simple surface, consisting of a number of Polygon pathes or facets)
--CREATE FUNCTION ST_Geometries(geom Geometry) RETURNS TABLE(geom Geometries) EXTERNAL NAME geom."Geometries";
--CREATE FUNCTION NumSurfaces(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumSurfaces";
--CREATE FUNCTION Surface(positionNum integer) RETURNS Geometry EXTERNAL NAME geom."SurfaceN";
--from Part 1
CREATE FUNCTION ST_NumPatches(geom Geometry) RETURNS integer --EXTERNAL NAME geom."NumPatches"; --same with NumSurfaces
BEGIN
	RETURN SELECT ST_NumGeometries(geom);
END;
GRANT EXECUTE ON FUNCTION ST_NumPatches(Geometry) TO PUBLIC;
CREATE FUNCTION ST_PatchN(geom Geometry, patchNum integer) RETURNS Geometry --EXTERNAL NAME geom."PatchN" --same with Surface
BEGIN
	RETURN SELECT ST_GeometryN(geom, patchNum);
END;
GRANT EXECUTE ON FUNCTION ST_PatchN(Geometry, integer) TO PUBLIC;
--BoundingPolygons
--IsClosed

-------------------------------------------
-- DEPRECATED BUT IMPLEMENTED BY postGIS --
-------------------------------------------

--Construct a Geometry from a WKT
CREATE FUNCTION ST_GeomFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_GeomFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_PointFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."PointFromText";
GRANT EXECUTE ON FUNCTION ST_PointFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_LineFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."LineFromText";
GRANT EXECUTE ON FUNCTION ST_LineFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_PolygonFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."PolygonFromText";
GRANT EXECUTE ON FUNCTION ST_PolygonFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_MPointFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPointFromText";
GRANT EXECUTE ON FUNCTION ST_MPointFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_MLineFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MLineFromText";
GRANT EXECUTE ON FUNCTION ST_MLineFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_MPolyFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPolyFromText";
GRANT EXECUTE ON FUNCTION ST_MPolyFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_GeomCollFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomCollFromText";
GRANT EXECUTE ON FUNCTION ST_GeomCollFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_BdPolyFromText(wkt string, srid integer) RETURNS Geometry external name geom."BdPolyFromText";
GRANT EXECUTE ON FUNCTION ST_BdPolyFromText(string, integer) TO PUBLIC;
CREATE FUNCTION ST_BdMPolyFromText(wkt string, srid integer) RETURNS Geometry external name geom."BdMPolyFromText";
GRANT EXECUTE ON FUNCTION ST_BdMPolyFromText(string, integer) TO PUBLIC;

--Construct a Geoemtry from a WKB
--CREATE FUNCTION ST_GeomFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromWKB";
--CREATE FUNCTION ST_PointFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."PointFromWKB";
--CREATE FUNCTION ST_LineFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_PolygonFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."PolygonFromWKB";
--CREATE FUNCTION ST_MPointFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPointFromWKB";
--CREATE FUNCTION ST_MLineFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."MLineFromWKB";
--CREATE FUNCTION ST_MPolyFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."MPolyFromWKB";
--CREATE FUNCTION ST_GeomCollFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomCollFromWKB";
--CREATE FUNCTION ST_BdPolyFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry external name geom."BdPolyFromWKB";
--CREATE FUNCTION ST_BdMPolyFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB, srid integer) RETURNS Geometry external name geom."BdMPolyFromWKB";

--CREATE FUNCTION ST_M(geom Geometry) RETURNS double EXTERNAL NAME geom."M"; --geos does not support M coordinate (at least in the c version)
--CREATE FUNCTION ST_CurveToLine RETURNS EXTERNAL NAME --geos does not support CIRCULARSTRING and does not have such function




-------------
-- PostGIS --
-------------

-------------------------------------------------------------------------
------------------------- Geometry Constructors -------------------------
-------------------------------------------------------------------------
-- Create Geometry from text (wkt)
CREATE FUNCTION ST_GeometryFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_GeometryFromText(string, integer) TO PUBLIC;

CREATE FUNCTION ST_GeomFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_GeomFromText(string) TO PUBLIC;
CREATE FUNCTION ST_GeometryFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_GeometryFromText(string) TO PUBLIC;
CREATE FUNCTION ST_PointFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."PointFromText";
GRANT EXECUTE ON FUNCTION ST_PointFromText(string) TO PUBLIC;
CREATE FUNCTION ST_LineFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."LineFromText";
GRANT EXECUTE ON FUNCTION ST_LineFromText(string) TO PUBLIC;
CREATE FUNCTION ST_PolygonFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."PolygonFromText";
GRANT EXECUTE ON FUNCTION ST_PolygonFromText(string) TO PUBLIC;
CREATE FUNCTION ST_MPointFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MPointFromText";
GRANT EXECUTE ON FUNCTION ST_MPointFromText(string) TO PUBLIC;
CREATE FUNCTION ST_MLineFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MLineFromText";
GRANT EXECUTE ON FUNCTION ST_MLineFromText(string) TO PUBLIC;
CREATE FUNCTION ST_MPolyFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."MPolyFromText";
GRANT EXECUTE ON FUNCTION ST_MPolyFromText(string) TO PUBLIC;
CREATE FUNCTION ST_GeomCollFromText(wkt string) RETURNS Geometry EXTERNAL NAME geom."GeomCollFromText";
GRANT EXECUTE ON FUNCTION ST_GeomCollFromText(string) TO PUBLIC;
--CREATE FUNCTION ST_GeogFromText(wkt string) RETURNS Geography external name geom."GeographyFromText";
--CREATE FUNCTION ST_GeographyFromText(wkt string) RETURNS Geography external name geom."GeographyFromText";


-- Create Geometry from raw representation (byte array)
--CREATE FUNCTION ST_LinestringFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB, srid smallint) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";

--CREATE FUNCTION ST_GeomFromWKB(wkb_raw WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."GeomFromWKB";
--CREATE FUNCTION ST_PointFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."PointFromWKB";
--CREATE FUNCTION ST_LineFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_LinestringFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geometry EXTERNAL NAME geom."LineFromWKB";
--CREATE FUNCTION ST_GeogFromWKB(wkb_arr WHATEVER_IS_STORED_IN_DB) RETURNS Geography EXTERNAL NAME geom."GeogFromWKB";

-- Create Geometry from simpler geometries
CREATE FUNCTION ST_MakePoint(x double, y double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
GRANT EXECUTE ON FUNCTION ST_MakePoint(double, double) TO PUBLIC;
CREATE FUNCTION ST_Point(x double, y double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
GRANT EXECUTE ON FUNCTION ST_Point(double, double) TO PUBLIC;
CREATE FUNCTION ST_MakePoint(x double, y double, z double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
GRANT EXECUTE ON FUNCTION ST_MakePoint(double, double, double) TO PUBLIC;
CREATE FUNCTION ST_MakePoint(x double, y double, z double, m double) RETURNS Geometry EXTERNAL NAME geom."MakePoint";
GRANT EXECUTE ON FUNCTION ST_MakePoint(double, double, double, double) TO PUBLIC;
CREATE FUNCTION ST_MakePointM(x double, y double, m double) RETURNS Geometry EXTERNAL NAME geom."MakePointM";
GRANT EXECUTE ON FUNCTION ST_MakePointM(double, double, double) TO PUBLIC;
--CREATE FUNCTION ST_MakeLine(geometry set geoms)?????
CREATE AGGREGATE ST_MakeLine(geom Geometry) RETURNS Geometry external name aggr."MakeLine";
GRANT EXECUTE ON AGGREGATE ST_MakeLine(Geometry) TO PUBLIC;
CREATE FUNCTION ST_MakeLine(geom1 Geometry, geom2 Geometry) RETURNS Geometry external name geom."MakeLine"; --two single geometries
GRANT EXECUTE ON FUNCTION ST_MakeLine(Geometry, Geometry) TO PUBLIC;
--CREATE FUNCTION ST_MakeLine(geoms_arr Geometry[]) RETURNS Geometry external name geom."MakeLine";
--CREATE FUNCTION ST_LineFromMultiPoint(pointGeom Geometry) RETURNS Geometry external name geom."LineFromMultiPoint"; --gets mutlipoint returns linestring
CREATE FUNCTION ST_MakeEnvelope(xmin double, ymin double, xmax double, ymax double, srid integer) RETURNS Geometry external name geom."MakeEnvelope";
GRANT EXECUTE ON FUNCTION ST_MakeEnvelope(double, double, double, double, integer) TO PUBLIC;
CREATE FUNCTION ST_MakeEnvelope(xmin double, ymin double, xmax double, ymax double) RETURNS Geometry external name geom."MakeEnvelope";
GRANT EXECUTE ON FUNCTION ST_MakeEnvelope(double, double, double, double) TO PUBLIC;
CREATE FUNCTION ST_MakePolygon(geom Geometry) RETURNS Geometry external name geom."MakePolygon"; --gets linestring
GRANT EXECUTE ON FUNCTION ST_MakePolygon(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_MakePolygon(outerGeom Geometry, interiorGeoms table(g Geometry)) RETURNS Geometry external name geom."MakePolygon"; --gets linestrings
CREATE FUNCTION ST_Polygon(geom Geometry, srid integer) RETURNS Geometry external name geom."MakePolygon"; --gets linestring
GRANT EXECUTE ON FUNCTION ST_Polygon(Geometry, integer) TO PUBLIC;
CREATE FUNCTION ST_MakeBox2D(lowLeftPointGeom Geometry, upRightPointGeom Geometry) RETURNS mbr external name geom."MakeBox2D"; --gets 2d points
GRANT EXECUTE ON FUNCTION ST_MakeBox2D(Geometry, Geometry) TO PUBLIC;
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

-------------------------------------------------------------------------
-------------------------- Geometry Accessors ---------------------------
-------------------------------------------------------------------------
CREATE FUNCTION GeometryType(geom Geometry) RETURNS string EXTERNAL NAME geom."GeometryType1";
GRANT EXECUTE ON FUNCTION GeometryType(Geometry) TO PUBLIC;
CREATE FUNCTION ST_CoordDim(geom Geometry) RETURNS integer EXTERNAL NAME geom."CoordDim";
GRANT EXECUTE ON FUNCTION ST_CoordDim(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_IsCollection(geom Geometry) RETURNS boolean EXTERNAL NAME
CREATE FUNCTION ST_IsValid(geom Geometry) RETURNS boolean EXTERNAL NAME geom."IsValid";
GRANT EXECUTE ON FUNCTION ST_IsValid(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_IsValid(geom Geometry, flags integer) RETURNS boolean EXTERNAL NAME
CREATE FUNCTION ST_IsValidReason(geom Geometry) RETURNS string EXTERNAL NAME geom."IsValidReason";
GRANT EXECUTE ON FUNCTION ST_IsValidReason(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_IsValidReason(geom Geometry, flags integer) RETURNS string EXTERNAL NAME
--CREATE FUNCTION ST_IsValidDetail(geom Geometry) RETURNS string EXTERNAL NAME geom."IsValidDetail";
--CREATE FUNCTION ST_IsValidDetail(geom Geometry, flags integer) RETURNS A_CUSTOM_ROW EXTERNAL NAME
--CREATE FUNCTION ST_NDims(geom Geometry) RETURNS integer EXTERNAL NAME
CREATE FUNCTION ST_NPoints(geom Geometry) RETURNS integer EXTERNAL NAME geom."NPoints";
GRANT EXECUTE ON FUNCTION ST_NPoints(Geometry) TO PUBLIC;
CREATE FUNCTION ST_NRings(geom Geometry) RETURNS integer EXTERNAL NAME geom."NRings"; --is meaningful for polygon and multipolygon
GRANT EXECUTE ON FUNCTION ST_NRings(Geometry) TO PUBLIC;
CREATE FUNCTION ST_NumInteriorRings(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumInteriorRings";
GRANT EXECUTE ON FUNCTION ST_NumInteriorRings(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_Summary(geom Geometry) RETURNS string EXTERNAL NAME
CREATE FUNCTION ST_XMax(geom Geometry) RETURNS double EXTERNAL NAME geom."XMaxFromWKB";
GRANT EXECUTE ON FUNCTION ST_XMax(Geometry) TO PUBLIC;
CREATE FUNCTION ST_XMax(box mbr) RETURNS double EXTERNAL NAME geom."XMaxFromMBR";
GRANT EXECUTE ON FUNCTION ST_XMax(mbr) TO PUBLIC;
CREATE FUNCTION ST_XMin(geom Geometry) RETURNS double EXTERNAL NAME geom."XMinFromWKB";
GRANT EXECUTE ON FUNCTION ST_XMin(Geometry) TO PUBLIC;
CREATE FUNCTION ST_XMin(box mbr) RETURNS double EXTERNAL NAME geom."XMinFromMBR";
GRANT EXECUTE ON FUNCTION ST_XMin(mbr) TO PUBLIC;
CREATE FUNCTION ST_YMax(geom Geometry) RETURNS double EXTERNAL NAME geom."YMaxFromWKB";
GRANT EXECUTE ON FUNCTION ST_YMax(Geometry) TO PUBLIC;
CREATE FUNCTION ST_YMax(box mbr) RETURNS double EXTERNAL NAME geom."YMaxFromMBR";
GRANT EXECUTE ON FUNCTION ST_YMax(mbr) TO PUBLIC;
CREATE FUNCTION ST_YMin(geom Geometry) RETURNS double EXTERNAL NAME geom."YMinFromWKB";
GRANT EXECUTE ON FUNCTION ST_YMin(Geometry) TO PUBLIC;
CREATE FUNCTION ST_YMin(box mbr) RETURNS double EXTERNAL NAME geom."YMinFromMBR";
GRANT EXECUTE ON FUNCTION ST_YMin(mbr) TO PUBLIC;
--GEOS creates only 2D Envelope
--CREATE FUNCTION ST_ZMax(geom Geometry) RETURNS double EXTERNAL NAME geom."ZMaxFromWKB";
--CREATE FUNCTION ST_ZMax(box mbr) RETURNS double EXTERNAL NAME geom."ZMaxFromMBR";
--CREATE FUNCTION ST_ZMin(geom Geometry) RETURNS double EXTERNAL NAME geom."ZMinFromWKB";
--CREATE FUNCTION ST_ZMin(box mbr) RETURNS double EXTERNAL NAME geom."ZMinFromMBR";
--CREATE FUNCTION ST_Zmflag(geom Geometry) RETURNS smallint EXTERNAL NAME --0=2d, 1=3dm, 2=3dz, 4=4d

-------------------------------------------------------------------------
--------------------------- Geometry Editors ----------------------------
-------------------------------------------------------------------------
--CREATE FUNCTION ST_AddPoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Affine RETURNS EXTERNAL NAME
CREATE FUNCTION ST_Force2D(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Force2D";
GRANT EXECUTE ON FUNCTION ST_Force2D(Geometry) TO PUBLIC;
CREATE FUNCTION ST_Force3D(geom Geometry) RETURNS Geometry EXTERNAL NAME geom."Force3D";
GRANT EXECUTE ON FUNCTION ST_Force3D(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_Force3DZ RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Force3DM RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Force4D RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_ForceCollection RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_ForceRHR RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LineMerge RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_CollectionExtract RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_CollectionHomogenize RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Multi RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_RemovePoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Reverse RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Rotate RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_RotateX RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_RotateY RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_RotateZ RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Scale RETURNS EXTERNAL NAME
CREATE FUNCTION ST_Segmentize(geom Geometry, sz double) RETURNS Geometry EXTERNAL NAME geom."Segmentize";
GRANT EXECUTE ON FUNCTION ST_Segmentize(Geometry, double) TO PUBLIC;
--CREATE FUNCTION ST_SetPoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_SnapToGrid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Snap RETURNS EXTERNAL NAME
CREATE FUNCTION getProj4(srid_in integer) RETURNS string
BEGIN
	RETURN SELECT proj4text FROM spatial_ref_sys WHERE srid=srid_in;
END;
GRANT EXECUTE ON FUNCTION getProj4(integer) TO PUBLIC;
CREATE FUNCTION InternalTransform(geom Geometry, srid_src integer, srid_dest integer, proj4_src string, proj4_dest string) RETURNS Geometry EXTERNAL NAME geom."Transform";
GRANT EXECUTE ON FUNCTION InternalTransform(Geometry, integer, integer, string, string) TO PUBLIC;
CREATE FUNCTION ST_Transform(geom Geometry, srid integer) RETURNS Geometry
BEGIN
	DECLARE srid_src integer;
	DECLARE proj4_src string;
	DECLARE proj4_dest string;

	SELECT st_srid(geom) INTO srid_src;
	SELECT getProj4(srid_src) INTO proj4_src;
	SELECT getProj4(srid) INTO proj4_dest;

	IF proj4_src IS NULL THEN
		RETURN SELECT InternalTransform(geom, srid_src, srid, 'null', proj4_dest);
	ELSE
		IF proj4_dest IS NULL THEN
			RETURN SELECT InternalTransform(geom, srid_src, srid, proj4_src, 'null');
		ELSE
			RETURN SELECT InternalTransform(geom, srid_src, srid, proj4_src, proj4_dest);
		END IF;
	END IF;
END;
GRANT EXECUTE ON FUNCTION ST_Transform(Geometry, integer) TO PUBLIC;

--Translate moves all points of a geometry dx, dy, dz
CREATE FUNCTION ST_Translate(geom Geometry, dx double, dy double) RETURNS Geometry EXTERNAL NAME geom."Translate";
GRANT EXECUTE ON FUNCTION ST_Translate(Geometry, double, double) TO PUBLIC;
CREATE FUNCTION ST_Translate(geom Geometry, dx double, dy double, dz double) RETURNS Geometry EXTERNAL NAME geom."Translate";
GRANT EXECUTE ON FUNCTION ST_Translate(Geometry, double, double, double) TO PUBLIC;
--CREATE FUNCTION ST_TransScale RETURNS EXTERNAL NAME

-------------------------------------------------------------------------
--------------------------- Geometry Outputs ----------------------------
-------------------------------------------------------------------------
--CREATE FUNCTION ST_AsEWKB RETURNS EXTERNAL NAME
CREATE FUNCTION ST_AsEWKT(geom Geometry) RETURNS string EXTERNAL NAME geom."AsEWKT";
GRANT EXECUTE ON FUNCTION ST_AsEWKT(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_AsGeoJSON RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsGML RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsHEXEWKB RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsKML RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsSVG RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsX3D RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_GeoHash RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AsLatLonText RETURNS EXTERNAL NAME

-------------------------------------------------------------------------
------------------------------ Operators --------------------------------
-------------------------------------------------------------------------

-------------------------------------------------------------------------
---------------- Spatial Relationships and Measurements -----------------
-------------------------------------------------------------------------
--CREATE FUNCTION ST_3DClosestPoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DDistance RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DDWithin RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DDFullyWithin RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DIntersects RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DLongestLine RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DMaxDistance RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DShortestLine RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Area(geog Geography, use_spheroid boolean) RETURNS flt EXTERNAL NAME geom."Area";
--CREATE FUNCTION ST_Azimuth RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_ClosestPoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_ContainsProperly RETURNS EXTERNAL NAME
CREATE FUNCTION ST_Covers(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Covers";
GRANT EXECUTE ON FUNCTION ST_Covers(Geometry, Geometry) TO PUBLIC;
--CREATE FUNCTION ST_Covers(geog1 Geography, geog2 Geography) RETURNS boolean EXTERNAL NAME geom."Covers";
CREATE FUNCTION ST_CoveredBy(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."CoveredBy";
GRANT EXECUTE ON FUNCTION ST_CoveredBy(Geometry, Geometry) TO PUBLIC;
--CREATE FUNCTION ST_CoveredBy(geog1 Geography, geog2 Geography) RETURNS boolean EXTERNAL NAME geom."CoveredBy";
--CREATE FUNCTION ST_LineCrossingDirection RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Distance(geog1 Geometry, geog2 Geometry) RETURNS double EXTERNAL NAME geom."Distance"
--CREATE FUNCTION ST_Distance(geog1 Geometry, geog2 Geometry, use_spheroid boolean) RETURNS double EXTERNAL NAME geom."Distance"
CREATE FUNCTION ST_DWithin2(geom1 Geometry, geom2 Geometry, bbox1 mbr, bbox2 mbr, dst double) RETURNS boolean EXTERNAL NAME geom."DWithin2";
GRANT EXECUTE ON FUNCTION ST_DWithin2(Geometry, Geometry, mbr, mbr, double) TO PUBLIC;
--CREATE FUNCTION ST_HausdorffDistance RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_MaxDistance RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Distance_Sphere RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Distance_Spheroid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_DFullyWithin RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_HasArc RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Intersects(geog1 Geography, geog2 Geography) RETURNS boolean EXTERNAL NAME geom."Intersects";
--CREATE FUNCTION ST_Length(geog Geography, use_spheroid boolean) RETURNS double EXTERNAL NAME geom."Length";
CREATE FUNCTION ST_Length2D(geom Geometry) RETURNS double EXTERNAL NAME geom."Length";
GRANT EXECUTE ON FUNCTION ST_Length2D(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_3DLength RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Length_Spheroid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Length2D_Spheroid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DLength_Spheroid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LongestLine RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_OrderingEquals RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Perimeter RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Perimeter2D RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_3DPerimeter RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Project RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Relate(geom1 Geometry, geom2 Geometry) RETURNS string EXTERNAL NAME geom."Relate";
--CREATE FUNCTION ST_Relate(geom1 Geometry, geom2 Geometry, boundary_node_rule integer) RETURNS string EXTERNAL NAME geom."Relate";
--CREATE FUNCTION ST_RelateMatch RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_ShortestLine RETURNS EXTERNAL NAME

-------------------------------------------------------------------------
------------------------- Geometry Processing ---------------------------
-------------------------------------------------------------------------
--CREATE FUNCTION ST_Buffer(geom Geometry, radius double, circle_quarters_num integer) RETURNS Geometry EXTERNAL NAME geom."Buffer";
--CREATE FUNCTION ST_Buffer(geom Geometry, radius double, buffer_style_parameters string) RETURNS Geometry EXTERNAL NAME geom."Buffer";
--CREATE FUNCTION ST_Buffer(geog Geography, radius double) RETURNS Geometry EXTERNAL NAME geom."Buffer";
--CREATE FUNCTION ST_BuildArea RETURNS EXTERNAL NAME
--collect is the same to union. POstGIS just has a more efficient implementation for it compared to union
CREATE FUNCTION ST_Collect(geom1 Geometry, geom2 Geometry) RETURNS Geometry EXTERNAL NAME geom."Collect";
GRANT EXECUTE ON FUNCTION ST_Collect(Geometry, Geometry) TO PUBLIC;
--CREATE FUNCTION ST_ConcaveHull RETURNS EXTERNAL NAME
CREATE FUNCTION ST_DelaunayTriangles(geom Geometry, tolerance double, flags integer) RETURNS Geometry EXTERNAL NAME geom."DelaunayTriangles";
GRANT EXECUTE ON FUNCTION ST_DelaunayTriangles(Geometry, double, integer) TO PUBLIC;
CREATE FUNCTION ST_Dump(geom Geometry) RETURNS TABLE(id string, polygonWKB Geometry) EXTERNAL NAME geom."Dump";
GRANT EXECUTE ON FUNCTION ST_Dump(Geometry) TO PUBLIC;
CREATE FUNCTION ST_DumpPoints(geom Geometry) RETURNS TABLE(path string, pointG Geometry) EXTERNAL NAME geom."DumpPoints";
GRANT EXECUTE ON FUNCTION ST_DumpPoints(Geometry) TO PUBLIC;
--CREATE FUNCTION ST_DumpRings RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_FlipCoordinates RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Intersection(geog1 Geography, geog2 Geography) RETURNS Geography EXTERNAL NAME geom."Intersection";
--CREATE FUNCTION ST_LineToCurve RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_MakeValid RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_MemUnion RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_MinimumBoundingCircle RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Polygonize RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Node RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_OffsetCurve RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_RemoveRepeatedPoints RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_SharedPaths RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Shift_Longitude RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Simplify RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_SimplifyPreserveTopology RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Split RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_Union(geometry set geoms)?????
--CREATE FUNCTION ST_UnaryUnion RETURNS EXTERNAL NAME

-------------------------------------------------------------------------
-------------------------- Linear Referencing ---------------------------
-------------------------------------------------------------------------
--CREATE FUNCTION ST_LineInterpolatePoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LineLocatePoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LineSubstring RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LocateAlong RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LocateBetween RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_LocateBetweenElevations RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_InterpolatePoint RETURNS EXTERNAL NAME
--CREATE FUNCTION ST_AddMeasure RETURNS EXTERNAL NAME

-------------------------------------------------------------------------
---------------------- Long Transactions Support ------------------------
-------------------------------------------------------------------------

-------------------------------------------------------------------------
----------------------- Miscellaneous Functions -------------------------
-------------------------------------------------------------------------

-------------------------------------------------------------------------
------------------------ Exceptional Functions --------------------------
-------------------------------------------------------------------------


-- CREATE FUNCTION Point(g Geometry) RETURNS Point external name geom.point;
-- CREATE FUNCTION Curve(g Geometry) RETURNS Curve external name geom.curve;
-- CREATE FUNCTION LineString(g Geometry) RETURNS LineString external name geom.linestring;
-- CREATE FUNCTION Surface(g Geometry) RETURNS Surface external name geom.surface;
-- CREATE FUNCTION Polygon(g Geometry) RETURNS Polygon external name geom.polygon;

-------------------------------------------------------------------------
----------------------- Handle SRID information -------------------------
-------------------------------------------------------------------------
--CREATE FUNCTION UpdateGeometrySRID(catalogn_name varchar, schema_name varchar, table_name varchar, column_name varchar, new_srid_in integer) RETURNS text -- external name geom.updateGeometrySRID;
--BEGIN
--
--END;

--CREATE FUNCTION UpdateGeometrySRID(schema_name varchar, table_name varchar, column_name varchar, new_srid_in integer) RETURNS text
--BEGIN
--	RETURN UpdateGeometrySRID('',schema_name, table_name, column_name, new_srid_in);
--END;
--CREATE FUNCTION UpdateGeometrySRID(table_name varchar, column_name varchar, new_srid_in integer) RETURNS text
--BEGIN
--	RETURN UpdateGeometrySRID('','', table_name, column_name, new_srid_in);
--END;


--DECLARE srid_src integer;
--DECLARE proj4_src string;
--DECLARE proj4_dest string;
--
--SELECT st_srid(geom) INTO srid_src;
--SELECT getProj4(srid_src) INTO proj4_src;
--SELECT getProj4(srid) INTO proj4_dest;
--
--IF proj4_src IS NULL THEN
--	RETURN SELECT InternalTransform(geom, srid_src, srid, 'null', proj4_dest);
--ELSE
--	IF proj4_dest IS NULL THEN
--		RETURN SELECT InternalTransform(geom, srid_src, srid, proj4_src, 'null');
--	ELSE
--		RETURN SELECT InternalTransform(geom, srid_src, srid, proj4_src, proj4_dest);
--	END IF;
--END IF;


-------------------------------------------------------------------------
---------------------------- Miscellaneous ------------------------------
-------------------------------------------------------------------------
CREATE FUNCTION Contains(a Geometry, x double, y double) RETURNS BOOLEAN external name geom."Contains";
GRANT EXECUTE ON FUNCTION Contains(Geometry, double, double) TO PUBLIC;
