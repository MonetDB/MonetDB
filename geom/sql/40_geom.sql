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

-- The srid in the *FromText Functions is currently not used
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
