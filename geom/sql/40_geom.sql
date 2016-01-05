-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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
CREATE FUNCTION Contains(a Geometry, x double, y double) RETURNS BOOLEAN external name geom."Contains";
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
