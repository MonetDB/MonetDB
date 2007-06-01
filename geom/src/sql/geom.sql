-- make sure you load the geom module before loading this sql module
CREATE TYPE Geometry EXTERNAL NAME wkb;
CREATE TYPE Point EXTERNAL NAME wkb;
CREATE TYPE Curve EXTERNAL NAME wkb;
CREATE TYPE LineString EXTERNAL NAME wkb;
CREATE TYPE Surface EXTERNAL NAME wkb;
CREATE TYPE Polygon EXTERNAL NAME wkb;

CREATE TYPE mbr EXTERNAL NAME mbr;

-- currently we only use mbr instead of
-- Envelope():Geometry
-- as that returns Geometry objects, and we prefer the explicit mbr's
-- minimum bounding rectangle (mbr)
CREATE FUNCTION mbr (p Point) RETURNS mbr external name geom.mbr;
CREATE FUNCTION mbr (c Curve) RETURNS mbr external name geom.mbr;
CREATE FUNCTION mbr (l LineString) RETURNS mbr external name geom.mbr;
CREATE FUNCTION mbr (s Surface) RETURNS mbr external name geom.mbr;
CREATE FUNCTION mbr (p Polygon) RETURNS mbr external name geom.mbr;
CREATE FUNCTION mbr (g Geometry) RETURNS mbr external name geom.mbr;

-- The srid in the *FromText Functions is currently not used
CREATE FUNCTION GeomFromText(wkt string, srid SMALLINT) RETURNS Geometry external name geom."GeomFromText";
CREATE FUNCTION PointFromText(wkt string, srid SMALLINT) RETURNS Point external name geom."PointFromText";
CREATE FUNCTION LineFromText(wkt string, srid SMALLINT) RETURNS LineString external name geom."LineFromText";
CREATE FUNCTION PolyFromText(wkt string, srid SMALLINT) RETURNS Polygon external name geom."PolyFromText";
-- alias
CREATE FUNCTION PolygonFromText(wkt string, srid SMALLINT) RETURNS Polygon external name geom."PolyFromText";

CREATE FUNCTION AsText(p Point) RETURNS STRING external name geom."AsText";
CREATE FUNCTION AsText(c Curve) RETURNS STRING external name geom."AsText";
CREATE FUNCTION AsText(l LineString) RETURNS STRING external name geom."AsText";
CREATE FUNCTION AsText(s Surface) RETURNS STRING external name geom."AsText";
CREATE FUNCTION AsText(p Polygon) RETURNS STRING external name geom."AsText";
CREATE FUNCTION AsText(g Geometry) RETURNS STRING external name geom."AsText";

CREATE FUNCTION Point(g Geometry) RETURNS Point external name geom.point;
CREATE FUNCTION Curve(g Geometry) RETURNS Curve external name geom.curve;
CREATE FUNCTION LineString(g Geometry) RETURNS LineString external name geom.linestring;
CREATE FUNCTION Surface(g Geometry) RETURNS Surface external name geom.surface;
CREATE FUNCTION Polygon(g Geometry) RETURNS Polygon external name geom.polygon;
