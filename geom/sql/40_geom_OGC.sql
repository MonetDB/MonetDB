-------------------------------------------------------------------------
--------------------- OGC - Simple Feature Access -----------------------
-------------------------------------------------------------------------
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
CREATE FUNCTION ST_Intersects(geom1 Geometry, geom2 Geometry) RETURNS boolean EXTERNAL NAME geom."Intersects";
GRANT EXECUTE ON FUNCTION ST_Intersects(Geometry, Geometry) TO PUBLIC;
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

--Functions on GeomCollection
CREATE FUNCTION ST_NumGeometries(geom Geometry) RETURNS integer EXTERNAL NAME geom."NumGeometries";
GRANT EXECUTE ON FUNCTION ST_NumGeometries(Geometry) TO PUBLIC;
CREATE FUNCTION ST_GeometryN(geom Geometry, positionNum integer) RETURNS Geometry EXTERNAL NAME geom."GeometryN";
GRANT EXECUTE ON FUNCTION ST_GeometryN(Geometry, integer) TO PUBLIC;

--TODO: Keep this?
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

--Construct a Geometry from a WKT (geom_io.c) 
--with SRID
CREATE FUNCTION ST_GeometryFromText(wkt string, srid integer) RETURNS Geometry EXTERNAL NAME geom."GeomFromText";
GRANT EXECUTE ON FUNCTION ST_GeometryFromText(string, integer) TO PUBLIC;
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

-- without SRID
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
