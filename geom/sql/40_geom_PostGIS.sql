-------------------------------------------------------------------------
------------------------------- PostGIS ---------------------------------
-------------------------------------------------------------------------

-------------------------------------------------------------------------
------------------------- Geometry Constructors -------------------------
-------------------------------------------------------------------------
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
CREATE AGGREGATE ST_MakeLine(geom Geometry) RETURNS Geometry external name geom."MakeLine";
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
CREATE FUNCTION ST_DWithin(geom1 Geometry, geom2 Geometry, dst double) RETURNS boolean EXTERNAL NAME geom."DWithin";
GRANT EXECUTE ON FUNCTION ST_DWithin(Geometry, Geometry, double) TO PUBLIC;
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
