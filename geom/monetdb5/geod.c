#include "geod.h"

/**
*  Convertions
*
**/

const double earth_radius = 6371.009;
const double earth_radius_meters = 6371009;
#ifndef M_PI
#define M_PI	((double) 3.14159265358979323846)	/* pi */
#endif
#ifndef M_PI_2
#define M_PI_2      1.57079632679489661923132169163975144   /* pi/2           */
#endif

/* Converts a longitude value in degrees to radians */
static double
deg2RadLongitude(double lon_degrees)
{
	//Convert
	double lon = M_PI * lon_degrees / 180.0;
	//Normalize
	if (lon == -1.0 * M_PI)
		return M_PI;
	if (lon == -2.0 * M_PI)
		return 0.0;
	if (lon > 2.0 * M_PI)
		lon = remainder(lon, 2.0 * M_PI);

	if (lon < -2.0 * M_PI)
		lon = remainder(lon, -2.0 * M_PI);

	if (lon > M_PI)
		lon = -2.0 * M_PI + lon;

	if (lon < -1.0 * M_PI)
		lon = 2.0 * M_PI + lon;

	if (lon == -2.0 * M_PI)
		lon *= -1.0;

	return lon;
}

/* Converts a latitude value in degrees to radians */
static double
deg2RadLatitude(double lat_degrees)
{
	//Convert
	double lat = M_PI * lat_degrees / 180.0;
	//Normalize
	if (lat > 2.0 * M_PI)
		lat = remainder(lat, 2.0 * M_PI);

	if (lat < -2.0 * M_PI)
		lat = remainder(lat, -2.0 * M_PI);

	if (lat > M_PI)
		lat = M_PI - lat;

	if (lat < -1.0 * M_PI)
		lat = -1.0 * M_PI - lat;

	if (lat > M_PI_2)
		lat = M_PI - lat;

	if (lat < -1.0 * M_PI_2)
		lat = -1.0 * M_PI - lat;

	return lat;
}

/* Converts the GeoPoint from degrees to radians latitude and longitude*/
static GeoPoint
deg2RadPoint(GeoPoint geo)
{
	geo.lon = deg2RadLongitude(geo.lon);
	geo.lat = deg2RadLatitude(geo.lat);
	return geo;
}

/**
 *  Converts a longitude value in radians to degrees
 */
static double
rad2DegLongitude(double lon_radians)
{
	//Convert
	double lon = lon_radians * 180.0 / M_PI;
	//Normalize
	if (lon > 360.0)
		lon = remainder(lon, 360.0);

	if (lon < -360.0)
		lon = remainder(lon, -360.0);

	if (lon > 180.0)
		lon = -360.0 + lon;

	if (lon < -180.0)
		lon = 360 + lon;

	if (lon == -180.0)
		return 180.0;

	if (lon == -360.0)
		return 0.0;

	return lon;
}

/**
 *  Converts a latitude value in radians to degrees
 */
static double
rad2DegLatitude(double lat_radians)
{
	//Convert
	double lat = lat_radians * 180.0 / M_PI;
	//Normalize
	if (lat > 360.0)
		lat = remainder(lat, 360.0);

	if (lat < -360.0)
		lat = remainder(lat, -360.0);

	if (lat > 180.0)
		lat = 180.0 - lat;

	if (lat < -180.0)
		lat = -180.0 - lat;

	if (lat > 90.0)
		lat = 180.0 - lat;

	if (lat < -90.0)
		lat = -180.0 - lat;

	return lat;
}

/* Converts the GeoPoint from degrees to radians latitude and longitude*/
static GeoPoint
rad2DegPoint(GeoPoint geo)
{
	geo.lon = rad2DegLongitude(geo.lon);
	geo.lat = rad2DegLatitude(geo.lat);
	return geo;
}

/* Converts the a GEOSGeom Point into a GeoPoint */
static GeoPoint
geoPointFromGeom(GEOSGeom geom)
{
	GeoPoint geo;
	GEOSGeomGetX(geom, &(geo.lon));
	GEOSGeomGetY(geom, &(geo.lat));
	return geo;
}

/* Converts the a GEOSGeom Line into GeoLines
   Argument must be a Line geometry. */
static GeoLines
geoLinesFromGeom(GEOSGeom geom)
{
	const GEOSCoordSequence *gcs = GEOSGeom_getCoordSeq(geom);
	GeoLines geo;
	geo.pointCount = GEOSGeomGetNumPoints(geom);
	geo.points = GDKmalloc(sizeof(GeoPoint) * geo.pointCount);
	for (int i = 0; i < geo.pointCount; i++)
		GEOSCoordSeq_getXY(gcs, i, &geo.points[i].lon, &geo.points[i].lat);
	geo.bbox = NULL;
	return geo;
}

static BoundingBox * boundingBoxLines(GeoLines lines);

/* Converts the a GEOSGeom Line into GeoPolygon (with exterior ring and zero-to-multiple interior rings)
   Argument must be a Polygon geometry. */
static GeoPolygon
geoPolygonFromGeom(GEOSGeom geom)
{
	GeoPolygon geo;
	//Get exterior ring GeoLines
	geo.exteriorRing = geoLinesFromGeom((GEOSGeom)GEOSGetExteriorRing(geom));
	geo.interiorRingsCount = GEOSGetNumInteriorRings(geom);
	//If there are interior rings, allocate space to their GeoLines representation
	if (geo.interiorRingsCount > 0)
		//TODO Malloc fail exception?
		geo.interiorRings = GDKmalloc(sizeof(GeoLines) * geo.interiorRingsCount);
	else
		geo.interiorRings = NULL;
	//Get interior rings GeoLines
	for (int i = 0; i < geo.interiorRingsCount; i++)
		geo.interiorRings[i] = geoLinesFromGeom((GEOSGeom)GEOSGetInteriorRingN(geom, i));
	// If the geometry doesn't have BoundingBoxe, calculate it
	geo.bbox = boundingBoxLines(geo.exteriorRing);
	return geo;
}

static GeoPoint
geoPointFromLatLon(double lon, double lat)
{
	GeoPoint geo;
	geo.lon = lon;
	geo.lat = lat;
	return geo;
}

static str
freeGeoLines(GeoLines lines) {
	str msg = MAL_SUCCEED;
	GDKfree(lines.points);
	if (lines.bbox)
		GDKfree(lines.bbox);
	return msg;
}

static str
freeGeoPolygon(GeoPolygon polygon) {
	str msg = MAL_SUCCEED;
	msg = freeGeoLines(polygon.exteriorRing);
	if (polygon.bbox)
		GDKfree(polygon.bbox);
	for (int i = 0; i < polygon.interiorRingsCount; i++)
		msg = freeGeoLines(polygon.interiorRings[i]);
	if (polygon.interiorRings)
		GDKfree(polygon.interiorRings);
	return msg;
}

static CartPoint3D
cartPointFromXYZ(double x, double y, double z)
{
	CartPoint3D cart;
	cart.x = x;
	cart.y = y;
	cart.z = z;
	return cart;
}

/* Converts Well-Known Bytes into Geos Geometries, if they are not NULL and have the same SRID (used for geographic functions) */
str
wkbGetCompatibleGeometries(wkb * const *a, wkb * const *b, GEOSGeom *ga, GEOSGeom *gb)
{
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*a) || is_wkb_nil(*b)) {
		(*ga) = NULL;
		(*gb) = NULL;
		return MAL_SUCCEED;
	}
	(*ga) = wkb2geos(*a);
	(*gb) = wkb2geos(*b);
	if ((*ga) == NULL || (*gb) == NULL)
		err = createException(MAL, "geom.wkbGetComplatibleGeometries", SQLSTATE(38000) "Geos operation wkb2geos failed");
	else if (GEOSGetSRID((*ga)) != GEOSGetSRID(*gb)) {
		GEOSGeom_destroy(*ga);
		GEOSGeom_destroy(*gb);
		err = createException(MAL, "geom.wkbGetComplatibleGeometries", SQLSTATE(38000) "Geometries of different SRID");
	}
	return err;
}

/**
* Convert spherical coordinates to cartesian coordinates on unit sphere.
* The inputs have to be in radians.
*/
static CartPoint3D
geo2cart(GeoPoint geo)
{
	CartPoint3D cart;
	cart.x = cos(geo.lat) * cos(geo.lon);
	cart.y = cos(geo.lat) * sin(geo.lon);
	cart.z = sin(geo.lat);
	return cart;
}

/**
* Convert spherical coordinates to cartesian coordinates on unit sphere.
* The inputs have to be in degrees.
*/
static CartPoint3D
geo2cartFromDegrees(GeoPoint geo)
{
	return geo2cart(deg2RadPoint(geo));
}

/* Convert cartesian coordinates to spherical coordinates on unit sphere */
static GeoPoint
cart2geo(CartPoint3D cart)
{
	GeoPoint geo;
	geo.lon = atan2(cart.y, cart.x);
	geo.lat = asin(cart.z);
	return geo;
}

/* Converts two lat/lon points into cartesian coordinates and creates a Line geometry */
static GEOSGeom
cartesianLineFromGeoPoints(GeoPoint p1, GeoPoint p2)
{
	CartPoint3D p1_cart, p2_cart;
	p1_cart = geo2cartFromDegrees(p1);
	p2_cart = geo2cartFromDegrees(p2);
	GEOSCoordSequence *lineSeq = GEOSCoordSeq_create(2, 3);
	GEOSCoordSeq_setXYZ(lineSeq, 0, p1_cart.x, p1_cart.y, p1_cart.z);
	GEOSCoordSeq_setXYZ(lineSeq, 1, p2_cart.x, p2_cart.y, p2_cart.z);
	return GEOSGeom_createLineString(lineSeq);
}

/**
*  Bounding Box functions
*
**/
/* Adds a Cartesian Point to the BoundingBox */
static void
boundingBoxAddPoint(BoundingBox *bb, CartPoint3D p)
{
	if (bb->xmin > p.x)
		bb->xmin = p.x;
	if (bb->xmax < p.x)
		bb->xmax = p.x;
	if (bb->ymin > p.y)
		bb->ymin = p.y;
	if (bb->ymax < p.y)
		bb->ymax = p.y;
	if (bb->zmin > p.z)
		bb->zmin = p.z;
	if (bb->zmax < p.z)
		bb->zmax = p.z;
}

/* Builds the BoundingBox for a GeoLines geometry */
static BoundingBox *
boundingBoxLines(GeoLines lines)
{
	CartPoint3D c;
	BoundingBox *bb = GDKzalloc(sizeof(BoundingBox));

	//If there are no segments, return NULL
	if (lines.pointCount == 0)
		return NULL;

	c = geo2cartFromDegrees(lines.points[0]);

	//Initialize the bounding box with the first point
	bb->xmin = bb->xmax = c.x;
	bb->ymin = bb->ymax = c.y;
	bb->zmin = bb->zmax = c.z;

	for (int i = 1; i < lines.pointCount; i++) {
		c = geo2cartFromDegrees(lines.points[i]);
		boundingBoxAddPoint(bb, c);
	}
	return bb;
}

static int
boundingBoxContainsPoint(BoundingBox bb, CartPoint3D pt)
{
	return bb.xmin <= pt.x && bb.xmax >= pt.x && bb.ymin <= pt.y && bb.ymax >= pt.y && bb.zmin <= pt.z && bb.zmax >= pt.z;
}

static BoundingBox*
boundingBoxCopy(BoundingBox bb)
{
	//TODO Malloc fail?
	BoundingBox *copy = GDKmalloc(sizeof(BoundingBox));
	copy->xmin = bb.xmin;
	copy->xmax = bb.xmax;
	copy->ymin = bb.ymin;
	copy->ymax = bb.ymax;
	copy->zmin = bb.zmin;
	copy->zmax = bb.zmax;
	return copy;
}

/* Returns a point outside of the polygon's bounding box, for Point-In-Polygon calculation */
static GeoPoint
pointOutsidePolygon(GeoPolygon polygon)
{
	BoundingBox bb = *(polygon.bbox);
	BoundingBox *bb2 = boundingBoxCopy(*(polygon.bbox));

	//TODO: From POSTGIS -> CHANGE
	double grow = M_PI / 180.0 / 60.0;
	CartPoint3D corners[8];
	while (grow < M_PI) {
		if (bb.xmin > -1)
			bb.xmin -= grow;
		if (bb.ymin > -1)
			bb.ymin -= grow;
		if (bb.zmin > -1)
			bb.zmin -= grow;
		if (bb.xmax < 1)
			bb.xmax += grow;
		if (bb.ymax < 1)
			bb.ymax += grow;
		if (bb.zmax < 1)
			bb.zmax += grow;

		corners[0].x = bb.xmin;
		corners[0].y = bb.ymin;
		corners[0].z = bb.zmin;

		corners[1].x = bb.xmin;
		corners[1].y = bb.ymax;
		corners[1].z = bb.zmin;

		corners[2].x = bb.xmin;
		corners[2].y = bb.ymin;
		corners[2].z = bb.zmax;

		corners[3].x = bb.xmax;
		corners[3].y = bb.ymin;
		corners[3].z = bb.zmin;

		corners[4].x = bb.xmax;
		corners[4].y = bb.ymax;
		corners[4].z = bb.zmin;

		corners[5].x = bb.xmax;
		corners[5].y = bb.ymin;
		corners[5].z = bb.zmax;

		corners[6].x = bb.xmin;
		corners[6].y = bb.ymax;
		corners[6].z = bb.zmax;

		corners[7].x = bb.xmax;
		corners[7].y = bb.ymax;
		corners[7].z = bb.zmax;

		for (int i = 0; i < 8; i++)
			if (!boundingBoxContainsPoint(*bb2, corners[i])) {
				CartPoint3D pt_cart = corners[i];
				GDKfree(bb2);
				return rad2DegPoint(cart2geo(pt_cart));
			}
		grow *= 2.0;
	}
	//TODO: Should this be the return value in case no point is found?
	return geoPointFromLatLon(0, 0);
}

/**
* Distance functions
*
**/
/**
* The haversine formula calculate the distance in meters between two lat/lon points
* The points must be measured in radians.
* This formula assumes a spherical model of the earth, which can lead to an error of about 0.3% compared to a ellipsoidal model.
*/
static double
haversine(GeoPoint a, GeoPoint b)
{
	double d_lon = b.lon - a.lon;
	double d_lat = b.lat - a.lat;
	double d = sin(d_lat / 2) * sin(d_lat / 2) + sin(d_lon / 2) * sin(d_lon / 2) * cos(b.lat) * cos(a.lat);
	double c = 2 * atan2(sqrt(d), sqrt(1 - d));
	//TODO: Same as the previous line (which one is best?) -> 2 * asin(sqrt(d));
	return earth_radius_meters * c;
}

/* Distance between two Points */
static double
geoDistancePointPoint(GeoPoint a, GeoPoint b)
{
	return haversine(deg2RadPoint(a), deg2RadPoint(b));
}

/* Calculates the distance between the perpendicular projection of a point in the Line */
static double
calculatePerpendicularDistance(GeoPoint p_geo, GeoPoint l1_geo, GeoPoint l2_geo)
{
	CartPoint3D l1, l2, p, projection;
	GeoPoint projection_geo;

	//First, convert the points to 3D cartesian coordinates
	l1 = geo2cartFromDegrees(l1_geo);
	l2 = geo2cartFromDegrees(l2_geo);
	p = geo2cartFromDegrees(p_geo);

	//Calculate the projection of point into the line
	double d_ab = (l2.z - l1.z) * (l2.z - l1.z) + (l2.y - l1.y) * (l2.y - l1.y) + (l2.x - l1.x) * (l2.x - l1.x);
	double t = (((p.x - l1.x) * (l2.x - l1.x)) + ((p.y - l1.y) * (l2.y - l1.y)) + ((p.z - l1.z) * (l2.z - l1.z))) / d_ab;

	//If t is not between 0 and 1, the projected point is not in the line, so there is no perpendicular, return huge number
	if (t < 0 || t > 1)
		return INT_MAX;

	//If the projection is in the line segment, build the point -> projection = l1 + t * (l2-l1)
	projection = cartPointFromXYZ(l1.x + t * (l2.x - l1.x), l1.y + t * (l2.y - l1.y), l1.z + t * (l2.z - l1.z));

	//Convert into geographic coordinates (radians)
	projection_geo = cart2geo(projection);

	//Calculate distance from original point to the projection
	return haversine(deg2RadPoint(p_geo), projection_geo);
}

/* Distance between Point and Line
   The returned distance is the minimum distance between the point and the line vertices
   and the perpendicular projection of the point in each line segment.  */
static double
geoDistancePointLine(GeoPoint point, GeoLines lines, double distance_min_limit)
{
	double distancePoint, distancePerpendicular, min_distance = INT_MAX;
	for (int i = 0; i < lines.pointCount-1; i++) {
		distancePoint = geoDistancePointPoint(point, lines.points[i]);
		distancePerpendicular = calculatePerpendicularDistance(point,lines.points[i],lines.points[i+1]);
		if (distancePoint < min_distance)
			min_distance = distancePoint;
		if (distancePerpendicular < min_distance)
			min_distance = distancePerpendicular;
		//Shortcut, if the geometries are already at their minimum distance
		if (min_distance <= distance_min_limit)
			return min_distance;
	}
	distancePoint = geoDistancePointPoint(point, lines.points[lines.pointCount-1]);
	return distancePoint < min_distance ? distancePoint : min_distance;
}

/* Distance between two Lines. */
static double
geoDistanceLineLine(GeoLines line1, GeoLines line2, double distance_min_limit)
{
	double distance, min_distance = INT_MAX;
	for (int i = 0; i < line1.pointCount; i++) {
		distance = geoDistancePointLine(line1.points[i], line2, distance_min_limit);
		if (distance < min_distance)
			min_distance = distance;
		//Shortcut, if the geometries are already at their minimum distance
		if (min_distance <= distance_min_limit)
			return min_distance;
	}
	for (int i = 0; i < line2.pointCount; i++) {
		for (int j = 0; j < line1.pointCount - 1; j++) {
			distance = calculatePerpendicularDistance(line2.points[i],line1.points[j],line1.points[j+1]);
			if (distance < min_distance)
				min_distance = distance;
			//Shortcut, if the geometries are already at their minimum distance
			if (min_distance <= distance_min_limit)
				return min_distance;
		}
	}
	return min_distance;
}

/* Checks if a Point is within a Polygon */
static bool
pointWithinPolygon(GeoPolygon polygon, GeoPoint point)
{
	int intersectionNum = 0;
	GEOSGeometry *segmentPolygon, *intersectionPoints;
	GeoLines polygonRing;

	//Get an point that's outside the polygon
	GeoPoint outsidePoint = pointOutsidePolygon(polygon);

	//No outside point was found, return false
	if (outsidePoint.lat == 0 && outsidePoint.lon == 0)
		return false;

	/*printf("Outside point: (%f %f)\n",outsidePoint.lon, outsidePoint.lat);
	fflush(stdout);*/

	//Construct a line between the outside point and the input point
	GEOSGeometry *outInLine = cartesianLineFromGeoPoints(point, outsidePoint);

	//TODO This is producing wrong results, review the intersection conditional
	//Count the number of intersections between the polygon exterior ring and the constructed line
	polygonRing = polygon.exteriorRing;
	for (int i = 0; i < polygonRing.pointCount-1; i++) {
		segmentPolygon = cartesianLineFromGeoPoints(polygonRing.points[i], polygonRing.points[i+1]);
		intersectionPoints = GEOSIntersection(segmentPolygon, outInLine);

		//If there is an intersection, a point will be returned (line when there is none)
		if (GEOSGeomTypeId(intersectionPoints) == GEOS_POINT)
			intersectionNum++;

		if (intersectionPoints != NULL)
			GEOSGeom_destroy(intersectionPoints);
		if (segmentPolygon != NULL)
			GEOSGeom_destroy(segmentPolygon);
	}

	//Count the number of intersections between the polygon interior rings and the constructed line
	for (int j = 0; j < polygon.interiorRingsCount; j++) {
		polygonRing = polygon.interiorRings[j];
		for (int i = 0; i < polygonRing.pointCount-1; i++) {
			segmentPolygon = cartesianLineFromGeoPoints(polygonRing.points[i], polygonRing.points[i+1]);
			intersectionPoints = GEOSIntersection(segmentPolygon, outInLine);

			//If there is an intersection, a point will be returned (line when there is none)
			if (GEOSGeomTypeId(intersectionPoints) == GEOS_POINT)
				intersectionNum++;

			if (intersectionPoints != NULL)
				GEOSGeom_destroy(intersectionPoints);
			if (segmentPolygon != NULL)
				GEOSGeom_destroy(segmentPolygon);
		}
	}

	if (outInLine != NULL)
		GEOSGeom_destroy(outInLine);

	//If even, the point is not within the polygon. If odd, it is within
	return intersectionNum % 2 == 1;
}

/* Distance between Point and Polygon.*/
static double
geoDistancePointPolygon(GeoPoint point, GeoPolygon polygon, double distance_min_limit)
{
	//Check if point is in polygon
	if (pointWithinPolygon(polygon, point))
		return 0;

	//Calculate distance from Point to the exterior and interior rings of the polygon
	double distance, min_distance = INT_MAX;
	//First, calculate distance to the exterior ring
	min_distance = geoDistancePointLine(point, polygon.exteriorRing, distance_min_limit);
	//Then, calculate distance to the interior rings
	for (int i = 0; i < polygon.interiorRingsCount; i++) {
		//Shortcut, if the geometries are already at their minimum distance
		if (min_distance <= distance_min_limit)
			return min_distance;
		distance = geoDistancePointLine(point, polygon.interiorRings[i], distance_min_limit);
		if (distance < min_distance)
			min_distance = distance;
	}
	return min_distance;
}

/* Distance between Line and Polygon. */
static double
geoDistanceLinePolygon(GeoLines line, GeoPolygon polygon, double distance_min_limit)
{
	double distance, min_distance = INT_MAX;
	//Calculate distance to all start vertices of the line
	for (int i = 0; i < line.pointCount; i++) {
		distance = geoDistancePointPolygon(line.points[i], polygon, distance_min_limit);

		//Short-cut in case the point is within the polygon
		if (distance <= distance_min_limit)
			return distance;

		if (distance < min_distance)
			min_distance = distance;
	}
	return min_distance;
}

/* Distance between two Polygons. */
static double
geoDistancePolygonPolygon(GeoPolygon polygon1, GeoPolygon polygon2, double distance_min_limit)
{
	double distance1, distance2;
	//Calculate the distance between the exterior ring of polygon1 and all segments of polygon2 (including the interior rings)
	distance1 = geoDistanceLinePolygon(polygon1.exteriorRing, polygon2, distance_min_limit);
	//Shortcut, if the geometries are already at their minimum distance
	if (distance1 <= distance_min_limit)
		return distance1;
	distance2 = geoDistanceLinePolygon(polygon2.exteriorRing, polygon1, distance_min_limit);
	return distance1 < distance2 ? distance1 : distance2;
}

/* Distance between two (non-collection) geometries. */
static double
geoDistanceSingle(GEOSGeom aGeom, GEOSGeom bGeom, double distance_min_limit)
{
	int dimA, dimB;
	double distance = INT_MAX;
	dimA = GEOSGeom_getDimensions(aGeom);
	dimB = GEOSGeom_getDimensions(bGeom);
	if (dimA == 0 && dimB == 0) {
		/* Point and Point */
		GeoPoint a = geoPointFromGeom(aGeom);
		GeoPoint b = geoPointFromGeom(bGeom);
		distance = geoDistancePointPoint(a, b);
	} else if (dimA == 0 && dimB == 1) {
		/* Point and Line/LinearRing */
		GeoPoint a = geoPointFromGeom(aGeom);
		GeoLines b = geoLinesFromGeom(bGeom);
		distance = geoDistancePointLine(a, b, distance_min_limit);
		freeGeoLines(b);
	} else if (dimA == 1 && dimB == 0) {
		/* Line/LinearRing and Point */
		GeoLines a = geoLinesFromGeom(aGeom);
		GeoPoint b = geoPointFromGeom(bGeom);
		distance = geoDistancePointLine(b, a, distance_min_limit);
		freeGeoLines(a);
	} else if (dimA == 1 && dimB == 1) {
		/* Line/LinearRing and Line/LinearRing */
		GeoLines a = geoLinesFromGeom(aGeom);
		GeoLines b = geoLinesFromGeom(bGeom);
		distance = geoDistanceLineLine(a, b, distance_min_limit);
		freeGeoLines(a);
		freeGeoLines(b);
	} else if (dimA == 0 && dimB == 2) {
		/* Point and Polygon */
		GeoPoint a = geoPointFromGeom(aGeom);
		GeoPolygon b = geoPolygonFromGeom(bGeom);
		distance = geoDistancePointPolygon(a, b, distance_min_limit);
		freeGeoPolygon(b);
	} else if (dimA == 2 && dimB == 0) {
		/* Polygon and Point */
		GeoPolygon a = geoPolygonFromGeom(aGeom);
		GeoPoint b = geoPointFromGeom(bGeom);
		distance = geoDistancePointPolygon(b, a, distance_min_limit);
		freeGeoPolygon(a);
	} else if (dimA == 1 && dimB == 2) {
		/* Line/LinearRing and Polygon */
		GeoLines a = geoLinesFromGeom(aGeom);
		GeoPolygon b = geoPolygonFromGeom(bGeom);
		distance = geoDistanceLinePolygon(a, b, distance_min_limit);
		freeGeoLines(a);
		freeGeoPolygon(b);
	} else if (dimA == 2 && dimB == 1) {
		/* Polygon and Line/LinearRing */
		GeoPolygon a = geoPolygonFromGeom(aGeom);
		GeoLines b = geoLinesFromGeom(bGeom);
		distance = geoDistanceLinePolygon(b, a, distance_min_limit);
		freeGeoPolygon(a);
		freeGeoLines(b);
	} else if (dimA == 2 && dimB == 2) {
		/* Polygon and Polygon */
		GeoPolygon a = geoPolygonFromGeom(aGeom);
		GeoPolygon b = geoPolygonFromGeom(bGeom);
		distance = geoDistancePolygonPolygon(a, b, distance_min_limit);
		freeGeoPolygon(a);
		freeGeoPolygon(b);
	}
	return distance;
}

//The distance_min_limit argument is used for DWithin and Intersects.
//If we get to the minimum distance for the predicate, return immediatly
//It is equal to 0 if the operation is Distance
static double
geoDistanceInternal(GEOSGeom a, GEOSGeom b, double distance_min_limit)
{
	int numGeomsA = GEOSGetNumGeometries(a), numGeomsB = GEOSGetNumGeometries(b);
	double distance, min_distance = INT_MAX;
	GEOSGeometry *geo1, *geo2;
	for (int i = 0; i < numGeomsA; i++) {
		geo1 = (GEOSGeometry *)GEOSGetGeometryN((const GEOSGeometry *)a, i);
		for (int j = 0; j < numGeomsB; j++) {
			geo2 = (GEOSGeometry *)GEOSGetGeometryN((const GEOSGeometry *)b, j);
			distance = geoDistanceSingle(geo1, geo2, distance_min_limit);
			//Shortcut, if the geometries are already at their minimum distance (0 in the case of normal Distance)
			if (distance <= distance_min_limit)
				return distance;
			if (distance < min_distance)
				min_distance = distance;
		}
	}
	return min_distance;
}

/**
* Distance
*
**/
/* Calculates the distance, in meters, between two geographic geometries with latitude/longitude coordinates */
str
wkbDistanceGeographic(dbl *out, wkb * const *a, wkb * const *b)
{
	str err = MAL_SUCCEED;
	GEOSGeom ga, gb;
	err = wkbGetCompatibleGeometries(a, b, &ga, &gb);
	if (ga && gb) {
		(*out) = geoDistanceInternal(ga, gb, 0);
	}
	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);
	return err;
}

/**
* Distance Within
*
**/
/* Checks if two geographic geometries are within d meters of one another */
str
wkbDWithinGeographic(bit *out, wkb * const *a, wkb * const *b, const dbl *d)
{
	str err = MAL_SUCCEED;
	GEOSGeom ga, gb;
	double distance;
	err = wkbGetCompatibleGeometries(a, b, &ga, &gb);
	if (ga && gb) {
		distance = geoDistanceInternal(ga, gb, *d);
		(*out) = (distance <= (*d));
	}
	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);
	return err;
}

/**
* Intersects
*
**/
/* Checks if two geographic geometries intersect at any point */
str
wkbIntersectsGeographic(bit *out, wkb * const *a, wkb * const *b)
{
	str err = MAL_SUCCEED;
	GEOSGeom ga, gb;
	double distance;
	err = wkbGetCompatibleGeometries(a, b, &ga, &gb);
	if (ga && gb) {
		distance = geoDistanceInternal(ga, gb, 0);
		(*out) = (distance == 0);
	}
	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);
	return err;
}

/* Checks if a Polygon covers a Line geometry */
static bool
geoPolygonCoversLine(GeoPolygon polygon, GeoLines lines)
{
	for (int i = 0; i < lines.pointCount; i++) {
		if (pointWithinPolygon(polygon, lines.points[i]) == false)
			return false;
	}
	return true;
}

/* Compares two GeoPoints, returns true if they're equal */
static bool
geoPointEquals(GeoPoint pointA, GeoPoint pointB)
{
	return (pointA.lat == pointB.lat) && (pointA.lon = pointB.lon);
}

//TODO Check if this works correctly
static bool
geoCoversSingle(GEOSGeom a, GEOSGeom b)
{
	int dimA = GEOSGeom_getDimensions(a), dimB = GEOSGeom_getDimensions(b);
	if (dimA < dimB)
		//If the dimension of A is smaller than B, then it must not cover it
		return false;

	if (dimA == 0){
		//A and B are Points
		GeoPoint pointA = geoPointFromGeom(a);
		GeoPoint pointB = geoPointFromGeom(b);
		return geoPointEquals(pointA, pointB);
	} else if (dimA == 1) {
		//A is Line
		//GeoLines lineA = geoLinesFromGeom(a);
		if (dimB == 0) {
			//B is Point
			//GeoPoint pointB = geoPointFromGeom(b);
			//return geoLineCoversPoint(lineA,pointB);
			return false;
		} else {
			//B is Line
			//GeoLines lineB = geoLinesFromGeom(b);
			//return geoLineCoversLine(lineA, lineB);
			return false;
		}
	} else if (dimA == 2) {
		//A is Polygon
		GeoPolygon polygonA = geoPolygonFromGeom(a);
		if (dimB == 0){
			//B is Point
			GeoPoint pointB = geoPointFromGeom(b);
			return pointWithinPolygon(polygonA, pointB);
		} else if (dimB == 1) {
			//B is Line
			GeoLines lineB = geoLinesFromGeom(b);
			return geoPolygonCoversLine(polygonA, lineB);
		} else {
			//B is Polygon
			GeoPolygon polygonB = geoPolygonFromGeom(b);
			//If every point in the exterior ring of B is covered, polygon B is covered by polygon A
			return geoPolygonCoversLine(polygonA, polygonB.exteriorRing);
		}
	} else
		return false;
}

static bool
geoCoversInternal(GEOSGeom a, GEOSGeom b)
{
	int numGeomsA = GEOSGetNumGeometries(a), numGeomsB = GEOSGetNumGeometries(b);
	GEOSGeometry *geo1, *geo2;
	for (int i = 0; i < numGeomsA; i++) {
		geo1 = (GEOSGeometry *)GEOSGetGeometryN((const GEOSGeometry *)a, i);
		for (int j = 0; j < numGeomsB; j++) {
			geo2 = (GEOSGeometry *)GEOSGetGeometryN((const GEOSGeometry *)b, j);
			if (geoCoversSingle(geo1, geo2) == 0)
				return 0;
		}
	}
	return 1;
}

/**
* Covers
*
**/
/* Checks if no point of Geometry B is outside Geometry A */
str
wkbCoversGeographic(bit *out, wkb * const *a, wkb * const *b)
{
	str err = MAL_SUCCEED;
	GEOSGeom ga, gb;
	err = wkbGetCompatibleGeometries(a, b, &ga, &gb);
	if (ga && gb)
		(*out) = geoCoversInternal(ga, gb);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return err;
}

/**
 * FILTER FUNCTIONS
 **/

static inline bit
geosDistanceWithin (GEOSGeom geom1, GEOSGeom geom2, dbl distance_within) {
	dbl distance = geoDistanceInternal(geom1, geom2, distance_within);
	return distance <= distance_within;
}

//TODO Change BUNappend with manual insertion into the result BAT
static str
filterSelectGeomGeomDoubleToBit(bat* outid, const bat *bid , const bat *sid, const wkb *wkb_const, dbl double_flag, bit anti, bit (*func) (GEOSGeom, GEOSGeom, dbl), const char *name)
{
	BAT *out = NULL, *b = NULL, *s = NULL;
	BATiter b_iter;
	struct canditer ci;
	GEOSGeom col_geom, const_geom;

	//Check if the geometry is null and convert to GEOS
	if ((const_geom = wkb2geos(wkb_const)) == NULL) {
		if ((out = BATdense(0, 0, 0)) == NULL)
			throw(MAL, name, GDK_EXCEPTION);
		*outid = out->batCacheid;
		BBPkeepref(out);
		return MAL_SUCCEED;
	}
	//get the BATs
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	//get the candidate lists
	if (sid && !is_bat_nil(*sid) && !(s = BATdescriptor(*sid))) {
		BBPunfix(b->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	canditer_init(&ci, b, s);
	//create a new BAT for the output
	if ((out = COLnew(0, ATOMindex("oid"), ci.ncand, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b_iter = bat_iterator(b);
	//Loop through column and compare with constant
	for (BUN i = 0; i < ci.ncand; i++) {
		oid c_oid = canditer_next(&ci);
		const wkb *col_wkb = BUNtvar(b_iter, c_oid - b->hseqbase);
		if ((col_geom = wkb2geos(col_wkb)) == NULL)
			continue;
		if (GEOSGetSRID(col_geom) != GEOSGetSRID(const_geom)) {
			GEOSGeom_destroy(col_geom);
			GEOSGeom_destroy(const_geom);
			bat_iterator_end(&b_iter);
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			BBPreclaim(out);
			throw(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
		}
		//Apply the (Geom, Geom, double) -> bit function
		bit cond = (*func)(col_geom, const_geom, double_flag);
		if (cond != anti) {
			if (BUNappend(out, &c_oid, false) != GDK_SUCCEED) {
				if (col_geom)
					GEOSGeom_destroy(col_geom);
				if (const_geom)
					GEOSGeom_destroy(const_geom);
				bat_iterator_end(&b_iter);
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPreclaim(out);
				throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		GEOSGeom_destroy(col_geom);
	}
	GEOSGeom_destroy(const_geom);
	bat_iterator_end(&b_iter);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	*outid = out->batCacheid;
	BBPkeepref(out);
	return MAL_SUCCEED;
}

static str
filterJoinGeomGeomDoubleToBit(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, double double_flag, const bat *ls_id, const bat *rs_id, bit nil_matches, const lng *estimate, bit anti, bit (*func) (GEOSGeom, GEOSGeom, dbl), const char *name)
{
	BAT *lres = NULL, *rres = NULL, *l = NULL, *r = NULL, *ls = NULL, *rs = NULL;
	BATiter l_iter, r_iter;
	str msg = MAL_SUCCEED;
	struct canditer l_ci, r_ci;
	GEOSGeom l_geom, r_geom;
	GEOSGeom *l_geoms = NULL, *r_geoms = NULL;
	BUN est;

	//get the input BATs
	l = BATdescriptor(*l_id);
	r = BATdescriptor(*r_id);
	if (l == NULL || r == NULL) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//get the candidate lists
	if (ls_id && !is_bat_nil(*ls_id) && !(ls = BATdescriptor(*ls_id)) && rs_id && !is_bat_nil(*rs_id) && !(rs = BATdescriptor(*rs_id))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto free;
	}
	canditer_init(&l_ci, l, ls);
	canditer_init(&r_ci, r, rs);
	//create new BATs for the output
	est = is_lng_nil(*estimate) || *estimate == 0 ? l_ci.ncand : (BUN) *estimate;
	if ((lres = COLnew(0, ATOMindex("oid"), est, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}
	if ((rres = COLnew(0, ATOMindex("oid"), est, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	//Allocate arrays for reutilizing GEOS type conversion
	if ((l_geoms = GDKmalloc(l_ci.ncand * sizeof(GEOSGeometry *))) == NULL || (r_geoms = GDKmalloc(r_ci.ncand * sizeof(GEOSGeometry *))) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	l_iter = bat_iterator(l);
	r_iter = bat_iterator(r);

	//Convert wkb to GEOS only once
	for (BUN i = 0; i < l_ci.ncand; i++) {
		oid l_oid = canditer_next(&l_ci);
		l_geoms[i] = wkb2geos((const wkb*) BUNtvar(l_iter, l_oid - l->hseqbase));
	}
	for (BUN j = 0; j < r_ci.ncand; j++) {
		oid r_oid = canditer_next(&r_ci);
		r_geoms[j] = wkb2geos((const wkb*)BUNtvar(r_iter, r_oid - r->hseqbase));
	}

	canditer_reset(&l_ci);
	for (BUN i = 0; i < l_ci.ncand; i++) {
		oid l_oid = canditer_next(&l_ci);
		l_geom = l_geoms[i];
		if (!nil_matches && l_geom == NULL)
			continue;
		canditer_reset(&r_ci);
		for (BUN j = 0; j < r_ci.ncand; j++) {
			oid r_oid = canditer_next(&r_ci);
			r_geom = r_geoms[j];
			//Null handling
			if (r_geom == NULL) {
				if (nil_matches && l_geom == NULL) {
					if (BUNappend(lres, &l_oid, false) != GDK_SUCCEED || BUNappend(rres, &r_oid, false) != GDK_SUCCEED) {
						msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
						bat_iterator_end(&l_iter);
						bat_iterator_end(&r_iter);
						goto free;
					}
				}
				else
					continue;
			}
			//TODO Do we need to do this check for every element?
			if (GEOSGetSRID(l_geom) != GEOSGetSRID(r_geom)) {
				msg = createException(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
				bat_iterator_end(&l_iter);
				bat_iterator_end(&r_iter);
				goto free;
			}
			//Apply the (Geom, Geom) -> bit function
			bit cond = (*func)(l_geom, r_geom, double_flag);
			if (cond != anti) {
				if (BUNappend(lres, &l_oid, false) != GDK_SUCCEED || BUNappend(rres, &r_oid, false) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					bat_iterator_end(&l_iter);
					bat_iterator_end(&r_iter);
					goto free;
				}
			}
		}
	}
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy(l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy(r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	bat_iterator_end(&l_iter);
	bat_iterator_end(&r_iter);
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	*lres_id = lres->batCacheid;
	BBPkeepref(lres);
	*rres_id = rres->batCacheid;
	BBPkeepref(rres);
	return MAL_SUCCEED;
free:
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy(l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy(r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	if (lres)
		BBPreclaim(lres);
	if (rres)
		BBPreclaim(rres);
	return msg;
}

str
wkbDWithinGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *d_id, const bat *ls_id, const bat *rs_id, const bit *nil_matches, const lng *estimate, const bit *anti) {
	double distance_within = 0;
	BAT *d = NULL;
	//Get the distance BAT and get the double value
	if ((d = BATdescriptor(*d_id)) == NULL) {
		throw(MAL, "geom.wkbDWithinGeographicJoin", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATcount(d) == 1) {
		distance_within = *((double*) Tloc(d, 0));
	}
	BBPunfix(d->batCacheid);
	return filterJoinGeomGeomDoubleToBit(lres_id,rres_id,l_id,r_id,distance_within,ls_id,rs_id,*nil_matches,estimate,*anti,geosDistanceWithin,"geom.wkbDWithinGeographicJoin");
}

str
wkbDWithinGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb * const *wkb_const, const dbl *distance_within, const bit *anti) {
	return filterSelectGeomGeomDoubleToBit(outid,bid,sid,*wkb_const,*distance_within,*anti,geosDistanceWithin,"geom.wkbDWithinGeographicSelect");
}

str
wkbIntersectsGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, const bit *nil_matches, const lng *estimate, const bit *anti) {
	return filterJoinGeomGeomDoubleToBit(lres_id,rres_id,l_id,r_id,0,ls_id,rs_id,*nil_matches,estimate,*anti,geosDistanceWithin,"geom.wkbIntersectsGeographicJoin");
}

str
wkbIntersectsGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb * const *wkb_const, const bit *anti) {
	return filterSelectGeomGeomDoubleToBit(outid,bid,sid,*wkb_const,0,*anti,geosDistanceWithin,"geom.wkbIntersectsGeographicSelect");
}

static inline CartPoint3D
crossProduct (const CartPoint3D *p1, const CartPoint3D *p2)
{
	CartPoint3D p3;
	p3.x = p1->y * p2->z - p1->z * p2->y;
	p3.y = p1->z * p2->x - p1->x * p2->z;
	p3.z = p1->x * p2->y - p1->y * p2->x;
	return p3;
}

static inline double
dotProduct (const CartPoint3D *p1, const CartPoint3D *p2)
{
	return (p1->x * p2->x) + (p1->y * p2->y) + (p1->z * p2->z);
}

//
static inline int
angleRotation (const CartPoint2D p1, const CartPoint2D p2, const CartPoint2D p3)
{
	// vector P = P1P2
	// vector Q = P1P3
	// side = || Q x P ||
	// Q and P are 2D vectors, so z component is 0
	double side = ((p3.x - p1.x) * (p2.y - p1.y) - (p2.x - p1.x) * (p3.y - p1.y));
	//
	if (side < 0)
		return -1;
	else if (side > 0)
		return 1;
	else
		return 0;
}

static void
normalize2D (CartPoint2D *p) {
	double c = sqrt(p->x * p->x + p->y * p->y);
	if (c == 0) {
		p->x = p->y = 0;
		return;
	}
	p->x = p->x / c;
	p->y = p->y / c;
}

static void
normalize3D (CartPoint3D *p) {
	double c = sqrt(p->x * p->x + p->y * p->y + p->z * p->z);
	if (c == 0) {
		p->x = p->y = p->z = 0;
		return;
	}
	p->x = p->x / c;
	p->y = p->y / c;
	p->z = p->z / c;
}

static inline bool
FP_EQUALS (double x, double y)
{
	return fabs(x-y) < 1e-12;
}

str
geodeticEdgeBoundingBox(const CartPoint3D* p1, const CartPoint3D* p2, BoundingBox* mbox)
{
	CartPoint3D p3, pn, ep3d;
	CartPoint3D e[6];
	CartPoint2D s1, s2, ep, o;
	int rotation_to_origin, rotation_to_ep;

    // check coinciding points
	if (FP_EQUALS(p1->x,p2->x) && FP_EQUALS(p1->y,p2->y) && FP_EQUALS(p1->z,p2->z))
		return MAL_SUCCEED;
    // check antipodal points
	if (FP_EQUALS(p1->x,-p2->x) && FP_EQUALS(p1->y,-p2->y) && FP_EQUALS(p1->z,-p2->z))
		throw(MAL, "geom.geodeticEdgeBoundingBox", SQLSTATE(38000) "Antipodal edge");

    // create the great circle plane coord system (p1, p3)
    // pn = p1 x p2
    // p3 = pn x p1
	// TODO handle the narrow and wide angle cases
	pn = crossProduct(p1,p2);
	normalize3D(&pn);
	p3 = crossProduct(&pn,p1);
	normalize3D(&p3);

    // represent p1, p2 with (s1, s2) 2-D space
    // s1.x = 1, s1.y = 0
    // s2.x = p2 * p1, s2.y = p2 * p3
	s1.x = 1;
	s1.y = 0;
	s2.x = dotProduct(p2, p1);
	s2.y = dotProduct(p2, &p3);
    // 2-D space origin
    // O.x = 0, O.y = 0
	o.x = 0;
	o.y = 0;

    // create 3D endpoints E.x, E.-x, ...
    // E.x = (1, 0, 0), E.-x = (-1, 0, 0) ...
	memset(e, 0, sizeof(CartPoint2D) * 6);
	e[0].x = e[1].y = e[2].z = 1;
	e[3].x = e[4].y = e[5].z = -1;

    // find the rotation between s1->s2 and s1->O
    // rot = norm( vec(s1,s2) x vec(s1,0))
	rotation_to_origin = angleRotation(s1,s2,o);

    // for every endpoint E
	for (int i = 0; i < 6; i++) {
		// project the endpoint in the 2-D space
		ep.x = dotProduct(&e[i],p1);
		ep.y = dotProduct(&e[i],&p3);
        // re-normalize it e.g. EP (for endpoint_projection)
		normalize2D(&ep);
        // ep_rot = norm( vec(s1,s2) x vec(s1,EP_end) )
		rotation_to_ep = angleRotation(s1,s2,ep);
		if (rotation_to_origin != rotation_to_ep) {
			// convert the 2-D EP into 3-D space
			ep3d.x = ep.x * p1->x + ep.y * p3.x;
			ep3d.y = ep.x * p1->y + ep.y * p3.y;
			ep3d.z = ep.x * p1->z + ep.y * p3.z;
            // expand the mbox in order to include 3-D representation of EP
			boundingBoxAddPoint(mbox,ep3d);
		}
	}
	return MAL_SUCCEED;
}
