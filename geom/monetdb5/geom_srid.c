#include "geom_srid.h"
#include "geom.h"

/* SRID functions */

/* returns the srid of the geometry */
str
wkbGetSRID(int *out, wkb **geomWKB)
{
	return wkbBasicInt(out, *geomWKB, GEOSGetSRID, "geom.GetSRID");
}

/* sets the srid of the geometry */
str
wkbSetSRID(wkb **resultGeomWKB, wkb **geomWKB, int *srid)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geomWKB) || is_int_nil(*srid)) {
		if ((*resultGeomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.setSRID", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geomWKB)) == NULL)
		throw(MAL, "geom.setSRID", SQLSTATE(38000) "Geos operation wkb2geos failed");

	GEOSSetSRID(geosGeometry, *srid);
	*resultGeomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*resultGeomWKB == NULL)
		throw(MAL, "geom.setSRID", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

/* Transformation functions (projections) */
#ifdef HAVE_PROJ

/* math.h files do not have M_PI defined */
#ifndef M_PI
#define M_PI	((double) 3.14159265358979323846)	/* pi */
#endif

/** convert degrees to radians */
static inline void
degrees2radians(double *x, double *y, double *z)
{
	double val = M_PI / 180.0;
	*x *= val;
	*y *= val;
	*z *= val;
}

/** convert radians to degrees */
static inline void
radians2degrees(double *x, double *y, double *z)
{
	double val = 180.0 / M_PI;
	*x *= val;
	*y *= val;
	*z *= val;
}

static str
transformCoordSeq(int idx, int coordinatesNum, projPJ proj4_src, projPJ proj4_dst, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;
	int *errorNum = 0;

	if (!GEOSCoordSeq_getX(gcs_old, idx, &x) ||
	    !GEOSCoordSeq_getY(gcs_old, idx, &y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, idx, &z)))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot get coordinates");

	/* check if the passed reference system is geographic (proj=latlong)
	 * and change the degrees to radians because pj_transform works with radians*/
	if (pj_is_latlong(proj4_src))
		degrees2radians(&x, &y, &z);

	pj_transform(proj4_src, proj4_dst, 1, 0, &x, &y, &z);

	errorNum = pj_get_errno_ref();
	if (*errorNum != 0) {
		if (coordinatesNum > 2)
			throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot transform point (%f %f %f): %s\n", x, y, z, pj_strerrno(*errorNum));
		else
			throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot transform point (%f %f): %s\n", x, y, pj_strerrno(*errorNum));
	}

	/* check if the destination reference system is geographic and change
	 * the destination coordinates from radians to degrees */
	if (pj_is_latlong(proj4_dst))
		radians2degrees(&x, &y, &z);

	if (!GEOSCoordSeq_setX(gcs_new, idx, x) ||
	    !GEOSCoordSeq_setY(gcs_new, idx, y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, idx, z)))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot set coordinates");

	return MAL_SUCCEED;
}

static str
transformPoint(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str ret = MAL_SUCCEED;

	*transformedGeometry = NULL;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* create the coordinates sequence for the transformed geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);
	if (gcs_new == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* create the transformed coordinates */
	ret = transformCoordSeq(0, coordinatesNum, proj4_src, proj4_dst, gcs_old, gcs_new);
	if (ret != MAL_SUCCEED) {
		GEOSCoordSeq_destroy(gcs_new);
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createPoint(gcs_new);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}

	return MAL_SUCCEED;
}

static str
transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the transformed geometry */
	*gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (*gcs_new == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	/* create the transformed coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = transformCoordSeq(i, coordinatesNum, proj4_src, proj4_dst, gcs_old, *gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(*gcs_new);
			*gcs_new = NULL;
			return ret;
		}
	}

	return MAL_SUCCEED;
}

static str
transformLineString(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst)
{
	GEOSCoordSeq coordSeq;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, proj4_src, proj4_dst);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLineString(coordSeq);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}

	return ret;
}

static str
transformLinearRing(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst)
{
	GEOSCoordSeq coordSeq = NULL;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, proj4_src, proj4_dst);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLinearRing(coordSeq);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}

	return ret;
}

static str
transformPolygon(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst, int srid)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*transformedGeometry = NULL;
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}

	ret = transformLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, proj4_src, proj4_dst);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	GEOSSetSRID(transformedExteriorRingGeometry, srid);

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*transformedGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}

	if(numInteriorRings > 0)
	{
		/* iterate over the interiorRing and transform each one of them */
		transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
		if (transformedInteriorRingGeometries == NULL) {
			*transformedGeometry = NULL;
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		for (i = 0; i < numInteriorRings; i++) {
			ret = transformLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), proj4_src, proj4_dst);
			if (ret != MAL_SUCCEED) {
				while (--i >= 0)
					GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
				GDKfree(transformedInteriorRingGeometries);
				GEOSGeom_destroy(transformedExteriorRingGeometry);
				*transformedGeometry = NULL;
				return ret;
			}
			GEOSSetSRID(transformedInteriorRingGeometries[i], srid);
		}
	}

	*transformedGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*transformedGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return ret;
}

static str
transformMultiGeometry(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, projPJ proj4_src, projPJ proj4_dst, int srid, int geometryType)
{
	int geometriesNum, subGeometryType, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	const GEOSGeometry *multiGeometry = NULL;
	str ret = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	if (geometriesNum == -1)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetNumGeometries failed");
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < geometriesNum; i++) {
		if ((multiGeometry = GEOSGetGeometryN(geosGeometry, i)) == NULL)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetGeometryN failed");
		else if ((subGeometryType = GEOSGeomTypeId(multiGeometry) + 1) == 0)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
		else {
			switch (subGeometryType) {
			case wkbPoint_mdb:
				ret = transformPoint(&transformedMultiGeometries[i], multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbLineString_mdb:
				ret = transformLineString(&transformedMultiGeometries[i], multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbLinearRing_mdb:
				ret = transformLinearRing(&transformedMultiGeometries[i], multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbPolygon_mdb:
				ret = transformPolygon(&transformedMultiGeometries[i], multiGeometry, proj4_src, proj4_dst, srid);
				break;
			default:
				ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos unknown geometry type");
				break;
			}
		}

		if (ret != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*transformedGeometry = NULL;
			return ret;
		}

		GEOSSetSRID(transformedMultiGeometries[i], srid);
	}

	*transformedGeometry = GEOSGeom_createCollection(geometryType - 1, transformedMultiGeometries, geometriesNum);
	if (*transformedGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return ret;
}

/* the following function is used in postgis to get projPJ from str.
 * it is necessary to do it in a detailed way like that because pj_init_plus
 * does not set all parameters correctly and I cannot test whether the
 * coordinate reference systems are geographic or not */
static projPJ
projFromStr(const char *projStr)
{
	int t;
	char *params[1024];	// one for each parameter
	char *loc;
	char *str;
	projPJ result;

	if (projStr == NULL)
		return NULL;

	str = GDKstrdup(projStr);
	if (str == NULL)
		return NULL;

	// first we split the string into a bunch of smaller strings,
	// based on the " " separator

	params[0] = str;	// 1st param, we'll null terminate at the " " soon

	t = 1;
	for (loc = strchr(str, ' '); loc != NULL; loc = strchr(loc, ' ')) {
		if (t == (int) (sizeof(params) / sizeof(params[0]))) {
			/* too many parameters */
			GDKfree(str);
			return NULL;
		}
		*loc++ = 0;	// null terminate and advance
		params[t++] = loc;
	}

	result = pj_init(t, params);
	GDKfree(str);

	return result;
}
#endif

/* It gets a geometry and transforms its coordinates to the provided srid */
str
wkbTransform(wkb **transformedWKB, wkb **geomWKB, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
{
#ifndef HAVE_PROJ
	*transformedWKB = NULL;
	(void) geomWKB;
	(void) srid_src;
	(void) srid_dst;
	(void) proj4_src_str;
	(void) proj4_dst_str;
	throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos function Not Implemented");
#else
	projPJ proj4_src, proj4_dst;
	GEOSGeom geosGeometry, transformedGeosGeometry;
	int geometryType = -1;

	str ret = MAL_SUCCEED;

	if (*geomWKB == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos wkb format is null");

	if (is_wkb_nil(*geomWKB) ||
	    is_int_nil(*srid_src) ||
	    is_int_nil(*srid_dst) ||
	    strNil(*proj4_src_str) ||
	    strNil(*proj4_dst_str)) {
		if ((*transformedWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if (strcmp(*proj4_src_str, "null") == 0)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_src);
	if (strcmp(*proj4_dst_str, "null") == 0)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_dst);

	proj4_src = /*pj_init_plus */ projFromStr(*proj4_src_str);
	proj4_dst = /*pj_init_plus */ projFromStr(*proj4_dst_str);
	if (proj4_src == NULL || proj4_dst == NULL) {
		if (proj4_src)
			pj_free(proj4_src);
		if (proj4_dst)
			pj_free(proj4_dst);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation pj_init failed");
	}

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*geomWKB);
	/* get the type of the geometry */
	geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	switch (geometryType) {
	case wkbPoint_mdb:
		ret = transformPoint(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
		break;
	case wkbLineString_mdb:
		ret = transformLineString(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
		break;
	case wkbLinearRing_mdb:
		ret = transformLinearRing(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
		break;
	case wkbPolygon_mdb:
		ret = transformPolygon(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst, *srid_dst);
		break;
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
		ret = transformMultiGeometry(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst, *srid_dst, geometryType);
		break;
	default:
		transformedGeosGeometry = NULL;
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos unknown geometry type");
	}

	if (ret == MAL_SUCCEED && transformedGeosGeometry) {
		/* set the new srid */
		GEOSSetSRID(transformedGeosGeometry, *srid_dst);
		/* get the wkb */
		if ((*transformedWKB = geos2wkb(transformedGeosGeometry)) == NULL)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation geos2wkb failed");
		/* destroy the geos geometries */
		GEOSGeom_destroy(transformedGeosGeometry);
	}

	pj_free(proj4_src);
	pj_free(proj4_dst);
	GEOSGeom_destroy(geosGeometry);

	return ret;
#endif
}
