/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a Wouter Scherphof, Niels Nes, Foteini Alvanaki
 * @* The simple geom module
 */

#include "geom.h"
#include "mal_exception.h"

int TYPE_mbr;

static wkb *geos2wkb(const GEOSGeometry *geosGeometry);

static inline int
geometryHasZ(int info)
{
	return (info & 0x02);
}

static inline int
geometryHasM(int info)
{
	return (info & 0x01);
}
static wkb wkb_nil = { ~0, 0 };

static wkb *
wkbNULLcopy(void)
{
	wkb *n = GDKmalloc(sizeof(wkb_nil));
	if (n)
		*n = wkb_nil;
	return n;
}

/* the first argument in the functions is the return variable */

#ifdef HAVE_PROJ

/** convert degrees to radians */
static void
degrees2radians(double *x, double *y, double *z)
{
	*x *= M_PI / 180.0;
	*y *= M_PI / 180.0;
	*z *= M_PI / 180.0;
}

/** convert radians to degrees */
static void
radians2degrees(double *x, double *y, double *z)
{
	*x *= 180.0 / M_PI;
	*y *= 180.0 / M_PI;
	*z *= 180.0 / M_PI;
}

static str
transformCoordSeq(int idx, int coordinatesNum, projPJ proj4_src, projPJ proj4_dst, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;
	int *errorNum = 0;

	if (!GEOSCoordSeq_getX(gcs_old, idx, &x) ||
	    !GEOSCoordSeq_getY(gcs_old, idx, &y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, idx, &z)))
		throw(MAL, "geom.Transform", "Couldn't get coordinates");

	/* check if the passed reference system is geographic (proj=latlong)
	 * and change the degrees to radians because pj_transform works with radians*/
	if (pj_is_latlong(proj4_src))
		degrees2radians(&x, &y, &z);

	pj_transform(proj4_src, proj4_dst, 1, 0, &x, &y, &z);

	errorNum = pj_get_errno_ref();
	if (*errorNum != 0) {
		if (coordinatesNum > 2)
			throw(MAL, "geom.Transform", "Couldn't transform point (%f %f %f): %s\n", x, y, z, pj_strerrno(*errorNum));
		else
			throw(MAL, "geom.Transform", "Couldn't transform point (%f %f): %s\n", x, y, pj_strerrno(*errorNum));
	}

	/* check if the destination reference system is geographic and change
	 * the destination coordinates from radians to degrees */
	if (pj_is_latlong(proj4_dst))
		radians2degrees(&x, &y, &z);

	if (!GEOSCoordSeq_setX(gcs_new, idx, x) ||
	    !GEOSCoordSeq_setY(gcs_new, idx, y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, idx, z)))
		throw(MAL, "geom.Transform", "Couldn't set coordinates");

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
		throw(MAL, "geom.Transform", "GEOSGeom_getCoordSeq failed");

	/* create the coordinates sequence for the transformed geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);
	if (gcs_new == NULL)
		throw(MAL, "geom.Transform", "GEOSGeom_getCoordSeq failed");

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
		throw(MAL, "geom.Transform", "GEOSGeom_getCoordSeq failed");
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
		throw(MAL, "geom.Transform", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.Transform", "GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the transformed geometry */
	*gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (*gcs_new == NULL)
		throw(MAL, "geom.Transform", "GEOSCoordSeq_create failed");

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
		throw(MAL, "geom.Transform", "GEOSGeom_createLineString failed");
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
		throw(MAL, "geom.Transform", "GEOSGeom_createLineString failed");
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
		throw(MAL, "geom.Transform", "GEOSGetExteriorRing failed");
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
		throw(MAL, "geom.Transform", "GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and transform each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*transformedGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Transform", MAL_MALLOC_FAIL);
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

	*transformedGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*transformedGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		ret = createException(MAL, "geom.Transform", "GEOSGeom_createPolygon failed");
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
		throw(MAL, "geom.Transform", "GEOSGetNumGeometries failed");
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Transform", MAL_MALLOC_FAIL);

	for (i = 0; i < geometriesNum; i++) {
		if ((multiGeometry = GEOSGetGeometryN(geosGeometry, i)) == NULL)
			ret = createException(MAL, "geom.Transform", "GEOSGetGeometryN failed");
		else if ((subGeometryType = GEOSGeomTypeId(multiGeometry) + 1) == 0)
			ret = createException(MAL, "geom.Transform", "GEOSGeomTypeId failed");
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
				ret = createException(MAL, "geom.Transform", "Unknown geometry type");
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
		ret = createException(MAL, "geom.Transform", "GEOSGeom_createCollection failed");
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
	throw(MAL, "geom.Transform", "Function Not Implemented");
#else
	projPJ proj4_src, proj4_dst;
	GEOSGeom geosGeometry, transformedGeosGeometry;
	int geometryType = -1;

	str ret = MAL_SUCCEED;

	if (*geomWKB == NULL)
		throw(MAL, "geom.Transform", "wkb is null");

	if (wkb_isnil(*geomWKB) ||
	    *srid_src == int_nil ||
	    *srid_dst == int_nil ||
	    strcmp(*proj4_src_str, str_nil) == 0 ||
	    strcmp(*proj4_dst_str, str_nil) == 0) {
		if ((*transformedWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Transform", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if (strcmp(*proj4_src_str, "null") == 0)
		throw(MAL, "geom.Transform", "Could not find in spatial_ref_sys srid %d\n", *srid_src);
	if (strcmp(*proj4_dst_str, "null") == 0)
		throw(MAL, "geom.Transform", "Could not find in spatial_ref_sys srid %d\n", *srid_dst);

	proj4_src = /*pj_init_plus */ projFromStr(*proj4_src_str);
	proj4_dst = /*pj_init_plus */ projFromStr(*proj4_dst_str);
	if (proj4_src == NULL || proj4_dst == NULL) {
		if (proj4_src)
			pj_free(proj4_src);
		if (proj4_dst)
			pj_free(proj4_dst);
		throw(MAL, "geom.Transform", "pj_init failed");
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
		ret = createException(MAL, "geom.Transform", "Unknown geometry type");
	}

	if (ret == MAL_SUCCEED && transformedGeosGeometry) {
		/* set the new srid */
		GEOSSetSRID(transformedGeosGeometry, *srid_dst);
		/* get the wkb */
		if ((*transformedWKB = geos2wkb(transformedGeosGeometry)) == NULL)
			ret = createException(MAL, "geom.Transform", "geos2wkb failed");
		/* destroy the geos geometries */
		GEOSGeom_destroy(transformedGeosGeometry);
	}

	pj_free(proj4_src);
	pj_free(proj4_dst);
	GEOSGeom_destroy(geosGeometry);

	return ret;
#endif
}

//gets a coord seq and forces it to have dim dimensions adding or removing extra dimensions
static str
forceDimCoordSeq(int idx, int coordinatesNum, int dim, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;

	//get the coordinates
	if (!GEOSCoordSeq_getX(gcs_old, idx, &x))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_getX failed");
	if (!GEOSCoordSeq_getY(gcs_old, idx, &y))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_getY failed");
	if (coordinatesNum > 2 && dim > 2 &&	//read it only if needed (dim >2)
	    !GEOSCoordSeq_getZ(gcs_old, idx, &z))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_getZ failed");

	//create the new coordinates
	if (!GEOSCoordSeq_setX(gcs_new, idx, x))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_setX failed");
	if (!GEOSCoordSeq_setY(gcs_new, idx, y))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_setY failed");
	if (dim > 2)
		if (!GEOSCoordSeq_setZ(gcs_new, idx, z))
			throw(MAL, "geom.ForceDim", "GEOSCoordSeq_setZ failed");
	return MAL_SUCCEED;
}

static str
forceDimPoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", "GEOSGeom_getCoordSeq failed");
	}

	/* create the coordinates sequence for the translated geometry */
	if ((gcs_new = GEOSCoordSeq_create(1, dim)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	ret = forceDimCoordSeq(0, coordinatesNum, dim, gcs_old, gcs_new);
	if (ret != MAL_SUCCEED) {
		*outGeometry = NULL;
		GEOSCoordSeq_destroy(gcs_new);
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", "GEOSGeom_createPoint failed");
	}

	return MAL_SUCCEED;
}

static str
forceDimLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.ForceDim", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, dim);
	if (gcs_new == NULL)
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_create failed");

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = forceDimCoordSeq(i, coordinatesNum, dim, gcs_old, gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return ret;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLineString(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", "GEOSGeom_createLineString failed");
	}

	return MAL_SUCCEED;

}

//Although linestring and linearRing are essentially the same we need to distinguish that when creating polygon from the rings
static str
forceDimLinearRing(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.ForceDim", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, dim);
	if (gcs_new == NULL)
		throw(MAL, "geom.ForceDim", "GEOSCoordSeq_create failed");

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = forceDimCoordSeq(i, coordinatesNum, dim, gcs_old, gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return ret;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", "GEOSGeom_createLinearRing failed");
	}

	return MAL_SUCCEED;
}

static str
forceDimPolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (!exteriorRingGeometry) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", "GEOSGetExteriorRing failed");
	}

	if ((ret = forceDimLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, dim)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return ret;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.ForceDim", "GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and translate each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.ForceDim", MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((ret = forceDimLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), dim)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return ret;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		ret = createException(MAL, "geom.ForceDim", "GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return ret;
}

static str forceDimGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim);
static str
forceDimMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.ForceDim", MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = forceDimGeometry(&transformedMultiGeometries[i], multiGeometry, dim)) != MAL_SUCCEED) {
			while (++i < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.ForceDim", "GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
forceDimGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return forceDimPoint(outGeometry, geosGeometry, dim);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return forceDimLineString(outGeometry, geosGeometry, dim);
	case wkbPolygon_mdb:
		return forceDimPolygon(outGeometry, geosGeometry, dim);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return forceDimMultiGeometry(outGeometry, geosGeometry, dim);
	default:
		throw(MAL, "geom.ForceDim", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbForceDim(wkb **outWKB, wkb **geomWKB, int *dim)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (wkb_isnil(*geomWKB) || *dim == int_nil) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ForceDim", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.ForceDim", "wkb2geos failed");
	}

	if ((err = forceDimGeometry(&outGeometry, geosGeometry, *dim)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.ForceDim", "geos2wkb failed");

	return MAL_SUCCEED;
}

static str
segmentizePoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry)
{
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;

	//nothing much to do. Just create a copy of the point
	//get the coordinates
	if ((gcs_old = GEOSGeom_getCoordSeq(geosGeometry)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", "GEOSGeom_getCoordSeq failed");
	}
	//create a copy of it
	if ((gcs_new = GEOSCoordSeq_clone(gcs_old)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", "GEOSCoordSeq_clone failed");
	}
	//create the geometry from the coordinates sequence
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.Segmentize", "GEOSGeom_createPoint failed");
	}

	return MAL_SUCCEED;
}

static str
segmentizeLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz, int isRing)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, additionalPoints = 0, i = 0, j = 0;
	double xl = 0.0, yl = 0.0, zl = 0.0;
	double *xCoords_org, *yCoords_org, *zCoords_org;
	str err = MAL_SUCCEED;

	//get the number of coordinates the geometry has
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	//get the coordinates of the points comprising the geometry
	if ((gcs_old = GEOSGeom_getCoordSeq(geosGeometry)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", "GEOSGeom_getCoordSeq failed");
	}
	//get the number of points in the geometry
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum)) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", "GEOSCoordSeq_getSize failed");
	}
	//store the points so that I do not have to read them multiple times using geos
	if ((xCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL " for %d double values", pointsNum);
	}
	if ((yCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		GDKfree(xCoords_org);
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL " for %d double values", pointsNum);
	}
	if ((zCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		GDKfree(xCoords_org);
		GDKfree(yCoords_org);
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL " for %d double values", pointsNum);
	}

	if (!GEOSCoordSeq_getX(gcs_old, 0, &xCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getX failed");
		goto bailout;
	}
	if (!GEOSCoordSeq_getY(gcs_old, 0, &yCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getY failed");
		goto bailout;
	}
	if (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, 0, &zCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getZ failed");
		goto bailout;
	}

	xl = xCoords_org[0];
	yl = yCoords_org[0];
	zl = zCoords_org[0];

	//check how many new points should be added
	for (i = 1; i < pointsNum; i++) {
		double dist;

		if (!GEOSCoordSeq_getX(gcs_old, i, &xCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getX failed");
			goto bailout;
		}
		if (!GEOSCoordSeq_getY(gcs_old, i, &yCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getY failed");
			goto bailout;
		}
		if (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, i, &zCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_getZ failed");
			goto bailout;
		}

		//compute the distance of the current point to the last added one
		while ((dist = sqrt(pow(xl - xCoords_org[i], 2) + pow(yl - yCoords_org[i], 2) + pow(zl - zCoords_org[i], 2))) > sz) {
//fprintf(stderr, "OLD : (%f, %f, %f) vs (%f, %f, %f) = %f\n", xl, yl, zl, xCoords_org[i], yCoords_org[i], zCoords_org[i], dist);
			additionalPoints++;
			//compute the point
			xl = xl + (xCoords_org[i] - xl) * sz / dist;
			yl = yl + (yCoords_org[i] - yl) * sz / dist;
			zl = zl + (zCoords_org[i] - zl) * sz / dist;
		}

		xl = xCoords_org[i];
		yl = yCoords_org[i];
		zl = zCoords_org[i];

	}
//fprintf(stderr, "Adding %d\n", additionalPoints);
	//create the coordinates sequence for the translated geometry
	if ((gcs_new = GEOSCoordSeq_create(pointsNum + additionalPoints, coordinatesNum)) == NULL) {
		*outGeometry = NULL;
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_create failed");
		goto bailout;
	}
	//add the first point
	if (!GEOSCoordSeq_setX(gcs_new, 0, xCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setX failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}
	if (!GEOSCoordSeq_setY(gcs_new, 0, yCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setY failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}
	if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, 0, zCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setZ failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}

	xl = xCoords_org[0];
	yl = yCoords_org[0];
	zl = zCoords_org[0];

	//check and add the rest of the points
	for (i = 1; i < pointsNum; i++) {
		//compute the distance of the current point to the last added one
		double dist;
		while ((dist = sqrt(pow(xl - xCoords_org[i], 2) + pow(yl - yCoords_org[i], 2) + pow(zl - zCoords_org[i], 2))) > sz) {
//fprintf(stderr, "OLD : (%f, %f, %f) vs (%f, %f, %f) = %f\n", xl, yl, zl, xCoords_org[i], yCoords_org[i], zCoords_org[i], dist);
			assert(j < additionalPoints);

			//compute intermediate point
			xl = xl + (xCoords_org[i] - xl) * sz / dist;
			yl = yl + (yCoords_org[i] - yl) * sz / dist;
			zl = zl + (zCoords_org[i] - zl) * sz / dist;

			//add the intermediate point
			if (!GEOSCoordSeq_setX(gcs_new, i + j, xl)) {
				err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setX failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}
			if (!GEOSCoordSeq_setY(gcs_new, i + j, yl)) {
				err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setY failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}
			if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, i + j, zl)) {
				err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setZ failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}

			j++;
		}

		//add the original point
		if (!GEOSCoordSeq_setX(gcs_new, i + j, xCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setX failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}
		if (!GEOSCoordSeq_setY(gcs_new, i + j, yCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setY failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}
		if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, i + j, zCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", "GEOSCoordSeq_setZ failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}

		xl = xCoords_org[i];
		yl = yCoords_org[i];
		zl = zCoords_org[i];

	}

	//create the geometry from the translated coordinates sequence
	if (isRing)
		*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	else
		*outGeometry = GEOSGeom_createLineString(gcs_new);

	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Segmentize", "GEOSGeom_%s failed", isRing ? "LinearRing" : "LineString");
		GEOSCoordSeq_destroy(gcs_new);
	}

  bailout:
	GDKfree(xCoords_org);
	GDKfree(yCoords_org);
	GDKfree(zCoords_org);

	return err;
}

static str
segmentizePolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str err;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", "GEOSGetExteriorRing failed");
	}

	if ((err = segmentizeLineString(&transformedExteriorRingGeometry, exteriorRingGeometry, sz, 1)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return err;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Segmentize", "GEOSGetInteriorRingN failed.");
	}
	//iterate over the interiorRing and segmentize each one of them
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = segmentizeLineString(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), sz, 1)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		err = createException(MAL, "geom.Segmentize", "GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return err;
}

static str segmentizeGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz);
static str
segmentizeMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = segmentizeGeometry(&transformedMultiGeometries[i], multiGeometry, sz)) != MAL_SUCCEED) {
			while (++i < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.Segmentize", "GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
segmentizeGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return segmentizePoint(outGeometry, geosGeometry);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return segmentizeLineString(outGeometry, geosGeometry, sz, 0);
	case wkbPolygon_mdb:
		return segmentizePolygon(outGeometry, geosGeometry, sz);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return segmentizeMultiGeometry(outGeometry, geosGeometry, sz);
	default:
		throw(MAL, "geom.Segmentize", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbSegmentize(wkb **outWKB, wkb **geomWKB, dbl *sz)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (wkb_isnil(*geomWKB) || *sz == dbl_nil) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Segmentize", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.Segmentize", "wkb2geos failed");
	}

	if ((err = segmentizeGeometry(&outGeometry, geosGeometry, *sz)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.Segmentize", "geos2wkb failed");

	return MAL_SUCCEED;
}

//gets a coord seq and moves it dx, dy, dz
static str
translateCoordSeq(int idx, int coordinatesNum, double dx, double dy, double dz, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;

	//get the coordinates
	if (!GEOSCoordSeq_getX(gcs_old, idx, &x))
		throw(MAL, "geom.Translate", "GEOSCoordSeq_getX failed");
	if (!GEOSCoordSeq_getY(gcs_old, idx, &y))
		throw(MAL, "geom.Translate", "GEOSCoordSeq_getY failed");
	if (coordinatesNum > 2)
		if (!GEOSCoordSeq_getZ(gcs_old, idx, &z))
			throw(MAL, "geom.Translate", "GEOSCoordSeq_getZ failed");

	//create new coordinates moved by dx, dy, dz
	if (!GEOSCoordSeq_setX(gcs_new, idx, (x + dx)))
		throw(MAL, "geom.Translate", "GEOSCoordSeq_setX failed");
	if (!GEOSCoordSeq_setY(gcs_new, idx, (y + dy)))
		throw(MAL, "geom.Translate", "GEOSCoordSeq_setY failed");
	if (coordinatesNum > 2)
		if (!GEOSCoordSeq_setZ(gcs_new, idx, (z + dz)))
			throw(MAL, "geom.Translate", "GEOSCoordSeq_setZ failed");

	return MAL_SUCCEED;
}

static str
translatePoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", "GEOSGeom_getCoordSeq failed");
	}

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", "GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	if ((err = translateCoordSeq(0, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
		GEOSCoordSeq_destroy(gcs_new);
		*outGeometry = NULL;
		return err;
	}

	/* create the geometry from the coordinates sequence */
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", "GEOSGeom_createPoint failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

static str
translateLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Translate", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(gcs_old, &pointsNum);

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", "GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		if ((err = translateCoordSeq(i, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return err;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLineString(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", "GEOSGeom_createLineString failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

//Necessary for composing a polygon from rings
static str
translateLinearRing(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Translate", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(gcs_old, &pointsNum);

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", "GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		if ((err = translateCoordSeq(i, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return err;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", "GEOSGeom_createLinearRing failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

static str
translatePolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str err = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", "GEOSGetExteriorRing failed");
	}

	if ((err = translateLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, dx, dy, dz)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return err;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Translate", "GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and translate each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Translate", MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = translateLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), dx, dy, dz)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		err = createException(MAL, "geom.Translate", "GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return err;
}

static str translateGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz);
static str
translateMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Translate", MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = translateGeometry(&transformedMultiGeometries[i], multiGeometry, dx, dy, dz)) != MAL_SUCCEED) {
			while (i++ < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.Translate", "GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
translateGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return translatePoint(outGeometry, geosGeometry, dx, dy, dz);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return translateLineString(outGeometry, geosGeometry, dx, dy, dz);
	case wkbPolygon_mdb:
		return translatePolygon(outGeometry, geosGeometry, dx, dy, dz);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return translateMultiGeometry(outGeometry, geosGeometry, dx, dy, dz);
	default:
		throw(MAL, "geom.Translate", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbTranslate(wkb **outWKB, wkb **geomWKB, dbl *dx, dbl *dy, dbl *dz)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (wkb_isnil(*geomWKB) || *dx == dbl_nil || *dy == dbl_nil || *dz == dbl_nil) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Translate", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.Translate", "wkb2geos failed");
	}

	if ((err = translateGeometry(&outGeometry, geosGeometry, *dx, *dy, *dz)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.Translate", "geos2wkb failed");

	return MAL_SUCCEED;
}

//It creates a Delaunay triangulation
//flag = 0 => returns a collection of polygons
//flag = 1 => returns a multilinestring
str
wkbDelaunayTriangles(wkb **outWKB, wkb **geomWKB, dbl *tolerance, int *flag)
{
	GEOSGeom outGeometry;
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geomWKB) || *tolerance == dbl_nil || *flag == int_nil) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.DelaunayTriangles", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	outGeometry = GEOSDelaunayTriangulation(geosGeometry, *tolerance, *flag);
	GEOSGeom_destroy(geosGeometry);
	if (outGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.DelaunayTriangles", "GEOSDelaunayTriangulation failed");
	}

	*outWKB = geos2wkb(outGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.DelaunayTriangles", "geos2wkb failed");

	return MAL_SUCCEED;
}

str
wkbPointOnSurface(wkb **resWKB, wkb **geomWKB)
{
	GEOSGeom geosGeometry, resGeosGeometry;

	if (wkb_isnil(*geomWKB)) {
		if ((*resWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.PointOnSurface", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*resWKB = NULL;
		throw(MAL, "geom.PointOnSurface", "wkb2geos failed");
	}

	resGeosGeometry = GEOSPointOnSurface(geosGeometry);
	if (resGeosGeometry == NULL) {
		*resWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointOnSurface", "GEOSPointOnSurface failed");
	}
	//set the srid of the point the same as the srid of the input geometry
	GEOSSetSRID(resGeosGeometry, GEOSGetSRID(geosGeometry));

	*resWKB = geos2wkb(resGeosGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(resGeosGeometry);

	if (*resWKB == NULL)
		throw(MAL, "geom.PointOnSurface", "geos2wkb failed");

	return MAL_SUCCEED;
}

static str
dumpGeometriesSingle(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	char *newPath = NULL;
	size_t pathLength = strlen(path);
	wkb *singleWKB = geos2wkb(geosGeometry);
	str err = MAL_SUCCEED;

	if (singleWKB == NULL)
		throw(MAL, "geom.Dump", "geos2wkb failed");

	//change the path only if it is empty
	if (pathLength == 0) {
		int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295

		(*lvl)++;

		newPath = GDKmalloc(lvlDigitsNum + 1);
		if (newPath == NULL) {
			GDKfree(singleWKB);
			throw(MAL, "geom.Dump", MAL_MALLOC_FAIL);
		}
		snprintf(newPath, lvlDigitsNum + 1, "%u", *lvl);
	} else {
		//remove the comma at the end of the path
		pathLength--;
		newPath = GDKmalloc(pathLength + 1);
		if (newPath == NULL) {
			GDKfree(singleWKB);
			throw(MAL, "geom.Dump", MAL_MALLOC_FAIL);
		}
		strncpy(newPath, path, pathLength);
		newPath[pathLength] = '\0';
	}
	if (BUNappend(idBAT, newPath, TRUE) != GDK_SUCCEED ||
	    BUNappend(geomBAT, singleWKB, TRUE) != GDK_SUCCEED)
		err = createException(MAL, "geom.Dump", "BUNappend failed");

	GDKfree(newPath);
	GDKfree(singleWKB);

	return err;
}

static str dumpGeometriesGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path);
static str
dumpGeometriesMulti(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int i;
	const GEOSGeometry *multiGeometry = NULL;
	unsigned int lvl = 0;
	size_t pathLength = strlen(path);
	char *newPath;
	str err = MAL_SUCCEED;

	int geometriesNum = GEOSGetNumGeometries(geosGeometry);

	pathLength += 10 + 1 + 1; /* 10 for lvl, 1 for ",", 1 for NULL byte */
	newPath = GDKmalloc(pathLength);
	if (newPath == NULL)
		throw(MAL, "geom.Dump", MAL_MALLOC_FAIL);

	for (i = 0; i < geometriesNum; i++) {
		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		if (multiGeometry == NULL) {
			err = createException(MAL, "geom.Dump", "GEOSGetGeometryN failed");
			break;
		}
		lvl++;

		snprintf(newPath, pathLength, "%s%u,", path, lvl);

		//*secondLevel = 0;
		err = dumpGeometriesGeometry(idBAT, geomBAT, multiGeometry, newPath);
		if (err != MAL_SUCCEED)
			break;
	}
	GDKfree(newPath);
	return err;
}

static str
dumpGeometriesGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	unsigned int lvl = 0;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
	case wkbPolygon_mdb:
		//Single Geometry
		return dumpGeometriesSingle(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		//Multi Geometry
		//check if the geometry was empty
		if (GEOSisEmpty(geosGeometry) == 1) {
			str err;
			//handle it as single
			if ((err = dumpGeometriesSingle(idBAT, geomBAT, geosGeometry, &lvl, path)) != MAL_SUCCEED)
				return err;
		}

		return dumpGeometriesMulti(idBAT, geomBAT, geosGeometry, path);
	default:
		throw(MAL, "geom.Dump", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbDump(bat *idBAT_id, bat *geomBAT_id, wkb **geomWKB)
{
	BAT *idBAT = NULL, *geomBAT = NULL;
	GEOSGeom geosGeometry;
	unsigned int geometriesNum;
	str err;

	if (wkb_isnil(*geomWKB)) {

		//create new empty BAT for the output
		if ((idBAT = COLnew(0, TYPE_str, 0, TRANSIENT)) == NULL) {
			*idBAT_id = bat_nil;
			throw(MAL, "geom.DumpPoints", "Error creating new BAT");
		}

		if ((geomBAT = COLnew(0, ATOMindex("wkb"), 0, TRANSIENT)) == NULL) {
			BBPunfix(idBAT->batCacheid);
			*geomBAT_id = bat_nil;
			throw(MAL, "geom.DumpPoints", "Error creating new BAT");
		}

		BBPkeepref(*idBAT_id = idBAT->batCacheid);

		BBPkeepref(*geomBAT_id = geomBAT->batCacheid);

		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);

	//count the number of geometries
	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	if ((idBAT = COLnew(0, TYPE_str, geometriesNum, TRANSIENT)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", "Error creating new BAT");
	}

	if ((geomBAT = COLnew(0, ATOMindex("wkb"), geometriesNum, TRANSIENT)) == NULL) {
		BBPunfix(idBAT->batCacheid);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", "Error creating new BAT");
	}

	err = dumpGeometriesGeometry(idBAT, geomBAT, geosGeometry, "");
	GEOSGeom_destroy(geosGeometry);
	if (err != MAL_SUCCEED) {
		BBPunfix(idBAT->batCacheid);
		BBPunfix(geomBAT->batCacheid);
		return err;
	}

	BBPkeepref(*idBAT_id = idBAT->batCacheid);
	BBPkeepref(*geomBAT_id = geomBAT->batCacheid);
	return MAL_SUCCEED;
}

static str
dumpPointsPoint(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	char *newPath = NULL;
	size_t pathLength = strlen(path);
	wkb *pointWKB = geos2wkb(geosGeometry);
	int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295
	str err = MAL_SUCCEED;

	(*lvl)++;
	newPath = GDKmalloc(pathLength + lvlDigitsNum + 1);
	if( newPath == NULL)
		throw(MAL, "geom.PointsPoint", MAL_MALLOC_FAIL);
	sprintf(newPath, "%s%u", path, *lvl);

	if (BUNappend(idBAT, newPath, TRUE) != GDK_SUCCEED ||
	    BUNappend(geomBAT, pointWKB, TRUE) != GDK_SUCCEED)
		err = createException(MAL, "geom.Dump", "BUNappend failed");

	GDKfree(newPath);
	GDKfree(pointWKB);

	return err;
}

static str
dumpPointsLineString(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int pointsNum = 0;
	str err;
	int i = 0;
	int check = 0;
	unsigned int lvl = 0;
	wkb *geomWKB = geos2wkb(geosGeometry);

	err = wkbNumPoints(&pointsNum, &geomWKB, &check);
	GDKfree(geomWKB);
	if (err != MAL_SUCCEED)
		return err;

	for (i = 0; i < pointsNum; i++) {
		GEOSGeometry *pointGeometry = GEOSGeomGetPointN(geosGeometry, i);

		if (pointGeometry == NULL)
			throw(MAL, "geom.DumpPoints", "GEOSGeomGetPointN failed");

		err = dumpPointsPoint(idBAT, geomBAT, pointGeometry, &lvl, path);
		GEOSGeom_destroy(pointGeometry);
	}

	return err;
}

static str
dumpPointsPolygon(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	const GEOSGeometry *exteriorRingGeometry;
	int numInteriorRings = 0, i = 0;
	str err;
	int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295
	size_t pathLength = strlen(path);
	char *newPath;
	char *extraStr = ",";
	int extraLength = 1;

	//get the exterior ring of the polygon
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (!exteriorRingGeometry)
		throw(MAL, "geom.DumpPoints", "GEOSGetExteriorRing failed");

	(*lvl)++;
	newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
	if( newPath == NULL)
		throw(MAL,"geom.dumpPointsPolygon",MAL_MALLOC_FAIL);
	sprintf(newPath, "%s%u%s", path, *lvl, extraStr);

	//get the points in the exterior ring
	err = dumpPointsLineString(idBAT, geomBAT, exteriorRingGeometry, newPath);
	GDKfree(newPath);
	if (err != MAL_SUCCEED)
		return err;

	//check the interior rings
	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1)
		throw(MAL, "geom.NumPoints", "GEOSGetNumInteriorRings failed");

	// iterate over the interiorRing and transform each one of them
	for (i = 0; i < numInteriorRings; i++) {
		(*lvl)++;
		lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295

		newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
		sprintf(newPath, "%s%u%s", path, *lvl, extraStr);

		err = dumpPointsLineString(idBAT, geomBAT, GEOSGetInteriorRingN(geosGeometry, i), newPath);
		GDKfree(newPath);
		if (err != MAL_SUCCEED)
			return err;
	}

	return MAL_SUCCEED;
}

static str dumpPointsGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path);
static str
dumpPointsMultiGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometriesNum, i;
	const GEOSGeometry *multiGeometry = NULL;
	str err;
	unsigned int lvl = 0;
	size_t pathLength = strlen(path);
	char *newPath = NULL;
	char *extraStr = ",";
	int extraLength = 1;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	for (i = 0; i < geometriesNum; i++) {
		int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295

		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		lvl++;

		newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
		if( newPath == NULL)
			throw(MAL,"geom.dumpPointsGeometry",MAL_MALLOC_FAIL);
		sprintf(newPath, "%s%u%s", path, lvl, extraStr);

		//*secondLevel = 0;
		err = dumpPointsGeometry(idBAT, geomBAT, multiGeometry, newPath);
		GDKfree(newPath);
		if (err != MAL_SUCCEED)
			return err;
	}

	return MAL_SUCCEED;
}

static str
dumpPointsGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	unsigned int lvl = 0;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return dumpPointsPoint(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return dumpPointsLineString(idBAT, geomBAT, geosGeometry, path);
	case wkbPolygon_mdb:
		return dumpPointsPolygon(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return dumpPointsMultiGeometry(idBAT, geomBAT, geosGeometry, path);
	default:
		throw(MAL, "geom.DumpPoints", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbDumpPoints(bat *idBAT_id, bat *geomBAT_id, wkb **geomWKB)
{
	BAT *idBAT = NULL, *geomBAT = NULL;
	GEOSGeom geosGeometry;
	int check = 0;
	int pointsNum;
	str err;

	if (wkb_isnil(*geomWKB)) {

		//create new empty BAT for the output
		if ((idBAT = COLnew(0, TYPE_str, 0, TRANSIENT)) == NULL) {
			*idBAT_id = int_nil;
			throw(MAL, "geom.DumpPoints", "Error creating new BAT");
		}

		if ((geomBAT = COLnew(0, ATOMindex("wkb"), 0, TRANSIENT)) == NULL) {
			BBPunfix(idBAT->batCacheid);
			*geomBAT_id = int_nil;
			throw(MAL, "geom.DumpPoints", "Error creating new BAT");
		}

		BBPkeepref(*idBAT_id = idBAT->batCacheid);

		BBPkeepref(*geomBAT_id = geomBAT->batCacheid);

		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);

	if ((err = wkbNumPoints(&pointsNum, geomWKB, &check)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		return err;
	}

	if ((idBAT = COLnew(0, TYPE_str, pointsNum, TRANSIENT)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", "Error creating new BAT");
	}

	if ((geomBAT = COLnew(0, ATOMindex("wkb"), pointsNum, TRANSIENT)) == NULL) {
		BBPunfix(idBAT->batCacheid);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", "Error creating new BAT");
	}

	err = dumpPointsGeometry(idBAT, geomBAT, geosGeometry, "");
	GEOSGeom_destroy(geosGeometry);
	if (err != MAL_SUCCEED) {
		BBPunfix(idBAT->batCacheid);
		BBPunfix(geomBAT->batCacheid);
		return err;
	}

	BBPkeepref(*idBAT_id = idBAT->batCacheid);
	BBPkeepref(*geomBAT_id = geomBAT->batCacheid);
	return MAL_SUCCEED;
}

str
geom_2_geom(wkb **resWKB, wkb **valueWKB, int *columnType, int *columnSRID)
{
	GEOSGeom geosGeometry;
	int geoCoordinatesNum = 2;
	int valueType = 0;

	int valueSRID = (*valueWKB)->srid;

	if (wkb_isnil(*valueWKB) || *columnType == int_nil || *columnSRID == int_nil) {
		*resWKB = wkbNULLcopy();
		if (*resWKB == NULL)
			throw(MAL, "calc.wkb", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*valueWKB);
	if (geosGeometry == NULL)
		throw(MAL, "calc.wkb", "wkb2geos failed");

	/* get the number of coordinates the geometry has */
	geoCoordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the type of the geometry */
	valueType = (GEOSGeomTypeId(geosGeometry) + 1) << 2;

	if (geoCoordinatesNum > 2)
		valueType += (1 << 1);
	if (geoCoordinatesNum > 3)
		valueType += 1;

	if (valueSRID != *columnSRID || valueType != *columnType) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "calc.wkb", "column needs geometry(%d, %d) and value is geometry(%d, %d)\n", *columnType, *columnSRID, valueType, valueSRID);
	}

	/* get the wkb from the geosGeometry */
	*resWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*resWKB == NULL)
		throw(MAL, "calc.wkb", "geos2wkb failed");

	return MAL_SUCCEED;
}

/*check if the geometry has z coordinate*/
str
geoHasZ(int *res, int *info)
{
	if (*info == int_nil)
		*res = int_nil;
	else if (geometryHasZ(*info))
		*res = 1;
	else
		*res = 0;
	return MAL_SUCCEED;

}

/*check if the geometry has m coordinate*/
str
geoHasM(int *res, int *info)
{
	if (*info == int_nil)
		*res = int_nil;
	else if (geometryHasM(*info))
		*res = 1;
	else
		*res = 0;
	return MAL_SUCCEED;
}

/*check the geometry subtype*/
/*returns the length of the resulting string*/
str
geoGetType(char **res, int *info, int *flag)
{
	if (*info == int_nil || *flag == int_nil) {
		if ((*res = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.getType", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((*res = GDKstrdup(geom_type2str(*info >> 2, *flag))) == NULL)
		throw(MAL, "geom.getType", MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* initialize geos */
str
geom_prelude(void *ret)
{
	(void) ret;
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), sizeof(oid), "mbr");
	geomcatalogfix_set(geom_catalog_upgrade);
	geomsqlfix_set(geom_sql_upgrade);

	return MAL_SUCCEED;
}

/* clean geos */
str
geom_epilogue(void *ret)
{
	(void) ret;
	libgeom_exit();
	return MAL_SUCCEED;
}

/* Check if fixed-sized atom mbr is null */
static int
mbr_isnil(mbr *m)
{
	if (m == NULL || m->xmin == flt_nil || m->ymin == flt_nil || m->xmax == flt_nil || m->ymax == flt_nil)
		return 1;
	return 0;
}

/* returns the size of variable-sized atom wkb */
static var_t
wkb_size(size_t len)
{
	if (len == ~(size_t) 0)
		len = 0;
	assert(offsetof(wkb, data) + len <= VAR_MAX);
	return (var_t) (offsetof(wkb, data) + len);
}

/* returns the size of variable-sized atom wkba */
static var_t
wkba_size(int items)
{
	var_t size;

	if (items == ~0)
		items = 0;
	size = (var_t) (offsetof(wkba, data) + items * sizeof(wkb *));
	assert(size <= VAR_MAX);

	return size;
}

#ifndef HAVE_STRNCASECMP
static int
strncasecmp(const char *s1, const char *s2, size_t n)
{
	int c1, c2;

	while (n > 0) {
		c1 = (unsigned char) *s1++;
		c2 = (unsigned char) *s2++;
		if (c1 == 0)
			return -c2;
		if (c2 == 0)
			return c1;
		if (c1 != c2 && tolower(c1) != tolower(c2))
			return tolower(c1) - tolower(c2);
		n--;
	}
	return 0;
}
#endif

/* Creates WKB representation (including srid) from WKT representation */
/* return number of parsed characters. */
static str
wkbFROMSTR_withSRID(char *geomWKT, int *len, wkb **geomWKB, int srid, size_t *nread)
{
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	GEOSWKTReader *WKT_reader;
	const char *polyhedralSurface = "POLYHEDRALSURFACE";
	const char *multiPolygon = "MULTIPOLYGON";
	char *geomWKT_original = NULL;
	size_t parsedCharacters = 0;

	*nread = 0;
	if (*len > 0) {
		/* we always allocate new memory */
		GDKfree(*geomWKB);
	}
	*len = 0;
	*geomWKB = NULL;
	if (strcmp(geomWKT, str_nil) == 0) {
		*geomWKB = wkbNULLcopy();
		if (*geomWKB == NULL)
			throw(MAL, "wkb.FromText", MAL_MALLOC_FAIL);
		*len = (int) sizeof(wkb_nil);
		return MAL_SUCCEED;
	}
	//check whether the representation is binary (hex)
	if (geomWKT[0] == '0') {
		str ret = wkbFromBinary(geomWKB, &geomWKT);

		if (ret != MAL_SUCCEED)
			return ret;
		*nread = strlen(geomWKT);
		*len = (int) wkb_size((*geomWKB)->len);
		return MAL_SUCCEED;
	}
	//check whether the geometry type is polyhedral surface
	//geos cannot handle this type of geometry but since it is
	//a special type of multipolygon I just change the type before
	//continuing. Of course this means that isValid for example does
	//not work correctly.
	if (strncasecmp(geomWKT, polyhedralSurface, strlen(polyhedralSurface)) == 0) {
		size_t sizeOfInfo = strlen(geomWKT) - strlen(polyhedralSurface);
		geomWKT_original = geomWKT;
		geomWKT = GDKmalloc(sizeOfInfo + strlen(multiPolygon) + 1);
		if( geomWKT == NULL)
			throw(MAL,"geomWKT",MAL_MALLOC_FAIL);
		strcpy(geomWKT, multiPolygon);
		memcpy(geomWKT + strlen(multiPolygon), &geomWKT_original[strlen(polyhedralSurface)], sizeOfInfo);
		geomWKT[sizeOfInfo + strlen(multiPolygon)] = '\0';
	}
	////////////////////////// UP TO HERE ///////////////////////////

	WKT_reader = GEOSWKTReader_create();
	geosGeometry = GEOSWKTReader_read(WKT_reader, geomWKT);
	GEOSWKTReader_destroy(WKT_reader);

	if (geosGeometry == NULL)
		throw(MAL, "wkb.FromText", "GEOSWKTReader_read failed");

	if (GEOSGeomTypeId(geosGeometry) == -1) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "wkb.FromText", "GEOSGeomTypeId failed");
	}

	GEOSSetSRID(geosGeometry, srid);
	/* the srid was lost with the transformation of the GEOSGeom to wkb
	 * so we decided to store it in the wkb */

	/* we have a GEOSGeometry with number of coordinates and SRID and we
	 * want to get the wkb out of it */
	*geomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	if (*geomWKB == NULL)
		throw(MAL, "wkb.FromText", "geos2wkb failed");

	*len = (int) wkb_size((*geomWKB)->len);

	if (geomWKT_original) {
		GDKfree(geomWKT);
		geomWKT = geomWKT_original;
	}

	parsedCharacters = strlen(geomWKT);
	assert(parsedCharacters <= GDK_int_max);
	*nread = parsedCharacters;
	return MAL_SUCCEED;
}

static int
wkbaFROMSTR_withSRID(char *fromStr, int *len, wkba **toArray, int srid)
{
	int items, i;
	size_t skipBytes = 0;

//IS THERE SPACE OR SOME OTHER CHARACTER?

	//read the number of items from the beginning of the string
	memcpy(&items, fromStr, sizeof(int));
	skipBytes += sizeof(int);
	*toArray = GDKmalloc(wkba_size(items));
	if( *toArray == NULL){
		GDKerror("FROMSTR_withSRID" MAL_MALLOC_FAIL);
		return 0;
	}

	for (i = 0; i < items; i++) {
		size_t parsedBytes;
		str err = wkbFROMSTR_withSRID(fromStr + skipBytes, len, &(*toArray)->data[i], srid, &parsedBytes);
		if (err != MAL_SUCCEED) {
			freeException(err);
			return 0;
		}
		skipBytes += parsedBytes;
	}

	assert(skipBytes <= GDK_int_max);
	return (int) skipBytes;
}

/* create the WKB out of the GEOSGeometry
 * It makes sure to make all checks before returning
 * the input geosGeometry should not be altered by this function
 * return NULL on error */
static wkb *
geos2wkb(const GEOSGeometry *geosGeometry)
{
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *geomWKB;

	// if the geosGeometry is NULL create a NULL WKB
	if (geosGeometry == NULL) {
		return wkbNULLcopy();
	}

	GEOS_setWKBOutputDims(GEOSGeom_getCoordinateDimension(geosGeometry));
	w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);

	if (w == NULL)
		return NULL;

	assert(wkbLen <= GDK_int_max);

	geomWKB = GDKmalloc(wkb_size(wkbLen));
	//If malloc failed create a NULL wkb
	if (geomWKB == NULL) {
		GEOSFree(w);
		return NULL;
	}

	geomWKB->len = (int) wkbLen;
	geomWKB->srid = GEOSGetSRID(geosGeometry);
	memcpy(&geomWKB->data, w, wkbLen);
	GEOSFree(w);

	return geomWKB;
}

/* gets the mbr from the geometry */
mbr *
mbrFromGeos(const GEOSGeom geosGeometry)
{
	GEOSGeom envelope;
	mbr *geomMBR;
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;

	geomMBR = GDKmalloc(sizeof(mbr));
	if (geomMBR == NULL)	//problem in reserving space
		return NULL;

	/* if input is null or GEOSEnvelope created exception then create a nill mbr */
	if (!geosGeometry || (envelope = GEOSEnvelope(geosGeometry)) == NULL) {
		*geomMBR = *mbrNULL();
		return geomMBR;
	}

	if ((GEOSGeomTypeId(envelope) + 1) == wkbPoint_mdb) {
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
		const GEOSCoordSequence *coords = GEOSGeom_getCoordSeq(envelope);
#else
		const GEOSCoordSeq coords = GEOSGeom_getCoordSeq(envelope);
#endif
		GEOSCoordSeq_getX(coords, 0, &xmin);
		GEOSCoordSeq_getY(coords, 0, &ymin);
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		geomMBR->xmin = (float) xmin;
		geomMBR->ymin = (float) ymin;
		geomMBR->xmax = (float) xmin;
		geomMBR->ymax = (float) ymin;
	} else {		// GEOSGeomTypeId(envelope) == GEOS_POLYGON
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
		const GEOSGeometry *ring = GEOSGetExteriorRing(envelope);
#else
		const GEOSGeom ring = GEOSGetExteriorRing(envelope);
#endif
		if (ring) {
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
			const GEOSCoordSequence *coords = GEOSGeom_getCoordSeq(ring);
#else
			const GEOSCoordSeq coords = GEOSGeom_getCoordSeq(ring);
#endif
			GEOSCoordSeq_getX(coords, 0, &xmin);	//left-lower corner
			GEOSCoordSeq_getY(coords, 0, &ymin);
			GEOSCoordSeq_getX(coords, 2, &xmax);	//right-upper corner
			GEOSCoordSeq_getY(coords, 2, &ymax);
			assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
			assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
			assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
			assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
			geomMBR->xmin = (float) xmin;
			geomMBR->ymin = (float) ymin;
			geomMBR->xmax = (float) xmax;
			geomMBR->ymax = (float) ymax;
		}
	}
	GEOSGeom_destroy(envelope);
	return geomMBR;
}

//Returns the wkb in a hex representation */
static char hexit[] = "0123456789ABCDEF";

str
wkbAsBinary(char **toStr, wkb **geomWKB)
{
	char *s;
	int i;

	if (wkb_isnil(*geomWKB)) {
		if ((*toStr = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsBinary", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((*toStr = GDKmalloc(1 + (*geomWKB)->len * 2)) == NULL)
		throw(MAL, "geom.AsBinary", MAL_MALLOC_FAIL);

	s = *toStr;
	for (i = 0; i < (*geomWKB)->len; i++) {
		int val = ((*geomWKB)->data[i] >> 4) & 0xf;
		*s++ = hexit[val];
		val = (*geomWKB)->data[i] & 0xf;
		*s++ = hexit[val];
//fprintf(stderr, "%d First: %c - Second: %c ==> Original %c (%d)\n", i, *(s-2), *(s-1), (*geomWKB)->data[i], (int)((*geomWKB)->data[i]));
	}
	*s = '\0';
	return MAL_SUCCEED;
}

static int
decit(char hex)
{
	switch (hex) {
	case '0':
		return 0;
	case '1':
		return 1;
	case '2':
		return 2;
	case '3':
		return 3;
	case '4':
		return 4;
	case '5':
		return 5;
	case '6':
		return 6;
	case '7':
		return 7;
	case '8':
		return 8;
	case '9':
		return 9;
	case 'A':
	case 'a':
		return 10;
	case 'B':
	case 'b':
		return 11;
	case 'C':
	case 'c':
		return 12;
	case 'D':
	case 'd':
		return 13;
	case 'E':
	case 'e':
		return 14;
	case 'F':
	case 'f':
		return 15;
	default:
		return -1;
	}
}

str
wkbFromBinary(wkb **geomWKB, char **inStr)
{
	size_t strLength, wkbLength, i;
	wkb *w;

	if (strcmp(*inStr, str_nil) == 0) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.FromBinary", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	strLength = strlen(*inStr);
	if (strLength & 1)
		throw(MAL, "geom.FromBinary", "odd length input string");

	wkbLength = strLength / 2;
	assert(wkbLength <= GDK_int_max);

	w = GDKmalloc(wkb_size(wkbLength));
	if (w == NULL)
		throw(MAL, "geom.FromBinary", MAL_MALLOC_FAIL);

	//compute the value for s
	for (i = 0; i < strLength; i += 2) {
		int firstHalf = decit((*inStr)[i]);
		int secondHalf = decit((*inStr)[i + 1]);
		if (firstHalf == -1 || secondHalf == -1) {
			GDKfree(w);
			throw(MAL, "geom.FromBinary", "incorrectly formatted input string");
		}
		w->data[i / 2] = (firstHalf << 4) | secondHalf;
	}

	w->len = (int) wkbLength;
	w->srid = 0;
	*geomWKB = w;

	return MAL_SUCCEED;
}

str
mbrFromMBR(mbr **w, mbr **src)
{
	*w = GDKmalloc(sizeof(mbr));
	if (*w == NULL)
		throw(MAL, "calc.mbr", MAL_MALLOC_FAIL);

	**w = **src;
	return MAL_SUCCEED;
}

str
wkbFromWKB(wkb **w, wkb **src)
{
	*w = GDKmalloc(wkb_size((*src)->len));
	if (*w == NULL)
		throw(MAL, "calc.wkb", MAL_MALLOC_FAIL);

	if (wkb_isnil(*src)) {
		**w = *wkbNULL();
	} else {
		(*w)->len = (*src)->len;
		(*w)->srid = (*src)->srid;
		memcpy((*w)->data, (*src)->data, (*src)->len);
	}
	return MAL_SUCCEED;
}

/* creates a wkb from the given textual representation */
/* int* tpe is needed to verify that the type of the FromText function used is the
 * same with the type of the geometry created from the wkt representation */
str
wkbFromText(wkb **geomWKB, str *geomWKT, int *srid, int *tpe)
{
	int len = 0;
	int te = 0;
	str err;
	size_t parsedBytes;

	*geomWKB = NULL;
	if (strcmp(*geomWKT, str_nil) == 0 || *srid == int_nil || *tpe == int_nil) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "wkb.FromText", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	err = wkbFROMSTR_withSRID(*geomWKT, &len, geomWKB, *srid, &parsedBytes);
	if (err != MAL_SUCCEED)
		return err;

	if (wkb_isnil(*geomWKB) || *tpe == 0 ||
	    *tpe == wkbGeometryCollection_mdb ||
	    ((te = *((*geomWKB)->data + 1) & 0x0f) + (*tpe > 2)) == *tpe) {
		return MAL_SUCCEED;
	}

	GDKfree(*geomWKB);
	*geomWKB = NULL;

	te += (te > 2);
	if (*tpe > 0 && te != *tpe)
		throw(SQL, "wkb.FromText", "Geometry not type '%d: %s' but '%d: %s' instead", *tpe, geom_type2str(*tpe, 0), te, geom_type2str(te, 0));
	throw(MAL, "wkb.FromText", "%s", "cannot parse string");
}

/* create textual representation of the wkb */
str
wkbAsText(char **txt, wkb **geomWKB, int *withSRID)
{
	int len = 0;
	char *wkt = NULL;
	const char *sridTxt = "SRID:";
	size_t len2 = 0;

	if (wkb_isnil(*geomWKB) || (withSRID && *withSRID == int_nil)) {
		if ((*txt = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsText", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if ((*geomWKB)->srid < 0)
		throw(MAL, "geom.AsText", "Negative SRID");

	if (wkbTOSTR(&wkt, &len, *geomWKB) == 0)
		throw(MAL, "geom.AsText", "Failed to create Text from Well Known Format");

	if (withSRID == NULL || *withSRID == 0) {	//accepting NULL withSRID to make internal use of it easier
		*txt = wkt;
		return MAL_SUCCEED;
	}

	/* 10 for maximum number of digits to represent an INT */
	len2 = strlen(wkt) + 10 + strlen(sridTxt) + 2;
	*txt = GDKmalloc(len2);
	if (*txt == NULL) {
		GDKfree(wkt);
		throw(MAL, "geom.AsText", MAL_MALLOC_FAIL);
	}

	snprintf(*txt, len2, "%s%d;%s", sridTxt, (*geomWKB)->srid, wkt);

	GDKfree(wkt);
	return MAL_SUCCEED;
}

str
wkbMLineStringToPolygon(wkb **geomWKB, str *geomWKT, int *srid, int *flag)
{
	int itemsNum = 0, i, type = wkbMultiLineString_mdb;
	str ret = MAL_SUCCEED;
	wkb *inputWKB = NULL;

	wkb **linestringsWKB;
	double *linestringsArea;
	bit ordered = 0;

	if (strcmp(*geomWKT, str_nil) == 0 || *srid == int_nil || *flag == int_nil) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MLineStringToPolygon", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*geomWKB = NULL;

	//make wkb from wkt
	ret = wkbFromText(&inputWKB, geomWKT, srid, &type);
	if (ret != MAL_SUCCEED)
		return ret;

	//read the number of linestrings in the input
	ret = wkbNumGeometries(&itemsNum, &inputWKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(inputWKB);
		return ret;
	}

	linestringsWKB = GDKmalloc(itemsNum * sizeof(wkb *));
	linestringsArea = GDKmalloc(itemsNum * sizeof(double));
	if (linestringsWKB == NULL || linestringsArea == NULL) {
		itemsNum = 0;
		ret = createException(MAL, "geom.MLineStringToPolygon", MAL_MALLOC_FAIL);
		goto bailout;
	}

	//create one polygon for each lineString and compute the area of each of them
	for (i = 1; i <= itemsNum; i++) {
		wkb *polygonWKB;

		ret = wkbGeometryN(&linestringsWKB[i - 1], &inputWKB, &i);
		if (ret != MAL_SUCCEED || linestringsWKB[i - 1] == NULL) {
			itemsNum = i - 1;
			goto bailout;
		}

		ret = wkbMakePolygon(&polygonWKB, &linestringsWKB[i - 1], NULL, srid);
		if (ret != MAL_SUCCEED) {
			itemsNum = i;
			goto bailout;
		}

		ret = wkbArea(&linestringsArea[i - 1], &polygonWKB);
		GDKfree(polygonWKB);
		if (ret != MAL_SUCCEED) {
			itemsNum = i;
			goto bailout;
		}
	}

	GDKfree(inputWKB);
	inputWKB = NULL;

	//order the linestrings with decreasing (polygons) area
	while (!ordered) {
		ordered = 1;

		for (i = 0; i < itemsNum - 1; i++) {
			if (linestringsArea[i + 1] > linestringsArea[i]) {
				//switch
				wkb *linestringWKB = linestringsWKB[i];
				double linestringArea = linestringsArea[i];

				linestringsWKB[i] = linestringsWKB[i + 1];
				linestringsArea[i] = linestringsArea[i + 1];

				linestringsWKB[i + 1] = linestringWKB;
				linestringsArea[i + 1] = linestringArea;

				ordered = 0;
			}
		}
	}

	if (*flag == 0) {
		//the biggest polygon is the external shell
		GEOSCoordSeq coordSeq_external;
		GEOSGeom externalGeometry, linearRingExternalGeometry, *internalGeometries, finalGeometry;

		externalGeometry = wkb2geos(linestringsWKB[0]);
		if (externalGeometry == NULL) {
			ret = createException(MAL, "geom.MLineStringToPolygon", "Error in wkb2geos");
			goto bailout;
		}

		coordSeq_external = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(externalGeometry));
		GEOSGeom_destroy(externalGeometry);
		if (coordSeq_external == NULL) {
			ret = createException(MAL, "geom.MLineStringToPolygon", "GEOSCoordSeq_clone failed");
			goto bailout;
		}
		linearRingExternalGeometry = GEOSGeom_createLinearRing(coordSeq_external);
		if (linearRingExternalGeometry == NULL) {
			GEOSCoordSeq_destroy(coordSeq_external);
			ret = createException(MAL, "geom.MLineStringToPolygon", "GEOSGeom_createLinearRing failed");
			goto bailout;
		}

		//all remaining should be internal
		internalGeometries = GDKmalloc((itemsNum - 1) * sizeof(GEOSGeom));
		if (internalGeometries == NULL) {
			GEOSGeom_destroy(linearRingExternalGeometry);
			ret = createException(MAL, "geom.MLineStringToPolygon", MAL_MALLOC_FAIL);
			goto bailout;
		}
		for (i = 1; i < itemsNum; i++) {
			GEOSCoordSeq coordSeq_internal;
			GEOSGeom internalGeometry;

			internalGeometry = wkb2geos(linestringsWKB[i]);
			if (internalGeometry == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", "Error in wkb2geos");
				goto bailout;
			}

			coordSeq_internal = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(internalGeometry));
			GEOSGeom_destroy(internalGeometry);
			if (coordSeq_internal == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", "Error in wkb2geos");
				goto bailout;
			}
			internalGeometries[i - 1] = GEOSGeom_createLinearRing(coordSeq_internal);
			if (internalGeometries[i - 1] == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				GEOSCoordSeq_destroy(coordSeq_internal);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", "GEOSGeom_createLinearRing failed");
				goto bailout;
			}
		}

		finalGeometry = GEOSGeom_createPolygon(linearRingExternalGeometry, internalGeometries, itemsNum - 1);
		GEOSGeom_destroy(linearRingExternalGeometry);
		if (finalGeometry == NULL) {
			for (i = 0; i < itemsNum - 1; i++)
				GEOSGeom_destroy(internalGeometries[i]);
			GDKfree(internalGeometries);
			ret = createException(MAL, "geom.MLineStringToPolygon", "Error creating Polygon from LinearRing");
			goto bailout;
		}
		GDKfree(internalGeometries);
		//check of the created polygon is valid
		if (GEOSisValid(finalGeometry) != 1) {
			//suppress the GEOS message
			GDKclrerr();

			GEOSGeom_destroy(finalGeometry);

			ret = createException(MAL, "geom.MLineStringToPolygon", "The provided MultiLineString does not create a valid Polygon");
			goto bailout;
		}

		GEOSSetSRID(finalGeometry, *srid);
		*geomWKB = geos2wkb(finalGeometry);
		GEOSGeom_destroy(finalGeometry);
		if (*geomWKB == NULL)
			ret = createException(MAL, "geom.MLineStringToPolygon", "geos2wkb failed");
	} else if (*flag == 1) {
		ret = createException(MAL, "geom.MLineStringToPolygon", "Multipolygon from string has not been defined");
	} else {
		ret = createException(MAL, "geom.MLineStringToPolygon", "Unknown flag");
	}

  bailout:
	GDKfree(inputWKB);
	for (i = 0; i < itemsNum; i++)
		GDKfree(linestringsWKB[i]);
	GDKfree(linestringsWKB);
	GDKfree(linestringsArea);
	return ret;
}

str
wkbMakePoint(wkb **out, dbl *x, dbl *y, dbl *z, dbl *m, int *zmFlag)
{
	GEOSGeom geosGeometry;
	GEOSCoordSeq seq;

	if (*x == dbl_nil || *y == dbl_nil || *z == dbl_nil || *m == dbl_nil || *zmFlag == int_nil) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakePoint", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	switch (*zmFlag) {
	case 0:			/* x, y */
		seq = GEOSCoordSeq_create(1, 2);
		break;
	case 1:			/* x, y, m */
	case 10:		/* x, y, z */
		seq = GEOSCoordSeq_create(1, 3);
		break;
	case 11:		/* x, y, z, m */
		throw(MAL, "geom.MakePoint", "POINTZM is not supported");
	default:
		throw(MAL, "geom.MakePoint", ILLEGAL_ARGUMENT);
	}

	if (seq == NULL)
		throw(MAL, "geom.MakePoint", "GEOSCoordSeq_create failed");

	if (!GEOSCoordSeq_setOrdinate(seq, 0, 0, *x) ||
	    !GEOSCoordSeq_setOrdinate(seq, 0, 1, *y) ||
	    (*zmFlag == 1 && !GEOSCoordSeq_setOrdinate(seq, 0, 2, *m)) ||
	    (*zmFlag == 10 && !GEOSCoordSeq_setOrdinate(seq, 0, 2, *z))) {
		GEOSCoordSeq_destroy(seq);
		throw(MAL, "geom.MakePoint", "GEOSCoordSeq_setOrdinate failed");
	}

	if ((geosGeometry = GEOSGeom_createPoint(seq)) == NULL) {
		GEOSCoordSeq_destroy(seq);
		throw(MAL, "geom.MakePoint", "Failed to create GEOSGeometry from the coordinates");
	}

	*out = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (wkb_isnil(*out)) {
		GDKfree(*out);
		*out = NULL;
		throw(MAL, "geom.MakePoint", "Failed to create WKB from GEOSGeometry");
	}

	return MAL_SUCCEED;
}

/* common code for functions that return integer */
static str
wkbBasicInt(int *out, wkb *geom, int (*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;

	if (wkb_isnil(geom)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	if ((geosGeometry = wkb2geos(geom)) == NULL)
		throw(MAL, name, "wkb2geos failed");

	*out = (*func) (geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	//if there was an error returned by geos
	if (GDKerrbuf && GDKerrbuf[0]) {
		//create an exception with this name
		ret = createException(MAL, name, "%s", GDKerrbuf);

		//clear the error buffer
		GDKclrerr();
	}

	return ret;
}

/* returns the type of the geometry as a string*/
str
wkbGeometryType(char **out, wkb **geomWKB, int *flag)
{
	int typeId = 0;
	str ret = MAL_SUCCEED;

	ret = wkbBasicInt(&typeId, *geomWKB, GEOSGeomTypeId, "geom.GeometryType");
	if (ret != MAL_SUCCEED)
		return ret;
	if (typeId != int_nil)	/* geoGetType deals with nil */
		typeId = (typeId + 1) << 2;
	return geoGetType(out, &typeId, flag);
}

/* returns the number of dimensions of the geometry */
str
wkbCoordDim(int *out, wkb **geom)
{
	return wkbBasicInt(out, *geom, GEOSGeom_getCoordinateDimension, "geom.CoordDim");
}

/* returns the inherent dimension of the geometry, e.g 0 for point */
str
wkbDimension(int *dimension, wkb **geomWKB)
{
	return wkbBasicInt(dimension, *geomWKB, GEOSGeom_getDimensions, "geom.Dimension");
}

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

	if (wkb_isnil(*geomWKB) || *srid == int_nil) {
		if ((*resultGeomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.setSRID", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geomWKB)) == NULL)
		throw(MAL, "geom.setSRID", "wkb2geos failed");

	GEOSSetSRID(geosGeometry, *srid);
	*resultGeomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*resultGeomWKB == NULL)
		throw(MAL, "geom.setSRID", "geos2wkb failed");

	return MAL_SUCCEED;
}

/* depending on the specific function it returns the X,Y or Z coordinate of a point */
str
wkbGetCoordinate(dbl *out, wkb **geom, int *dimNum)
{
	GEOSGeom geosGeometry;
	const GEOSCoordSequence *gcs;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom) || *dimNum == int_nil) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = dbl_nil;
		throw(MAL, "geom.GetCoordinate", "wkb2geos failed");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPoint_mdb) {
		char *geomSTR;

		GEOSGeom_destroy(geosGeometry);
		if ((err = wkbAsText(&geomSTR, geom, NULL)) != MAL_SUCCEED)
			return err;
		err = createException(MAL, "geom.GetCoordinate", "Geometry %s not a Point", geomSTR);
		GDKfree(geomSTR);
		return err;
	}

	gcs = GEOSGeom_getCoordSeq(geosGeometry);
	/* gcs shouldn't be freed, it's internal to the GEOSGeom */

	if (gcs == NULL) {
		err = createException(MAL, "geom.GetCoordinate", "GEOSGeom_getCoordSeq failed");
	} else if (!GEOSCoordSeq_getOrdinate(gcs, 0, *dimNum, out))
		err = createException(MAL, "geom.GetCoordinate", "GEOSCoordSeq_getOrdinate failed");
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/*common code for functions that return geometry */
static str
wkbBasic(wkb **out, wkb **geom, GEOSGeometry *(*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry, outGeometry;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geom)) == NULL) {
		*out = NULL;
		throw(MAL, name, "wkb2geos failed");
	}

	if ((outGeometry = (*func) (geosGeometry)) == NULL) {
		err = createException(MAL, name, "GEOS%s failed", name + 5);
	} else {
		//set the srid equal to the srid of the initial geometry
		if ((*geom)->srid)	//GEOSSetSRID has assertion for srid != 0
			GEOSSetSRID(outGeometry, (*geom)->srid);

		if ((*out = geos2wkb(outGeometry)) == NULL)
			err = createException(MAL, name, MAL_MALLOC_FAIL);

		GEOSGeom_destroy(outGeometry);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

str
wkbBoundary(wkb **boundaryWKB, wkb **geomWKB)
{
	return wkbBasic(boundaryWKB, geomWKB, GEOSBoundary, "geom.Boundary");
}

str
wkbEnvelope(wkb **out, wkb **geom)
{
	return wkbBasic(out, geom, GEOSEnvelope, "geom.Envelope");
}

str
wkbEnvelopeFromCoordinates(wkb **out, dbl *xmin, dbl *ymin, dbl *xmax, dbl *ymax, int *srid)
{
	GEOSGeom geosGeometry, linearRingGeometry;
	GEOSCoordSeq coordSeq;

	if (*xmin == dbl_nil || *ymin == dbl_nil || *xmax == dbl_nil || *ymax == dbl_nil || *srid == int_nil) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeEnvelope", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//create the coordinates sequence
	if ((coordSeq = GEOSCoordSeq_create(5, 2)) == NULL)
		throw(MAL, "geom.MakeEnvelope", "GEOSCoordSeq_create failed");

	//set the values
	if (!GEOSCoordSeq_setX(coordSeq, 0, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 0, *ymin) ||
	    !GEOSCoordSeq_setX(coordSeq, 1, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 1, *ymax) ||
	    !GEOSCoordSeq_setX(coordSeq, 2, *xmax) ||
	    !GEOSCoordSeq_setY(coordSeq, 2, *ymax) ||
	    !GEOSCoordSeq_setX(coordSeq, 3, *xmax) ||
	    !GEOSCoordSeq_setY(coordSeq, 3, *ymin) ||
	    !GEOSCoordSeq_setX(coordSeq, 4, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 4, *ymin)) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.MakeEnvelope", "GEOSCoordSeq_setX/Y failed");
	}

	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq);
	if (linearRingGeometry == NULL) {
		//Gives segmentation fault GEOSCoordSeq_destroy(coordSeq);
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.MakeEnvelope", "Error creating LinearRing from coordinates");
	}

	geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
	if (geosGeometry == NULL) {
		GEOSGeom_destroy(linearRingGeometry);
		throw(MAL, "geom.MakeEnvelope", "Error creating Polygon from LinearRing");
	}

	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

str
wkbMakePolygon(wkb **out, wkb **external, bat *internalBAT_id, int *srid)
{
	GEOSGeom geosGeometry, externalGeometry, linearRingGeometry;
	bit closed = 0;
	GEOSCoordSeq coordSeq_copy;
	str err;

	if (wkb_isnil(*external) || *srid == int_nil) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Polygon", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	externalGeometry = wkb2geos(*external);
	if (externalGeometry == NULL)
		throw(MAL, "geom.Polygon", MAL_MALLOC_FAIL);

	//check the type of the external geometry
	if ((GEOSGeomTypeId(externalGeometry) + 1) != wkbLineString_mdb) {
		*out = NULL;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", "Geometries should be LineString");
	}
	//check whether the linestring is closed
	if ((err = wkbIsClosed(&closed, external)) != MAL_SUCCEED) {
		GEOSGeom_destroy(externalGeometry);
		return err;
	}
	if (!closed) {
		*out = NULL;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", "LineString should be closed");
	}
	//create a copy of the coordinates
	coordSeq_copy = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(externalGeometry));
	GEOSGeom_destroy(externalGeometry);
	if (coordSeq_copy == NULL)
		throw(MAL, "geom.Polygon", "GEOSCoordSeq_clone failed");

	//create a linearRing using the copy of the coordinates
	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq_copy);
	if (linearRingGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq_copy);
		throw(MAL, "geom.Polygon", "GEOSGeom_createLinearRing failed");
	}

	//create a polygon using the linearRing
	if (internalBAT_id == NULL) {
		geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
		if (geosGeometry == NULL) {
			*out = NULL;
			GEOSGeom_destroy(linearRingGeometry);
			throw(MAL, "geom.Polygon", "Error creating Polygon from LinearRing");
		}
	} else {
		/* TODO: Looks like incomplete code: what should be
		 * done with internalBAT_id? --sjoerd */
		geosGeometry = NULL;
	}

	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

//Gets two Point or LineString geometries and returns a line
str
wkbMakeLine(wkb **out, wkb **geom1WKB, wkb **geom2WKB)
{
	GEOSGeom outGeometry, geom1Geometry, geom2Geometry;
	GEOSCoordSeq outCoordSeq = NULL;
	const GEOSCoordSequence *geom1CoordSeq = NULL, *geom2CoordSeq = NULL;
	unsigned int i = 0, geom1Size = 0, geom2Size = 0;
	unsigned geom1Dimension = 0, geom2Dimension = 0;
	double x, y, z;
	str err = MAL_SUCCEED;

	*out = NULL;
	if (wkb_isnil(*geom1WKB) || wkb_isnil(*geom2WKB)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeLine", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geom1Geometry = wkb2geos(*geom1WKB);
	if (!geom1Geometry) {
		*out = NULL;
		throw(MAL, "geom.MakeLine", "wkb2geos failed");
	}

	geom2Geometry = wkb2geos(*geom2WKB);
	if (!geom2Geometry) {
		*out = NULL;
		GEOSGeom_destroy(geom1Geometry);
		throw(MAL, "geom.MakeLine", "wkb2geos failed");
	}
	//make sure the geometries are of the same srid
	if (GEOSGetSRID(geom1Geometry) != GEOSGetSRID(geom2Geometry)) {
		err = createException(MAL, "geom.MakeLine", "Geometries of different SRID");
		goto bailout;
	}
	//check the types of the geometries
	if (GEOSGeomTypeId(geom1Geometry) + 1 != wkbPoint_mdb &&
		 GEOSGeomTypeId(geom1Geometry) + 1 != wkbLineString_mdb &&
		 GEOSGeomTypeId(geom2Geometry) + 1 != wkbPoint_mdb &&
		 GEOSGeomTypeId(geom2Geometry) + 1 != wkbLineString_mdb) {
		err = createException(MAL, "geom.MakeLine", "Geometries should be Point or LineString");
		goto bailout;
	}
	//get the coordinate sequences of the geometries
	if ((geom1CoordSeq = GEOSGeom_getCoordSeq(geom1Geometry)) == NULL ||
		 (geom2CoordSeq = GEOSGeom_getCoordSeq(geom2Geometry)) == NULL) {
		err = createException(MAL, "geom.MakeLine", "GEOSGeom_getCoordSeq failed");
		goto bailout;
	}
	//make sure that the dimensions of the geometries are the same
	if (!GEOSCoordSeq_getDimensions(geom1CoordSeq, &geom1Dimension) ||
		 !GEOSCoordSeq_getDimensions(geom2CoordSeq, &geom2Dimension)) {
		err = createException(MAL, "geom.MakeLine", "GEOSGeom_getDimensions failed");
		goto bailout;
	}
	if (geom1Dimension != geom2Dimension) {
		err = createException(MAL, "geom.MakeLine", "Geometries should be of the same dimension");
		goto bailout;
	}
	//get the number of coordinates in the two geometries
	if (!GEOSCoordSeq_getSize(geom1CoordSeq, &geom1Size) ||
		 !GEOSCoordSeq_getSize(geom2CoordSeq, &geom2Size)) {
		err = createException(MAL, "geom.MakeLine", "GEOSGeom_getSize failed");
		goto bailout;
	}
	//create the coordSeq for the new geometry
	if ((outCoordSeq = GEOSCoordSeq_create(geom1Size + geom2Size, geom1Dimension)) == NULL) {
		err = createException(MAL, "geom.MakeLine", "GEOSCoordSeq_create failed");
		goto bailout;
	}
	for (i = 0; i < geom1Size; i++) {
		GEOSCoordSeq_getX(geom1CoordSeq, i, &x);
		GEOSCoordSeq_getY(geom1CoordSeq, i, &y);
		if (!GEOSCoordSeq_setX(outCoordSeq, i, x) ||
		    !GEOSCoordSeq_setY(outCoordSeq, i, y)) {
			err = createException(MAL, "geom.MakeLine", "GEOSCoordSeq_set[XY] failed");
			goto bailout;
		}
		if (geom1Dimension > 2) {
			GEOSCoordSeq_getZ(geom1CoordSeq, i, &z);
			if (!GEOSCoordSeq_setZ(outCoordSeq, i, z)) {
				err = createException(MAL, "geom.MakeLine", "GEOSCoordSeq_setZ failed");
				goto bailout;
			}
		}
	}
	for (i = 0; i < geom2Size; i++) {
		GEOSCoordSeq_getX(geom2CoordSeq, i, &x);
		GEOSCoordSeq_getY(geom2CoordSeq, i, &y);
		if (!GEOSCoordSeq_setX(outCoordSeq, i + geom1Size, x) ||
		    !GEOSCoordSeq_setY(outCoordSeq, i + geom1Size, y)) {
			err = createException(MAL, "geom.MakeLine", "GEOSCoordSeq_set[XY] failed");
			goto bailout;
		}
		if (geom2Dimension > 2) {
			GEOSCoordSeq_getZ(geom2CoordSeq, i, &z);
			if (!GEOSCoordSeq_setZ(outCoordSeq, i + geom1Size, z)) {
				err = createException(MAL, "geom.MakeLine", "GEOSCoordSeq_setZ failed");
				goto bailout;
			}
		}
	}

	if ((outGeometry = GEOSGeom_createLineString(outCoordSeq)) == NULL) {
		err = createException(MAL, "geom.MakeLine", "GEOSGeom_createLineString failed");
		goto bailout;
	}
	outCoordSeq = NULL;

	GEOSSetSRID(outGeometry, GEOSGetSRID(geom1Geometry));
	*out = geos2wkb(outGeometry);
	GEOSGeom_destroy(outGeometry);

  bailout:
	if (outCoordSeq)
		GEOSCoordSeq_destroy(outCoordSeq);
	GEOSGeom_destroy(geom1Geometry);
	GEOSGeom_destroy(geom2Geometry);

	return err;
}

//Gets a BAT with geometries and returns a single LineString
str
wkbMakeLineAggr(wkb **outWKB, bat *inBAT_id)
{
	BAT *inBAT = NULL;
	BATiter inBAT_iter;
	BUN i;
	wkb *aWKB, *bWKB;
	str err;

	//get the BATs
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "geom.MakeLine", RUNTIME_OBJECT_MISSING);
	}
	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);

	/* TODO: what should be returned if the input BAT is less than
	 * two rows? --sjoerd */
	if (BATcount(inBAT) == 0) {
		BBPunfix(inBAT->batCacheid);
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeLine", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	aWKB = (wkb *) BUNtail(inBAT_iter, 0);
	if (BATcount(inBAT) == 1) {
		err = wkbFromWKB(outWKB, &aWKB);
		BBPunfix(inBAT->batCacheid);
		if (err) {
			freeException(err);
			throw(MAL, "geom.MakeLine", MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	bWKB = (wkb *) BUNtail(inBAT_iter, 1);
	//create the first line using the first two geometries
	err = wkbMakeLine(outWKB, &aWKB, &bWKB);

	// add one more segment for each following row
	for (i = 2; err == MAL_SUCCEED && i < BATcount(inBAT); i++) {
		aWKB = *outWKB;
		bWKB = (wkb *) BUNtail(inBAT_iter, i);
		*outWKB = NULL;

		err = wkbMakeLine(outWKB, &aWKB, &bWKB);
		GDKfree(aWKB);
	}

	BBPunfix(inBAT->batCacheid);

	return err;
}

/* Returns the first or last point of a linestring */
static str
wkbBorderPoint(wkb **out, wkb **geom, GEOSGeometry *(*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry;
	GEOSGeom new;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*out = NULL;
	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		throw(MAL, name, "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		err = createException(MAL, name, "Geometry not a LineString");
	} else {
		new = (*func) (geosGeometry);
		if (new == NULL) {
			err = createException(MAL, name, "GEOSGeomGet%s failed", name + 5);
		} else {
			*out = geos2wkb(new);
			GEOSGeom_destroy(new);
		}
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the first point in a linestring */
str
wkbStartPoint(wkb **out, wkb **geom)
{
	return wkbBorderPoint(out, geom, GEOSGeomGetStartPoint, "geom.StartPoint");
}

/* Returns the last point in a linestring */
str
wkbEndPoint(wkb **out, wkb **geom)
{
	return wkbBorderPoint(out, geom, GEOSGeomGetEndPoint, "geom.EndPoint");
}

static str
numPointsLineString(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	/* get the coordinates of the points comprising the geometry */
	const GEOSCoordSequence *coordSeq = GEOSGeom_getCoordSeq(geosGeometry);

	if (coordSeq == NULL)
		throw(MAL, "geom.NumPoints", "GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(coordSeq, out)) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "GEOSGeomGetNumPoints failed");
	}

	return MAL_SUCCEED;
}

static str
numPointsPolygon(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	const GEOSGeometry *exteriorRingGeometry;
	int numInteriorRings = 0, i = 0;
	str err;
	unsigned int pointsN = 0;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (!exteriorRingGeometry) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "GEOSGetExteriorRing failed");
	}
	//get the points in the exterior ring
	if ((err = numPointsLineString(out, exteriorRingGeometry)) != MAL_SUCCEED) {
		*out = int_nil;
		return err;
	}
	pointsN = *out;

	//check the interior rings
	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "GEOSGetNumInteriorRings failed");
	}
	// iterate over the interiorRing and transform each one of them
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = numPointsLineString(out, GEOSGetInteriorRingN(geosGeometry, i))) != MAL_SUCCEED) {
			*out = int_nil;
			return err;
		}
		pointsN += *out;
	}

	*out = pointsN;
	return MAL_SUCCEED;
}

static str numPointsGeometry(unsigned int *out, const GEOSGeometry *geosGeometry);
static str
numPointsMultiGeometry(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	int geometriesNum, i;
	const GEOSGeometry *multiGeometry = NULL;
	str err;
	unsigned int pointsN = 0;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	for (i = 0; i < geometriesNum; i++) {
		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		if ((err = numPointsGeometry(out, multiGeometry)) != MAL_SUCCEED) {
			*out = int_nil;
			return err;
		}
		pointsN += *out;
	}

	*out = pointsN;
	return MAL_SUCCEED;
}

static str
numPointsGeometry(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return numPointsLineString(out, geosGeometry);
	case wkbPolygon_mdb:
		return numPointsPolygon(out, geosGeometry);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return numPointsMultiGeometry(out, geosGeometry);
	default:
		throw(MAL, "geom.NumPoints", "%s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

/* Returns the number of points in a geometry */
str
wkbNumPoints(int *out, wkb **geom, int *check)
{
	GEOSGeom geosGeometry;
	int geometryType = 0;
	str err = MAL_SUCCEED;
	char *geomSTR = NULL;
	unsigned int pointsNum;

	if (wkb_isnil(*geom) || *check == int_nil) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "wkb2geos failed");
	}

	geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	if (*check && geometryType != wkbLineString_mdb) {
		*out = int_nil;
		GEOSGeom_destroy(geosGeometry);

		if ((err = wkbAsText(&geomSTR, geom, NULL)) == MAL_SUCCEED) {
			err = createException(MAL, "geom.NumPoints", "Geometry %s not a LineString", geomSTR);
			GDKfree(geomSTR);
		}
		return err;
	}

	if ((err = numPointsGeometry(&pointsNum, geosGeometry)) != MAL_SUCCEED) {
		*out = int_nil;
		GEOSGeom_destroy(geosGeometry);
		return err;
	}

	if (pointsNum > INT_MAX) {
		GEOSGeom_destroy(geosGeometry);
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "Overflow");
	}

	*out = pointsNum;
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* Returns the n-th point of the geometry */
str
wkbPointN(wkb **out, wkb **geom, int *n)
{
	int rN = -1;
	GEOSGeom geosGeometry;
	GEOSGeom new;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom) || *n == int_nil) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.PointN", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = NULL;
		throw(MAL, "geom.PointN", "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", "Geometry not a LineString");
	}
	//check number of points
	rN = GEOSGeomGetNumPoints(geosGeometry);
	if (rN == -1) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", "GEOSGeomGetNumPoints failed");
	}

	if (rN <= *n || *n < 0) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", "Unable to retrieve point %d (not enough points)", *n);
	}

	if ((new = GEOSGeomGetPointN(geosGeometry, *n)) == NULL) {
		err = createException(MAL, "geom.PointN", "GEOSGeomGetPointN failed");
	} else {
		if ((*out = geos2wkb(new)) == NULL)
			err = createException(MAL, "geom.PointN", "GEOSGeomGetPointN failed");
		GEOSGeom_destroy(new);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the exterior ring of the polygon*/
str
wkbExteriorRing(wkb **exteriorRingWKB, wkb **geom)
{
	GEOSGeom geosGeometry;
	const GEOSGeometry *exteriorRingGeometry;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom)) {
		if ((*exteriorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ExteriorRing", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*exteriorRingWKB = NULL;
		throw(MAL, "geom.ExteriorRing", "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_POLYGON) {
		*exteriorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.ExteriorRing", "Geometry not a Polygon");

	}
	/* get the exterior ring of the geometry */
	if ((exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry)) == NULL)
		err = createException(MAL, "geom.ExteriorRing", "GEOSGetExteriorRing failed");
	else {
		/* get the wkb representation of it */
		if ((*exteriorRingWKB = geos2wkb(exteriorRingGeometry)) == NULL)
			err = createException(MAL, "geom.ExteriorRing", MAL_MALLOC_FAIL);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the n-th interior ring of a polygon */
str
wkbInteriorRingN(wkb **interiorRingWKB, wkb **geom, int *ringNum)
{
	GEOSGeom geosGeometry = NULL;
	const GEOSGeometry *interiorRingGeometry;
	int rN = -1;
	str err = MAL_SUCCEED;

	//initialize to NULL
	*interiorRingWKB = NULL;

	if (wkb_isnil(*geom) || *ringNum == int_nil) {
		if ((*interiorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.InteriorRingN", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*interiorRingWKB = NULL;
		throw(MAL, "geom.InteriorRingN", "wkb2geos failed");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPolygon_mdb) {
		*interiorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRingN", "Geometry not a Polygon");

	}
	//check number of internal rings
	rN = GEOSGetNumInteriorRings(geosGeometry);
	if (rN == -1) {
		*interiorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRingN", "GEOSGetInteriorRingN failed.");
	}

	if (rN < *ringNum || *ringNum <= 0) {
		GEOSGeom_destroy(geosGeometry);
		//NOT AN ERROR throw(MAL, "geom.interiorRingN", "GEOSGetInteriorRingN failed. Not enough interior rings");
		if ((*interiorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.InteriorRingN", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	/* get the interior ring of the geometry */
	if ((interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, *ringNum - 1)) == NULL) {
		err = createException(MAL, "geom.InteriorRingN", "GEOSGetInteriorRingN failed");
	} else {
		/* get the wkb representation of it */
		if ((*interiorRingWKB = geos2wkb(interiorRingGeometry)) == NULL)
			err = createException(MAL, "geom.InteriorRingN", MAL_MALLOC_FAIL);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

str
wkbInteriorRings(wkba **geomArray, wkb **geomWKB)
{
	int interiorRingsNum = 0, i = 0;
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;

	if (wkb_isnil(*geomWKB)) {
		if ((*geomArray = GDKmalloc(wkba_size(~0))) == NULL)
			throw(MAL, "geom.InteriorRings", MAL_MALLOC_FAIL);
		**geomArray = *wkbaNULL();
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.InteriorRings", "Error in wkb2geos");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPolygon_mdb) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.interiorRings", "Geometry not a Polygon");

	}

	ret = wkbNumRings(&interiorRingsNum, geomWKB, &i);

	if (ret != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		return ret;
	}

	*geomArray = GDKmalloc(wkba_size(interiorRingsNum));
	if (*geomArray == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRings", MAL_MALLOC_FAIL);
	}
	(*geomArray)->itemsNum = interiorRingsNum;

	for (i = 0; i < interiorRingsNum; i++) {
		const GEOSGeometry *interiorRingGeometry;
		wkb *interiorRingWKB;

		// get the interior ring of the geometry
		interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, i);
		if (interiorRingGeometry == NULL) {
			while (--i >= 0)
				GDKfree((*geomArray)->data[i]);
			GDKfree(*geomArray);
			GEOSGeom_destroy(geosGeometry);
			*geomArray = NULL;
			throw(MAL, "geom.InteriorRings", "GEOSGetInteriorRingN failed");
		}
		// get the wkb representation of it
		interiorRingWKB = geos2wkb(interiorRingGeometry);
		if (interiorRingWKB == NULL) {
			while (--i >= 0)
				GDKfree((*geomArray)->data[i]);
			GDKfree(*geomArray);
			GEOSGeom_destroy(geosGeometry);
			*geomArray = NULL;
			throw(MAL, "geom.InteriorRings", "Error in wkb2geos");
		}

		(*geomArray)->data[i] = interiorRingWKB;
	}
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* Returns the number of interior rings in the first polygon of the provided geometry
 * plus the exterior ring depending on the value of exteriorRing*/
str
wkbNumRings(int *out, wkb **geom, int *exteriorRing)
{
	str ret = MAL_SUCCEED;
	bit empty;
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geom) || *exteriorRing == int_nil) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geom)) != MAL_SUCCEED) {
		return ret;
	}
	if (empty) {
		//the geometry is empty
		*out = 0;
		return MAL_SUCCEED;
	}
	//check the type of the geometry
	geosGeometry = wkb2geos(*geom);

	if (geosGeometry == NULL)
		throw(MAL, "geom.NumRings", "Problem converting WKB to GEOS");

	if (GEOSGeomTypeId(geosGeometry) + 1 == wkbMultiPolygon_mdb) {
		//use the first polygon as done by PostGIS
		wkb *new = geos2wkb(GEOSGetGeometryN(geosGeometry, 0));
		if (new == NULL) {
			ret = createException(MAL, "geom.NumRings", MAL_MALLOC_FAIL);
		} else {
			ret = wkbBasicInt(out, new, GEOSGetNumInteriorRings, "geom.NumRings");
			GDKfree(new);
		}
	} else if (GEOSGeomTypeId(geosGeometry) + 1 == wkbPolygon_mdb) {
		ret = wkbBasicInt(out, *geom, GEOSGetNumInteriorRings, "geom.NumRings");
	} else {
		//It is not a polygon so the number of rings is 0
		*out = -*exteriorRing; /* compensate for += later */
	}

	GEOSGeom_destroy(geosGeometry);

	if (ret != MAL_SUCCEED)
		return ret;

	*out += *exteriorRing;

	return MAL_SUCCEED;
}

/* it handles functions that take as input a single geometry and return Boolean */
static str
wkbBasicBoolean(bit *out, wkb **geom, char (*func) (const GEOSGeometry *), const char *name)
{
	int ret;
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geom)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL)
		throw(MAL, name, "wkb2geom failed");

	ret = (*func) (geosGeometry);	//it is supposed to return char but treating it as such gives wrong results
	GEOSGeom_destroy(geosGeometry);

	if (ret == 2)
		throw(MAL, name, "GEOSis%s failed", name + 7);

	*out = ret;

	return MAL_SUCCEED;
}

/* the function checks whether the geometry is closed. GEOS works only with
 * linestring geometries but PostGIS returns true in any geometry that is not
 * a linestring. I made it to be like PostGIS */
static str
geosIsClosed(bit *out, const GEOSGeometry *geosGeometry)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	int i = 0;
	str err;
	int geometriesNum;

	*out = bit_nil;

	switch (geometryType) {
	case -1:
		throw(MAL, "geom.IsClosed", "GEOSGeomTypeId failed");
	case wkbPoint_mdb:
	case wkbPolygon_mdb:
	case wkbMultiPoint_mdb:
	case wkbMultiPolygon_mdb:
		//In all these case it is always true
		*out = 1;
		break;
	case wkbLineString_mdb:
		//check
		if ((i = GEOSisClosed(geosGeometry)) == 2)
			throw(MAL, "geom.IsClosed", "GEOSisClosed failed");
		*out = i;
		break;
	case wkbMultiLineString_mdb:
	case wkbGeometryCollection_mdb:
		//check each one separately
		geometriesNum = GEOSGetNumGeometries(geosGeometry);
		if (geometriesNum < 0)
			throw(MAL, "geom.IsClosed", "GEOSGetNumGeometries failed");

		for (i = 0; i < geometriesNum; i++) {
			const GEOSGeometry *gN = GEOSGetGeometryN(geosGeometry, i);
			if (!gN)
				throw(MAL, "geom.IsClosed", "GEOSGetGeometryN failed");

			if ((err = geosIsClosed(out, gN)) != MAL_SUCCEED) {
				return err;
			}

			if (!*out)	//no reason to check further logical AND will always be 0
				return MAL_SUCCEED;
		}

		break;
	default:
		throw(MAL, "geom.IsClosed", "Unknown geometry type");
	}

	return MAL_SUCCEED;
}

str
wkbIsClosed(bit *out, wkb **geomWKB)
{
	str err;
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geomWKB)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	//if empty geometry return false
	if ((err = wkbIsEmpty(out, geomWKB)) != MAL_SUCCEED) {
		return err;
	}
	if (*out) {
		*out = 0;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL)
		throw(MAL, "geom.IsClosed", "wkb2geos failed");

	err = geosIsClosed(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return err;
}

str
wkbIsEmpty(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisEmpty, "geom.IsEmpty");
}

str
wkbIsRing(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisRing, "geom.IsRing");
}

str
wkbIsSimple(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisSimple, "geom.IsSimple");
}

/*geom prints a message saying the reason why the geometry is not valid but
 * since there is also isValidReason I skip this here */
str
wkbIsValid(bit *out, wkb **geomWKB)
{
	str err = wkbBasicBoolean(out, geomWKB, GEOSisValid, "geom.IsValid");
	/* GOESisValid may cause GDKerror to be called: ignore it */
	if (err == MAL_SUCCEED)
		GDKclrerr();
	return err;
}

str
wkbIsValidReason(char **reason, wkb **geomWKB)
{
	GEOSGeom geosGeometry;
	char *GEOSReason = NULL;

	if (wkb_isnil(*geomWKB)) {
		if ((*reason = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.IsValidReason", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL)
		throw(MAL, "geom.IsValidReason", "wkb2geom failed");

	GEOSReason = GEOSisValidReason(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GEOSReason == NULL)
		throw(MAL, "geom.IsValidReason", "GEOSisValidReason failed");

	*reason = GDKstrdup(GEOSReason);
	GEOSFree(GEOSReason);
	if (*reason == NULL)
		throw(MAL, "geom.IsValidReason", MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* I should check it since it does not work */
str
wkbIsValidDetail(char **out, wkb **geom)
{
	int res = -1;
	char *GEOSreason = NULL;
	GEOSGeom GEOSlocation = NULL;
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geom)) {
		if ((*out = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.IsValidReason", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if ((geosGeometry = wkb2geos(*geom)) == NULL) {
		*out = NULL;
		throw(MAL, "geom.IsValidDetail", "wkb2geos failed");
	}

	res = GEOSisValidDetail(geosGeometry, 1, &GEOSreason, &GEOSlocation);

	GEOSGeom_destroy(geosGeometry);

	if (res == 2) {
		throw(MAL, "geom.IsValidDetail", "GEOSisValidDetail failed");
	}

	*out = GDKstrdup(GEOSreason);

	GEOSFree(GEOSreason);
	GEOSGeom_destroy(GEOSlocation);

	if (*out == NULL)
		throw(MAL, "geom.IsValidReason", MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* returns the area of the geometry */
str
wkbArea(dbl *out, wkb **geomWKB)
{
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geomWKB)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*out = dbl_nil;
		throw(MAL, "geom.Area", "wkb2geos failed");
	}

	if (!GEOSArea(geosGeometry, out)) {
		GEOSGeom_destroy(geosGeometry);
		*out = dbl_nil;
		throw(MAL, "geom.Area", "GEOSArea failed");
	}

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* returns the centroid of the geometry */
str
wkbCentroid(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry;
	GEOSGeom outGeometry;

	if (wkb_isnil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Centroid", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL)
		throw(MAL, "geom.Centroid", MAL_MALLOC_FAIL);

	outGeometry = GEOSGetCentroid(geosGeometry);
	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));	//the centroid has the same SRID with the the input geometry
	*out = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	return MAL_SUCCEED;

}

/*  Returns the 2-dimensional Cartesian minimum distance (based on spatial ref) between two geometries in projected units */
str
wkbDistance(dbl *out, wkb **a, wkb **b)
{
	GEOSGeom ga, gb;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*a) || wkb_isnil(*b)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	ga = wkb2geos(*a);
	gb = wkb2geos(*b);
	if (ga == NULL || gb == NULL) {
		if (ga)
			GEOSGeom_destroy(ga);
		if (gb)
			GEOSGeom_destroy(gb);
		*out = dbl_nil;
		throw(MAL, "geom.Distance", "wkb2geos failed");
	}

	if (GEOSGetSRID(ga) != GEOSGetSRID(gb)) {
		err = createException(MAL, "geom.Distance", "Geometries of different SRID");
	} else if (!GEOSDistance(ga, gb, out)) {
		err = createException(MAL, "geom.Distance", "GEOSDistance failed");
	}

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return err;
}

/* Returns the 2d length of the geometry if it is a linestring or multilinestring */
str
wkbLength(dbl *out, wkb **a)
{
	GEOSGeom geosGeometry;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*a)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*a);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.Length", "wkb2geos failed");
	}

	if (!GEOSLength(geosGeometry, out))
		err = createException(MAL, "geom.Length", "GEOSLength failed");

	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns a geometry that represents the convex hull of this geometry.
 * The convex hull of a geometry represents the minimum convex geometry
 * that encloses all geometries within the set. */
str
wkbConvexHull(wkb **out, wkb **geom)
{
	str ret = MAL_SUCCEED;
	GEOSGeom geosGeometry;
	GEOSGeom convexHullGeometry = NULL;

	if (wkb_isnil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ConvexHull", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geom)) == NULL)
		throw(MAL, "geom.ConvexHull", MAL_MALLOC_FAIL);

	if ((convexHullGeometry = GEOSConvexHull(geosGeometry)) == NULL) {
		ret = createException(MAL, "geom.ConvexHull", "GEOSConvexHull failed");
	} else {
		GEOSSetSRID(convexHullGeometry, (*geom)->srid);
		*out = geos2wkb(convexHullGeometry);
		GEOSGeom_destroy(convexHullGeometry);
		if (*out == NULL)
			ret = createException(MAL, "geom.ConvexHull", "geos2wkb failed");
	}
	GEOSGeom_destroy(geosGeometry);

	return ret;

}

/* Gets two geometries and returns a new geometry */
static str
wkbanalysis(wkb **out, wkb **geom1WKB, wkb **geom2WKB, GEOSGeometry *(*func) (const GEOSGeometry *, const GEOSGeometry *), const char *name)
{
	GEOSGeom outGeometry, geom1Geometry, geom2Geometry;
	str err = MAL_SUCCEED;

	if (wkb_isnil(*geom1WKB) || wkb_isnil(*geom2WKB)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geom1Geometry = wkb2geos(*geom1WKB);
	geom2Geometry = wkb2geos(*geom2WKB);
	if (geom1Geometry == NULL || geom2Geometry == NULL) {
		*out = NULL;
		if (geom1Geometry)
			GEOSGeom_destroy(geom1Geometry);
		if (geom2Geometry)
			GEOSGeom_destroy(geom2Geometry);
		throw(MAL, name, "wkb2geos failed");
	}

	//make sure the geometries are of the same srid
	if (GEOSGetSRID(geom1Geometry) != GEOSGetSRID(geom2Geometry)) {
		err = createException(MAL, name, "Geometries of different SRID");
	} else if ((outGeometry = (*func) (geom1Geometry, geom2Geometry)) == NULL) {
		err = createException(MAL, name, "GEOS%s failed", name + 5);
	} else {
		GEOSSetSRID(outGeometry, GEOSGetSRID(geom1Geometry));
		*out = geos2wkb(outGeometry);
		GEOSGeom_destroy(outGeometry);
	}
	GEOSGeom_destroy(geom1Geometry);
	GEOSGeom_destroy(geom2Geometry);

	return err;
}

str
wkbIntersection(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSIntersection, "geom.Intersection");
}

str
wkbUnion(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSUnion, "geom.Union");
}

//Gets a BAT with geometries and returns a single LineString
str
wkbUnionAggr(wkb **outWKB, bat *inBAT_id)
{
	BAT *inBAT = NULL;
	BATiter inBAT_iter;
	BUN i;
	str err;
	wkb *aWKB, *bWKB;

	//get the BATs
	if (!(inBAT = BATdescriptor(*inBAT_id))) {
		throw(MAL, "geom.Union", "Problem retrieving BATs");
	}

	if (BATcount(inBAT) == 0) {
		BBPunfix(inBAT->batCacheid);
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Union", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);

	aWKB = (wkb *) BUNtail(inBAT_iter, 0);
	if (BATcount(inBAT) == 1) {
		err = wkbFromWKB(outWKB, &aWKB);
		BBPunfix(inBAT->batCacheid);
		if (err) {
			freeException(err);
			throw(MAL, "geom.Union", MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	bWKB = (wkb *) BUNtail(inBAT_iter, 1);
	//create the first union using the first two geometries
	err = wkbUnion(outWKB, &aWKB, &bWKB);
	for (i = 2; err == MAL_SUCCEED && i < BATcount(inBAT); i++) {
		aWKB = *outWKB;
		bWKB = (wkb *) BUNtail(inBAT_iter, i);
		*outWKB = NULL;

		err = wkbUnion(outWKB, &aWKB, &bWKB);
		GDKfree(aWKB);
	}

	BBPunfix(inBAT->batCacheid);

	return err;

}

str
wkbDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSDifference, "geom.Difference");
}

str
wkbSymDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSSymDifference, "geom.SymDifference");
}

/* Returns a geometry that represents all points whose distance from this Geometry is less than or equal to distance. */
str
wkbBuffer(wkb **out, wkb **geom, dbl *distance)
{
	GEOSGeom geosGeometry;
	GEOSGeom new;

	if (wkb_isnil(*geom) || *distance == dbl_nil) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Buffer", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.Buffer", "wkb2geos failed");
	}

	if ((new = GEOSBuffer(geosGeometry, *distance, 18)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Buffer", "GEOSBuffer failed");
	}
	*out = geos2wkb(new);
	GEOSGeom_destroy(new);
	GEOSGeom_destroy(geosGeometry);

	if (*out == NULL)
		throw(MAL, "geom.Buffer", MAL_MALLOC_FAIL);

	(*out)->srid = (*geom)->srid;

	return MAL_SUCCEED;
}

/* Gets two geometries and returns a Boolean by comparing them */
static str
wkbspatial(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, char (*func) (const GEOSGeometry *, const GEOSGeometry *), const char *name)
{
	int res;
	GEOSGeom geosGeometry_a, geosGeometry_b;

	if (wkb_isnil(*geomWKB_a) || wkb_isnil(*geomWKB_b)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry_a = wkb2geos(*geomWKB_a);
	geosGeometry_b = wkb2geos(*geomWKB_b);
	if (geosGeometry_a == NULL || geosGeometry_b == NULL) {
		if (geosGeometry_a)
			GEOSGeom_destroy(geosGeometry_a);
		if (geosGeometry_b)
			GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, name, "wkb2geos failed");
	}

	if (GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, name, "Geometries of different SRID");
	}

	res = (*func) (geosGeometry_a, geosGeometry_b);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);

	if (res == 2)
		throw(MAL, name, "GEOS%s failed", name + 5);

	*out = res;

	return MAL_SUCCEED;
}

str
wkbContains(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSContains, "geom.Contains");
}

str
wkbCrosses(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCrosses, "geom.Crosses");
}

str
wkbDisjoint(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSDisjoint, "geom.Disjoint");
}

str
wkbEquals(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSEquals, "geom.Equals");
}

str
wkbIntersects(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSIntersects, "geom.Intersects");
}

str
wkbOverlaps(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSOverlaps, "geom.Overlaps");
}

str
wkbRelate(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, str *pattern)
{
	int res;
	GEOSGeom geosGeometry_a, geosGeometry_b;

	if (wkb_isnil(*geomWKB_a) || wkb_isnil(*geomWKB_b) || strcmp(*pattern, str_nil) == 0) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry_a = wkb2geos(*geomWKB_a);
	geosGeometry_b = wkb2geos(*geomWKB_b);
	if (geosGeometry_a == NULL || geosGeometry_b == NULL) {
		if (geosGeometry_a)
			GEOSGeom_destroy(geosGeometry_a);
		if (geosGeometry_b)
			GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, "geom.RelatePattern", "wkb2geos failed");
	}

	if (GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, "geom.RelatePattern", "Geometries of different SRID");
	}

	res = GEOSRelatePattern(geosGeometry_a, geosGeometry_b, *pattern);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);

	if (res == 2)
		throw(MAL, "geom.RelatePattern", "GEOSRelatePattern failed");

	*out = res;

	return MAL_SUCCEED;
}

str
wkbTouches(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSTouches, "geom.Touches");
}

str
wkbWithin(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSWithin, "geom.Within");
}

str
wkbCovers(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCovers, "geom.Covers");
}

str
wkbCoveredBy(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCoveredBy, "geom.CoveredBy");
}

str
wkbDWithin(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, dbl *distance)
{
	double distanceComputed;
	str err;

	if (wkb_isnil(*geomWKB_a) || wkb_isnil(*geomWKB_b) || *distance == dbl_nil) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if ((err = wkbDistance(&distanceComputed, geomWKB_a, geomWKB_b)) != MAL_SUCCEED) {
		return err;
	}

	*out = (distanceComputed <= *distance);

	return MAL_SUCCEED;
}

/*returns the n-th geometry in a multi-geometry */
str
wkbGeometryN(wkb **out, wkb **geom, const int *geometryNum)
{
	int geometriesNum = -1;
	GEOSGeom geosGeometry = NULL;

	//no geometry at this position
	if (wkb_isnil(*geom) || *geometryNum == int_nil || *geometryNum <= 0) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.GeometryN", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);

	if (geosGeometry == NULL) {
		*out = NULL;
		throw(MAL, "geom.GeometryN", "wkb2geos failed");
	}

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	if (geometriesNum < 0) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.GeometryN", "GEOSGetNumGeometries failed");
	}
	if (geometriesNum == 1 || //geometry is not a multi geometry
	    geometriesNum < *geometryNum) { //no geometry at this position
		GEOSGeom_destroy(geosGeometry);
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.GeometryN", MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSGetGeometryN(geosGeometry, *geometryNum - 1));
	GEOSGeom_destroy(geosGeometry);
	if (*out == NULL)
		throw(MAL, "geom.GeometryN", MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* returns the number of geometries */
str
wkbNumGeometries(int *out, wkb **geom)
{
	GEOSGeom geosGeometry;

	if (wkb_isnil(*geom)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = int_nil;
		throw(MAL, "geom.NumGeometries", "wkb2geos failed");
	}

	*out = GEOSGetNumGeometries(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	if (*out < 0) {
		*out = int_nil;
		throw(MAL, "geom.GeometryN", "GEOSGetNumGeometries failed");
	}

	return MAL_SUCCEED;
}

/* MBR */

/* Creates the mbr for the given geom_geometry. */
str
wkbMBR(mbr **geomMBR, wkb **geomWKB)
{
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;
	bit empty;

	//check if the geometry is nil
	if (wkb_isnil(*geomWKB)) {
		if ((*geomMBR = GDKmalloc(sizeof(mbr))) == NULL)
			throw(MAL, "geom.MBR", MAL_MALLOC_FAIL);
		**geomMBR = *mbrNULL();
		return MAL_SUCCEED;
	}
	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}
	if (empty) {
		if ((*geomMBR = GDKmalloc(sizeof(mbr))) == NULL)
			throw(MAL, "geom.MBR", MAL_MALLOC_FAIL);
		**geomMBR = *mbrNULL();
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*geomMBR = NULL;
		throw(MAL, "geom.MBR", "Problem converting GEOS to WKB");
	}

	*geomMBR = mbrFromGeos(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (*geomMBR == NULL || mbr_isnil(*geomMBR)) {
		GDKfree(*geomMBR);
		*geomMBR = NULL;
		throw(MAL, "wkb.mbr", "Failed to create mbr");
	}

	return MAL_SUCCEED;
}

str
wkbBox2D(mbr **box, wkb **point1, wkb **point2)
{
	GEOSGeom point1_geom, point2_geom;
	double xmin = 0.0, ymin = 0.0, xmax = 0.0, ymax = 0.0;
	str err = MAL_SUCCEED;

	//check null input
	if (wkb_isnil(*point1) || wkb_isnil(*point2)) {
		if ((*box = GDKmalloc(sizeof(mbr))) == NULL)
			throw(MAL, "geom.MakeBox2D", MAL_MALLOC_FAIL);
		**box = *mbrNULL();
		return MAL_SUCCEED;
	}
	//check input not point geometries
	point1_geom = wkb2geos(*point1);
	point2_geom = wkb2geos(*point2);
	if (point1_geom == NULL || point2_geom == NULL) {
		if (point1_geom)
			GEOSGeom_destroy(point1_geom);
		if (point2_geom)
			GEOSGeom_destroy(point2_geom);
		throw(MAL, "geom.MakeBox2D", MAL_MALLOC_FAIL);
	}
	if (GEOSGeomTypeId(point1_geom) + 1 != wkbPoint_mdb ||
	    GEOSGeomTypeId(point2_geom) + 1 != wkbPoint_mdb) {
		err = createException(MAL, "geom.MakeBox2D", "Geometries should be points");
	} else if (GEOSGeomGetX(point1_geom, &xmin) == -1 ||
		   GEOSGeomGetY(point1_geom, &ymin) == -1 ||
		   GEOSGeomGetX(point2_geom, &xmax) == -1 ||
		   GEOSGeomGetY(point2_geom, &ymax) == -1) {

		err = createException(MAL, "geom.MakeBox2D", "Error in reading the points' coordinates");
	} else {
		//Assign the coordinates. Ensure that they are in correct order
		*box = GDKmalloc(sizeof(mbr));
		(*box)->xmin = (float) (xmin < xmax ? xmin : xmax);
		(*box)->ymin = (float) (ymin < ymax ? ymin : ymax);
		(*box)->xmax = (float) (xmax > xmin ? xmax : xmin);
		(*box)->ymax = (float) (ymax > ymin ? ymax : ymin);
	}
	GEOSGeom_destroy(point1_geom);
	GEOSGeom_destroy(point2_geom);

	return err;
}

static str
mbrrelation_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB, str (*func)(bit *, mbr **, mbr **))
{
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (wkb_isnil(*geom1WKB) || wkb_isnil(*geom2WKB)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(&geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}

	ret = (*func) (out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);

	return ret;
}

/*returns true if the two
 * 	mbrs overlap */
str
mbrOverlaps(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else			//they cannot overlap if b2 is left, right, above or below b1
		*out = !((*b2)->ymax < (*b1)->ymin ||
			 (*b2)->ymin > (*b1)->ymax ||
			 (*b2)->xmax < (*b1)->xmin ||
			 (*b2)->xmin > (*b1)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of the two geometries overlap */
str
mbrOverlaps_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlaps);
}

/* returns true if b1 is above b2 */
str
mbrAbove(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin > (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is above the mbr of geom2 */
str
mbrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrAbove);
}

/* returns true if b1 is below b2 */
str
mbrBelow(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax < (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is below the mbr of geom2 */
str
mbrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrBelow);
}

/* returns true if box1 is left of box2 */
str
mbrLeft(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax < (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the left of the mbr of geom2 */
str
mbrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrLeft);
}

/* returns true if box1 is right of box2 */
str
mbrRight(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin > (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the right of the mbr of geom2 */
str
mbrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrRight);
}

/* returns true if box1 overlaps or is above box2 when only the Y coordinate is considered*/
str
mbrOverlapOrAbove(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin >= (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is above the mbr of geom2 */
str
mbrOverlapOrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrAbove);
}

/* returns true if box1 overlaps or is below box2 when only the Y coordinate is considered*/
str
mbrOverlapOrBelow(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax <= (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is below the mbr of geom2 */
str
mbrOverlapOrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrBelow);
}

/* returns true if box1 overlaps or is left of box2 when only the X coordinate is considered*/
str
mbrOverlapOrLeft(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax <= (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the left of the mbr of geom2 */
str
mbrOverlapOrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrLeft);
}

/* returns true if box1 overlaps or is right of box2 when only the X coordinate is considered*/
str
mbrOverlapOrRight(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin >= (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the right of the mbr of geom2 */
str
mbrOverlapOrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrRight);
}

/* returns true if b1 is contained in b2 */
str
mbrContained(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = bit_nil;
	else
		*out = (((*b1)->xmin >= (*b2)->xmin) && ((*b1)->xmax <= (*b2)->xmax) && ((*b1)->ymin >= (*b2)->ymin) && ((*b1)->ymax <= (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is contained in the mbr of geom2 */
str
mbrContained_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrContained);
}

/*returns true if b1 contains b2 */
str
mbrContains(bit *out, mbr **b1, mbr **b2)
{
	return mbrContained(out, b2, b1);
}

/*returns true if the mbrs of geom1 contains the mbr of geom2 */
str
mbrContains_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrContains);
}

/* returns true if the boxes are the same */
str
mbrEqual(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) && mbr_isnil(*b2))
		*out = 1;
	else if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = (((*b1)->xmin == (*b2)->xmin) && ((*b1)->xmax == (*b2)->xmax) && ((*b1)->ymin == (*b2)->ymin) && ((*b1)->ymax == (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 and the mbr of geom2 are the same */
str
mbrEqual_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrEqual);
}

/* returns the Euclidean distance of the centroids of the boxes */
str
mbrDistance(dbl *out, mbr **b1, mbr **b2)
{
	double b1_Cx = 0.0, b1_Cy = 0.0, b2_Cx = 0.0, b2_Cy = 0.0;

	if (mbr_isnil(*b1) || mbr_isnil(*b2)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}
	//compute the centroids of the two polygons
	b1_Cx = ((*b1)->xmin + (*b1)->xmax) / 2.0;
	b1_Cy = ((*b1)->ymin + (*b1)->ymax) / 2.0;
	b2_Cx = ((*b2)->xmin + (*b2)->xmax) / 2.0;
	b2_Cy = ((*b2)->ymin + (*b2)->ymax) / 2.0;

	//compute the euclidean distance
	*out = sqrt(pow(b2_Cx - b1_Cx, 2.0) + pow(b2_Cy - b1_Cy, 2.0));

	return MAL_SUCCEED;
}

/*returns the Euclidean distance of the centroids of the mbrs of the two geometries */
str
mbrDistance_wkb(dbl *out, wkb **geom1WKB, wkb **geom2WKB)
{
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (wkb_isnil(*geom1WKB) || wkb_isnil(*geom2WKB)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(&geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}

	ret = mbrDistance(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);

	return ret;
}

/* get Xmin, Ymin, Xmax, Ymax coordinates of mbr */
str
wkbCoordinateFromMBR(dbl *coordinateValue, mbr **geomMBR, int *coordinateIdx)
{
	//check if the MBR is null
	if (mbr_isnil(*geomMBR) || *coordinateIdx == int_nil) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	switch (*coordinateIdx) {
	case 1:
		*coordinateValue = (*geomMBR)->xmin;
		break;
	case 2:
		*coordinateValue = (*geomMBR)->ymin;
		break;
	case 3:
		*coordinateValue = (*geomMBR)->xmax;
		break;
	case 4:
		*coordinateValue = (*geomMBR)->ymax;
		break;
	default:
		throw(MAL, "geom.coordinateFromMBR", "Unrecognized coordinateIdx: %d\n", *coordinateIdx);
	}

	return MAL_SUCCEED;
}

str
wkbCoordinateFromWKB(dbl *coordinateValue, wkb **geomWKB, int *coordinateIdx)
{
	mbr *geomMBR;
	str ret = MAL_SUCCEED;
	bit empty;

	if (wkb_isnil(*geomWKB) || *coordinateIdx == int_nil) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}

	if (empty) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	if ((ret = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED)
		return ret;

	ret = wkbCoordinateFromMBR(coordinateValue, &geomMBR, coordinateIdx);

	GDKfree(geomMBR);

	return ret;
}

str
mbrFromString(mbr **w, str *src)
{
	int len = *w ? (int) sizeof(mbr) : 0;
	char *errbuf;
	str ex;

	if (mbrFROMSTR(*src, &len, w))
		return MAL_SUCCEED;
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "mbr.FromString", "%s", errbuf);

	GDKclrerr();

	return ex;
}

str
wkbIsnil(bit *r, wkb **v)
{
	*r = wkb_isnil(*v);
	return MAL_SUCCEED;
}

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY)
{
	if ((*res = GDKmalloc(sizeof(mbr))) == NULL)
		throw(MAL, "geom.mbr", MAL_MALLOC_FAIL);
	if (*minX == flt_nil || *minY == flt_nil || *maxX == flt_nil || *maxY == flt_nil)
		**res = *mbrNULL();
	else {
		(*res)->xmin = *minX;
		(*res)->ymin = *minY;
		(*res)->xmax = *maxX;
		(*res)->ymax = *maxY;
	}
	return MAL_SUCCEED;
}

/***********************************************/
/************* wkb type functions **************/
/***********************************************/

/* Creates the string representation (WKT) of a WKB */
/* return length of resulting string. */
int
wkbTOSTR(char **geomWKT, int *len, wkb *geomWKB)
{
	char *wkt = NULL;
	size_t dstStrLen = 5;	/* "nil" */

	/* from WKB to GEOSGeometry */
	GEOSGeom geosGeometry = wkb2geos(geomWKB);

	if (geosGeometry) {
		size_t l;
		GEOSWKTWriter *WKT_wr = GEOSWKTWriter_create();
		//set the number of dimensions in the writer so that it can
		//read correctly the geometry coordinates
		GEOSWKTWriter_setOutputDimension(WKT_wr, GEOSGeom_getCoordinateDimension(geosGeometry));
		GEOSWKTWriter_setTrim(WKT_wr, 1);
		wkt = GEOSWKTWriter_write(WKT_wr, geosGeometry);
		l = strlen(wkt);
		assert(l < GDK_int_max);
		dstStrLen = l + 2;	/* add quotes */
		GEOSWKTWriter_destroy(WKT_wr);
		GEOSGeom_destroy(geosGeometry);
	}

	if (wkt) {
		if (*len < (int) dstStrLen + 1) {
			*len = (int) dstStrLen + 1;
			GDKfree(*geomWKT);
			*geomWKT = GDKmalloc(*len);
			if( *geomWKT == NULL){
				GDKerror("wkbTOSTR" MAL_MALLOC_FAIL);
				return 0;
			}
		}
		snprintf(*geomWKT, *len, "\"%s\"", wkt);
		GEOSFree(wkt);
	} else {
		if (*len < 4) {
			GDKfree(*geomWKT);
			*geomWKT = GDKmalloc(*len = 4);
			if( *geomWKT == NULL){
				GDKerror("wkbTOSTR" MAL_MALLOC_FAIL);
				return 0;
			}
		}
		strcpy(*geomWKT, "nil");
	}

	assert(dstStrLen <= GDK_int_max);
	return (int) dstStrLen;
}

int
wkbFROMSTR(char *geomWKT, int *len, wkb **geomWKB)
{
	size_t parsedBytes;
	str err;

	err = wkbFROMSTR_withSRID(geomWKT, len, geomWKB, 0, &parsedBytes);
	if (err != MAL_SUCCEED) {
		freeException(err);
		return 0;
	}
	return (int) parsedBytes;
}

BUN
wkbHASH(wkb *w)
{
	int i;
	BUN h = 0;

	for (i = 0; i < (w->len - 1); i += 2) {
		int a = *(w->data + i), b = *(w->data + i + 1);
		h = (h << 3) ^ (h >> 11) ^ (h >> 17) ^ (b << 8) ^ a;
	}
	return h;
}

/* returns a pointer to a null wkb */
wkb *
wkbNULL(void)
{
	return (&wkb_nil);
}

int
wkbCOMP(wkb *l, wkb *r)
{
	int len = l->len;

	if (len != r->len)
		return len - r->len;

	if (len == ~(int) 0)
		return (0);

	return memcmp(l->data, r->data, len);
}

/* read wkb from log */
wkb *
wkbREAD(wkb *a, stream *s, size_t cnt)
{
	int len;
	int srid;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if (geomversion_get())
		srid = 0;
	else if (mnstr_readInt(s, &srid) != 1)
		return NULL;
	if ((a = GDKmalloc(wkb_size(len))) == NULL)
		return NULL;
	a->len = len;
	a->srid = srid;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

/* write wkb to log */
gdk_return
wkbWRITE(wkb *a, stream *s, size_t cnt)
{
	int len = a->len;
	int srid = a->srid;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, len))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (!mnstr_writeInt(s, srid))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (len > 0 &&		/* 64bit: check for overflow */
	    mnstr_write(s, (char *) a->data, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

var_t
wkbPUT(Heap *h, var_t *bun, wkb *val)
{
	char *base;

	*bun = HEAP_malloc(h, wkb_size(val->len));
	base = h->base;
	if (*bun) {
		memcpy(&base[*bun], (char *) val, wkb_size(val->len));
		h->dirty = 1;
	}
	return *bun;
}

void
wkbDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}

int
wkbLENGTH(wkb *p)
{
	var_t len = wkb_size(p->len);
	assert(len <= GDK_int_max);
	return (int) len;
}

void
wkbHEAP(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

/***********************************************/
/************* mbr type functions **************/
/***********************************************/

#define MBR_WKTLEN 256

/* TOSTR: print atom in a string. */
/* return length of resulting string. */
int
mbrTOSTR(char **dst, int *len, mbr *atom)
{
	static char tempWkt[MBR_WKTLEN];
	size_t dstStrLen = 3;

	if (!mbr_isnil(atom)) {
		snprintf(tempWkt, MBR_WKTLEN, "BOX (%f %f, %f %f)", atom->xmin, atom->ymin, atom->xmax, atom->ymax);
		dstStrLen = strlen(tempWkt) + 2;
		assert(dstStrLen < GDK_int_max);
	}

	if (*len < (int) dstStrLen + 1 || *dst == NULL) {
		GDKfree(*dst);
		*dst = GDKmalloc(*len = (int) dstStrLen + 1);
		if( *dst == NULL){
			GDKerror("mbrTOSTR" MAL_MALLOC_FAIL);
			return 0;
		}
	}

	if (dstStrLen > 3)
		snprintf(*dst, *len, "\"%s\"", tempWkt);
	else
		strcpy(*dst, "nil");
	return (int) dstStrLen;
}

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */
int
mbrFROMSTR(char *src, int *len, mbr **atom)
{
	int nil = 0;
	size_t nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL;	/* The geometry object that is parsed from the src string. */
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
	char *c;

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && (strstr(src, "mbr") == src || strstr(src, "MBR") == src) && (c = strstr(src, "(")) != NULL) {
		/* Parse the mbr */
		if ((c - src) != 3 && (c - src) != 4) {
			GDKerror("ParseException: Expected a string like 'MBR(0 0,1 1)' or 'MBR (0 0,1 1)'\n");
			return 0;
		}

		if (sscanf(c, "(%lf %lf,%lf %lf)", &xmin, &ymin, &xmax, &ymax) != 4) {
			GDKerror("ParseException: Not enough coordinates.\n");
			return 0;
		}
	} else if (!nil && (geosMbr = GEOSGeomFromWKT(src)) == NULL)
		return 0;

	if (*len < (int) sizeof(mbr)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = sizeof(mbr));
		if( *atom == NULL){
			GDKerror("mbrFROMSTR" MAL_MALLOC_FAIL);
			return 0;
		}
	}
	if (nil) {
		nchars = 3;
		**atom = *mbrNULL();
	} else if (geosMbr == NULL) {
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
		(*atom)->xmin = (float) xmin;
		(*atom)->ymin = (float) ymin;
		(*atom)->xmax = (float) xmax;
		(*atom)->ymax = (float) ymax;
		nchars = strlen(src);
	}
	if (geosMbr)
		GEOSGeom_destroy(geosMbr);
	assert(nchars <= GDK_int_max);
	return (int) nchars;
}

/* HASH: compute a hash value. */
/* returns a positive integer hash value */
BUN
mbrHASH(mbr *atom)
{
	return (BUN) (((int) atom->xmin * (int)atom->ymin) *((int) atom->xmax * (int)atom->ymax));
}

/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */
static mbr mbrNIL = {
	GDK_flt_min,		/* flt_nil */
	GDK_flt_min,
	GDK_flt_min,
	GDK_flt_min
};

mbr *
mbrNULL(void)
{
	return &mbrNIL;
}

/* COMP: compare two mbrs. */
/* returns int <0 if l<r, 0 if l==r, >0 else */
int
mbrCOMP(mbr *l, mbr *r)
{
	/* simple lexicographical ordering on (x,y) */
	int res;
	if (l->xmin == r->xmin)
		res = (l->ymin < r->ymin) ? -1 : (l->ymin != r->ymin);
	else
		res = (l->xmin < r->xmin) ? -1 : 1;
	if (res == 0) {
		if (l->xmax == r->xmax)
			res = (l->ymax < r->ymax) ? -1 : (l->ymax != r->ymax);
		else
			res = (l->xmax < r->xmax) ? -1 : 1;
	}
	return res;
}

/* read mbr from log */
mbr *
mbrREAD(mbr *A, stream *s, size_t cnt)
{
	mbr *a = A;
	mbr *c;
	size_t i;
	int v[4];
	flt vals[4];

	if (a == NULL && (a = GDKmalloc(cnt * sizeof(mbr))) == NULL)
		return NULL;
	for (i = 0, c = a; i < cnt; i++, c++) {
		if (!mnstr_readIntArray(s, v, 4)) {
			if (a != A)
				GDKfree(a);
			return NULL;
		}
		memcpy(vals, v, 4 * sizeof(int));
		c->xmin = vals[0];
		c->ymin = vals[1];
		c->xmax = vals[2];
		c->ymax = vals[3];
	}
	return a;
}

/* write mbr to log */
gdk_return
mbrWRITE(mbr *c, stream *s, size_t cnt)
{
	size_t i;
	flt vals[4];
	int v[4];

	for (i = 0; i < cnt; i++, c++) {
		vals[0] = c->xmin;
		vals[1] = c->ymin;
		vals[2] = c->xmax;
		vals[3] = c->ymax;
		memcpy(v, vals, 4 * sizeof(int));
		if (!mnstr_writeIntArray(s, v, 4))
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/************************************************/
/************* wkba type functions **************/
/************************************************/

/* Creates the string representation of a wkb_array */
/* return length of resulting string. */

/* StM: Open question / ToDo:
 * why is len of type int,
 * while the returned length (correctly!) is of type size_t ?
 * (not only here, but also elsewhere in this file / the geom code)
 */
int
wkbaTOSTR(char **toStr, int *len, wkba *fromArray)
{
	int items = fromArray->itemsNum, i;
	int itemsNumDigits = (int) ceil(log10(items));
	size_t dataSize;	//, skipBytes=0;
	char **partialStrs;
	char *nilStr = "nil";
	char *toStrPtr = NULL, *itemsNumStr = GDKmalloc((itemsNumDigits + 1) * sizeof(char));

	if( itemsNumStr == NULL){
		GDKerror("wkbaTOSTR" MAL_MALLOC_FAIL);
		return 0;
	}
	sprintf(itemsNumStr, "%d", items);
	dataSize = strlen(itemsNumStr);

	//reserve space for an array with pointers to the partial strings, i.e. for each wkbTOSTR
	partialStrs = GDKzalloc(items * sizeof(char *));
	//create the string version of each wkb
	for (i = 0; i < items; i++) {
		int llen = 0;
		dataSize += wkbTOSTR(&partialStrs[i], &llen, fromArray->data[i]) - 2;	//remove quotes

		if (strcmp(partialStrs[i], nilStr) == 0) {
			GDKfree(itemsNumStr);
			while (i >= 0)
				GDKfree(partialStrs[i--]);
			GDKfree(partialStrs);
			if (*len < 4 || *toStr == NULL) {
				GDKfree(*toStr);
				*toStr = GDKmalloc(*len = 4);
				if( *toStr == NULL){
					GDKerror("wkbaTOSTR" MAL_MALLOC_FAIL);
					return 0;
				}
			}
			strcpy(*toStr, "nil");
			return 3;
		}
	}

	//add [] around itemsNum
	dataSize += 2;
	//add ", " before each item
	dataSize += 2 * sizeof(char) * items;

	//copy all partial strings to a single one
	if (*len < (int) dataSize + 3 || *toStr == NULL) {
		GDKfree(*toStr);
		*toStr = GDKmalloc(*len = (int) dataSize + 3);	/* plus quotes + termination character */
	}
	toStrPtr = *toStr;
	*toStrPtr++ = '\"';
	*toStrPtr++ = '[';
	strcpy(toStrPtr, itemsNumStr);
	toStrPtr += strlen(itemsNumStr);
	*toStrPtr++ = ']';
	for (i = 0; i < items; i++) {
		if (i == 0)
			*toStrPtr++ = ':';
		else
			*toStrPtr++ = ',';
		*toStrPtr++ = ' ';

		//strcpy(toStrPtr, partialStrs[i]);
		memcpy(toStrPtr, &partialStrs[i][1], strlen(partialStrs[i]) - 2);
		toStrPtr += strlen(partialStrs[i]) - 2;
		GDKfree(partialStrs[i]);
	}

	*toStrPtr++ = '\"';
	*toStrPtr = '\0';

	GDKfree(partialStrs);
	GDKfree(itemsNumStr);

	assert(strlen(*toStr) + 1 < (size_t) GDK_int_max);
	*len = (int) strlen(*toStr) + 1;
	return (int) (toStrPtr - *toStr);
}

/* return number of parsed characters. */
int
wkbaFROMSTR(char *fromStr, int *len, wkba **toArray)
{
	return wkbaFROMSTR_withSRID(fromStr, len, toArray, 0);
}

/* returns a pointer to a null wkba */
wkba *
wkbaNULL(void)
{
	static wkba nullval = {~0};

	return &nullval;
}

BUN
wkbaHASH(wkba *wArray)
{
	int j, i;
	BUN h = 0;

	for (j = 0; j < wArray->itemsNum; j++) {
		wkb *w = wArray->data[j];
		for (i = 0; i < (w->len - 1); i += 2) {
			int a = *(w->data + i), b = *(w->data + i + 1);
			h = (h << 3) ^ (h >> 11) ^ (h >> 17) ^ (b << 8) ^ a;
		}
	}
	return h;
}

int
wkbaCOMP(wkba *l, wkba *r)
{
	int i, res = 0;;

	//compare the number of items
	if (l->itemsNum != r->itemsNum)
		return l->itemsNum - r->itemsNum;

	if (l->itemsNum == ~(int) 0)
		return (0);

	//compare each wkb separately
	for (i = 0; i < l->itemsNum; i++)
		res += wkbCOMP(l->data[i], r->data[i]);

	return res;
}

/* read wkb from log */
wkba *
wkbaREAD(wkba *a, stream *s, size_t cnt)
{
	int items, i;

	(void) cnt;
	assert(cnt == 1);

	if (mnstr_readInt(s, &items) != 1)
		return NULL;

	if ((a = GDKmalloc(wkba_size(items))) == NULL)
		return NULL;

	a->itemsNum = items;

	for (i = 0; i < items; i++)
		wkbREAD(a->data[i], s, cnt);

	return a;
}

/* write wkb to log */
gdk_return
wkbaWRITE(wkba *a, stream *s, size_t cnt)
{
	int i, items = a->itemsNum;
	gdk_return ret = GDK_SUCCEED;

	(void) cnt;
	assert(cnt == 1);

	if (!mnstr_writeInt(s, items))
		return GDK_FAIL;
	for (i = 0; i < items; i++) {
		ret = wkbWRITE(a->data[i], s, cnt);

		if (ret != GDK_SUCCEED)
			return ret;
	}
	return GDK_SUCCEED;
}

var_t
wkbaPUT(Heap *h, var_t *bun, wkba *val)
{
	char *base;

	*bun = HEAP_malloc(h, wkba_size(val->itemsNum));
	base = h->base;
	if (*bun) {
		memcpy(&base[*bun], (char *) val, wkba_size(val->itemsNum));
		h->dirty = 1;
	}
	return *bun;
}

void
wkbaDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}

int
wkbaLENGTH(wkba *p)
{
	var_t len = wkba_size(p->itemsNum);
	assert(len <= GDK_int_max);
	return (int) len;
}

void
wkbaHEAP(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

geom_export str wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y);
geom_export str wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y);

static inline double
isLeft(double P0x, double P0y, double P1x, double P1y, double P2x, double P2y)
{
	return ((P1x - P0x) * (P2y - P0y)
		- (P2x - P0x) * (P1y - P0y));
}

static str
pnpoly(int *out, int nvert, dbl *vx, dbl *vy, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt;
	int j = 0, nv;
	bit *cs = NULL;

	/*Get the BATs */
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment */
	if (bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
	}

	/*Create output BAT */
	if ((bo = COLnew(bpx->hseqbase, TYPE_bit, BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs */
	px = (dbl *) Tloc(bpx, 0);
	py = (dbl *) Tloc(bpy, 0);

	nv = nvert - 1;
	cnt = BATcount(bpx);
	cs = (bit *) Tloc(bo, 0);
	for (i = 0; i < cnt; i++) {
		int wn = 0;
		for (j = 0; j < nv; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j + 1] > py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) > 0)
						++wn;
			} else {
				if (vy[j + 1] <= py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn & 1;
	}

	bo->tsorted = bo->trevsorted = 0;
	bo->tkey = 0;
	BATsetcount(bo, cnt);
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

static str
pnpolyWithHoles(bat *out, int nvert, dbl *vx, dbl *vy, int nholes, dbl **hx, dbl **hy, int *hn, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt = 0;
	int j = 0, h = 0;
	bit *cs = NULL;

	/*Get the BATs */
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment */
	if (bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
	}

	/*Create output BAT */
	if ((bo = COLnew(bpx->hseqbase, TYPE_bit, BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs */
	px = (dbl *) Tloc(bpx, 0);
	py = (dbl *) Tloc(bpy, 0);
	cnt = BATcount(bpx);
	cs = (bit *) Tloc(bo, 0);
	for (i = 0; i < cnt; i++) {
		int wn = 0;

		/*First check the holes */
		for (h = 0; h < nholes; h++) {
			int nv = hn[h] - 1;
			wn = 0;
			for (j = 0; j < nv; j++) {
				if (hy[h][j] <= py[i]) {
					if (hy[h][j + 1] > py[i])
						if (isLeft(hx[h][j], hy[h][j], hx[h][j + 1], hy[h][j + 1], px[i], py[i]) > 0)
							++wn;
				} else {
					if (hy[h][j + 1] <= py[i])
						if (isLeft(hx[h][j], hy[h][j], hx[h][j + 1], hy[h][j + 1], px[i], py[i]) < 0)
							--wn;
				}
			}

			/*It is in one of the holes */
			if (wn) {
				break;
			}
		}

		if (wn)
			continue;

		/*If not in any of the holes, check inside the Polygon */
		for (j = 0; j < nvert - 1; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j + 1] > py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) > 0)
						++wn;
			} else {
				if (vy[j + 1] <= py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn & 1;
	}
	bo->tsorted = bo->trevsorted = 0;
	bo->tkey = 0;
	BATsetcount(bo, cnt);
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

#define POLY_NUM_VERT 120
#define POLY_NUM_HOLE 10

str
wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y)
{
	double *vert_x, *vert_y, **holes_x = NULL, **holes_y = NULL;
	int *holes_n = NULL, j;
	wkb *geom = NULL;
	str err = MAL_SUCCEED;
	str geom_str = NULL;
	char *str2, *token, *subtoken;
	char *saveptr1 = NULL, *saveptr2 = NULL;
	int nvert = 0, nholes = 0;

	geom = (wkb *) *a;

	if ((err = wkbAsText(&geom_str, &geom, NULL)) != MAL_SUCCEED) {
		return err;
	}
	token = strchr(geom_str, '(');
	token += 2;

	/*Lets get the polygon */
	token = strtok_r(token, ")", &saveptr1);
	vert_x = GDKmalloc(POLY_NUM_VERT * sizeof(double));
	if ( vert_x == NULL){
		GDKfree(geom_str);
		throw(MAL,"wkbContains_point_bat", MAL_MALLOC_FAIL);
	}
	vert_y = GDKmalloc(POLY_NUM_VERT * sizeof(double));
	if ( vert_y == NULL){
		GDKfree(geom_str);
		GDKfree(vert_x);
		throw(MAL,"wkbContains_point_bat", MAL_MALLOC_FAIL);
	}

	for (str2 = token;; str2 = NULL) {
		subtoken = strtok_r(str2, ",", &saveptr2);
		if (subtoken == NULL)
			break;
		sscanf(subtoken, "%lf %lf", &vert_x[nvert], &vert_y[nvert]);
		nvert++;
		if ((nvert % POLY_NUM_VERT) == 0) {
			vert_x = GDKrealloc(vert_x, nvert * 2 * sizeof(double));
			vert_y = GDKrealloc(vert_y, nvert * 2 * sizeof(double));
		}
	}

	token = strtok_r(NULL, ")", &saveptr1);
	if (token) {
		holes_x = GDKzalloc(POLY_NUM_HOLE * sizeof(double *));
		holes_y = GDKzalloc(POLY_NUM_HOLE * sizeof(double *));
		holes_n = GDKzalloc(POLY_NUM_HOLE * sizeof(double *));
	}
	/*Lets get all the holes */
	while (token) {
		int nhole = 0;
		token = strchr(token, '(');
		if (!token)
			break;
		token++;

		if (!holes_x[nholes])
			holes_x[nholes] = GDKzalloc(POLY_NUM_VERT * sizeof(double));
		if (!holes_y[nholes])
			holes_y[nholes] = GDKzalloc(POLY_NUM_VERT * sizeof(double));

		for (str2 = token;; str2 = NULL) {
			subtoken = strtok_r(str2, ",", &saveptr2);
			if (subtoken == NULL)
				break;
			sscanf(subtoken, "%lf %lf", &holes_x[nholes][nhole], &holes_y[nholes][nhole]);
			nhole++;
			if ((nhole % POLY_NUM_VERT) == 0) {
				holes_x[nholes] = GDKrealloc(holes_x[nholes], nhole * 2 * sizeof(double));
				holes_y[nholes] = GDKrealloc(holes_y[nholes], nhole * 2 * sizeof(double));
			}
		}

		holes_n[nholes] = nhole;
		nholes++;
		if ((nholes % POLY_NUM_HOLE) == 0) {
			holes_x = GDKrealloc(holes_x, nholes * 2 * sizeof(double *));
			holes_y = GDKrealloc(holes_y, nholes * 2 * sizeof(double *));
			holes_n = GDKrealloc(holes_n, nholes * 2 * sizeof(int));
		}
		token = strtok_r(NULL, ")", &saveptr1);
	}

	if (nholes)
		err = pnpolyWithHoles(out, (int) nvert, vert_x, vert_y, nholes, holes_x, holes_y, holes_n, point_x, point_y);
	else {
		err = pnpoly(out, (int) nvert, vert_x, vert_y, point_x, point_y);
	}

	GDKfree(geom_str);
	GDKfree(vert_x);
	GDKfree(vert_y);
	if (holes_x && holes_y && holes_n) {
		for (j = 0; j < nholes; j++) {
			GDKfree(holes_x[j]);
			GDKfree(holes_y[j]);
		}
		GDKfree(holes_x);
		GDKfree(holes_y);
		GDKfree(holes_n);
	}

	return err;
}

str
wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y)
{
	(void) a;
	(void) point_x;
	(void) point_y;
	*out = TRUE;
	return MAL_SUCCEED;
}
