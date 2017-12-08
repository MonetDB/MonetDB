/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f libgeom
 * @a Niels Nes
 *
 * @* The simple geom library
 */

#include "monetdb_config.h"
#include "libgeom.h"

#include <math.h>

void
libgeom_init(void)
{
	initGEOS((GEOSMessageHandler) GDKerror, (GEOSMessageHandler) GDKerror);
	GEOS_setWKBByteOrder(1);	/* NDR (little endian) */
	printf("# MonetDB/GIS module loaded\n");
	fflush(stdout);		/* make merovingian see this *now* */
}

void
libgeom_exit(void)
{
	finishGEOS();
}

int
wkb_isnil(wkb *w)
{
	if (!w || w->len == ~0)
		return 1;
	return 0;
}


/* Function getMbrGeos
 * Creates an mbr holding the lower left and upper right coordinates
 * of a GEOSGeom.
 */
/*
int
getMbrGeos(mbr *res, const GEOSGeom geosGeometry)
{
	GEOSGeom envelope;
	//int coordinatesNum  = 0; 
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;

	if (!geosGeometry || (envelope = GEOSEnvelope(geosGeometry)) == NULL)
		return 0;

	// get the number of coordinates the geometry has
	//coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);

	if (GEOSGeomTypeId(envelope) == GEOS_POINT) {
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
		const GEOSCoordSequence *coords = GEOSGeom_getCoordSeq(envelope);
#else
		const GEOSCoordSeq coords = GEOSGeom_getCoordSeq(envelope);
#endif
		GEOSCoordSeq_getX(coords, 0, &xmin);
		GEOSCoordSeq_getY(coords, 0, &ymin);
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		res->xmin = (float) xmin;
		res->ymin = (float) ymin;
		res->xmax = (float) xmin;
		res->ymax = (float) ymin;
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
			GEOSCoordSeq_getX(coords, 0, &xmin);
			GEOSCoordSeq_getY(coords, 0, &ymin);
			GEOSCoordSeq_getX(coords, 2, &xmax);
			GEOSCoordSeq_getY(coords, 2, &ymax);
			assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
			assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
			assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
			assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
			res->xmin = (float) xmin;
			res->ymin = (float) ymin;
			res->xmax = (float) xmax;
			res->ymax = (float) ymax;
		}
	}
	GEOSGeom_destroy(envelope);
	return 1;
}
*/

GEOSGeom
wkb2geos(wkb *geomWKB)
{
	GEOSGeom geosGeometry;

	if (wkb_isnil(geomWKB))
		return NULL;

	geosGeometry = GEOSGeomFromWKB_buf((unsigned char *) geomWKB->data, geomWKB->len);

	if (geosGeometry != NULL)
		GEOSSetSRID(geosGeometry, geomWKB->srid);

	return geosGeometry;
}

/* Function getMbrGeom
 * A wrapper for getMbrGeos on a geom_geometry.
 */
/*
int
getMbrGeom(mbr *res, wkb *geom)
{
	GEOSGeom geosGeometry = wkb2geos(geom);

	if (geosGeometry) {
		int r = getMbrGeos(res, geosGeometry);
		GEOSGeom_destroy(geosGeometry);
		return r;
	}
	return 0;
}
*/

const char *
geom_type2str(int t, int flag)
{
	if (flag == 0) {
		switch (t) {
		//case wkbGeometry:
		//      return "GEOMETRY";
		case wkbPoint_mdb:
			return "POINT";
		case wkbLineString_mdb:
			return "LINESTRING";
		case wkbLinearRing_mdb:
			return "LINEARRING";
		case wkbPolygon_mdb:
			return "POLYGON";
		case wkbMultiPoint_mdb:
			return "MULTIPOINT";
		case wkbMultiLineString_mdb:
			return "MULTILINESTRING";
		case wkbMultiPolygon_mdb:
			return "MULTIPOLYGON";
		case wkbGeometryCollection_mdb:
			return "GEOMETRYCOLLECTION";
		}
	} else if (flag == 1) {
		switch (t) {
		//case wkbGeometry:
		//      return "ST_Geometry";
		case wkbPoint_mdb:
			return "ST_Point";
		case wkbLineString_mdb:
			return "ST_LineString";
		case wkbLinearRing_mdb:
			return "ST_LinearRing";
		case wkbPolygon_mdb:
			return "ST_Polygon";
		case wkbMultiPoint_mdb:
			return "ST_MultiPoint";
		case wkbMultiLineString_mdb:
			return "ST_MultiLinestring";
		case wkbMultiPolygon_mdb:
			return "ST_MultiPolygon";
		case wkbGeometryCollection_mdb:
			return "ST_GeometryCollection";
		}
	}
	return "UKNOWN";
}


/*
str
geomerty_2_geometry(wkb *res, wkb **geom, int *columnType, int *columnSRID, int *valueSRID)
{

	//char* geomStr;
	//int len = 0;
	//fprintf(stderr, "geometry_2_geometry\n");
	//wkbTOSTR(&geomStr, &len, *geom);
	if (*geom != NULL)
		fprintf(stderr, "type:%d - wkbTOSTR cannot be seen at this point\n", *columnType);

	if (res == NULL)
		fprintf(stderr, "-> ");

	fprintf(stderr, "%d vs %d\n", *columnSRID, *valueSRID);
	return "0";
}
*/
