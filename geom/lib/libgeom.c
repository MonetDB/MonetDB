/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f libgeom
 * @a Niels Nes
 *
 * @* The simple geom library
 */

#include <monetdb_config.h>
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
	if (!w ||w->len == ~0)
		return 1;
	return 0;
}


/* Function getMbrGeos
 * Creates an mbr holding the lower left and upper right coordinates
 * of a GEOSGeom.
 */
int
getMbrGeos(mbr *res, const GEOSGeom geosGeometry)
{
	GEOSGeom envelope;
	double xmin, ymin, xmax, ymax;

	if (!geosGeometry || (envelope = GEOSEnvelope(geosGeometry)) == NULL)
		return 0;

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
	} else {		/* GEOSGeomTypeId(envelope) == GEOS_POLYGON */
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

GEOSGeom wkb2geos(wkb* geomWKB) {
	GEOSGeom geosGeometry;
	
	if(wkb_isnil(geomWKB))
		return NULL;
	
	geosGeometry = GEOSGeomFromWKB_buf((unsigned char *)((geomWKB)->data), (geomWKB)->len);
	GEOSSetSRID(geosGeometry, (geomWKB)->srid);
//fprintf(stderr, "wkb2geos: srid=%d\n", (geomWKB)->srid);
	return geosGeometry;
}

/* Function getMbrGeom
 * A wrapper for getMbrGeos on a geom_geometry.
 */
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

const char* geom_type2str(int t, int flag){
	if(flag == 0) {
		switch (t) {
		//case wkbGeometry:
		//	return "GEOMETRY";
		case wkbPoint:
			return "POINT";
		case wkbLineString:
			return "LINESTRING";
		case wkbPolygon:
			return "POLYGON";
		case wkbMultiPoint:
			return "MULTIPOINT";
		case wkbMultiLineString:
			return "MULTILINESTRING";
		case wkbMultiPolygon:
			return "MULTIPOLYGON";
		case wkbGeometryCollection:
			return "GEOMETRYCOLLECTION";
		} 
	}
	else if(flag == 1) {
		switch (t) {
		//case wkbGeometry:
		//	return "ST_Geometry";
		case wkbPoint:
			return "ST_Point";
		case wkbLineString:
			return "ST_Linestring";
		case wkbPolygon:
			return "ST_Polygon";
		case wkbMultiPoint:
			return "ST_MultiPoint";
		case wkbMultiLineString:
			return "ST_MultiLinestring";
		case wkbMultiPolygon:
			return "ST_MultiPolygon";
		case wkbGeometryCollection:
			return "ST_GeometryCollection";
		}
	}
	return "UKNOWN";
}


/*
str geomerty_2_geometry(wkb *res, wkb **geom, int* columnType, int* columnSRID, int* valueSRID) {

//char* geomStr;
//int len = 0;
//fprintf(stderr, "geometry_2_geometry\n");
//wkbTOSTR(&geomStr, &len, *geom);
if(*geom != NULL)
fprintf(stderr, "type:%d - wkbTOSTR cannot be seen at this point\n", *columnType);

if(res == NULL)
        fprintf(stderr, "-> ");

        fprintf(stderr, "%d vs %d\n", *columnSRID, *valueSRID);
                return "0";
}*/
