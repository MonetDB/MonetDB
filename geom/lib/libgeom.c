/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * @f libgeom
 * @a Niels Nes
 *
 * @* The simple geom library
 */

#include "monetdb_config.h"
#include "libgeom.h"

static void __attribute__((__format__(__printf__, 1, 2)))
geomerror(_In_z_ _Printf_format_string_ const char *fmt, ...)
{
	va_list va;
	char err[256];
	va_start(va, fmt);
	vsnprintf(err, sizeof(err), fmt, va);
	GDKtracer_log(__FILE__, __func__, __LINE__, M_CRITICAL,
		      GDK, NULL, "%s", err);
	va_end(va);
}

void
libgeom_init(void)
{
	initGEOS((GEOSMessageHandler) geomerror, (GEOSMessageHandler) geomerror);
    // TODO: deprecated call REMOVE	
	GEOS_setWKBByteOrder(1);	/* NDR (little endian) */
	printf("# MonetDB/GIS module loaded\n");
	fflush(stdout);		/* make merovingian see this *now* */
}

void
libgeom_exit(void)
{
	finishGEOS();
}

bool
is_wkb_nil(const wkb *w)
{
	if (!w || w->len == ~0)
		return 1;
	return 0;
}

/* returns the size of variable-sized atom wkb */
var_t
wkb_size(size_t len)
{
	if (len == ~(size_t) 0)
		len = 0;
	assert(offsetof(wkb, data) + len <= VAR_MAX);
	return (var_t) (offsetof(wkb, data) + len);
}

wkb *
wkbNULLcopy(void)
{
	wkb *n = GDKmalloc(sizeof(wkb_nil));
	if (n)
		*n = wkb_nil;
	return n;
}

GEOSGeom
wkb2geos(const wkb *geomWKB)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(geomWKB))
		return NULL;

	geosGeometry = GEOSGeomFromWKB_buf((unsigned char *) geomWKB->data, geomWKB->len);

	if (geosGeometry != NULL)
		GEOSSetSRID(geosGeometry, geomWKB->srid);

	return geosGeometry;
}

/* create the WKB out of the GEOSGeometry
 * It makes sure to make all checks before returning
 * the input geosGeometry should not be altered by this function
 * return NULL on error */
wkb *
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

