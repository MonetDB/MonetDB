/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * @f libgeom
 * @a Niels Nes
 *
 * @* The simple geom library
 */

#include "monetdb_config.h"
#include "libgeom.h"

GEOSContextHandle_t geoshandle;

static void __attribute__((__format__(__printf__, 1, 2)))
geomerror(_In_z_ _Printf_format_string_ const char *fmt, ...)
{
	va_list va;
	char err[256];
	va_start(va, fmt);
	vsnprintf(err, sizeof(err), fmt, va);
	GDKtracer_log(__FILE__, __func__, __LINE__, M_ERROR,
		      GDK, NULL, "%s", err);
	va_end(va);
}

void
libgeom_init(void)
{
	//geoshandle = initGEOS_r((GEOSMessageHandler) geomerror, (GEOSMessageHandler) geomerror);
	geoshandle = GEOS_init_r ();
    GEOSContext_setNoticeHandler_r(geoshandle, (GEOSMessageHandler) geomerror);
    GEOSContext_setErrorHandler_r(geoshandle, (GEOSMessageHandler) geomerror);
    // TODO: deprecated call REMOVE
	GEOS_setWKBByteOrder_r(geoshandle, 1);	/* NDR (little endian) */
	printf("# MonetDB/GIS module loaded\n");
	fflush(stdout);		/* make merovingian see this *now* */
}

void
libgeom_exit(void)
{
	GEOS_finish_r(geoshandle);
}

bool
is_wkb_nil(const wkb *w)
{
	if (!w || w->len == ~0)
		return 1;
	return 0;
}

GEOSGeom
wkb2geos(const wkb *geomWKB)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(geomWKB))
		return NULL;

	geosGeometry = GEOSGeomFromWKB_buf((unsigned char *) geomWKB->data, geomWKB->len);

	if (geosGeometry != NULL)
		GEOSSetSRID_r(geoshandle, geosGeometry, geomWKB->srid);

	return geosGeometry;
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

