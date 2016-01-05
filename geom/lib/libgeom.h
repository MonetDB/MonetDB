/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef LIBGEOM_H
#define LIBGEOM_H

#include <gdk.h>

/*
 * @+ Geos
 * The geom library is based on the geos (Geometry Engine Open Source) library.
 */

#ifdef WIN32
#ifndef LIBGEOM
#define libgeom_export extern __declspec(dllimport)
#else
#define libgeom_export extern __declspec(dllexport)
#endif
#else
#define libgeom_export extern
#endif

#include "geos_c.h"

typedef struct mbr {
	float xmin;
	float ymin;
	float xmax;
	float ymax;
} mbr;

/*
 * @+
 * Geometry objects have 4 attributes: type, bbox, SRID and data. This
 * implementation uses a varized atom for the data, which stores the WKB format
 * as defined by OpenGIS.
 */

/* 'WKB'

   http://edndoc.esri.com/arcsde/9.0/general_topics/wkb_representation.htm

==Geometry Type byte==


All serialized geometries start with a single byte
encoding geometry type (lower nibble) and flags
(higher nibble).

First Byte is for the order (little (1) or big endian)

Geometry Type Byte:

     [BSZM] [TTTT]

Flags values:

      B = 16 byte BOX2DFLOAT4 follows (probably not aligned) [before SRID]
      S = 4 byte SRID attached (0= not attached (-1), 1= attached)
      ZM = dimensionality (hasZ, hasM)

Type values:

0 = GEOMETRY1 = POINT
2 = CURVE
3 = LINESTRING
4 = SURFACE
5 = POLYGON
6 = COLLECTION
7 = MULTIPOINT
8 = MULTICURVE
9 = MULTILINESTRING
10 = MULTISURFACE
11 = MULTIPOLYGON

*/

typedef enum wkb_type {
	wkbPoint = 1,
	wkbLineString = 2,
	wkbPolygon = 3,
	wkbMultiPoint = 4,
	wkbMultiLineString = 5,
	wkbMultiPolygon = 6,
	wkbGeometryCollection = 7
} wkb_type;

libgeom_export const char *geom_type2str(int t);

typedef struct wkb {
	int len;
	char data[FLEXIBLE_ARRAY_MEMBER];
} wkb;

typedef struct {
	unsigned char type;
	mbr bbox;
	int SRID;
	wkb wkb;
} geom_geometry;

libgeom_export void libgeom_init(void);
libgeom_export void libgeom_exit(void);

/* Macro wkb2geos
 * Returns a GEOSGeom, created from a geom_geometry.
 * On failure, returns NULL.
 */
#define wkb2geos( geom ) \
	wkb_isnil((geom))? NULL: \
	GEOSGeomFromWKB_buf((unsigned char *)((geom)->data), (geom)->len)

libgeom_export int wkb_isnil(wkb *wkbp);
libgeom_export int getMbrGeos(mbr *mbr, const GEOSGeom geosGeometry);
libgeom_export int getMbrGeom(mbr *res, wkb *geom);

#endif /* LIBGEOM_H */
