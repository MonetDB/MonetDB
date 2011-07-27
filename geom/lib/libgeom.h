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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	char data[1];
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
