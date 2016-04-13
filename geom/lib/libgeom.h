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

#include <geos_c.h>

#ifdef HAVE_PROJ
#include "proj_api.h" //it is needed to transform from one srid to another
#endif

/* geos does not support 3d envelope */
typedef struct mbr {
	float xmin;
	float ymin;
//	float zmin;
//	float mmin;
	
	float xmax;
	float ymax;
//	float zmax;
//	float mmax;
	
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

0 = GEOMETRY
1 = POINT
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
	//wkbGeometry_mbd = 0,
	wkbPoint_mdb = 1,
	wkbLineString_mdb = 2,
	wkbLinearRing_mdb = 3,
	wkbPolygon_mdb = 4,
	wkbMultiPoint_mdb = 5,
	wkbMultiLineString_mdb = 6,
	wkbMultiPolygon_mdb = 7,
	wkbGeometryCollection_mdb = 8
} wkb_type;

libgeom_export const char *geom_type2str(int t, int flag);

typedef struct wkb {
	int len;
	int srid;
	char data[FLEXIBLE_ARRAY_MEMBER];
} wkb;

typedef struct wkba {
	int itemsNum; //the number of wkbs
	wkb* data[FLEXIBLE_ARRAY_MEMBER]; //the wkbs
} wkba;

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
//#define wkb2geos( geom ) wkb_isnil((geom))? NULL: GEOSGeomFromWKB_buf((unsigned char *)((geom)->data), (geom)->len)
#define mbr_nil mbrFromGeos(NULL); 

libgeom_export int wkb_isnil(wkb *wkbp);
libgeom_export int getMbrGeos(mbr *mbr, const GEOSGeom geosGeometry);
libgeom_export int getMbrGeom(mbr *res, wkb *geom);
libgeom_export GEOSGeom wkb2geos(wkb* geomWKB);

//libgeom_export str geomerty_2_geometry(wkb *res, wkb **geom, int* columnType, int* columnSRID, int* valueSRID);

#endif /* LIBGEOM_H */
