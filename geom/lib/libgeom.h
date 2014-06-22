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
#include "proj_api.h" //it is needed to transform from one srid to another

typedef struct mbr {
	float xmin;
	float ymin;
	float xmax;
	float ymax;
	//mserver could not start with z coordinate
	//float zmin;
	//float zmax;
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
	//wkbGeometry = 0,
	wkbPoint = 1,
	wkbLineString = 2,
	wkbPolygon = 4,
	wkbMultiPoint = 5,
	wkbMultiLineString = 6,
	wkbMultiPolygon = 7,
	wkbGeometryCollection = 8
} wkb_type;

libgeom_export const char *geom_type2str(int t, int flag);

typedef struct wkb {
	int len;
	int srid;
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
//#define wkb2geos( geom ) wkb_isnil((geom))? NULL: GEOSGeomFromWKB_buf((unsigned char *)((geom)->data), (geom)->len)
#define wkb_nil geos2wkb(NULL);

libgeom_export int wkb_isnil(wkb *wkbp);
libgeom_export int getMbrGeos(mbr *mbr, const GEOSGeom geosGeometry);
libgeom_export int getMbrGeom(mbr *res, wkb *geom);
libgeom_export GEOSGeom wkb2geos(wkb* geomWKB);

//libgeom_export str geomerty_2_geometry(wkb *res, wkb **geom, int* columnType, int* columnSRID, int* valueSRID);

#endif /* LIBGEOM_H */
