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
 * @a Wouter Scherphof, Niels Nes
 * @* The simple geom module
 */

#include <monetdb_config.h>
#include <mal.h>
#include <mal_atom.h>
#include <mal_exception.h>
#include "libgeom.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#ifdef WIN32
#ifndef LIBGEOM
#define geom_export extern __declspec(dllimport)
#else
#define geom_export extern __declspec(dllexport)
#endif
#else
#define geom_export extern
#endif

int TYPE_mbr;

geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);

geom_export wkb *wkbNULL(void);
geom_export mbr *mbrNULL(void);

geom_export int wkbFROMSTR(char *src, int *len, wkb **atom);
geom_export str wkbFromText(wkb **w, str *wkt, int *tpe);




//geom_export int mbrFROMSTR(char *src, int *len, mbr **atom);
//geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
//geom_export BUN mbrHASH(mbr *atom);
//geom_export int mbrCOMP(mbr *l, mbr *r);
//geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
//geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
//geom_export str mbrFromString(mbr **w, str *src);
//geom_export str mbrFromMBR(mbr **w, mbr **src);
//geom_export int wkbTOSTR(char **dst, int *len, wkb *atom);
//geom_export str wkbFromString(wkb **w, str *wkt);
//geom_export str wkbFromWKB(wkb **w, wkb **src);
///geom_export BUN wkbHASH(wkb *w);
//geom_export int wkbCOMP(wkb *l, wkb *r);
//geom_export str wkbIsnil(bit *r, wkb **v);
//geom_export str wkbAsText(str *r, wkb **w);
//geom_export void wkbDEL(Heap *h, var_t *index);
//geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
//geom_export int wkbWRITE(wkb *a, stream *s, size_t cnt);
//geom_export int wkbLENGTH(wkb *p);
//geom_export void wkbHEAP(Heap *heap, size_t capacity);
//geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
//geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);
//geom_export str wkbMBR(mbr **res, wkb **geom);
//geom_export wkb *geos2wkb(GEOSGeom geosGeometry);
//geom_export str wkbgetcoordX(double *out, wkb **geom);
//geom_export str wkbgetcoordY(double *out, wkb **geom);
//geom_export str wkbcreatepoint(wkb **out, dbl *x, dbl *y);
//geom_export str wkbcreatepoint_bat(int *out, int *x, int *y);
//geom_export str mbroverlaps(bit *out, mbr **b1, mbr **b2);
//geom_export str wkbDimension(int *out, wkb **geom);
//geom_export str wkbGeometryTypeId(int *out, wkb **geom);
//geom_export str wkbSRID(int *out, wkb **geom);
//geom_export str wkbIsEmpty(bit *out, wkb **geom);
//geom_export str wkbIsSimple(bit *out, wkb **geom);
//geom_export str wkbEnvelope(wkb **out, wkb **geom);
//geom_export str wkbBoundary(wkb **out, wkb **geom);
//geom_export str wkbConvexHull(wkb **out, wkb **geom);
//geom_export str wkbEquals(bit *out, wkb **a, wkb **b);
//geom_export str wkbDisjoint(bit *out, wkb **a, wkb **b);
//geom_export str wkbIntersect(bit *out, wkb **a, wkb **b);
//geom_export str wkbTouches(bit *out, wkb **a, wkb **b);
//geom_export str wkbCrosses(bit *out, wkb **a, wkb **b);
//geom_export str wkbWithin(bit *out, wkb **a, wkb **b);
//geom_export str wkbContains(bit *out, wkb **a, wkb **b);
//geom_export str wkbOverlaps(bit *out, wkb **a, wkb **b);
//geom_export str wkbRelate(bit *out, wkb **a, wkb **b, str *pattern);
//geom_export str wkbArea(dbl *out, wkb **a);
//geom_export str wkbLength(dbl *out, wkb **a);
//geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
//geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
//geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
//geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
//geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
//geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);
//geom_export str wkbCentroid(wkb **out, wkb **geom);
//geom_export str wkbStartPoint(wkb **out, wkb **geom);
//geom_export str wkbEndPoint(wkb **out, wkb **geom);
//geom_export str wkbNumPoints(int *out, wkb **geom);
//geom_export str wkbPointN(wkb **out, wkb **geom, short *n);


/* initialise the geos library */
bat* geom_prelude(void) {
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), sizeof(oid), "mbr");
	return NULL;
}

/* close the geos library */
void geom_epilogue(void) {
	libgeom_exit();
}

/*
 * Implementation of fixed-sized atom mbr.
 */
/* check if given mbr is null 
 * it is null if any of its coordinates is null */
//static int mbr_isnil(mbr *m) {
//	if (!m || m->xmin == flt_nil || m->ymin == flt_nil ||
//	    m->xmax == flt_nil || m->ymax == flt_nil)
//		return 1;
//	return 0;
//}

/*
 * Implementation of variable-sized atom wkb.
 */
static var_t wkb_size(size_t len)
{
	if (len == ~(size_t) 0)
		len = 0;
	assert(sizeof(wkb) - 1 + len <= VAR_MAX);
	return (var_t) (sizeof(wkb) - 1 + len);
}

/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */
mbr* mbrNULL(void) {
	static mbr mbrNIL;
	mbrNIL.xmin = flt_nil;
	mbrNIL.ymin = flt_nil;
	mbrNIL.xmax = flt_nil;
	mbrNIL.ymax = flt_nil;
	return (&mbrNIL);
}

/* returns pointer to a null wkb */
wkb* wkbNULL(void) {
	static wkb nullval;

	nullval.len = ~(int) 0;
	return (&nullval);
}


/* FROMSTR: parse string to @1. */
/* return number of parsed characters. */
int wkbFROMSTR(char *src, int *len, wkb **atom) {
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	unsigned char *wkbSer = NULL;	/* The "well known binary" serialization of the geometry object. */
	size_t wkbLen = 0;		/* The length of the wkbSer string. */
	int nil = 0;

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && (geosGeometry = GEOSGeomFromWKT(src)) == NULL) {
		goto return_nil;
	}

	if (!nil && GEOSGeomTypeId(geosGeometry) == -1) {
		GEOSGeom_destroy(geosGeometry);
		goto return_nil;
	}
	if (!nil) {
		wkbSer = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);
		GEOSGeom_destroy(geosGeometry);
	}
	if (*atom == NULL || *len < (int) wkb_size(wkbLen)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = (int) wkb_size(wkbLen));
	}
	if (!wkbSer) {
		**atom = *wkbNULL();
	} else {
		assert(wkbLen <= GDK_int_max);
		(*atom)->len = (int) wkbLen;
		memcpy(&(*atom)->data, wkbSer, wkbLen);
		GEOSFree(wkbSer);
	}
	wkbLen = strlen(src);
	assert(wkbLen <= GDK_int_max);
	return (int) wkbLen;
  return_nil:
	if ((size_t) *len < sizeof(wkb)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = (int) sizeof(wkb));
	}
	**atom = *wkbNULL();
	return 0;
}


str wkbFromText(wkb **w, str *wkt, int *tpe) {
	int len = 0, te = *tpe;
	char *errbuf;
	str ex;
	
	*w = NULL;
	if (wkbFROMSTR(*wkt, &len, w) &&
	    (wkb_isnil(*w) || *tpe == wkbGeometryCollection ||
	     (te = *((*w)->data + 1) & 0x0f) == *tpe))
		return MAL_SUCCEED;
	if (*w == NULL)
		*w = (wkb *) GDKmalloc(sizeof(wkb));
	**w = *wkbNULL();
	if (te != *tpe)
		throw(MAL, "wkb.FromText", "Attempt to read Geometry type '%s' with function for Geometry type '%s'", geom_type2str(*tpe), geom_type2str(te));
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "wkb.FromText", "%s", errbuf);

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}

//Geometry Constructors
//function BdPolyFromText(wkt:str, srid:sht) :wkb; 
//function BdMPolyFromText(wkt:str, srid:sht) :wkb;
//function Box2dFromGeoHash(???) RETURNS mbr external name geom."Box2dFromGeoHash";
//function GeographyFromText(wkt:str) :Geography;
//function GeogFromWKB(wkb_arr byte[]) :Geography;  
//function GeomCollFromText(wkt:str, srid:sht) :wkb;
//function GeomCollFromText(wkt:str) :wkb;
//function GeomFromEWKB
//function GeomFromEWKT
//function GeometryFromText(wkt:str, srid:sht) :wkb;
//function GeometryFromText(wkt:str) :wkb;
//function GeomFromGML
//function GeomFromGeoJSON
//function GeomFromKML
//function GMLToSQL
//function GeomFromText(wkt:str, srid:sht) :wkb;
//function GeomFromText(wkt:str) :wkb;
//function GeomFromWKB(wkb_arr byte[], srid:sht) :wkb;
//function GeomFromWKB(wkb_arr byte[]) :wkb;
//function LineFromMultiPoint(pointGeom:wkb) :wkb;
//function LineFromText(wkt:str, srid:sht) :wkb;
//function LineFromText(wkt:str) :wkb;
//function LineFromWKB(wkb_arr byte[], srid:sht) :wkb;
//function LineFromWKB(wkb_arr byte[]) :wkb;
//function LinestringFromWKB(wkb_arr byte[], srid:sht) :wkb";
//function LinestringFromWKB(wkb_arr byte[]) :wkb;
//function MakeBox2D(lowLeftPointGeom:wkb, upRightPointGeom:wkb) :mbr;
//function 3DMakeBox
//function MakeLine(geometry set geoms)?????
//function MakeLine(geom1:wkb, geom2:wkb) :wkb;
//function MakeLine(geoms_arr:wkb[]) :wkb;
//function MakeEnvelope(xmin:dbl, ymin:dbl, xmax:dbl, ymax:dbl, srid:sht) :wkb;
//function MakePolygon(linestringGeom:wkb) :wkb;
//function MakePolygon(outerLinestringGeom:wkb, interiorLinestringGeoms:wkb[]) :wkb;
//function MakePoint(x:dbl, y:dbl) :wkb; 
//function MakePoint(x:dbl, y:dbl, z:dbl) :wkb;
//function MakePoint(x:dbl, y:dbl, z:dbl, m:dbl) :wkb;
//function MakePointM(x:dbl, y:dbl, m:dbl) :wkb;
//function MLineFromText(wkt:str, srid:sht) :wkb;
//function MLineFromText(wkt:str) :wkb;
//function MPointFromText(wkt:str, srid:sht) :wkb;
//function MPointFromText(wkt:str) :wkb;
//function MPolyFromText(wkt:str, srid:sht) :wkb;
//function MPolyFromText(wkt:str) :wkb;
//function Point(x:dbl, y:dbl) :wkb;
//function PointFromGeoHash
//function PointFromText(wkt:str, srid:sht) :wkb; 
//function PointFromText(wkt:str) :wkb;
//function PointFromWKB(wkb_arr byte[], srid:sht) :wkb;
//function PointFromWKB(wkb_arr byte[]) :wkb;
//function Polygon(linestringGeom:wkb, srid:sht) :wkb;
//function PolygonFromText(wkt:str, srid:sht) :wkb;

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */



