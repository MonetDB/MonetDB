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

geom_export wkb *wkbNULL(void);
geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);
geom_export mbr *mbrNULL(void);
geom_export int mbrFROMSTR(char *src, int *len, mbr **atom);
geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
geom_export BUN mbrHASH(mbr *atom);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export str mbrFromString(mbr **w, str *src);
geom_export str mbrFromMBR(mbr **w, mbr **src);
geom_export int wkbTOSTR(char **dst, int *len, wkb *atom);
geom_export int wkbFROMSTR(char *src, int *len, wkb **atom);
geom_export str wkbFromString(wkb **w, str *wkt);
geom_export str wkbFromWKB(wkb **w, wkb **src);
geom_export str wkbFromText(wkb **w, str *wkt, int *tpe);
geom_export BUN wkbHASH(wkb *w);
geom_export int wkbCOMP(wkb *l, wkb *r);
geom_export wkb *wkbNULL(void);
geom_export str wkbIsnil(bit *r, wkb **v);
geom_export str wkbAsText(str *r, wkb **w);
geom_export void wkbDEL(Heap *h, var_t *index);
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export int wkbWRITE(wkb *a, stream *s, size_t cnt);
geom_export int wkbLENGTH(wkb *p);
geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);
geom_export str wkbMBR(mbr **res, wkb **geom);
geom_export wkb *geos2wkb(GEOSGeom geosGeometry);
geom_export str wkbgetcoordX(double *out, wkb **geom);
geom_export str wkbgetcoordY(double *out, wkb **geom);
geom_export str wkbcreatepoint(wkb **out, dbl *x, dbl *y);
geom_export str wkbcreatepoint_bat(int *out, int *x, int *y);
geom_export str mbroverlaps(bit *out, mbr **b1, mbr **b2);
geom_export str wkbDimension(int *out, wkb **geom);
geom_export str wkbGeometryTypeId(int *out, wkb **geom);
geom_export str wkbSRID(int *out, wkb **geom);
geom_export str wkbIsEmpty(bit *out, wkb **geom);
geom_export str wkbIsSimple(bit *out, wkb **geom);
geom_export str wkbEnvelope(wkb **out, wkb **geom);
geom_export str wkbBoundary(wkb **out, wkb **geom);
geom_export str wkbConvexHull(wkb **out, wkb **geom);
geom_export str wkbEquals(bit *out, wkb **a, wkb **b);
geom_export str wkbDisjoint(bit *out, wkb **a, wkb **b);
geom_export str wkbIntersect(bit *out, wkb **a, wkb **b);
geom_export str wkbTouches(bit *out, wkb **a, wkb **b);
geom_export str wkbCrosses(bit *out, wkb **a, wkb **b);
geom_export str wkbWithin(bit *out, wkb **a, wkb **b);
geom_export str wkbContains(bit *out, wkb **a, wkb **b);
geom_export str wkbOverlaps(bit *out, wkb **a, wkb **b);
geom_export str wkbRelate(bit *out, wkb **a, wkb **b, str *pattern);
geom_export str wkbArea(dbl *out, wkb **a);
geom_export str wkbLength(dbl *out, wkb **a);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);

bat *
geom_prelude(void)
{
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), sizeof(oid), "mbr");
	return NULL;
}

void
geom_epilogue(void)
{
	libgeom_exit();
}

/*
 * Implementation of fixed-sized atom mbr.
 */
static int
mbr_isnil(mbr *m)
{
	if (!m || m->xmin == flt_nil || m->ymin == flt_nil ||
	    m->xmax == flt_nil || m->ymax == flt_nil)
		return 1;
	return 0;
}

/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */

mbr *
mbrNULL(void)
{
	static mbr mbrNIL;
	mbrNIL.xmin = flt_nil;
	mbrNIL.ymin = flt_nil;
	mbrNIL.xmax = flt_nil;
	mbrNIL.ymax = flt_nil;
	return (&mbrNIL);
}

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */

int
mbrFROMSTR(char *src, int *len, mbr **atom)
{
	int nil = 0;
	int nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL; /* The geometry object that is parsed from the src string. */

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && (geosMbr = GEOSGeomFromWKT(src)) == NULL)
		return 0;

	if (*len < (int) sizeof(mbr)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = sizeof(mbr));
	}
	if (nil) {
		nchars = 3;
		**atom = *mbrNULL();
	} else if (getMbrGeos(*atom, geosMbr)) {
		size_t l = strlen(src);
		assert(l <= GDK_int_max);
		nchars = (int) l;
	}
	if (geosMbr)
		GEOSGeom_destroy(geosMbr);
	return nchars;
}

#define MBR_WKTLEN 256

/* TOSTR: print atom in a string. */
/* return length of resulting string. */

int
mbrTOSTR(char **dst, int *len, mbr *atom)
{
	static char tempWkt[MBR_WKTLEN];
	size_t dstStrLen = 3;

	if (!mbr_isnil(atom)) {
		snprintf(tempWkt, MBR_WKTLEN, "BOX (%f %f, %f %f)",
			 atom->xmin, atom->ymin, atom->xmax, atom->ymax);
		dstStrLen = strlen(tempWkt) + 2;
		assert(dstStrLen < GDK_int_max);
	}

	if (*len < (int) dstStrLen + 1) {
		if (*dst)
			GDKfree(*dst);
		*dst = GDKmalloc(*len = (int) dstStrLen + 1);
	}

	if (dstStrLen > 3)
		snprintf(*dst, *len, "\"%s\"", tempWkt);
	else
		strcpy(*dst, "nil");
	return (int) dstStrLen;
}

/* HASH: compute a hash value. */
/* returns a positive integer hash value */

BUN
mbrHASH(mbr *atom)
{
	return (BUN) (((int) atom->xmin * (int)atom->ymin) *((int) atom->xmax * (int)atom->ymax));
}

/* COMP: compare two mbrs. */
/* returns int <0 if l<r, 0 if l==r, >0 else */

int
mbrCOMP(mbr *l, mbr *r)
{
	/* simple lexicographical ordering on (x,y) */
	int res;
	if (l->xmin == r->xmin)
		res = (l->ymin < r->ymin) ? -1 : (l->ymin == r->ymin) ? 0 : 1;
	else
		res = (l->xmin < r->xmin) ? -1 : 1;
	if (res == 0) {
		if (l->xmax == r->xmax)
			res = (l->ymax < r->ymax) ? -1 : (l->ymax == r->ymax) ? 0 : 1;
		else
			res = (l->xmax < r->xmax) ? -1 : 1;
	}
	return res;
}

mbr *
mbrREAD(mbr *a, stream *s, size_t cnt)
{
	mbr *c;
	size_t i;
	int v[4];
	flt vals[4];

	for (i = 0, c = a; i < cnt; i++, c++) {
		if (!mnstr_readIntArray(s, v, 4))
			return NULL;
		memcpy(vals, v, 4 * sizeof(int));
		c->xmin = vals[0];
		c->ymin = vals[1];
		c->xmax = vals[2];
		c->ymax = vals[3];
	}
	return a;
}

int
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

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}

str
mbrFromMBR(mbr **w, mbr **src)
{
	*w = (mbr *) GDKmalloc(sizeof(mbr));

	**w = **src;
	return MAL_SUCCEED;
}

/*
 * Implementation of variable-sized atom wkb.
 */

static var_t
wkb_size(size_t len)
{
	if (len == ~(size_t) 0)
		len = 0;
	assert(sizeof(wkb) - 1 + len <= VAR_MAX);
	return (var_t) (sizeof(wkb) - 1 + len);
}

/* TOSTR: print atom in a string. */
/* return length of resulting string. */

int
wkbTOSTR(char **dst, int *len, wkb *atom)
{
	char *wkt = NULL;
	int dstStrLen = 3;			/* "nil" */
	GEOSGeom geosGeometry = wkb2geos(atom);

	if (geosGeometry) {
		size_t l;
		wkt = GEOSGeomToWKT(geosGeometry);
		l = strlen(wkt);
		assert(l < GDK_int_max);
		dstStrLen = (int) l + 2;	/* add quotes */
		GEOSGeom_destroy(geosGeometry);
	}

	if (*len < dstStrLen + 1) {	/* + 1 for the '\0' */
		if (*dst)
			GDKfree(*dst);
		*dst = GDKmalloc(*len = dstStrLen + 1);
	}

	if (wkt) {
		snprintf(*dst, *len, "\"%s\"", wkt);
		GEOSFree(wkt);
	} else {
		strcpy(*dst, "nil");
	}

	return dstStrLen;
}

/* FROMSTR: parse string to @1. */
/* return number of parsed characters. */

int
wkbFROMSTR(char *src, int *len, wkb **atom)
{
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

str
wkbFromString(wkb **w, str *wkt)
{
	int len = 0;
	char *errbuf;
	str ex;

	if (wkbFROMSTR(*wkt, &len, w))
		return MAL_SUCCEED;
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "wkb.FromString", "%s", errbuf);

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}

str
wkbFromWKB(wkb **w, wkb **src)
{
	*w = (wkb *) GDKmalloc(wkb_size((*src)->len));

	if (wkb_isnil(*src)) {
		**w = *wkbNULL();
	} else {
		(*w)->len = (*src)->len;
		memcpy(&(*w)->data, &(*src)->data, (*src)->len);
	}
	return MAL_SUCCEED;
}

str
wkbFromText(wkb **w, str *wkt, int *tpe)
{
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
		throw(MAL, "wkb.FromText", "Geometry type '%s' not found", geom_type2str(*tpe));
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

wkb *
wkbNULL(void)
{
	static wkb nullval;

	nullval.len = ~(int) 0;
	return (&nullval);
}

str
wkbIsnil(bit *r, wkb **v)
{
	*r = wkb_isnil(*v);
	return MAL_SUCCEED;
}

str
wkbAsText(str *r, wkb **w)
{
	int len = 0;

	wkbTOSTR(r, &len, *w);
	if (len)
		return MAL_SUCCEED;
	throw(MAL, "geom.AsText", "Failed to create Text from Well Known Format");
}

void
wkbDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}

wkb *
wkbREAD(wkb *a, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_readInt(s, &len))
		return NULL;
	if ((a = GDKmalloc(wkb_size(len))) == NULL)
		return NULL;
	a->len = len;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

int
wkbWRITE(wkb *a, stream *s, size_t cnt)
{
	int len = a->len;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, len))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (len > 0 &&			/* 64bit: check for overflow */
	    mnstr_write(s, (char *) a->data, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
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

var_t
wkbPUT(Heap *h, var_t *bun, wkb *val)
{
	char *base;

	*bun = HEAP_malloc(h, wkb_size(val->len));
	base = h->base;
	if (*bun)
		memcpy(&base[*bun << GDK_VARSHIFT], (char *) val, wkb_size(val->len));
	return *bun;
}

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY)
{
	if ((*res = (mbr *) GDKmalloc(sizeof(mbr))) == NULL)
		throw(MAL, "geom.mbr", MAL_MALLOC_FAIL);
	if (*minX == flt_nil || *minY == flt_nil ||
	    *maxX == flt_nil || *maxY == flt_nil)
		**res = *mbrNULL();
	else {
		(*res)->xmin = *minX;
		(*res)->ymin = *minY;
		(*res)->xmax = *maxX;
		(*res)->ymax = *maxY;
	}
	return MAL_SUCCEED;
}

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
wkbMBR(mbr **res, wkb **geom)
{
	*res = (mbr *) GDKmalloc(sizeof(mbr));
	if (*res != NULL) {
		if (wkb_isnil(*geom)) {
			**res = *mbrNULL();
			return MAL_SUCCEED;
		} else if (getMbrGeom(*res, *geom))
			return MAL_SUCCEED;
	}
	throw(MAL, "geom.mbr", "Failed to create mbr");
}

wkb *
geos2wkb(GEOSGeom geosGeometry)
{
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *atom;

	if (geosGeometry != NULL)
		w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);

	atom = GDKmalloc(wkb_size(wkbLen));
	if (atom == NULL)
		return NULL;

	if (geosGeometry == NULL || w == NULL) {
		*atom = *wkbNULL();
	} else {
		assert(wkbLen <= GDK_int_max);
		atom->len = (int) wkbLen;
		memcpy(&atom->data, w, wkbLen);
		GEOSFree(w);
	}
	return atom;
}

static str
wkbgetcoordXY(double *out, wkb **geom,
	      int (*func)(const GEOSCoordSequence *, unsigned int, double *),
	      const char *name)
{
	int ret;
	GEOSGeom geosGeometry = wkb2geos(*geom);
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
	const GEOSCoordSequence *gcs;
#else
	const GEOSCoordSeq gcs;
#endif

	if (!geosGeometry) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	gcs = GEOSGeom_getCoordSeq(geosGeometry);

	if (!gcs) {
		throw(MAL, name, "GEOSGeom_getCoordSeq failed");
	}

	/* we could also check if geom is a LineString, LinearRing or
	 * Point */
	ret = (*func)(gcs, 0, out);

	/* gcs shouldn't be freed, it's internal to the GEOSGeom */
	GEOSGeom_destroy(geosGeometry);

	if (ret == 0)
		throw(MAL, name, "GEOSCoordSeq_get%s failed", name + 5);

	return MAL_SUCCEED;
}

str
wkbgetcoordX(double *out, wkb **geom)
{
	return wkbgetcoordXY(out, geom, GEOSCoordSeq_getX, "geom.X");
}

str
wkbgetcoordY(double *out, wkb **geom)
{
	return wkbgetcoordXY(out, geom, GEOSCoordSeq_getY, "geom.Y");
}

str
wkbcreatepoint(wkb **out, dbl *x, dbl *y)
{
	GEOSCoordSeq pnt;
	if (*x == dbl_nil || *y == dbl_nil) {
		if ((*out = GDKmalloc(sizeof(wkb))) != NULL)
			**out = *wkbNULL();
	} else {
		pnt = GEOSCoordSeq_create(1, 2);
		GEOSCoordSeq_setX(pnt, 0, *x);
		GEOSCoordSeq_setY(pnt, 0, *y);
		*out = geos2wkb(GEOSGeom_createPoint(pnt));
		GEOSCoordSeq_destroy(pnt);
	}
	if (*out == NULL)
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
wkbcreatepoint_bat(int *out, int *ix, int *iy)
{
	BAT *bo = NULL, *bx = NULL, *by = NULL;
	dbl *x = NULL, *y = NULL;
	BUN i, o;
	wkb *p = NULL;

	if ((bx = BATdescriptor(*ix)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((by = BATdescriptor(*iy)) == NULL) {
		BBPreleaseref(bx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if (!BAThdense(bx) ||
	    !BAThdense(by) ||
	    bx->hseqbase != by->hseqbase ||
	    BATcount(bx) != BATcount(by)) {
		BBPreleaseref(bx->batCacheid);
		BBPreleaseref(by->batCacheid);
		throw(MAL, "geom.point", "both arguments must have dense and aligned heads");
	}

	if ((bo = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(bx), TRANSIENT)) == NULL) {
		BBPreleaseref(bx->batCacheid);
		BBPreleaseref(by->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}
	BATseqbase(bo, bx->hseqbase);
	bo->hdense = TRUE;
	bo->hsorted = TRUE;
	bo->H->nonil = TRUE;
	BATkey(bo, TRUE);

	x = (dbl *) Tloc(bx, BUNfirst(bx));
	y = (dbl *) Tloc(by, BUNfirst(bx));
	for (i = 0, o = BUNlast(bo); i < BATcount(bx); i++, o++) {
		str err = NULL;
		if ((err = wkbcreatepoint(&p, &x[i], &y[i])) != MAL_SUCCEED) {
			BBPreleaseref(bx->batCacheid);
			BBPreleaseref(by->batCacheid);
			BBPreleaseref(bo->batCacheid);
			throw(MAL, "geom.point", "%s", err);
		}
		tfastins_nocheck(bo, o, p, Tsize(bo));
		GDKfree(p);
		p = NULL;
	}

	BATsetcount(bo, BATcount(bx));
	bo->batDirty = TRUE;
	bo->tdense = FALSE;
	bo->tsorted = BATcount(bo) <= 1;
	bo->trevsorted = BATcount(bo) <= 1;
	bo->hrevsorted = BATcount(bo) <= 1;
	bo->T->nonil = (bx->T->nonil && by->T->nonil);
	BATkey(BATmirror(bo), (bx->tkey && by->tkey));

	BBPreleaseref(bx->batCacheid);
	BBPreleaseref(by->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;

  bunins_failed:
	if (p)
		GDKfree(p);
	BBPreleaseref(bx->batCacheid);
	BBPreleaseref(by->batCacheid);
	BBPreleaseref(bo->batCacheid);
	throw(MAL, "geom.point", "bunins failed");
}

str
mbroverlaps(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = !((*b2)->ymax < (*b1)->ymin || (*b2)->ymin > (*b1)->ymax || (*b2)->xmax < (*b1)->xmin || (*b2)->xmin > (*b1)->xmax);
	return MAL_SUCCEED;
}

static str
wkbbasic(int *out, wkb **geom, int (*func)(const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	*out = (*func)(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, name, "GEOSGeom_getDimensions failed");
	return MAL_SUCCEED;

}

str
wkbDimension(int *out, wkb **geom)
{
	return wkbbasic(out, geom, GEOSGeom_getDimensions, "geom.Dimension");
}

str
wkbGeometryTypeId(int *out, wkb **geom)
{
	return wkbbasic(out, geom, GEOSGeomTypeId, "geom.GeometryTypeId");
}

str
wkbSRID(int *out, wkb **geom)
{
	return wkbbasic(out, geom, GEOSGetSRID, "geom.SRID");
}

str
wkbIsEmpty(bit *out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSisEmpty(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.IsEmpty", "GEOSisEmpty failed");
	return MAL_SUCCEED;

}

str
wkbIsSimple(bit *out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSisSimple(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.IsSimple", "GEOSisSimple failed");
	return MAL_SUCCEED;

}

str
wkbEnvelope(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSEnvelope(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.Envelope", "GEOSEnvelope failed");
	return MAL_SUCCEED;

}

str
wkbBoundary(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSBoundary(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.Boundary", "GEOSBoundary failed");
	return MAL_SUCCEED;

}

str
wkbConvexHull(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSConvexHull(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.ConvexHull", "GEOSConvexHull failed");
	return MAL_SUCCEED;

}

static str
wkbspatial(bit *out, wkb **a, wkb **b, char (*func)(const GEOSGeometry *, const GEOSGeometry *))
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = (*func)(ga, gb);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;
}

str
wkbEquals(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSEquals);
}

str
wkbDisjoint(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSDisjoint);
}

str
wkbIntersect(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSIntersects);
}

str
wkbTouches(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSTouches);
}

str
wkbCrosses(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSCrosses);
}

str
wkbWithin(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSWithin);
}

str
wkbContains(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSContains);
}

str
wkbOverlaps(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSOverlaps);
}

str
wkbRelate(bit *out, wkb **a, wkb **b, str *pattern)
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSRelatePattern(ga, gb, *pattern);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;
}

str
wkbArea(dbl *out, wkb **a)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);

	if (!ga) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSArea(ga, out) == 0)
		ret = "GEOSArea failed";

	GEOSGeom_destroy(ga);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Area", "%s", ret);
	return ret;
}

str
wkbLength(dbl *out, wkb **a)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);

	if (!ga) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSLength(ga, out) == 0)
		ret = "GEOSLength failed";

	GEOSGeom_destroy(ga);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Length", "%s", ret);
	return ret;
}

str
wkbDistance(dbl *out, wkb **a, wkb **b)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = dbl_nil;
		return ret;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = dbl_nil;
		return ret;
	}
	if (!ga && !gb) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSDistance(ga, gb, out) == 0)
		ret = "GEOSDistance failed";

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Distance", "%s", ret);
	return ret;
}

static str
wkbanalysis(wkb **out, wkb **a, wkb **b,
	    GEOSGeometry *(*func)(const GEOSGeometry *, const GEOSGeometry *))
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb((*func)(ga, gb));

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	if (*out != NULL)
		return MAL_SUCCEED;

	throw(MAL, "geom.@1", "@2 failed");
}

str
wkbIntersection(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSIntersection);
}

str
wkbUnion(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSUnion);
}

str
wkbDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSDifference);
}

str
wkbSymDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSSymDifference);
}

str
wkbBuffer(wkb **out, wkb **geom, dbl *distance)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = geos2wkb(NULL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSBuffer(geosGeometry, *distance, -1));

	GEOSGeom_destroy(geosGeometry);

	if (*out != NULL)
		return MAL_SUCCEED;

	throw(MAL, "geom.Buffer", "GEOSBuffer failed");
}
