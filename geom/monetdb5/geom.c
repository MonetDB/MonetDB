/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
geom_export str geom_prelude(void *ret);
geom_export str geom_epilogue(void *ret);
geom_export mbr *mbrNULL(void);
geom_export int mbrFROMSTR(const char *src, int *len, mbr **atom);
geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
geom_export BUN mbrHASH(mbr *atom);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export gdk_return mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export str mbrFromString(mbr **w, const str *src);
geom_export str mbrFromMBR(mbr **w, mbr **src);
geom_export int wkbTOSTR(char **dst, int *len, wkb *atom);
geom_export int wkbFROMSTR(const char *src, int *len, wkb **atom);
geom_export str wkbFromString(wkb **w, const str *wkt);
geom_export str wkbFromWKB(wkb **w, wkb **src);
geom_export str wkbFromText(wkb **w, const str *wkt, const int *tpe);
geom_export BUN wkbHASH(wkb *w);
geom_export int wkbCOMP(wkb *l, wkb *r);
geom_export wkb *wkbNULL(void);
geom_export str wkbAsText(str *r, wkb **w);
geom_export void wkbDEL(Heap *h, var_t *index);
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export gdk_return wkbWRITE(wkb *a, stream *s, size_t cnt);
geom_export int wkbLENGTH(wkb *p);
geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
geom_export str ordinatesMBR(mbr **res, const flt *minX, const flt *minY, const flt *maxX, const flt *maxY);
geom_export str wkbMBR(mbr **res, wkb **geom);
geom_export wkb *geos2wkb(GEOSGeom geosGeometry);
geom_export str wkbgetcoordX(dbl *out, wkb **geom);
geom_export str wkbgetcoordY(dbl *out, wkb **geom);
geom_export str wkbcreatepoint(wkb **out, const dbl *x, const dbl *y);
geom_export str wkbcreatepoint_bat(bat *out, const bat *x, const bat *y);
geom_export double isLeft( double P0x, double P0y, double P1x, double P1y, double P2x, double P2y);
geom_export str wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y);
geom_export str wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y);
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
geom_export str wkbRelate(bit *out, wkb **a, wkb **b, const str *pattern);
geom_export str wkbArea(dbl *out, wkb **a);
geom_export str wkbLength(dbl *out, wkb **a);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, const dbl *distance);

str
geom_prelude(void *ret)
{
	(void) ret;
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), sizeof(oid), "mbr");
	return MAL_SUCCEED;
}

str
geom_epilogue(void *ret)
{
	(void) ret;
	libgeom_exit();
	return MAL_SUCCEED;
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

static mbr mbrNIL = {GDK_flt_min, GDK_flt_min, GDK_flt_min, GDK_flt_min};

mbr *
mbrNULL(void)
{
	return &mbrNIL;
}

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */

int
mbrFROMSTR(const char *src, int *len, mbr **atom)
{
	int nil = 0;
	int nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL; /* The geometry object that is parsed from the src string. */
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
	char *c;

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && strstr(src,"BOX") ==  src && (c = strstr(src,"(")) != NULL) {
		/* Parse the mbr */
		if ((c - src) != 3 && (c - src) != 4) {
			GDKerror("ParseException: Expected a string like 'BOX(0 0,1 1)' or 'BOX (0 0,1 1)'\n");
			return 0;
		}

		if (sscanf(c,"(%lf %lf,%lf %lf)", &xmin, &ymin, &xmax, &ymax) != 4) {
			GDKerror("ParseException: Not enough coordinates.\n");
			return 0;
		}
	} else if (!nil && (geosMbr = GEOSGeomFromWKT(src)) == NULL)
		return 0;

	if (*len < (int) sizeof(mbr)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = sizeof(mbr));
	}
	if (nil) {
		nchars = 3;
		**atom = *mbrNULL();
	} else if (geosMbr == NULL) {
		size_t l;
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
		(*atom)->xmin = (float) xmin;
		(*atom)->ymin = (float) ymin;
		(*atom)->xmax = (float) xmax;
		(*atom)->ymax = (float) ymax;
		l = strlen(src);
		assert(l <= GDK_int_max);
		nchars = (int) l;
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
	char tempWkt[MBR_WKTLEN];
	size_t dstStrLen;

	if (!mbr_isnil(atom)) {
		snprintf(tempWkt, MBR_WKTLEN, "\"BOX (%f %f, %f %f)\"",
			 atom->xmin, atom->ymin, atom->xmax, atom->ymax);
		dstStrLen = strlen(tempWkt);
	} else {
		strcpy(tempWkt, "nil");
		dstStrLen = 3;
	}

	if (*len < (int) dstStrLen + 1) {
		if (*dst)
			GDKfree(*dst);
		*dst = GDKmalloc(*len = (int) dstStrLen + 1);
	}

	if (dstStrLen > 3)
		snprintf(*dst, *len, "%s", tempWkt);
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
		res = (l->ymin < r->ymin) ? -1 : (l->ymin != r->ymin);
	else
		res = (l->xmin < r->xmin) ? -1 : 1;
	if (res == 0) {
		if (l->xmax == r->xmax)
			res = (l->ymax < r->ymax) ? -1 : (l->ymax != r->ymax);
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

gdk_return
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
mbrFromString(mbr **w, const str *src)
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
	assert(offsetof(wkb, data) + len <= VAR_MAX);
	return (var_t) (offsetof(wkb, data) + len);
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
wkbFROMSTR(const char *src, int *len, wkb **atom)
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
wkbFromString(wkb **w, const str *wkt)
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
wkbFromText(wkb **w, const str *wkt, const int *tpe)
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

	if (len == ~0)
		return (0);

	return memcmp(l->data, r->data, len);
}

static wkb wkb_nil = {~0};

wkb *
wkbNULL(void)
{
	return &wkb_nil;
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
	if (mnstr_readInt(s, &len) != 1)
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

gdk_return
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
ordinatesMBR(mbr **res, const flt *minX, const flt *minY, const flt *maxX, const flt *maxY)
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
wkbgetcoordXY(dbl *out, wkb **geom,
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
wkbgetcoordX(dbl *out, wkb **geom)
{
	return wkbgetcoordXY(out, geom, GEOSCoordSeq_getX, "geom.X");
}

str
wkbgetcoordY(dbl *out, wkb **geom)
{
	return wkbgetcoordXY(out, geom, GEOSCoordSeq_getY, "geom.Y");
}

str
wkbcreatepoint(wkb **out, const dbl *x, const dbl *y)
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
wkbcreatepoint_bat(bat *out, const bat *ix, const bat *iy)
{
	BAT *bo = NULL, *bx = NULL, *by = NULL;
	dbl *x = NULL, *y = NULL;
	BUN i;
	wkb *p = NULL;

	if ((bx = BATdescriptor(*ix)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((by = BATdescriptor(*iy)) == NULL) {
		BBPunfix(bx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ( bx->htype != TYPE_void ||
		 by->htype != TYPE_void ||
	    bx->hseqbase != by->hseqbase ||
	    BATcount(bx) != BATcount(by)) {
		BBPunfix(bx->batCacheid);
		BBPunfix(by->batCacheid);
		throw(MAL, "geom.point", "both arguments must have dense and aligned heads");
	}

	if ((bo = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(bx), TRANSIENT)) == NULL) {
		BBPunfix(bx->batCacheid);
		BBPunfix(by->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}
	BATseqbase(bo, bx->hseqbase);

	x = (dbl *) Tloc(bx, BUNfirst(bx));
	y = (dbl *) Tloc(by, BUNfirst(bx));
	for (i = 0; i < BATcount(bx); i++) {
		str err = NULL;
		if ((err = wkbcreatepoint(&p, &x[i], &y[i])) != MAL_SUCCEED) {
			str msg;
			BBPunfix(bx->batCacheid);
			BBPunfix(by->batCacheid);
			BBPunfix(bo->batCacheid);
			msg = createException(MAL, "geom.point", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(bo,p,TRUE);
		GDKfree(p);
		p = NULL;
	}

	BATsetcount(bo, BATcount(bx));
    BATsettrivprop(bo);
    BATderiveProps(bo,FALSE);
	BBPunfix(bx->batCacheid);
	BBPunfix(by->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

	inline double
isLeft( double P0x, double P0y, double P1x, double P1y, double P2x, double P2y)
{
	return ( (P1x - P0x) * (P2y - P0y)
			- (P2x -  P0x) * (P1y - P0y) );
}

static str
pnpoly_(int *out, int nvert, dbl *vx, dbl *vy, int *point_x, int *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt;
	int j = 0, nv;
	bit *cs = NULL;

	/*Get the BATs*/
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment*/
	if ( bpx->htype != TYPE_void ||
			bpy->htype != TYPE_void ||
			bpx->hseqbase != bpy->hseqbase ||
			BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
	}

	/*Create output BAT*/
	if ((bo = BATnew(TYPE_void, ATOMindex("bit"), BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}
	BATseqbase(bo, bpx->hseqbase);

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs*/
	px = (dbl *) Tloc(bpx, BUNfirst(bpx));
	py = (dbl *) Tloc(bpy, BUNfirst(bpx));

	nv = nvert -1;
	cnt = BATcount(bpx);
	cs = (bit*) Tloc(bo,BUNfirst(bo));
	for (i = 0; i < cnt; i++) {
		int wn = 0;
		for (j = 0; j < nv; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j+1] > py[i])
					if (isLeft( vx[j], vy[j], vx[j+1], vy[j+1], px[i], py[i]) > 0)
						++wn;
			}
			else {
				if (vy[j+1]  <= py[i])
					if (isLeft( vx[j], vy[j], vx[j+1], vy[j+1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn & 1;
	}

	BATsetcount(bo,cnt);
	BATderiveProps(bo,FALSE);
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

static str
pnpolyWithHoles_(bat *out, int nvert, dbl *vx, dbl *vy, int nholes, dbl **hx, dbl **hy, int *hn, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt = 0;
	int j = 0, h = 0;
	bit *cs = NULL;

	/*Get the BATs*/
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment*/
	if ( bpx->htype != TYPE_void ||
			bpy->htype != TYPE_void ||
			bpx->hseqbase != bpy->hseqbase ||
			BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
	}

	/*Create output BAT*/
	if ((bo = BATnew(TYPE_void, ATOMindex("bit"), BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}
	BATseqbase(bo, bpx->hseqbase);

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs*/
	px = (dbl *) Tloc(bpx, BUNfirst(bpx));
	py = (dbl *) Tloc(bpy, BUNfirst(bpx));
	cnt = BATcount(bpx);
	cs = (bit*) Tloc(bo,BUNfirst(bo));
	for (i = 0; i < cnt; i++) {
		int wn = 0;

		/*First check the holes*/
		for (h = 0; h < nholes; h++) {
			int nv = hn[h]-1;
			wn = 0;
			for (j = 0; j < nv; j++) {
				if (hy[h][j] <= py[i]) {
					if (hy[h][j+1] > py[i])
						if (isLeft( hx[h][j], hy[h][j], hx[h][j+1], hy[h][j+1], px[i], py[i]) > 0)
							++wn;
				}
				else {
					if (hy[h][j+1]  <= py[i])
						if (isLeft( hx[h][j], hy[h][j], hx[h][j+1], hy[h][j+1], px[i], py[i]) < 0)
							--wn;
				}
			}

			/*It is in one of the holes*/
			if (wn) {
				break;
			}
		}

		if (wn)
			continue;

		/*If not in any of the holes, check inside the Polygon*/
		for (j = 0; j < nvert-1; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j+1] > py[i])
					if (isLeft( vx[j], vy[j], vx[j+1], vy[j+1], px[i], py[i]) > 0)
						++wn;
			}
			else {
				if (vy[j+1]  <= py[i])
					if (isLeft( vx[j], vy[j], vx[j+1], vy[j+1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn&1;
	}
	BATsetcount(bo,cnt);
	BATderiveProps(bo,FALSE);
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

#define POLY_NUM_VERT 120
#define POLY_NUM_HOLE 10

str
wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y) 
{
	double *vert_x, *vert_y, **holes_x = NULL, **holes_y= NULL;
	int *holes_n= NULL, j;
	wkb *geom = NULL;
	str msg = NULL;


	str err = NULL;
	str geom_str = NULL;
	char *str2, *token, *subtoken;
	char *saveptr1 = NULL, *saveptr2 = NULL;
	int nvert = 0, nholes = 0;

	geom = (wkb*) *a;

	if ((err = wkbAsText(&geom_str, &geom)) != MAL_SUCCEED) {
		msg = createException(MAL, "geom.Contain_point_bat", "%s", err);
		GDKfree(err);
		return msg;
	}
	geom_str = strchr(geom_str, '(');
	geom_str+=2;

	/*Lets get the polygon*/
	token = strtok_r(geom_str, ")", &saveptr1);
	vert_x = GDKmalloc(POLY_NUM_VERT*sizeof(double));
	vert_y = GDKmalloc(POLY_NUM_VERT*sizeof(double));

	for (str2 = token; ; str2 = NULL) {
		subtoken = strtok_r(str2, ",", &saveptr2);
		if (subtoken == NULL)
			break;
		sscanf(subtoken, "%lf %lf", &vert_x[nvert], &vert_y[nvert]);
		nvert++;
		if ((nvert%POLY_NUM_VERT) == 0) {
			vert_x = GDKrealloc(vert_x, nvert*2*sizeof(double));
			vert_y = GDKrealloc(vert_y, nvert*2*sizeof(double));
		}
	}

	token = strtok_r(NULL, ")", &saveptr1);
	if (token) {
		holes_x = GDKzalloc(POLY_NUM_HOLE*sizeof(double*));
		holes_y = GDKzalloc(POLY_NUM_HOLE*sizeof(double*));
		holes_n = GDKzalloc(POLY_NUM_HOLE*sizeof(double*));
	}
	/*Lets get all the holes*/
	while (token) {
		int nhole = 0;
		token = strchr(token, '(');
		if (!token)
			break;
		token++;

		if (!holes_x[nholes])
			holes_x[nholes] = GDKzalloc(POLY_NUM_VERT*sizeof(double));
		if (!holes_y[nholes])
			holes_y[nholes] = GDKzalloc(POLY_NUM_VERT*sizeof(double));

		for (str2 = token; ; str2 = NULL) {
			subtoken = strtok_r(str2, ",", &saveptr2);
			if (subtoken == NULL)
				break;
			sscanf(subtoken, "%lf %lf", &holes_x[nholes][nhole], &holes_y[nholes][nhole]);
			nhole++;
			if ((nhole%POLY_NUM_VERT) == 0) {
				holes_x[nholes] = GDKrealloc(holes_x[nholes], nhole*2*sizeof(double));
				holes_y[nholes] = GDKrealloc(holes_y[nholes], nhole*2*sizeof(double));
			}
		}

		holes_n[nholes] = nhole;
		nholes++;
		if ((nholes%POLY_NUM_HOLE) == 0) {
			holes_x = GDKrealloc(holes_x, nholes*2*sizeof(double*));
			holes_y = GDKrealloc(holes_y, nholes*2*sizeof(double*));
			holes_n = GDKrealloc(holes_n, nholes*2*sizeof(int));
		}
		token = strtok_r(NULL, ")", &saveptr1);
	}

	if (nholes)
		msg = pnpolyWithHoles_(out, (int) nvert, vert_x, vert_y, nholes, holes_x, holes_y, holes_n, point_x, point_y);
	else {
		msg = pnpoly_(out, (int) nvert, vert_x, vert_y, point_x, point_y);
	}

	GDKfree(vert_x);
	GDKfree(vert_y);
	if (holes_x && holes_y && holes_n) {
		for (j = 0; j < nholes; j ++) {
			GDKfree(holes_x[j]);
			GDKfree(holes_y[j]);
		}
		GDKfree(holes_x);
		GDKfree(holes_y);
		GDKfree(holes_n);
	}

	return msg;
}

str
wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y) 
{
	(void)a;
	(void)point_x;
	(void)point_y;
	*out = TRUE;
	return MAL_SUCCEED;
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
wkbRelate(bit *out, wkb **a, wkb **b, const str *pattern)
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);
	char res;

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

	res = GEOSRelatePattern(ga, gb, *pattern);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	if (res == 2)
		throw(MAL, "geom.Relate", GDK_EXCEPTION);
	*out = (bit) res;
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
wkbBuffer(wkb **out, wkb **geom, const dbl *distance)
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
