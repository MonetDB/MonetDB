/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "geom_atoms.h"
#include "gdk.h"
#include "gdk_system.h"

/***********************************************/
/************* wkb type functions **************/
/***********************************************/

/* Creates the string representation (WKT) of a WKB */
/* return length of resulting string. */
ssize_t
wkbTOSTR(allocator *ma, char **geomWKT, size_t *len, const void *GEOMWKB, bool external)
{
	assert(ma);
	const wkb *geomWKB = GEOMWKB;
	char *wkt = NULL;
	size_t dstStrLen = 5;	/* "nil" */

	/* from WKB to GEOSGeometry */
	GEOSGeom geosGeometry = wkb2geos(geomWKB);

	if (geosGeometry) {
		size_t l;
		GEOSWKTWriter *WKT_wr = GEOSWKTWriter_create_r(geoshandle);
		//set the number of dimensions in the writer so that it can
		//read correctly the geometry coordinates
		GEOSWKTWriter_setOutputDimension_r(geoshandle, WKT_wr, GEOSGeom_getCoordinateDimension_r(geoshandle, geosGeometry));
		GEOSWKTWriter_setTrim_r(geoshandle, WKT_wr, 1);
		wkt = GEOSWKTWriter_write_r(geoshandle, WKT_wr, geosGeometry);
		if (wkt == NULL) {
			GDKerror("GEOSWKTWriter_write failed\n");
			return -1;
		}
		GEOSWKTWriter_destroy_r(geoshandle, WKT_wr);
		GEOSGeom_destroy_r(geoshandle, geosGeometry);

		l = strlen(wkt);
		dstStrLen = l;
		if (external)
			dstStrLen += 2;	/* add quotes */
		if (*len < dstStrLen + 1 || *geomWKT == NULL) {
			*len = dstStrLen + 1;
			////GDKfree(*geomWKT);
			assert(ma);
			if ((*geomWKT = ma_alloc(ma, *len)) == NULL) {
				GEOSFree_r(geoshandle, wkt);
				return -1;
			}
		}
		if (external)
			snprintf(*geomWKT, *len, "\"%s\"", wkt);
		else
			strcpy(*geomWKT, wkt);
		GEOSFree_r(geoshandle, wkt);

		return (ssize_t) dstStrLen;
	}

	/* geosGeometry == NULL */
	if (*len < 4 || *geomWKT == NULL) {
		////GDKfree(*geomWKT);
		if ((*geomWKT = ma_alloc(ma, *len = 4)) == NULL)
			return -1;
	}
	if (external) {
		strcpy(*geomWKT, "nil");
		return 3;
	}
	strcpy(*geomWKT, str_nil);
	return 1;
}

ssize_t
wkbFROMSTR(allocator *ma, const char *geomWKT, size_t *len, void **GEOMWKB, bool external)
{
	assert(ma);
	wkb **geomWKB = (wkb **) GEOMWKB;
	size_t parsedBytes;
	str err;

	if (external && strncmp(geomWKT, "nil", 3) == 0) {
		*geomWKB = wkbNULLcopy(ma);
		if (*geomWKB == NULL)
			return -1;
		return 3;
	}
	err = wkbFROMSTR_withSRID(ma, geomWKT, len, geomWKB, 0, &parsedBytes);
	if (err != MAL_SUCCEED) {
		GDKerror("%s", getExceptionMessageAndState(err));
		return -1;
	}
	return (ssize_t) parsedBytes;
}

BUN
wkbHASH(const void *W)
{
	const wkb *w = W;
	int i;
	BUN h = 0;

	for (i = 0; i < (w->len - 1); i += 2) {
		BUN a = ((unsigned char *) w->data)[i];
		BUN b = ((unsigned char *) w->data)[i + 1];
#if '\377' < 0					/* char is signed? */
		/* maybe sign extend */
		if (a & 0x80)
			a |= ~(BUN)0x7f;
		if (b & 0x80)
			b |= ~(BUN)0x7f;
#endif
		h = (h << 3) ^ (h >> 11) ^ (h >> 17) ^ (b << 8) ^ a;
	}
	return h;
}

/* returns a pointer to a null wkb */
const void *
wkbNULL(void)
{
	return &wkb_nil;
}

int
wkbCOMP(const void *L, const void *R)
{
	const wkb *l = L, *r = R;

	if (l->srid != r->srid)
		return l->srid - r->srid;

	int len = l->len;

	if (len != r->len)
		return len - r->len;

	if (len == ~(int) 0)
		return (0);

	return memcmp(l->data, r->data, len);
}

bool
wkbEQ(const void *L, const void *R)
{
	const wkb *l = L, *r = R;

	if (l->srid != r->srid)
		return false;

	int len = l->len;

	if (len != r->len)
		return false;

	if (len == ~(int) 0)
		return true;

	return memcmp(l->data, r->data, len) == 0;
}

/* read wkb from log */
void *
wkbREAD(allocator *ma, void *A, size_t *dstlen, stream *s, size_t cnt)
{
	wkb *a = A;
	int len;
	int srid;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if (mnstr_readInt(s, &srid) != 1)
		return NULL;
	size_t wkblen = (size_t) wkb_size(len);
	if (a == NULL || *dstlen < wkblen) {
		if (ma) {
			a = ma_realloc(ma, a, wkblen, *dstlen);
		} else {
			GDKfree(a);
			a = GDKmalloc(wkblen);
		}
		if (a == NULL)
			return NULL;
		*dstlen = wkblen;
	}
	a->len = len;
	a->srid = srid;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		//GDKfree(a);
		return NULL;
	}
	return a;
}

/* write wkb to log */
gdk_return
wkbWRITE(const void *A, stream *s, size_t cnt)
{
	const wkb *a = A;
	int len = a->len;
	int srid = a->srid;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, len))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (!mnstr_writeInt(s, srid))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (len > 0 &&		/* 64bit: check for overflow */
	    mnstr_write(s, (char *) a->data, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

var_t
wkbPUT(BAT *b, var_t *bun, const void *VAL)
{
	const wkb *val = VAL;
	char *base;

	*bun = HEAP_malloc(b, wkb_size(val->len));
	base = b->tvheap->base;
	if (*bun != (var_t) -1) {
		memcpy(&base[*bun], val, wkb_size(val->len));
		b->tvheap->dirty = true;
	}
	return *bun;
}

void
wkbDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}

size_t
wkbLENGTH(const void *P)
{
	const wkb *p = P;
	var_t len = wkb_size(p->len);
	assert(len <= GDK_int_max);
	return (size_t) len;
}

gdk_return
wkbHEAP(Heap *heap, size_t capacity)
{
	return HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

/* Non-atom WKB functions */
wkb *
wkbNULLcopy(allocator *ma)
{
	assert(ma);
	wkb *n = ma_alloc(ma, sizeof(wkb_nil));
	if (n)
		*n = wkb_nil;
	return n;
}

wkb *
wkbCopy(allocator *ma, const wkb* src)
{
	assert(ma);
	wkb *n = ma_alloc(ma, wkb_size(src->len));
	if (n) {
		n->len = src->len;
		n->srid = src->srid;
		memcpy(n->data, src->data, src->len);
	}
	return n;
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

/* Creates WKB representation (including srid) from WKT representation */
/* return number of parsed characters. */
str
wkbFROMSTR_withSRID(allocator *ma, const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread)
{
	assert(ma);
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	GEOSWKTReader *WKT_reader;
	static const char polyhedralSurface[] = "POLYHEDRALSURFACE";
	static const char multiPolygon[] = "MULTIPOLYGON";
	char *geomWKT_new = NULL;
	size_t parsedCharacters = 0;

	*nread = 0;

	if (strNil(geomWKT)) {
		if (*len < sizeof(wkb_nil)) {
			*len = sizeof(wkb_nil);
			*geomWKB = ma_alloc(ma, *len);
		}
		if (*geomWKB == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**geomWKB = wkb_nil;
		return MAL_SUCCEED;
	}
	//check whether the representation is binary (hex)
	if (geomWKT[0] == '0') {
		str ret = wkbFromBinaryWithBuffer(ma, geomWKB, len, &geomWKT);

		if (ret != MAL_SUCCEED)
			return ret;
		*nread = strlen(geomWKT);
		*len = (size_t) wkb_size((*geomWKB)->len);
		return MAL_SUCCEED;
	}
	//check whether the geometry type is polyhedral surface
	//geos cannot handle this type of geometry but since it is
	//a special type of multipolygon I just change the type before
	//continuing. Of course this means that isValid for example does
	//not work correctly.
	allocator *ta = MT_thread_getallocator();
	allocator_state state = ma_open(ta);
	if (strncasecmp(geomWKT, polyhedralSurface, strlen(polyhedralSurface)) == 0) {
		size_t sizeOfInfo = strlen(geomWKT) - strlen(polyhedralSurface) + strlen(multiPolygon) + 1;
		geomWKT_new = ma_alloc(ta, sizeOfInfo);
		if (geomWKT_new == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(geomWKT_new, sizeOfInfo, "%s%s", multiPolygon, geomWKT + strlen(polyhedralSurface));
		geomWKT = geomWKT_new;
	}
	////////////////////////// UP TO HERE ///////////////////////////

	WKT_reader = GEOSWKTReader_create_r(geoshandle);
	if (WKT_reader == NULL) {
		ma_close(ta, &state);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_create failed");
	}
	geosGeometry = GEOSWKTReader_read_r(geoshandle, WKT_reader, geomWKT);
	GEOSWKTReader_destroy_r(geoshandle, WKT_reader);

	if (geosGeometry == NULL) {
		ma_close(ta, &state);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_read failed");
	}

	if (GEOSGeomTypeId_r(geoshandle, geosGeometry) == -1) {
		GEOSGeom_destroy_r(geoshandle, geosGeometry);
		ma_close(ta, &state);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
	}

	GEOSSetSRID_r(geoshandle, geosGeometry, srid);
	/* the srid was lost with the transformation of the GEOSGeom to wkb
	 * so we decided to store it in the wkb */

	/* we have a GEOSGeometry with number of coordinates and SRID and we
	 * want to get the wkb out of it */
	*geomWKB = geos2wkb(ma, geomWKB, len, geosGeometry);
	GEOSGeom_destroy_r(geoshandle, geosGeometry);
	if (*geomWKB == NULL) {
		ma_close(ta, &state);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation geos2wkb failed");
	}

	*len = (size_t) wkb_size((*geomWKB)->len);
	parsedCharacters = strlen(geomWKT);
	assert(parsedCharacters <= GDK_int_max);

	ma_close(ta, &state);
	*nread = parsedCharacters;
	return MAL_SUCCEED;
}

/***********************************************/
/************* mbr type functions **************/
/***********************************************/

#define MBR_WKTLEN 256

/* TOSTR: print atom in a string. */
/* return length of resulting string. */
ssize_t
mbrTOSTR(allocator *ma, char **dst, size_t *len, const void *ATOM, bool external)
{
	assert(ma);
	const mbr *atom = ATOM;
	char tempWkt[MBR_WKTLEN];
	size_t dstStrLen;

	if (!is_mbr_nil(atom)) {
		dstStrLen = (size_t) snprintf(tempWkt, sizeof(tempWkt),
					      "BOX (%f %f, %f %f)",
					      atom->xmin, atom->ymin,
					      atom->xmax, atom->ymax);
	} else {
		tempWkt[0] = 0;	/* not used */
		dstStrLen = 0;
	}

	if (*len < dstStrLen + 4 || *dst == NULL) {
		////GDKfree(*dst);
		if ((*dst = ma_alloc(ma, *len = dstStrLen + 4)) == NULL)
			return -1;
	}

	if (dstStrLen > 4) {
		if (external) {
			snprintf(*dst, *len, "\"%s\"", tempWkt);
			dstStrLen += 2;
		} else {
			strcpy(*dst, tempWkt);
		}
	} else if (external) {
		strcpy(*dst, "nil");
		dstStrLen = 3;
	} else {
		strcpy(*dst, str_nil);
		dstStrLen = 1;
	}
	return (ssize_t) dstStrLen;
}

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */
ssize_t
mbrFROMSTR(allocator *ma, const char *src, size_t *len, void **ATOM, bool external)
{
	assert(ma);
	mbr **atom = (mbr **) ATOM;
	size_t nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL;	/* The geometry object that is parsed from the src string. */
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
	const char *c;

	if (*len < sizeof(mbr) || *atom == NULL) {
		// //GDKfree(*atom);
		if ((*atom = ma_alloc(ma, *len = sizeof(mbr))) == NULL)
			return -1;
	}
	if (external && strncmp(src, "nil", 3) == 0) {
		**atom = mbrNIL;
		return 3;
	}
	if (strNil(src)) {
		**atom = mbrNIL;
		return 1;
	}

	if ((strstr(src, "mbr") == src || strstr(src, "MBR") == src
	     || strstr(src, "box") == src || strstr(src, "BOX") == src)
	    && (c = strstr(src, "(")) != NULL) {
		/* Parse the mbr */
		if ((c - src) != 3 && (c - src) != 4) {
			GDKerror("ParseException: Expected a string like 'MBR(0 0,1 1)' or 'MBR (0 0,1 1)'\n");
			return -1;
		}

		if (sscanf(c, "(%lf %lf,%lf %lf)", &xmin, &ymin, &xmax, &ymax) != 4) {
			GDKerror("ParseException: Not enough coordinates.\n");
			return -1;
		}
	} else if ((geosMbr = GEOSGeomFromWKT(src)) == NULL) {
		GDKerror("GEOSGeomFromWKT failed\n");
		return -1;
	}

	if (geosMbr == NULL) {
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
		(*atom)->xmin = (float) xmin;
		(*atom)->ymin = (float) ymin;
		(*atom)->xmax = (float) xmax;
		(*atom)->ymax = (float) ymax;
		nchars = strlen(src);
	}
	if (geosMbr)
		GEOSGeom_destroy_r(geoshandle, geosMbr);
	assert(nchars <= GDK_int_max);
	return (ssize_t) nchars;
}

/* HASH: compute a hash value. */
/* returns a positive integer hash value */
BUN
mbrHASH(const void *ATOM)
{
	const mbr *atom = ATOM;
	return ATOMhash(TYPE_flt, &atom->xmin) ^ ATOMhash(TYPE_flt, &atom->ymin) ^
		ATOMhash(TYPE_flt, &atom->xmax) ^ ATOMhash(TYPE_flt, &atom->ymax);
}

const void *
mbrNULL(void)
{
	return &mbrNIL;
}

/* COMP: compare two mbrs. */
/* returns int <0 if l<r, 0 if l==r, >0 else */
int
mbrCOMP(const void *L, const void *R)
{
	/* simple lexicographical ordering on (x,y) */
	const mbr *l = L, *r = R;
	int res;
	if (is_mbr_nil(l))
		return -!is_mbr_nil(r);
	if (is_mbr_nil(r))
		return 1;
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

bool
mbrEQ(const void *L, const void *R)
{
	const mbr *l = L, *r = R;
	if (is_mbr_nil(l))
		return is_mbr_nil(r);
	if (is_mbr_nil(r))
		return false;
	return l->xmin == r->xmin && l->ymin == r->ymin && l->xmax == r->xmax && l->ymax == r->ymax;
}

/* read mbr from log */
void *
mbrREAD(allocator *ma, void *A, size_t *dstlen, stream *s, size_t cnt)
{
	mbr *a = A;
	mbr *c;
	size_t i;
	int v[4];
	flt vals[4];

	if (a == NULL || *dstlen < cnt * sizeof(mbr)) {
		if (ma) {
			a = ma_realloc(ma, a, cnt * sizeof(mbr), *dstlen);
		} else {
			GDKfree(a);
			a = GDKmalloc(cnt * sizeof(mbr));
		}
		if (a == NULL)
			return NULL;
		*dstlen = cnt * sizeof(mbr);
	}
	for (i = 0, c = a; i < cnt; i++, c++) {
		if (!mnstr_readIntArray(s, v, 4)) {
			if (a != A)
				//GDKfree(a);
			return NULL;
		}
		memcpy(vals, v, 4 * sizeof(int));
		c->xmin = vals[0];
		c->ymin = vals[1];
		c->xmax = vals[2];
		c->ymax = vals[3];
	}
	return a;
}

/* write mbr to log */
gdk_return
mbrWRITE(const void *C, stream *s, size_t cnt)
{
	const mbr *c = C;
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

/* Non-atom mbr functions */
/* Check if fixed-sized atom mbr is null */
bool
is_mbr_nil(const mbr *m)
{
	return (m == NULL || is_flt_nil(m->xmin) || is_flt_nil(m->ymin) || is_flt_nil(m->xmax) || is_flt_nil(m->ymax));
}

/* MBR FUNCTIONS */
/* MBR */

/* Creates the mbr for the given geom_geometry. */
str
wkbMBR(Client ctx, mbr **geomMBR, wkb **geomWKB)
{
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;
	bit empty;
	allocator *ma = ctx->ma;
	assert(ma);

	//check if the geometry is nil
	if (is_wkb_nil(*geomWKB)) {
		if ((*geomMBR = ma_alloc(ma, sizeof(mbr))) == NULL)
			throw(MAL, "geom.MBR", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**geomMBR = mbrNIL;
		return MAL_SUCCEED;
	}
	//check if the geometry is empty
	if ((ret = wkbIsEmpty(ctx, &empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}
	if (empty) {
		if ((*geomMBR = ma_alloc(ma, sizeof(mbr))) == NULL)
			throw(MAL, "geom.MBR", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**geomMBR = mbrNIL;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*geomMBR = NULL;
		throw(MAL, "geom.MBR", SQLSTATE(38000) "Geos problem converting GEOS to WKB");
	}

	*geomMBR = mbrFromGeos(geosGeometry);

	GEOSGeom_destroy_r(geoshandle, geosGeometry);

	if (*geomMBR == NULL || is_mbr_nil(*geomMBR)) {
		//GDKfree(*geomMBR);
		*geomMBR = NULL;
		throw(MAL, "wkb.mbr", SQLSTATE(38000) "Geos failed to create mbr");
	}

	return MAL_SUCCEED;
}

str
wkbBox2D(Client ctx, mbr **box, wkb **point1, wkb **point2)
{
	(void) ctx;
	GEOSGeom point1_geom, point2_geom;
	double xmin = 0.0, ymin = 0.0, xmax = 0.0, ymax = 0.0;
	str err = MAL_SUCCEED;
	allocator *ma = ctx->curprg->def->ma;
	assert(ma);

	//check null input
	if (is_wkb_nil(*point1) || is_wkb_nil(*point2)) {
		if ((*box = ma_alloc(ma, sizeof(mbr))) == NULL)
			throw(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**box = mbrNIL;
		return MAL_SUCCEED;
	}
	//check input not point geometries
	point1_geom = wkb2geos(*point1);
	point2_geom = wkb2geos(*point2);
	if (point1_geom == NULL || point2_geom == NULL) {
		if (point1_geom)
			GEOSGeom_destroy_r(geoshandle, point1_geom);
		if (point2_geom)
			GEOSGeom_destroy_r(geoshandle, point2_geom);
		throw(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (GEOSGeomTypeId_r(geoshandle, point1_geom) + 1 != wkbPoint_mdb ||
	    GEOSGeomTypeId_r(geoshandle, point2_geom) + 1 != wkbPoint_mdb) {
		err = createException(MAL, "geom.MakeBox2D", SQLSTATE(38000) "Geometries should be points");
	} else if (GEOSGeomGetX_r(geoshandle, point1_geom, &xmin) == -1 ||
		   GEOSGeomGetY_r(geoshandle, point1_geom, &ymin) == -1 ||
		   GEOSGeomGetX_r(geoshandle, point2_geom, &xmax) == -1 ||
		   GEOSGeomGetY_r(geoshandle, point2_geom, &ymax) == -1) {

		err = createException(MAL, "geom.MakeBox2D", SQLSTATE(38000) "Geos error in reading the points' coordinates");
	} else {
		//Assign the coordinates. Ensure that they are in correct order
		*box = ma_alloc(ma, sizeof(mbr));
		if (*box == NULL) {
			err = createException(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			(*box)->xmin = (float) (xmin < xmax ? xmin : xmax);
			(*box)->ymin = (float) (ymin < ymax ? ymin : ymax);
			(*box)->xmax = (float) (xmax > xmin ? xmax : xmin);
			(*box)->ymax = (float) (ymax > ymin ? ymax : ymin);
		}
	}
	GEOSGeom_destroy_r(geoshandle, point1_geom);
	GEOSGeom_destroy_r(geoshandle, point2_geom);

	return err;
}

static str
mbrrelation_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB, str (*func)(Client, bit *, mbr **, mbr **))
{
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(ctx, &geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(ctx, &geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		//GDKfree(geom1MBR);
		return ret;
	}

	ret = (*func) (ctx, out, &geom1MBR, &geom2MBR);

	//GDKfree(geom1MBR);
	//GDKfree(geom2MBR);

	return ret;
}

/*returns true if the two mbrs overlap */
str
mbrOverlaps(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else			//they cannot overlap if b2 is left, right, above or below b1
		*out = !((*b2)->ymax < (*b1)->ymin ||
			 (*b2)->ymin > (*b1)->ymax ||
			 (*b2)->xmax < (*b1)->xmin ||
			 (*b2)->xmin > (*b1)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of the two geometries overlap */
str
mbrOverlaps_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrOverlaps);
}

/* returns true if b1 is above b2 */
str
mbrAbove(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin > (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is above the mbr of geom2 */
str
mbrAbove_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrAbove);
}

/* returns true if b1 is below b2 */
str
mbrBelow(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax < (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is below the mbr of geom2 */
str
mbrBelow_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrBelow);
}

/* returns true if box1 is left of box2 */
str
mbrLeft(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax < (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the left of the mbr of geom2 */
str
mbrLeft_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrLeft);
}

/* returns true if box1 is right of box2 */
str
mbrRight(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin > (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the right of the mbr of geom2 */
str
mbrRight_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrRight);
}

/* returns true if box1 overlaps or is above box2 when only the Y coordinate is considered*/
str
mbrOverlapOrAbove(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin >= (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is above the mbr of geom2 */
str
mbrOverlapOrAbove_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrOverlapOrAbove);
}

/* returns true if box1 overlaps or is below box2 when only the Y coordinate is considered*/
str
mbrOverlapOrBelow(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax <= (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is below the mbr of geom2 */
str
mbrOverlapOrBelow_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrOverlapOrBelow);
}

/* returns true if box1 overlaps or is left of box2 when only the X coordinate is considered*/
str
mbrOverlapOrLeft(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax <= (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the left of the mbr of geom2 */
str
mbrOverlapOrLeft_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrOverlapOrLeft);
}

/* returns true if box1 overlaps or is right of box2 when only the X coordinate is considered*/
str
mbrOverlapOrRight(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin >= (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the right of the mbr of geom2 */
str
mbrOverlapOrRight_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrOverlapOrRight);
}

/* returns true if b1 is contained in b2 */
str
mbrContained(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = (((*b1)->xmin >= (*b2)->xmin) && ((*b1)->xmax <= (*b2)->xmax) && ((*b1)->ymin >= (*b2)->ymin) && ((*b1)->ymax <= (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is contained in the mbr of geom2 */
str
mbrContained_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrContained);
}

/*returns true if b1 contains b2 */
str
mbrContains(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	return mbrContained(ctx, out, b2, b1);
}

/*returns true if the mbrs of geom1 contains the mbr of geom2 */
str
mbrContains_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrContains);
}

/* returns true if the boxes are the same */
str
mbrEqual(Client ctx, bit *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	if (is_mbr_nil(*b1) && is_mbr_nil(*b2))
		*out = 1;
	else if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = 0;
	else
		*out = (((*b1)->xmin == (*b2)->xmin) && ((*b1)->xmax == (*b2)->xmax) && ((*b1)->ymin == (*b2)->ymin) && ((*b1)->ymax == (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 and the mbr of geom2 are the same */
str
mbrEqual_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	return mbrrelation_wkb(ctx, out, geom1WKB, geom2WKB, mbrEqual);
}

str
mbrDiagonal(dbl *out, mbr **b)
{
	double side_a = .0, side_b = .0;

	if (is_mbr_nil(*b)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	side_a = (*b)->xmax - (*b)->xmin;
	side_b = (*b)->ymax - (*b)->ymin;

	*out = sqrt(pow(side_a, 2.0) + pow(side_b, 2.0));

	return MAL_SUCCEED;
}

/* returns the Euclidean distance of the centroids of the boxes */
str
mbrDistance(Client ctx, dbl *out, mbr **b1, mbr **b2)
{
	(void) ctx;
	double b1_Cx = 0.0, b1_Cy = 0.0, b2_Cx = 0.0, b2_Cy = 0.0;

	if (is_mbr_nil(*b1) || is_mbr_nil(*b2)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}
	//compute the centroids of the two polygons
	b1_Cx = ((*b1)->xmin + (*b1)->xmax) / 2.0;
	b1_Cy = ((*b1)->ymin + (*b1)->ymax) / 2.0;
	b2_Cx = ((*b2)->xmin + (*b2)->xmax) / 2.0;
	b2_Cy = ((*b2)->ymin + (*b2)->ymax) / 2.0;

	//compute the euclidean distance
	*out = sqrt(pow(b2_Cx - b1_Cx, 2.0) + pow(b2_Cy - b1_Cy, 2.0));

	return MAL_SUCCEED;
}

/*returns the Euclidean distance of the centroids of the mbrs of the two geometries */
str
mbrDistance_wkb(Client ctx, dbl *out, wkb **geom1WKB, wkb **geom2WKB)
{
	(void) ctx;
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(ctx, &geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(ctx, &geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		//GDKfree(geom1MBR);
		return ret;
	}

	ret = mbrDistance(ctx, out, &geom1MBR, &geom2MBR);

	//GDKfree(geom1MBR);
	//GDKfree(geom2MBR);

	return ret;
}

/* get Xmin, Ymin, Xmax, Ymax coordinates of mbr */
str
wkbCoordinateFromMBR(Client ctx, dbl *coordinateValue, mbr **geomMBR, int *coordinateIdx)
{
	(void) ctx;
	//check if the MBR is null
	if (is_mbr_nil(*geomMBR) || is_int_nil(*coordinateIdx)) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	switch (*coordinateIdx) {
	case 1:
		*coordinateValue = (*geomMBR)->xmin;
		break;
	case 2:
		*coordinateValue = (*geomMBR)->ymin;
		break;
	case 3:
		*coordinateValue = (*geomMBR)->xmax;
		break;
	case 4:
		*coordinateValue = (*geomMBR)->ymax;
		break;
	default:
		throw(MAL, "geom.coordinateFromMBR", SQLSTATE(38000) "Geos unrecognized coordinateIdx: %d\n", *coordinateIdx);
	}

	return MAL_SUCCEED;
}

str
wkbCoordinateFromWKB(Client ctx, dbl *coordinateValue, wkb **geomWKB, int *coordinateIdx)
{
	(void) ctx;
	mbr *geomMBR;
	str ret = MAL_SUCCEED;
	bit empty;

	if (is_wkb_nil(*geomWKB) || is_int_nil(*coordinateIdx)) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	//check if the geometry is empty
	if ((ret = wkbIsEmpty(ctx, &empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}

	if (empty) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	if ((ret = wkbMBR(ctx, &geomMBR, geomWKB)) != MAL_SUCCEED)
		return ret;

	ret = wkbCoordinateFromMBR(ctx, coordinateValue, &geomMBR, coordinateIdx);

	//GDKfree(geomMBR);

	return ret;
}

str
mbrFromString(Client ctx, mbr **w, const char **src)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len = *w ? sizeof(mbr) : 0;
	char *errbuf;
	str ex;

	if (mbrFROMSTR(ma, *src, &len, (void **) w, false) >= 0)
		return MAL_SUCCEED;
	//GDKfree(*w);
	*w = NULL;
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "mbr.FromString", SQLSTATE(38000) "Geos %s", errbuf);

	GDKclrerr();

	return ex;
}

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
mbrIntersects(Client ctx, bit *out, mbr** mbr1, mbr** mbr2) {
	(void) ctx;
	if (((*mbr1)->ymax < (*mbr2)->ymin) || ((*mbr1)->ymin > (*mbr2)->ymax))
		(*out) = false;
	else if (((*mbr1)->xmax < (*mbr2)->xmin) || ((*mbr1)->xmin > (*mbr2)->xmax))
    	(*out) = false;
	else
		(*out) = true;
	return MAL_SUCCEED;
}
