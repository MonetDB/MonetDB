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

#include "geom_atoms.h"

/***********************************************/
/************* wkb type functions **************/
/***********************************************/

/* Creates the string representation (WKT) of a WKB */
/* return length of resulting string. */
ssize_t
wkbTOSTR(char **geomWKT, size_t *len, const void *GEOMWKB, bool external)
{
	const wkb *geomWKB = GEOMWKB;
	char *wkt = NULL;
	size_t dstStrLen = 5;	/* "nil" */

	/* from WKB to GEOSGeometry */
	GEOSGeom geosGeometry = wkb2geos(geomWKB);

	if (geosGeometry) {
		size_t l;
		GEOSWKTWriter *WKT_wr = GEOSWKTWriter_create();
		//set the number of dimensions in the writer so that it can
		//read correctly the geometry coordinates
		GEOSWKTWriter_setOutputDimension(WKT_wr, GEOSGeom_getCoordinateDimension(geosGeometry));
		GEOSWKTWriter_setTrim(WKT_wr, 1);
		wkt = GEOSWKTWriter_write(WKT_wr, geosGeometry);
		if (wkt == NULL) {
			GDKerror("GEOSWKTWriter_write failed\n");
			return -1;
		}
		GEOSWKTWriter_destroy(WKT_wr);
		GEOSGeom_destroy(geosGeometry);

		l = strlen(wkt);
		dstStrLen = l;
		if (external)
			dstStrLen += 2;	/* add quotes */
		if (*len < dstStrLen + 1 || *geomWKT == NULL) {
			*len = dstStrLen + 1;
			GDKfree(*geomWKT);
			if ((*geomWKT = GDKmalloc(*len)) == NULL) {
				GEOSFree(wkt);
				return -1;
			}
		}
		if (external)
			snprintf(*geomWKT, *len, "\"%s\"", wkt);
		else
			strcpy(*geomWKT, wkt);
		GEOSFree(wkt);

		return (ssize_t) dstStrLen;
	}

	/* geosGeometry == NULL */
	if (*len < 4 || *geomWKT == NULL) {
		GDKfree(*geomWKT);
		if ((*geomWKT = GDKmalloc(*len = 4)) == NULL)
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
wkbFROMSTR(const char *geomWKT, size_t *len, void **GEOMWKB, bool external)
{
	wkb **geomWKB = (wkb **) GEOMWKB;
	size_t parsedBytes;
	str err;

	if (external && strncmp(geomWKT, "nil", 3) == 0) {
		*geomWKB = wkbNULLcopy();
		if (*geomWKB == NULL)
			return -1;
		return 3;
	}
	err = wkbFROMSTR_withSRID(geomWKT, len, geomWKB, 0, &parsedBytes);
	if (err != MAL_SUCCEED) {
		GDKerror("%s", getExceptionMessageAndState(err));
		freeException(err);
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
		return -1;

	int len = l->len;

	if (len != r->len)
		return len - r->len;

	if (len == ~(int) 0)
		return (0);

	return memcmp(l->data, r->data, len);
}

/* read wkb from log */
void *
wkbREAD(void *A, size_t *dstlen, stream *s, size_t cnt)
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
		if ((a = GDKrealloc(a, wkblen)) == NULL)
			return NULL;
		*dstlen = wkblen;
	}
	a->len = len;
	a->srid = srid;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		GDKfree(a);
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
wkbNULLcopy(void)
{
	wkb *n = GDKmalloc(sizeof(wkb_nil));
	if (n)
		*n = wkb_nil;
	return n;
}

wkb *
wkbCopy(const wkb* src)
{
	wkb *n = GDKmalloc(wkb_size(src->len));
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
wkbFROMSTR_withSRID(const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread)
{
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	GEOSWKTReader *WKT_reader;
	const char *polyhedralSurface = "POLYHEDRALSURFACE";
	const char *multiPolygon = "MULTIPOLYGON";
	char *geomWKT_new = NULL;
	size_t parsedCharacters = 0;

	*nread = 0;

	/* we always allocate new memory */
	GDKfree(*geomWKB);
	*len = 0;
	*geomWKB = NULL;

	if (strNil(geomWKT)) {
		*geomWKB = wkbNULLcopy();
		if (*geomWKB == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*len = sizeof(wkb_nil);
		return MAL_SUCCEED;
	}
	//check whether the representation is binary (hex)
	if (geomWKT[0] == '0') {
		str ret = wkbFromBinary(geomWKB, &geomWKT);

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
	if (strncasecmp(geomWKT, polyhedralSurface, strlen(polyhedralSurface)) == 0) {
		size_t sizeOfInfo = strlen(geomWKT) - strlen(polyhedralSurface) + strlen(multiPolygon) + 1;
		geomWKT_new = GDKmalloc(sizeOfInfo);
		if (geomWKT_new == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(geomWKT_new, sizeOfInfo, "%s%s", multiPolygon, geomWKT + strlen(polyhedralSurface));
		geomWKT = geomWKT_new;
	}
	////////////////////////// UP TO HERE ///////////////////////////

	WKT_reader = GEOSWKTReader_create();
	if (WKT_reader == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_create failed");
	}
	geosGeometry = GEOSWKTReader_read(WKT_reader, geomWKT);
	GEOSWKTReader_destroy(WKT_reader);

	if (geosGeometry == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSWKTReader_read failed");
	}

	if (GEOSGeomTypeId(geosGeometry) == -1) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
	}

	GEOSSetSRID(geosGeometry, srid);
	/* the srid was lost with the transformation of the GEOSGeom to wkb
	 * so we decided to store it in the wkb */

	/* we have a GEOSGeometry with number of coordinates and SRID and we
	 * want to get the wkb out of it */
	*geomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	if (*geomWKB == NULL) {
		if (geomWKT_new)
			GDKfree(geomWKT_new);
		throw(MAL, "wkb.FromText", SQLSTATE(38000) "Geos operation geos2wkb failed");
	}

	*len = (size_t) wkb_size((*geomWKB)->len);
	parsedCharacters = strlen(geomWKT);
	assert(parsedCharacters <= GDK_int_max);

	GDKfree(geomWKT_new);

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
mbrTOSTR(char **dst, size_t *len, const void *ATOM, bool external)
{
	const mbr *atom = ATOM;
	char tempWkt[MBR_WKTLEN];
	size_t dstStrLen;

	if (!is_mbr_nil(atom)) {
		dstStrLen = (size_t) snprintf(tempWkt, MBR_WKTLEN,
					      "BOX (%f %f, %f %f)",
					      atom->xmin, atom->ymin,
					      atom->xmax, atom->ymax);
	} else {
		tempWkt[0] = 0;	/* not used */
		dstStrLen = 0;
	}

	if (*len < dstStrLen + 4 || *dst == NULL) {
		GDKfree(*dst);
		if ((*dst = GDKmalloc(*len = dstStrLen + 4)) == NULL)
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
mbrFROMSTR(const char *src, size_t *len, void **ATOM, bool external)
{
	mbr **atom = (mbr **) ATOM;
	size_t nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL;	/* The geometry object that is parsed from the src string. */
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
	const char *c;

	if (*len < sizeof(mbr) || *atom == NULL) {
		GDKfree(*atom);
		if ((*atom = GDKmalloc(*len = sizeof(mbr))) == NULL)
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
		GEOSGeom_destroy(geosMbr);
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

/* read mbr from log */
void *
mbrREAD(void *A, size_t *dstlen, stream *s, size_t cnt)
{
	mbr *a = A;
	mbr *c;
	size_t i;
	int v[4];
	flt vals[4];

	if (a == NULL || *dstlen < cnt * sizeof(mbr)) {
		if ((a = GDKrealloc(a, cnt * sizeof(mbr))) == NULL)
			return NULL;
		*dstlen = cnt * sizeof(mbr);
	}
	for (i = 0, c = a; i < cnt; i++, c++) {
		if (!mnstr_readIntArray(s, v, 4)) {
			if (a != A)
				GDKfree(a);
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
wkbMBR(mbr **geomMBR, wkb **geomWKB)
{
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;
	bit empty;

	//check if the geometry is nil
	if (is_wkb_nil(*geomWKB)) {
		if ((*geomMBR = GDKmalloc(sizeof(mbr))) == NULL)
			throw(MAL, "geom.MBR", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**geomMBR = mbrNIL;
		return MAL_SUCCEED;
	}
	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}
	if (empty) {
		if ((*geomMBR = GDKmalloc(sizeof(mbr))) == NULL)
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

	GEOSGeom_destroy(geosGeometry);

	if (*geomMBR == NULL || is_mbr_nil(*geomMBR)) {
		GDKfree(*geomMBR);
		*geomMBR = NULL;
		throw(MAL, "wkb.mbr", SQLSTATE(38000) "Geos failed to create mbr");
	}

	return MAL_SUCCEED;
}

str
wkbBox2D(mbr **box, wkb **point1, wkb **point2)
{
	GEOSGeom point1_geom, point2_geom;
	double xmin = 0.0, ymin = 0.0, xmax = 0.0, ymax = 0.0;
	str err = MAL_SUCCEED;

	//check null input
	if (is_wkb_nil(*point1) || is_wkb_nil(*point2)) {
		if ((*box = GDKmalloc(sizeof(mbr))) == NULL)
			throw(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**box = mbrNIL;
		return MAL_SUCCEED;
	}
	//check input not point geometries
	point1_geom = wkb2geos(*point1);
	point2_geom = wkb2geos(*point2);
	if (point1_geom == NULL || point2_geom == NULL) {
		if (point1_geom)
			GEOSGeom_destroy(point1_geom);
		if (point2_geom)
			GEOSGeom_destroy(point2_geom);
		throw(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (GEOSGeomTypeId(point1_geom) + 1 != wkbPoint_mdb ||
	    GEOSGeomTypeId(point2_geom) + 1 != wkbPoint_mdb) {
		err = createException(MAL, "geom.MakeBox2D", SQLSTATE(38000) "Geometries should be points");
	} else if (GEOSGeomGetX(point1_geom, &xmin) == -1 ||
		   GEOSGeomGetY(point1_geom, &ymin) == -1 ||
		   GEOSGeomGetX(point2_geom, &xmax) == -1 ||
		   GEOSGeomGetY(point2_geom, &ymax) == -1) {

		err = createException(MAL, "geom.MakeBox2D", SQLSTATE(38000) "Geos error in reading the points' coordinates");
	} else {
		//Assign the coordinates. Ensure that they are in correct order
		*box = GDKmalloc(sizeof(mbr));
		if (*box == NULL) {
			err = createException(MAL, "geom.MakeBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			(*box)->xmin = (float) (xmin < xmax ? xmin : xmax);
			(*box)->ymin = (float) (ymin < ymax ? ymin : ymax);
			(*box)->xmax = (float) (xmax > xmin ? xmax : xmin);
			(*box)->ymax = (float) (ymax > ymin ? ymax : ymin);
		}
	}
	GEOSGeom_destroy(point1_geom);
	GEOSGeom_destroy(point2_geom);

	return err;
}

static str
mbrrelation_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB, str (*func)(bit *, mbr **, mbr **))
{
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(&geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}

	ret = (*func) (out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);

	return ret;
}

/*returns true if the two mbrs overlap */
str
mbrOverlaps(bit *out, mbr **b1, mbr **b2)
{
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
mbrOverlaps_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlaps);
}

/* returns true if b1 is above b2 */
str
mbrAbove(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin > (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is above the mbr of geom2 */
str
mbrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrAbove);
}

/* returns true if b1 is below b2 */
str
mbrBelow(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax < (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is below the mbr of geom2 */
str
mbrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrBelow);
}

/* returns true if box1 is left of box2 */
str
mbrLeft(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax < (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the left of the mbr of geom2 */
str
mbrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrLeft);
}

/* returns true if box1 is right of box2 */
str
mbrRight(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin > (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the right of the mbr of geom2 */
str
mbrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrRight);
}

/* returns true if box1 overlaps or is above box2 when only the Y coordinate is considered*/
str
mbrOverlapOrAbove(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymin >= (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is above the mbr of geom2 */
str
mbrOverlapOrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrAbove);
}

/* returns true if box1 overlaps or is below box2 when only the Y coordinate is considered*/
str
mbrOverlapOrBelow(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->ymax <= (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is below the mbr of geom2 */
str
mbrOverlapOrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrBelow);
}

/* returns true if box1 overlaps or is left of box2 when only the X coordinate is considered*/
str
mbrOverlapOrLeft(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmax <= (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the left of the mbr of geom2 */
str
mbrOverlapOrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrLeft);
}

/* returns true if box1 overlaps or is right of box2 when only the X coordinate is considered*/
str
mbrOverlapOrRight(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = ((*b1)->xmin >= (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the right of the mbr of geom2 */
str
mbrOverlapOrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrOverlapOrRight);
}

/* returns true if b1 is contained in b2 */
str
mbrContained(bit *out, mbr **b1, mbr **b2)
{
	if (is_mbr_nil(*b1) || is_mbr_nil(*b2))
		*out = bit_nil;
	else
		*out = (((*b1)->xmin >= (*b2)->xmin) && ((*b1)->xmax <= (*b2)->xmax) && ((*b1)->ymin >= (*b2)->ymin) && ((*b1)->ymax <= (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is contained in the mbr of geom2 */
str
mbrContained_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrContained);
}

/*returns true if b1 contains b2 */
str
mbrContains(bit *out, mbr **b1, mbr **b2)
{
	return mbrContained(out, b2, b1);
}

/*returns true if the mbrs of geom1 contains the mbr of geom2 */
str
mbrContains_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrContains);
}

/* returns true if the boxes are the same */
str
mbrEqual(bit *out, mbr **b1, mbr **b2)
{
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
mbrEqual_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB)
{
	return mbrrelation_wkb(out, geom1WKB, geom2WKB, mbrEqual);
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
mbrDistance(dbl *out, mbr **b1, mbr **b2)
{
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
mbrDistance_wkb(dbl *out, wkb **geom1WKB, wkb **geom2WKB)
{
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if (ret != MAL_SUCCEED) {
		return ret;
	}

	ret = wkbMBR(&geom2MBR, geom2WKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}

	ret = mbrDistance(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);

	return ret;
}

/* get Xmin, Ymin, Xmax, Ymax coordinates of mbr */
str
wkbCoordinateFromMBR(dbl *coordinateValue, mbr **geomMBR, int *coordinateIdx)
{
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
wkbCoordinateFromWKB(dbl *coordinateValue, wkb **geomWKB, int *coordinateIdx)
{
	mbr *geomMBR;
	str ret = MAL_SUCCEED;
	bit empty;

	if (is_wkb_nil(*geomWKB) || is_int_nil(*coordinateIdx)) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geomWKB)) != MAL_SUCCEED) {
		return ret;
	}

	if (empty) {
		*coordinateValue = dbl_nil;
		return MAL_SUCCEED;
	}

	if ((ret = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED)
		return ret;

	ret = wkbCoordinateFromMBR(coordinateValue, &geomMBR, coordinateIdx);

	GDKfree(geomMBR);

	return ret;
}

str
mbrFromString(mbr **w, const char **src)
{
	size_t len = *w ? sizeof(mbr) : 0;
	char *errbuf;
	str ex;

	if (mbrFROMSTR(*src, &len, (void **) w, false) >= 0)
		return MAL_SUCCEED;
	GDKfree(*w);
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
ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY)
{
	if ((*res = GDKmalloc(sizeof(mbr))) == NULL)
		throw(MAL, "geom.mbr", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (is_flt_nil(*minX) || is_flt_nil(*minY) || is_flt_nil(*maxX) || is_flt_nil(*maxY))
		**res = mbrNIL;
	else {
		(*res)->xmin = *minX;
		(*res)->ymin = *minY;
		(*res)->xmax = *maxX;
		(*res)->ymax = *maxY;
	}
	return MAL_SUCCEED;
}

str
mbrIntersects(bit *out, mbr** mbr1, mbr** mbr2) {
	if (((*mbr1)->ymax < (*mbr2)->ymin) || ((*mbr1)->ymin > (*mbr2)->ymax))
		(*out) = false;
	else if (((*mbr1)->xmax < (*mbr2)->xmin) || ((*mbr1)->xmin > (*mbr2)->xmax))
    	(*out) = false;
	else
		(*out) = true;
	return MAL_SUCCEED;
}

/************************************************/
/************* wkba type functions **************/
/************************************************/

/* Creates the string representation of a wkb_array */
/* return length of resulting string. */
ssize_t
wkbaTOSTR(char **toStr, size_t *len, const void *FROMARRAY, bool external)
{
	const wkba *fromArray = FROMARRAY;
	int items = fromArray->itemsNum, i;
	int itemsNumDigits = (int) ceil(log10(items));
	size_t dataSize;	//, skipBytes=0;
	char **partialStrs;
	char *toStrPtr = NULL, *itemsNumStr = GDKmalloc(itemsNumDigits + 1);

	if (itemsNumStr == NULL)
		return -1;

	dataSize = (size_t) snprintf(itemsNumStr, itemsNumDigits + 1, "%d", items);

	// reserve space for an array with pointers to the partial
	// strings, i.e. for each wkbTOSTR
	partialStrs = GDKzalloc(items * sizeof(char *));
	if (partialStrs == NULL) {
		GDKfree(itemsNumStr);
		return -1;
	}
	//create the string version of each wkb
	for (i = 0; i < items; i++) {
		size_t llen = 0;
		ssize_t ds;
		ds = wkbTOSTR(&partialStrs[i], &llen, fromArray->data[i], false);
		if (ds < 0) {
			GDKfree(itemsNumStr);
			while (i >= 0)
				GDKfree(partialStrs[i--]);
			GDKfree(partialStrs);
			return -1;
		}
		dataSize += ds;

		if (strNil(partialStrs[i])) {
			GDKfree(itemsNumStr);
			while (i >= 0)
				GDKfree(partialStrs[i--]);
			GDKfree(partialStrs);
			if (*len < 4 || *toStr == NULL) {
				GDKfree(*toStr);
				if ((*toStr = GDKmalloc(*len = 4)) == NULL)
					return -1;
			}
			if (external) {
				strcpy(*toStr, "nil");
				return 3;
			}
			strcpy(*toStr, str_nil);
			return 1;
		}
	}

	//add [] around itemsNum
	dataSize += 2;
	//add ", " before each item
	dataSize += 2 * sizeof(char) * items;

	//copy all partial strings to a single one
	if (*len < dataSize + 3 || *toStr == NULL) {
		GDKfree(*toStr);
		*toStr = GDKmalloc(*len = dataSize + 3);	/* plus quotes + termination character */
		if (*toStr == NULL) {
			for (i = 0; i < items; i++)
				GDKfree(partialStrs[i]);
			GDKfree(partialStrs);
			GDKfree(itemsNumStr);
			return -1;
		}
	}
	toStrPtr = *toStr;
	if (external)
		*toStrPtr++ = '\"';
	*toStrPtr++ = '[';
	strcpy(toStrPtr, itemsNumStr);
	toStrPtr += strlen(itemsNumStr);
	*toStrPtr++ = ']';
	for (i = 0; i < items; i++) {
		if (i == 0)
			*toStrPtr++ = ':';
		else
			*toStrPtr++ = ',';
		*toStrPtr++ = ' ';

		//strcpy(toStrPtr, partialStrs[i]);
		memcpy(toStrPtr, partialStrs[i], strlen(partialStrs[i]));
		toStrPtr += strlen(partialStrs[i]);
		GDKfree(partialStrs[i]);
	}

	if (external)
		*toStrPtr++ = '\"';
	*toStrPtr = '\0';

	GDKfree(partialStrs);
	GDKfree(itemsNumStr);

	return (ssize_t) (toStrPtr - *toStr);
}

static ssize_t wkbaFROMSTR_withSRID(const char *fromStr, size_t *len, wkba **toArray, int srid);

/* return number of parsed characters. */
ssize_t
wkbaFROMSTR(const char *fromStr, size_t *len, void **TOARRAY, bool external)
{
	wkba **toArray = (wkba **) TOARRAY;
	if (external && strncmp(fromStr, "nil", 3) == 0) {
		size_t sz = wkba_size(~0);
		if ((*len < sz || *toArray == NULL)
		    && (*toArray = GDKmalloc(sz)) == NULL)
			return -1;
		**toArray = wkba_nil;
		return 3;
	}
	return wkbaFROMSTR_withSRID(fromStr, len, toArray, 0);
}

/* returns a pointer to a null wkba */
const void *
wkbaNULL(void)
{
	return &wkba_nil;
}

BUN
wkbaHASH(const void *WARRAY)
{
	const wkba *wArray = WARRAY;
	int j, i;
	BUN h = 0;

	for (j = 0; j < wArray->itemsNum; j++) {
		wkb *w = wArray->data[j];
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
	}
	return h;
}

int
wkbaCOMP(const void *L, const void *R)
{
	const wkba *l = L, *r = R;
	int i, res = 0;

	//compare the number of items
	if (l->itemsNum != r->itemsNum)
		return l->itemsNum - r->itemsNum;

	if (l->itemsNum == ~(int) 0)
		return (0);

	//compare each wkb separately
	for (i = 0; i < l->itemsNum; i++)
		res += wkbCOMP(l->data[i], r->data[i]);

	return res;
}

/* read wkb from log */
void *
wkbaREAD(void *A, size_t *dstlen, stream *s, size_t cnt)
{
	wkba *a = A;
	int items, i;

	(void) cnt;
	assert(cnt == 1);

	if (mnstr_readInt(s, &items) != 1)
		return NULL;

	size_t wkbalen = (size_t) wkba_size(items);
	if (a == NULL || *dstlen < wkbalen) {
		if ((a = GDKrealloc(a, wkbalen)) == NULL)
			return NULL;
		*dstlen = wkbalen;
	}

	a->itemsNum = items;

	for (i = 0; i < items; i++) {
		size_t wlen = 0;
		a->data[i] = wkbREAD(NULL, &wlen, s, cnt);
	}

	return a;
}

/* write wkb to log */
gdk_return
wkbaWRITE(const void *A, stream *s, size_t cnt)
{
	const wkba *a = A;
	int i, items = a->itemsNum;
	gdk_return ret = GDK_SUCCEED;

	(void) cnt;
	assert(cnt == 1);

	if (!mnstr_writeInt(s, items))
		return GDK_FAIL;
	for (i = 0; i < items; i++) {
		ret = wkbWRITE(a->data[i], s, cnt);

		if (ret != GDK_SUCCEED)
			return ret;
	}
	return GDK_SUCCEED;
}

var_t
wkbaPUT(BAT *b, var_t *bun, const void *VAL)
{
	const wkba *val = VAL;
	char *base;

	*bun = HEAP_malloc(b, wkba_size(val->itemsNum));
	base = b->tvheap->base;
	if (*bun != (var_t) -1) {
		memcpy(&base[*bun], val, wkba_size(val->itemsNum));
		b->tvheap->dirty = true;
	}
	return *bun;
}

void
wkbaDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}

size_t
wkbaLENGTH(const void *P)
{
	const wkba *p = P;
	var_t len = wkba_size(p->itemsNum);
	assert(len <= GDK_int_max);
	return (size_t) len;
}

gdk_return
wkbaHEAP(Heap *heap, size_t capacity)
{
	return HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

/* Non-atom WKBA functions */
/* returns the size of variable-sized atom wkba */
var_t
wkba_size(int items)
{
	var_t size;

	if (items == ~0)
		items = 0;
	size = (var_t) (offsetof(wkba, data) + items * sizeof(wkb *));
	assert(size <= VAR_MAX);

	return size;
}

static ssize_t
wkbaFROMSTR_withSRID(const char *fromStr, size_t *len, wkba **toArray, int srid)
{
	int items, i;
	size_t skipBytes = 0;

//IS THERE SPACE OR SOME OTHER CHARACTER?

	//read the number of items from the beginning of the string
	memcpy(&items, fromStr, sizeof(int));
	skipBytes += sizeof(int);
	*toArray = GDKmalloc(wkba_size(items));
	if (*toArray == NULL)
		return -1;

	for (i = 0; i < items; i++) {
		size_t parsedBytes;
		str err = wkbFROMSTR_withSRID(fromStr + skipBytes, len, &(*toArray)->data[i], srid, &parsedBytes);
		if (err != MAL_SUCCEED) {
			GDKerror("%s", getExceptionMessageAndState(err));
			freeException(err);
			return -1;
		}
		skipBytes += parsedBytes;
	}

	assert(skipBytes <= GDK_int_max);
	return (ssize_t) skipBytes;
}

/* Only use of WKBA in exported functions */
str
wkbInteriorRings(wkba **geomArray, wkb **geomWKB)
{
	int interiorRingsNum = 0, i = 0;
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(*geomWKB)) {
		if ((*geomArray = GDKmalloc(wkba_size(~0))) == NULL)
			throw(MAL, "geom.InteriorRings", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		**geomArray = wkba_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.InteriorRings", SQLSTATE(38000) "Geos operation  wkb2geos failed");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPolygon_mdb) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.interiorRings", SQLSTATE(38000) "Geometry not a Polygon");

	}

	ret = wkbNumRings(&interiorRingsNum, geomWKB, &i);

	if (ret != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		return ret;
	}

	*geomArray = GDKmalloc(wkba_size(interiorRingsNum));
	if (*geomArray == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRings", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	(*geomArray)->itemsNum = interiorRingsNum;

	for (i = 0; i < interiorRingsNum; i++) {
		const GEOSGeometry *interiorRingGeometry;
		wkb *interiorRingWKB;

		// get the interior ring of the geometry
		interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, i);
		if (interiorRingGeometry == NULL) {
			while (--i >= 0)
				GDKfree((*geomArray)->data[i]);
			GDKfree(*geomArray);
			GEOSGeom_destroy(geosGeometry);
			*geomArray = NULL;
			throw(MAL, "geom.InteriorRings", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed");
		}
		// get the wkb representation of it
		interiorRingWKB = geos2wkb(interiorRingGeometry);
		if (interiorRingWKB == NULL) {
			while (--i >= 0)
				GDKfree((*geomArray)->data[i]);
			GDKfree(*geomArray);
			GEOSGeom_destroy(geosGeometry);
			*geomArray = NULL;
			throw(MAL, "geom.InteriorRings", SQLSTATE(38000) "Geos operation wkb2geos failed");
		}

		(*geomArray)->data[i] = interiorRingWKB;
	}
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}
