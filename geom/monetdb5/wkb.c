#include "geom.h"
#include "wkb.h"
#include "geom_io.h"

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
		int a = *(w->data + i), b = *(w->data + i + 1);
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
