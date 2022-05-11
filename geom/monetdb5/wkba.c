#include "geom.h"
#include "wkba.h"
#include "geom_io.h"
#include "wkb.h"

/* non atom functions */

/* returns the size of variable-sized atom wkba */
static var_t
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

/************************************************/
/********** wkba atom type functions ************/
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
			int a = *(w->data + i), b = *(w->data + i + 1);
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
