/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * @f blob
 * @v 1.0
 * @a Wilko Quak, Peter Boncz, M. Kersten, N. Nes
 * @+ The blob data type
 * The datatype 'blob' introduced here illustrates the power
 * in the hands of a programmer to extend the functionality of the
 * Monet GDK library. It consists of an interface specification for
 * the necessary operators, a startup routine to register the
 * type in thekernel, and some additional operators used outside
 * the kernel itself.
 *
 * The 'blob' data type is used in many database engines to
 * store a variable sized atomary value.
 * Its definition forms a generic base to store arbitrary structures
 * in the database, without knowing its internal coding, layout,
 * or interpretation.
 *
 * The blob memory layout consists of first 4 bytes containing
 * the bytes-size of the blob (excluding the integer), and then just binary data.
 *
 * @+ Module Definition
 */
#include "monetdb_config.h"
#include "blob.h"

int TYPE_blob;

mal_export str BLOBprelude(void *ret);

mal_export int BLOBcmp(const blob *l, const blob *r);
mal_export BUN BLOBhash(const blob *b);
mal_export const blob *BLOBnull(void);
mal_export var_t BLOBput(Heap *h, var_t *bun, const blob *val);
mal_export void BLOBdel(Heap *h, var_t *index);
mal_export size_t BLOBlength(const blob *p);
mal_export void BLOBheap(Heap *heap, size_t capacity);
mal_export str BLOBtoblob(blob **retval, str *s);
mal_export str BLOBnitems(int *ret, blob **b);
mal_export int BLOBget(Heap *h, int *bun, int *l, blob **val);
mal_export blob * BLOBread(blob *a, stream *s, size_t cnt);
mal_export gdk_return BLOBwrite(const blob *a, stream *s, size_t cnt);

mal_export str BLOBblob_blob(blob **d, blob **s);
mal_export str BLOBblob_fromstr(blob **b, const char **d);

static blob nullval = {
	~(size_t) 0
};

#define is_blob_nil(x)	((x)->nitems == nullval.nitems)

str
BLOBprelude(void *ret)
{
	(void) ret;
	TYPE_blob = ATOMindex("blob");
	return MAL_SUCCEED;
}

var_t
blobsize(size_t nitems)
{
	if (nitems == nullval.nitems)
		nitems = 0;
	assert(offsetof(blob, data) + nitems <= VAR_MAX);
	return (var_t) (offsetof(blob, data) + nitems);
}

static char hexit[] = "0123456789ABCDEF";

/*
 * @- Wrapping section
 * This section contains the wrappers to re-use the implementation
 * section of the blob modules from MonetDB 4.3
 * @-
 */
int
BLOBcmp(const blob *l, const blob *r)
{
	int c;
	if (is_blob_nil(r))
		return !is_blob_nil(l);
	if (is_blob_nil(l))
		return -1;
	if (l->nitems < r->nitems) {
		c = memcmp(l->data, r->data, l->nitems);
		if (c == 0)
			return -1;
	} else {
		c = memcmp(l->data, r->data, r->nitems);
		if (c == 0)
			return l->nitems > r->nitems;
	}
	return c;
}

void
BLOBdel(Heap *h, var_t *idx)
{
	HEAP_free(h, *idx);
}

BUN
BLOBhash(const blob *b)
{
	return (BUN) b->nitems;
}

const blob *
BLOBnull(void)
{
	return &nullval;
}

blob *
BLOBread(blob *a, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if ((a = GDKmalloc(len)) == NULL)
		return NULL;
	if (mnstr_read(s, (char *) a, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

gdk_return
BLOBwrite(const blob *a, stream *s, size_t cnt)
{
	var_t len = blobsize(a->nitems);

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, (int) len) /* 64bit: check for overflow */ ||
		mnstr_write(s, a, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

size_t
BLOBlength(const blob *p)
{
	var_t l = blobsize(p->nitems); /* 64bit: check for overflow */
	assert(l <= GDK_int_max);
	return (size_t) l;
}

void
BLOBheap(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

var_t
BLOBput(Heap *h, var_t *bun, const blob *val)
{
	char *base = NULL;

	*bun = HEAP_malloc(h, blobsize(val->nitems));
 	base = h->base;
	if (*bun) {
		memcpy(&base[*bun], val, blobsize(val->nitems));
		h->dirty = true;
	}
	return *bun;
}

str
BLOBnitems(int *ret, blob **b)
{
	if (is_blob_nil(*b)) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	assert((*b)->nitems <INT_MAX);
	*ret = (int) (*b)->nitems;
	return MAL_SUCCEED;
}

str
BLOBtoblob(blob **retval, str *s)
{
	size_t len = strLen(*s);
	blob *b = (blob *) GDKmalloc(blobsize(len));

	if( b == NULL)
		throw(MAL, "blob.toblob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->nitems = len;
	memcpy(b->data, *s, len);
	*retval = b;
	return MAL_SUCCEED;
}

ssize_t
BLOBtostr(str *tostr, size_t *l, const blob *p, bool external)
{
	char *s;
	size_t i;
	size_t expectedlen;

	if (is_blob_nil(p))
		expectedlen = external ? 4 : 2;
	else
		expectedlen = 24 + (p->nitems * 3);
	if (*l < expectedlen || *tostr == NULL) {
		GDKfree(*tostr);
		*tostr = GDKmalloc(expectedlen);
		if (*tostr == NULL)
			return -1;
		*l = expectedlen;
	}
	if (is_blob_nil(p)) {
		if (external) {
			strcpy(*tostr, "nil");
			return 3;
		}
		strcpy(*tostr, str_nil);
		return 1;
	}

	s = *tostr;

	for (i = 0; i < p->nitems; i++) {
		int val = (p->data[i] >> 4) & 15;

		*s++ = hexit[val];
		val = p->data[i] & 15;
		*s++ = hexit[val];
	}
	*s = '\0';
	return (ssize_t) (s - *tostr);
}

ssize_t
BLOBfromstr(const char *instr, size_t *l, blob **val, bool external)
{
	size_t i;
	size_t nitems;
	var_t nbytes;
	blob *result;
	const char *s = instr;

	if (strNil(instr) || (external && strncmp(instr, "nil", 3) == 0)) {
		nbytes = blobsize(0);
		if (*l < nbytes || *val == NULL) {
			GDKfree(*val);
			if ((*val = GDKmalloc(nbytes)) == NULL)
				return -1;
		}
		**val = nullval;
		return strNil(instr) ? 1 : 3;
	}

	/* count hexits and check for hexits/space
	 */
	for (i = nitems = 0; instr[i]; i++) {
		if (isxdigit((unsigned char) instr[i]))
			nitems++;
		else if (!isspace((unsigned char) instr[i])) {
			GDKerror("Illegal char in blob\n");
			return -1;
		}
	}
	if (nitems % 2 != 0) {
		GDKerror("Illegal blob length '%zu' (should be even)\n", nitems);
		return -1;
	}
	nitems /= 2;
	nbytes = blobsize(nitems);

	if (*l < nbytes || *val == NULL) {
		GDKfree(*val);
		*val = GDKmalloc(nbytes);
		if( *val == NULL)
			return -1;
		*l = (size_t) nbytes;
	}
	result = *val;
	result->nitems = nitems;

	/*
	   // Read the values of the blob.
	 */
	for (i = 0; i < nitems; ++i) {
		char res = 0;

		for (;;) {
			if (isdigit((unsigned char) *s)) {
				res = *s - '0';
			} else if (*s >= 'A' && *s <= 'F') {
				res = 10 + *s - 'A';
			} else if (*s >= 'a' && *s <= 'f') {
				res = 10 + *s - 'a';
			} else {
				assert(isspace((unsigned char) *s));
				s++;
				continue;
			}
			break;
		}
		s++;
		res <<= 4;
		for (;;) {
			if (isdigit((unsigned char) *s)) {
				res += *s - '0';
			} else if (*s >= 'A' && *s <= 'F') {
				res += 10 + *s - 'A';
			} else if (*s >= 'a' && *s <= 'f') {
				res += 10 + *s - 'a';
			} else {
				assert(isspace((unsigned char) *s));
				s++;
				continue;
			}
			break;
		}
		s++;

		result->data[i] = res;
	}

	return (ssize_t) (s - instr);
}

str
BLOBblob_blob(blob **d, blob **s)
{
	size_t len = blobsize((*s)->nitems);
	blob *b;

	*d = b = GDKmalloc(len);
	if (b == NULL)
		throw(MAL,"blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->nitems = (*s)->nitems;
	if (!is_blob_nil(b) && b->nitems != 0)
		memcpy(b->data, (*s)->data, b->nitems);
	return MAL_SUCCEED;
}

str
BLOBblob_fromstr(blob **b, const char **s)
{
	size_t len = 0;

	if (BLOBfromstr(*s, &len, b, false) < 0)
		throw(MAL, "blob", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
