/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
int TYPE_sqlblob;

mal_export str BLOBprelude(void *ret);

mal_export int BLOBtostr(str *tostr, int *l, blob *pin);
mal_export int BLOBfromstr(char *instr, int *l, blob **val);
mal_export int BLOBnequal(blob *l, blob *r);
mal_export BUN BLOBhash(blob *b);
mal_export blob * BLOBnull(void);
mal_export var_t BLOBput(Heap *h, var_t *bun, blob *val);
mal_export void BLOBdel(Heap *h, var_t *index);
mal_export int BLOBlength(blob *p);
mal_export void BLOBheap(Heap *heap, size_t capacity);
mal_export int SQLBLOBfromstr(char *instr, int *l, blob **val);
mal_export int SQLBLOBtostr(str *tostr, int *l, blob *pin);
mal_export str BLOBtoblob(blob **retval, str *s);
mal_export str BLOBfromblob(str *retval, blob **b);
mal_export str BLOBfromidx(str *retval, blob **binp, int *index);
mal_export str BLOBnitems(int *ret, blob *b);
mal_export int BLOBget(Heap *h, int *bun, int *l, blob **val);
mal_export blob * BLOBread(blob *a, stream *s, size_t cnt);
mal_export gdk_return BLOBwrite(blob *a, stream *s, size_t cnt);

mal_export str BLOBblob_blob(blob **d, blob **s);
mal_export str BLOBblob_fromstr(blob **b, str *d);

mal_export str BLOBsqlblob_fromstr(sqlblob **b, str *d);

str
BLOBprelude(void *ret)
{
	(void) ret;
	TYPE_blob = ATOMindex("blob");
	TYPE_sqlblob = ATOMindex("sqlblob");
	return MAL_SUCCEED;
}

var_t
blobsize(size_t nitems)
{
	if (nitems == ~(size_t) 0)
		nitems = 0;
	assert(offsetof(blob, data) + nitems <= VAR_MAX);
	return (var_t) (offsetof(blob, data) + nitems);
}

static var_t
blob_put(Heap *h, var_t *bun, blob *val)
{
	char *base = NULL;

	*bun = HEAP_malloc(h, blobsize(val->nitems));
 	base = h->base;
	if (*bun) {
		memcpy(&base[*bun << GDK_VARSHIFT], (char *) val, blobsize(val->nitems));
		h->dirty = 1;
	}
	return *bun;
}

static int
blob_nequal(blob *l, blob *r)
{
	size_t len = l->nitems;

	if (len != r->nitems)
		return (1);

	if (len == ~(size_t) 0)
		return (0);

	return memcmp(l->data, r->data, len) != 0;
}

static void
blob_del(Heap *h, var_t *idx)
{
	HEAP_free(h, *idx);
}

static BUN
blob_hash(blob *b)
{
	return (BUN) b->nitems;
}

static blob *
blob_null(void)
{
	static blob nullval;

	nullval.nitems = ~(size_t) 0;
	return (&nullval);
}

static blob *
blob_read(blob *a, stream *s, size_t cnt)
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

static int
blob_length(blob *p)
{
	var_t l = blobsize(p->nitems); /* 64bit: check for overflow */
	assert(l <= GDK_int_max);
	return (int) l; /* 64bit: check for overflow */
}


static void
blob_heap(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

static char hexit[] = "0123456789ABCDEF";

static int
blob_tostr(str *tostr, int *l, blob *p)
{
	char *s;
	size_t i;
	size_t expectedlen;

	if (p->nitems == ~(size_t) 0)
		expectedlen = 4;
	else
		expectedlen = 24 + (p->nitems * 3);
	if (*l < 0 || (size_t) * l < expectedlen) {
		if (*tostr != NULL)
			GDKfree(*tostr);
		*tostr = (str) GDKmalloc(expectedlen);
		if( *tostr == NULL)
			return 0;
		*l = (int) expectedlen;
	}
	if (p->nitems == ~(size_t) 0) {
		strcpy(*tostr, "nil");
		return 3;
	}

	sprintf(*tostr, "(" SZFMT ":", p->nitems);
	s = *tostr + strlen(*tostr);

	for (i = 0; i < p->nitems; i++) {
		int val = (p->data[i] >> 4) & 15;

		*s++ = ' ';
		*s++ = hexit[val];
		val = p->data[i] & 15;
		*s++ = hexit[val];
	}
	*s++ = ')';
	*s = '\0';
	return (int) (s - *tostr); /* 64bit: check for overflow */
}

/* SQL 99 compatible BLOB output string
 * differs from the MonetDB BLOB output in that it does not start with a size
 * no brackets and no spaces in between the hexits
 */
int
sqlblob_tostr(str *tostr, int *l, const blob *p)
{
	char *s;
	size_t i;
	size_t expectedlen;

	if (p->nitems == ~(size_t) 0)
		expectedlen = 4;
	else
		expectedlen = 24 + (p->nitems * 3);
	if (*l < 0 || (size_t) * l < expectedlen) {
		if (*tostr != NULL)
			GDKfree(*tostr);
		*tostr = (str) GDKmalloc(expectedlen);
		if( *tostr == NULL)
			return 0;
		*l = (int) expectedlen;
	}
	if (p->nitems == ~(size_t) 0) {
		strcpy(*tostr, "nil");
		return 3;
	}

	strcpy(*tostr, "\0");
	s = *tostr;

	for (i = 0; i < p->nitems; i++) {
		int val = (p->data[i] >> 4) & 15;

		*s++ = hexit[val];
		val = p->data[i] & 15;
		*s++ = hexit[val];
	}
	*s = '\0';
	return (int) (s - *tostr); /* 64bit: check for overflow */
}

static int
blob_fromstr(char *instr, int *l, blob **val)
{
	size_t i;
	size_t nitems;
	var_t nbytes;
	blob *result;
	char *s = instr;

	s = strchr(s, '(');
	if (s == NULL) {
		GDKerror("Missing ( in blob\n");
		*val = (blob *) NULL;
		return (0);
	}
	nitems = (size_t) strtoul(s + 1, &s, 10);
	if (s == NULL) {
		GDKerror("Missing nitems in blob\n");
		*val = (blob *) NULL;
		return (0);
	}
#if SIZEOF_SIZE_T > SIZEOF_INT
	if (nitems > 0x7fffffff) {
		GDKerror("Blob too large\n");
		*val = (blob *) NULL;
		return (0);
	}
#endif
	nbytes = blobsize(nitems);
	s = strchr(s, ':');
	if (s == NULL) {
		GDKerror("Missing ':' in blob\n");
		*val = (blob *) NULL;
		return (0);
	}
	++s;

	if (*val == (blob *) NULL) {
		*val = (blob *) GDKmalloc(nbytes);
		*l = (int) nbytes;
	} else if (*l < 0 || (size_t) * l < nbytes) {
		GDKfree(*val);
		*val = (blob *) GDKmalloc(nbytes);
		*l = (int) nbytes;
	}
	if( *val == NULL)
		return 0;
	
	result = *val;
	result->nitems = nitems;

	/*
	   // Read the values of the blob.
	 */
	for (i = 0; i < nitems; ++i) {
		char res = 0;

		if (*s == ' ')
			s++;

		if (*s >= '0' && *s <= '9') {
			res = *s - '0';
		} else if (*s >= 'A' && *s <= 'F') {
			res = 10 + *s - 'A';
		} else if (*s >= 'a' && *s <= 'f') {
			res = 10 + *s - 'a';
		} else {
			break;
		}
		s++;
		res <<= 4;
		if (*s >= '0' && *s <= '9') {
			res += *s - '0';
		} else if (*s >= 'A' && *s <= 'F') {
			res += 10 + *s - 'A';
		} else if (*s >= 'a' && *s <= 'f') {
			res += 10 + *s - 'a';
		} else {
			break;
		}
		s++;		/* skip space */

		result->data[i] = res;
	}

	if (i < nitems) {
		GDKerror("blob_fromstr: blob too short \n");
		return -1;
	}

	s = strchr(s, ')');
	if (s == 0) {
		GDKerror("blob_fromstr: Missing ')' in blob\n");
	}

	return (int) (s - instr);
}

/* SQL 99 compatible BLOB input string
 * differs from the MonetDB BLOB input in that it does not start with a size
 * no brackets and no spaces in between the hexits
 */
int
sqlblob_fromstr(char *instr, int *l, blob **val)
{
	size_t i;
	size_t nitems;
	var_t nbytes;
	blob *result;
	char *s = instr;
	int nil = 0;

	/* since the string is built of (only) hexits the number of bytes
	 * required for it is the length of the string divided by two
	 */
	i = strlen(instr);
	if (i % 2 == 1) {
		GDKerror("sqlblob_fromstr: Illegal blob length '" SZFMT "' (should be even)\n", i);
		instr = 0;
		nil = 1;
	}
	nitems = i / 2;
	nbytes = blobsize(nitems);

	if (*val == (blob *) NULL) {
		*val = (blob *) GDKmalloc(nbytes);
		*l = (int) nbytes;
	} else if (*l < 0 || (size_t) * l < nbytes) {
		GDKfree(*val);
		*val = (blob *) GDKmalloc(nbytes);
		*l = (int) nbytes;
	}
	if( *val == NULL)
		return 0;
	if (nil) {
		**val = *blob_null();
		return 0;
	}
	result = *val;
	result->nitems = nitems;

	/*
	   // Read the values of the blob.
	 */
	for (i = 0; i < nitems; ++i) {
		char res = 0;

		if (*s >= '0' && *s <= '9') {
			res = *s - '0';
		} else if (*s >= 'A' && *s <= 'F') {
			res = 10 + *s - 'A';
		} else if (*s >= 'a' && *s <= 'f') {
			res = 10 + *s - 'a';
		} else {
			GDKerror("sqlblob_fromstr: Illegal char '%c' in blob\n", *s);
		}
		s++;
		res <<= 4;
		if (*s >= '0' && *s <= '9') {
			res += *s - '0';
		} else if (*s >= 'A' && *s <= 'F') {
			res += 10 + *s - 'A';
		} else if (*s >= 'a' && *s <= 'f') {
			res += 10 + *s - 'a';
		} else {
			GDKerror("sqlblob_fromstr: Illegal char '%c' in blob\n", *s);
		}
		s++;

		result->data[i] = res;
	}

	return (int) (s - instr);
}


static str
fromblob_idx(str *retval, blob *b, int *idx)
{
	str s, p = b->data + *idx;
	str r, q = b->data + b->nitems;

	for (r = p; r < q; r++) {
		if (*r == 0)
			break;
	}
	*retval = s = (str) GDKmalloc(1 + r - p);
	if( *retval == NULL)
		throw(MAL, "blob.tostring", MAL_MALLOC_FAIL);
	for (; p < r; p++, s++)
		*s = *p;
	*s = 0;
	return MAL_SUCCEED;
}

/*
 * @- Wrapping section
 * This section contains the wrappers to re-use the implementation
 * section of the blob modules from MonetDB 4.3
 * @-
 */
int
BLOBnequal(blob *l, blob *r)
{
	return blob_nequal(l, r);
}

void
BLOBdel(Heap *h, var_t *idx)
{
	blob_del(h, idx);
}

BUN
BLOBhash(blob *b)
{
	return blob_hash(b);
}

blob *
BLOBnull(void)
{
	blob *b= (blob*) GDKmalloc(offsetof(blob, data));
	if( b )
		b->nitems = ~(size_t) 0;
	return b;
}

blob *
BLOBread(blob *a, stream *s, size_t cnt)
{
	return blob_read(a,s,cnt);
}

gdk_return
BLOBwrite(blob *a, stream *s, size_t cnt)
{
	var_t len = blobsize(a->nitems);

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, (int) len) /* 64bit: check for overflow */ ||
		mnstr_write(s, (char *) a, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

int
BLOBlength(blob *p)
{
	return blob_length(p);
}

void
BLOBheap(Heap *heap, size_t capacity)
{
	blob_heap(heap, capacity);
}

var_t
BLOBput(Heap *h, var_t *bun, blob *val)
{
	return blob_put(h, bun, val);
}

str
BLOBnitems(int *ret, blob *b)
{
	assert(b->nitems <INT_MAX);
	*ret = (int) b->nitems;
	return MAL_SUCCEED;
}

int
BLOBtostr(str *tostr, int *l, blob *pin)
{
	return blob_tostr(tostr, l, pin);
}

int
BLOBfromstr(char *instr, int *l, blob **val)
{
	return blob_fromstr(instr, l, val);
}

str
BLOBfromidx(str *retval, blob **binp, int *idx)
{
	return fromblob_idx(retval, *binp, idx);
}

str
BLOBfromblob(str *retval, blob **b)
{
	int zero = 0;

	return fromblob_idx(retval, *b, &zero);
}

str
BLOBtoblob(blob **retval, str *s)
{
	int len = strLen(*s);
	blob *b = (blob *) GDKmalloc(blobsize(len));

	if( b == NULL)
		throw(MAL, "blob.toblob", MAL_MALLOC_FAIL);
	b->nitems = len;
	memcpy(b->data, *s, len);
	*retval = b;
	return MAL_SUCCEED;
}

int
SQLBLOBtostr(str *retval, int *l, blob *b)
{
	return sqlblob_tostr(retval, l, b);
}

int
SQLBLOBfromstr(char *s, int *l, blob **val)
{
	return sqlblob_fromstr(s, l, val);
}

str
BLOBblob_blob(blob **d, blob **s)
{
	size_t len = blobsize((*s)->nitems);
	blob *b;

	if( (*s)->nitems == ~(size_t) 0){
		*d= BLOBnull();
	} else {
		*d= b= (blob *) GDKmalloc(len);
		if( b == NULL)
			throw(MAL,"blob_blob",MAL_MALLOC_FAIL);
		b->nitems = len;
		memcpy(b->data, (*s)->data, len);
	}
	return MAL_SUCCEED;
}

str
BLOBblob_fromstr(blob **b, str *s)
{
	int len = 0;

	blob_fromstr(*s, &len, b);
	return MAL_SUCCEED;
}


str
BLOBsqlblob_fromstr(sqlblob **b, str *s)
{
	int len = 0;

	sqlblob_fromstr(*s, &len, b);
	return MAL_SUCCEED;
}
