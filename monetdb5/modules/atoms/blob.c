/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_client.h"
#include "mal_interpreter.h"

int TYPE_blob;

static blob nullval = {
	~(size_t) 0
};

#define is_blob_nil(x)	((x)->nitems == nullval.nitems)

static str
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
static int
BLOBcmp(const void *L, const void *R)
{
	const blob *l = L, *r = R;
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

static void
BLOBdel(Heap *h, var_t *idx)
{
	HEAP_free(h, *idx);
}

static BUN
BLOBhash(const void *B)
{
	const blob *b = B;
	return (BUN) b->nitems;
}

static const void *
BLOBnull(void)
{
	return &nullval;
}

static void *
BLOBread(void *A, size_t *dstlen, stream *s, size_t cnt)
{
	blob *a = A;
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1 || len < 0)
		return NULL;
	if (a == NULL || *dstlen < (size_t) len) {
		if ((a = GDKrealloc(a, (size_t) len)) == NULL)
			return NULL;
		*dstlen = (size_t) len;
	}
	if (mnstr_read(s, (char *) a, (size_t) len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

static gdk_return
BLOBwrite(const void *A, stream *s, size_t cnt)
{
	const blob *a = A;
	var_t len = blobsize(a->nitems);

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, (int) len) /* 64bit: check for overflow */ ||
		mnstr_write(s, a, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

static size_t
BLOBlength(const void *P)
{
	const blob *p = P;
	var_t l = blobsize(p->nitems); /* 64bit: check for overflow */
	assert(l <= GDK_int_max);
	return (size_t) l;
}

static void
BLOBheap(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

static var_t
BLOBput(BAT *b, var_t *bun, const void *VAL)
{
	const blob *val = VAL;
	char *base = NULL;

	*bun = HEAP_malloc(b, blobsize(val->nitems));
 	base = b->tvheap->base;
	if (*bun) {
		memcpy(&base[*bun], val, blobsize(val->nitems));
		b->tvheap->dirty = true;
	}
	return *bun;
}

static str
BLOBnitems(int *ret, blob **b)
{
	if (is_blob_nil(*b)) {
		*ret = int_nil;
	} else {
		assert((*b)->nitems < INT_MAX);
		*ret = (int) (*b)->nitems;
	}
	return MAL_SUCCEED;
}

static str
BLOBnitems_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			blob *b = (blob*) BUNtvar(bi, p1);

			if (is_blob_nil(b)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				assert((int) b->nitems < INT_MAX);
				vals[i] = (int) b->nitems;
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			blob *b = (blob*) BUNtvar(bi, p1);

			if (is_blob_nil(b)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				assert((int) b->nitems < INT_MAX);
				vals[i] = (int) b->nitems;
			}
		}
	}

bailout:
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	if (b)
		BBPunfix(b->batCacheid);
	if (bs)
		BBPunfix(bs->batCacheid);
	return msg;
}

static str
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
BLOBtostr(str *tostr, size_t *l, const void *P, bool external)
{
	const blob *p = P;
	char *s;
	size_t i;
	size_t expectedlen;

	if (is_blob_nil(p))
		expectedlen = external ? 4 : 2;
	else
		expectedlen = p->nitems * 2 + 1;
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

static ssize_t
BLOBfromstr(const char *instr, size_t *l, void **VAL, bool external)
{
	blob **val = (blob **) VAL;
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

static str
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

static str
BLOBblob_blob_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = NULL;
	struct canditer ci;
	BUN q;
	oid off;
	bool nils = false;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPkeepref(*res = b->batCacheid); /* nothing to convert, return */
		return MAL_SUCCEED;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	bi = bat_iterator(b);
	if (!(dst = COLnew(ci.hseq, TYPE_blob, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			blob *v = (blob*) BUNtvar(bi, p);

			if (tfastins_nocheckVAR(dst, i, v, Tsize(dst)) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_blob_nil(v);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next(&ci) - off);
			blob *v = (blob*) BUNtvar(bi, p);

			if (tfastins_nocheckVAR(dst, i, v, Tsize(dst)) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_blob_nil(v);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg) {
		BATsetcount(dst, q);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = BATcount(dst) <= 1;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		BBPkeepref(*res = dst->batCacheid);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
BLOBblob_fromstr(blob **b, const char **s)
{
	size_t len = 0;

	if (BLOBfromstr(*s, &len, (void **) b, false) < 0)
		throw(MAL, "blob", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_atom blob_init_atoms[] = {
 { .name="blob", .tostr=BLOBtostr, .fromstr=BLOBfromstr, .cmp=BLOBcmp, .hash=BLOBhash, .null=BLOBnull, .read=BLOBread, .write=BLOBwrite, .put=BLOBput, .del=BLOBdel, .length=BLOBlength, .heap=BLOBheap, },  { .cmp=NULL }
};
static mel_func blob_init_funcs[] = {
 command("blob", "blob", BLOBblob_blob, false, "Noop routine.", args(1,2, arg("",blob),arg("s",blob))),
 command("blob", "blob", BLOBblob_fromstr, false, "", args(1,2, arg("",blob),arg("s",str))),
 command("blob", "toblob", BLOBtoblob, false, "store a string as a blob.", args(1,2, arg("",blob),arg("v",str))),
 command("blob", "nitems", BLOBnitems, false, "get the number of bytes in this blob.", args(1,2, arg("",int),arg("b",blob))),
 pattern("batblob", "nitems", BLOBnitems_bulk, false, "", args(1,2, batarg("",int),batarg("b",blob))),
 pattern("batblob", "nitems", BLOBnitems_bulk, false, "", args(1,3, batarg("",int),batarg("b",blob),batarg("s",oid))),
 command("blob", "prelude", BLOBprelude, false, "", args(1,1, arg("",void))),
 command("calc", "blob", BLOBblob_blob, false, "", args(1,2, arg("",blob),arg("b",blob))),
 command("batcalc", "blob", BLOBblob_blob_bulk, false, "", args(1,3, batarg("",blob),batarg("b",blob),batarg("s",oid))),
 command("calc", "blob", BLOBblob_fromstr, false, "", args(1,2, arg("",blob),arg("s",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_blob_mal)
{ mal_module("blob", blob_init_atoms, blob_init_funcs); }
