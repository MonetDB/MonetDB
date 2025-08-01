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

/*
 * @a M. L. Kersten, P. Boncz
 * @* Atomic types
 * The Binary Association Table library assumes efficient
 * implementation of the atoms making up the binary association.  This
 * section describes the preliminaries for handling both built-in and
 * user-defined atomic types.
 * New types, such as point and polygons, can be readily added to this
 * collection.
 */
/*
 * @- inline comparison routines
 * Return 0 on l==r, < 0 iff l < r, >0 iff l > r
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "gdk_private.h"

/* the *Cmp functions return a value less than zero if the first
 * argument is less than the second; they return zero if the two
 * values are equal; and they return a value greater than zero if the
 * first argument is greater than the second.  Remember that in all
 * cases, nil is considered smaller than any other value and nil is
 * equal to itself (this has repercussions for the floating point
 * implementation if and when its NIL value is the floating point
 * NaN). */

static int
mskCmp(const msk *l, const msk *r)
{
	return (*l > *r) - (*l < *r);
}

static int
bteCmp(const bte *l, const bte *r)
{
	return (*l > *r) - (*l < *r);
}

static int
shtCmp(const sht *l, const sht *r)
{
	return (*l > *r) - (*l < *r);
}

static int
intCmp(const int *l, const int *r)
{
	return (*l > *r) - (*l < *r);
}

static int
fltCmp(const flt *l, const flt *r)
{
	return is_flt_nil(*l) ? -!is_flt_nil(*r) : is_flt_nil(*r) ? 1 : (*l > *r) - (*l < *r);
}

static int
lngCmp(const lng *l, const lng *r)
{
	return (*l > *r) - (*l < *r);
}

#ifdef HAVE_HGE
static int
hgeCmp(const hge *l, const hge *r)
{
	return (*l > *r) - (*l < *r);
}
#endif

static int
dblCmp(const dbl *l, const dbl *r)
{
	return is_dbl_nil(*l) ? -!is_dbl_nil(*r) : is_dbl_nil(*r) ? 1 : (*l > *r) - (*l < *r);
}

/*
 * @- inline hash routines
 * Return some positive integer derived from one atom value.
 */
static BUN
bteHash(const bte *v)
{
	return (BUN) mix_bte(*(const unsigned char *) v);
}

static BUN
shtHash(const sht *v)
{
	return (BUN) mix_sht(*(const unsigned short *) v);
}

static BUN
intHash(const int *v)
{
	return (BUN) mix_int(*(const unsigned int *) v);
}

static BUN
lngHash(const lng *v)
{
	return (BUN) mix_lng(*(const ulng *) v);
}

#ifdef HAVE_HGE
static BUN
hgeHash(const hge *v)
{
	return (BUN) mix_hge(*(const uhge *) v);
}
#endif

static BUN
fltHash(const flt *v)
{
	if (is_flt_nil(*v))
		return (BUN) mix_int(GDK_int_min);
	if (*v == 0)
		return (BUN) mix_int(0);
	return (BUN) mix_int(*(const unsigned int *) v);
}

static BUN
dblHash(const dbl *v)
{
	if (is_dbl_nil(*v))
		return (BUN) mix_lng(GDK_lng_min);
	if (*v == 0)
		return (BUN) mix_lng(0);
	return (BUN) mix_lng(*(const ulng *) v);
}

/*
 * @+ Standard Atoms
 */

/*
 * @+ Atomic Type Interface
 * The collection of built-in types supported for BATs can be extended
 * easily.  In essence, the user should specify conversion routines
 * from values stored anywhere in memory to its equivalent in the BAT,
 * and vice verse.  Some routines are required for coercion and to
 * support the BAT administration.
 *
 * A new type is incrementally build using the routine
 * ATOMallocate(id).  The parameter id denotes the type name; an entry
 * is created if the type is so far unknown.
 *
 * The size describes the amount of space to be reserved in the BUN.
 *
 * The routine put takes a pointer to a memory resident copy and
 * prepares a persistent copy in the BAT passed.  The inverse
 * operation is get.  A new value can be directly included into the
 * BAT using new, which should prepare a null-value representation.  A
 * value is removed from the BAT store using del, which can take care
 * of garbage collection and BAT administration.
 *
 * The pair tostr and fromstr should convert a reference to a
 * persistent value to a memory resident string equivalent. FromStr
 * takes a string and applies a put to store it within a BAT.  They
 * are used to prepare for readable output/input and to support
 * coercion.
 *
 * The routines cmp and eq are comparison routines used to build
 * access structures. The null returns a reference to a null value
 * representation.
 *
 * The incremental atom construction uses hardwired properties.  This
 * should be improved later on.
 */
static MT_Lock GDKatomLock = MT_LOCK_INITIALIZER(GDKatomLock);

int
ATOMallocate(const char *id)
{
	int t;

	if (strlen(id) >= IDLENGTH) {
		GDKerror("name too long");
		return int_nil;
	}

	MT_lock_set(&GDKatomLock);
	t = ATOMindex(id);
	if (t < 0) {
		t = -t;
		if (t == GDKatomcnt) {
			if (GDKatomcnt == MAXATOMS) {
				MT_lock_unset(&GDKatomLock);
				GDKerror("too many types");
				return int_nil;
			}
			GDKatomcnt++;
		}
		BATatoms[t] = (atomDesc) {
			.size = sizeof(int),	/* default */
			.linear = true,		/* default */
			.storage = t,		/* default */
		};
		strcpy(BATatoms[t].name, id);
	}
	MT_lock_unset(&GDKatomLock);
	return t;
}

int
ATOMindex(const char *nme)
{
	int t, j = GDKatomcnt;

	for (t = 0; t < GDKatomcnt; t++) {
		if (!BATatoms[t].name[0]) {
			if (j == GDKatomcnt)
				j = t;
		} else if (strcmp(nme, BATatoms[t].name) == 0) {
			return t;
		}

	}
	return -j;
}

const char *
ATOMname(int t)
{
	return t >= 0 && t < GDKatomcnt && *BATatoms[t].name ? BATatoms[t].name : "null";
}

bool
ATOMisdescendant(int tpe, int parent)
{
	int cur = -1;

	while (cur != tpe) {
		cur = tpe;
		if (cur == parent)
			return true;
		tpe = ATOMstorage(tpe);
	}
	return false;
}


const bte bte_nil = GDK_bte_min-1;
const sht sht_nil = GDK_sht_min-1;
const int int_nil = GDK_int_min-1;
#ifdef NAN_CANNOT_BE_USED_AS_INITIALIZER
/* Definition of NAN is seriously broken on Intel compiler (at least
 * in some versions), so we work around it. */
const union _flt_nil_t _flt_nil_ = {
	.l = UINT32_C(0x7FC00000)
};
const union _dbl_nil_t _dbl_nil_ = {
	.l = UINT64_C(0x7FF8000000000000)
};
#else
const flt flt_nil = NAN;
const dbl dbl_nil = NAN;
#endif
const lng lng_nil = GDK_lng_min-1;
#ifdef HAVE_HGE
const hge hge_nil = GDK_hge_min-1;
#endif
const oid oid_nil = (oid) 1 << (sizeof(oid) * 8 - 1);
const ptr ptr_nil = NULL;
const uuid uuid_nil = {0};

ptr
ATOMnil(int t)
{
	const void *src = ATOMnilptr(t);
	size_t len = ATOMlen(ATOMtype(t), src);
	ptr dst = GDKmalloc(len);

	if (dst)
		memcpy(dst, src, len);
	return dst;
}

/*
 * @- Atomic ADT functions
 */
size_t
ATOMlen(int t, const void *src)
{
	size_t (*l)(const void *) = BATatoms[t].atomLen;

	return l ? (*l) (src) : ATOMsize(t);
}

gdk_return
ATOMheap(int t, Heap *hp, size_t cap)
{
	gdk_return (*h) (Heap *, size_t) = BATatoms[t].atomHeap;

	if (h) {
		return (*h) (hp, cap);
	}
	return GDK_SUCCEED;
}

/*
 * Atom print avoids coercion to strings for built-in types.
 * The comparison against the NULL value is hard coded for speed.
 */
#define LINE_LEN	60

int
ATOMprint(int t, const void *p, stream *s)
{
	ssize_t (*tostr) (char **, size_t *, const void *, bool);
	ssize_t res;

	if (p && t >= 0 && t < GDKatomcnt && (tostr = BATatoms[t].atomToStr)) {
		size_t sz;

		if (t < TYPE_date) {
			char buf[dblStrlen], *addr = buf;	/* use memory from stack */

			sz = dblStrlen;
			res = (*tostr) (&addr, &sz, p, true);
			if (res > 0)
				res = mnstr_write(s, buf, (size_t) res, 1);
		} else {
			str buf = NULL;

			sz = 0;
			res = (*tostr) (&buf, &sz, p, true);
			if (res > 0)
				res = mnstr_write(s, buf, (size_t) res, 1);
			GDKfree(buf);
		}
	} else {
		res = mnstr_write(s, "nil", 1, 3);
	}
	if (res < 0)
		GDKsyserror("ATOMprint: write failure\n");
	return (int) res;
}


char *
ATOMformat(int t, const void *p)
{
	ssize_t (*tostr) (char **, size_t *, const void *, bool);

	if (p && 0 <= t && t < GDKatomcnt && (tostr = BATatoms[t].atomToStr)) {
		size_t sz = 0;
		char *buf = NULL;
		ssize_t res = (*tostr) (&buf, &sz, p, true);
		if (res < 0 && buf) {
			GDKfree(buf);
			buf = NULL;
		}
		return buf;
	}
	return GDKstrdup("nil");
}

ptr
ATOMdup(int t, const void *p)
{
	size_t len = ATOMlen(t, p);
	ptr n = GDKmalloc(len);

	if (n)
		memcpy(n, p, len);
	return n;
}

/*
 * @* Builtin Atomic Operator Implementations
 *
 * @+ Atom-from-String Conversions
 * These routines convert from string to atom. They are used during
 * conversion and BAT import. In order to avoid unnecessary
 * malloc()/free() sequences, the conversion functions have a meta
 * 'dst' pointer to a destination region, and an integer* 'len'
 * parameter, that denotes the length of that region (a char region
 * for ToStr functions, an atom region from FromStr conversions). Only
 * if necessary will the conversion routine do a GDKfree()/GDKmalloc()
 * sequence, and increment the 'len'.  Passing a pointer to a nil-ptr
 * as 'dst' and/or a *len==0 is valid; the conversion function will
 * then alloc some region for you.
 */
#define atommem(size)					\
	do {						\
		if (*dst == NULL || *len < (size)) {	\
			GDKfree(*dst);			\
			*len = (size);			\
			*dst = GDKmalloc(*len);		\
			if (*dst == NULL) {		\
				*len = 0;		\
				return -1;		\
			}				\
		}					\
	} while (0)

#define is_ptr_nil(val)		((val) == ptr_nil)

#define atomtostr(TYPE, FMT, FMTCAST)					\
ssize_t									\
TYPE##ToStr(char **dst, size_t *len, const TYPE *src, bool external)	\
{									\
	atommem(TYPE##Strlen);						\
	if (is_##TYPE##_nil(*src)) {					\
		if (external) {						\
			strcpy(*dst, "nil");				\
			return 3;					\
		}							\
		strcpy(*dst, str_nil);					\
		return 1;						\
	}								\
	return snprintf(*dst, *len, FMT, FMTCAST *src);			\
}

#define base10(x)	((x) - '0')

#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult16(x)	((x) << 4)

static void *
voidRead(void *a, size_t *dstlen, stream *s, size_t cnt)
{
	(void) dstlen;
	(void) s;
	(void) cnt;
	return a;
}

static gdk_return
voidWrite(const void *a, stream *s, size_t cnt)
{
	(void) a;
	(void) s;
	(void) cnt;
	return GDK_SUCCEED;
}

/*
 * Converts string values such as TRUE/FALSE/true/false etc to 1/0/NULL.
 * Switched from byte-to-byte compare to library function strncasecmp,
 * experiments showed that library function is even slightly faster and we
 * now also support True/False (and trUe/FAlSE should this become a thing).
 */
static ssize_t
mskFromStr(const char *src, size_t *len, msk **dst, bool external)
{
	const char *p = src;

	(void) external;
	atommem(sizeof(msk));

	if (strNil(src))
		return -1;

	while (GDKisspace(*p))
		p++;
	if (*p == '0') {
		**dst = 0;
		p++;
	} else if (*p == '1') {
		**dst = 1;
		p++;
	} else {
		return -1;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

static ssize_t
mskToStr(char **dst, size_t *len, const msk *src, bool external)
{
	(void) external;
	atommem(2);
	strcpy(*dst, *src ? "1" : "0");
	return 1;
}

ssize_t
bitFromStr(const char *src, size_t *len, bit **dst, bool external)
{
	const char *p = src;

	atommem(sizeof(bit));

	**dst = bit_nil;

	if (strNil(src))
		return 1;

	while (GDKisspace(*p))
		p++;
	if (*p == '0') {
		**dst = FALSE;
		p++;
	} else if (*p == '1') {
		**dst = TRUE;
		p++;
	} else if (strncasecmp(p, "true",  4) == 0) {
		**dst = TRUE;
		p += 4;
	} else if (strncasecmp(p, "false", 5) == 0) {
		**dst = FALSE;
		p += 5;
	} else if (external && strncasecmp(p, "nil",   3) == 0) {
		p += 3;
	} else {
		return -1;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

ssize_t
bitToStr(char **dst, size_t *len, const bit *src, bool external)
{
	atommem(6);

	if (is_bit_nil(*src)) {
		if (external) {
			strcpy(*dst, "nil");
			return 3;
		}
		strcpy(*dst, str_nil);
		return 1;
	}
	if (*src) {
		strcpy(*dst, "true");
		return 4;
	}
	strcpy(*dst, "false");
	return 5;
}


/*
 * numFromStr parses the head of the string for a number, accepting an
 * optional sign. The code has been prepared to continue parsing by
 * returning the number of characters read.  Both overflow and
 * incorrect syntax (not a number) result in the function returning 0
 * and setting the destination to nil.
 */
struct maxdiv {
	/* if we want to multiply a value with scale, the value must
	 * be no larger than maxval for there to not be overflow */
#ifdef HAVE_HGE
	hge scale, maxval;
#else
	lng scale, maxval;
#endif
};
static const struct maxdiv maxdiv[] = {
#ifdef HAVE_HGE
	/* maximum hge value: 170141183460469231731687303715884105727 (2**127-1)
	 * GCC doesn't currently support integer constants that don't
	 * fit in 8 bytes, so we split large values up*/
	{(hge) LL_CONSTANT(1), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000000000U)+ (hge) LL_CONSTANT(1687303715884105727)},
	{(hge) LL_CONSTANT(10), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000000000) + (hge) LL_CONSTANT(168730371588410572)},
	{(hge) LL_CONSTANT(100), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000000000) + (hge) LL_CONSTANT(16873037158841057)},
	{(hge) LL_CONSTANT(1000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000000) + (hge) LL_CONSTANT(1687303715884105)},
	{(hge) LL_CONSTANT(10000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000000) + (hge) LL_CONSTANT(168730371588410)},
	{(hge) LL_CONSTANT(100000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000000) + (hge) LL_CONSTANT(16873037158841)},
	{(hge) LL_CONSTANT(1000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000000) + (hge) LL_CONSTANT(1687303715884)},
	{(hge) LL_CONSTANT(10000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000000) + (hge) LL_CONSTANT(168730371588)},
	{(hge) LL_CONSTANT(100000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000000) + (hge) LL_CONSTANT(16873037158)},
	{(hge) LL_CONSTANT(1000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000000) + (hge) LL_CONSTANT(1687303715)},
	{(hge) LL_CONSTANT(10000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000000) + (hge) LL_CONSTANT(168730371)},
	{(hge) LL_CONSTANT(100000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000000) + (hge) LL_CONSTANT(16873037)},
	{(hge) LL_CONSTANT(1000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000000) + (hge) LL_CONSTANT(1687303)},
	{(hge) LL_CONSTANT(10000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000000) + (hge) LL_CONSTANT(168730)},
	{(hge) LL_CONSTANT(100000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100000) + (hge) LL_CONSTANT(16873)},
	{(hge) LL_CONSTANT(1000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10000) + (hge) LL_CONSTANT(1687)},
	{(hge) LL_CONSTANT(10000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(1000) + (hge) LL_CONSTANT(168)},
	{(hge) LL_CONSTANT(100000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(100) + (hge) LL_CONSTANT(16)},
	{(hge) LL_CONSTANT(1000000000000000000), (hge) LL_CONSTANT(17014118346046923173U) * LL_CONSTANT(10) + (hge) LL_CONSTANT(1)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1), (hge) LL_CONSTANT(17014118346046923173U)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10), (hge) LL_CONSTANT(1701411834604692317)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100), (hge) LL_CONSTANT(170141183460469231)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000), (hge) LL_CONSTANT(17014118346046923)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000), (hge) LL_CONSTANT(1701411834604692)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000), (hge) LL_CONSTANT(170141183460469)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000), (hge) LL_CONSTANT(17014118346046)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000), (hge) LL_CONSTANT(1701411834604)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000), (hge) LL_CONSTANT(170141183460)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000), (hge) LL_CONSTANT(17014118346)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000), (hge) LL_CONSTANT(1701411834)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000), (hge) LL_CONSTANT(170141183)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000), (hge) LL_CONSTANT(17014118)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000), (hge) LL_CONSTANT(1701411)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000), (hge) LL_CONSTANT(170141)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000), (hge) LL_CONSTANT(17014)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000), (hge) LL_CONSTANT(1701)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(100000000000000000), (hge) LL_CONSTANT(170)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(1000000000000000000), (hge) LL_CONSTANT(17)},
	{(hge) LL_CONSTANT(10000000000000000000U) * LL_CONSTANT(10000000000000000000U),(hge) LL_CONSTANT(1)},
#else
	/* maximum lng value: 9223372036854775807 (2**63-1) */
	{LL_CONSTANT(1), LL_CONSTANT(9223372036854775807)},
	{LL_CONSTANT(10), LL_CONSTANT(922337203685477580)},
	{LL_CONSTANT(100), LL_CONSTANT(92233720368547758)},
	{LL_CONSTANT(1000), LL_CONSTANT(9223372036854775)},
	{LL_CONSTANT(10000), LL_CONSTANT(922337203685477)},
	{LL_CONSTANT(100000), LL_CONSTANT(92233720368547)},
	{LL_CONSTANT(1000000), LL_CONSTANT(9223372036854)},
	{LL_CONSTANT(10000000), LL_CONSTANT(922337203685)},
	{LL_CONSTANT(100000000), LL_CONSTANT(92233720368)},
	{LL_CONSTANT(1000000000), LL_CONSTANT(9223372036)},
	{LL_CONSTANT(10000000000), LL_CONSTANT(922337203)},
	{LL_CONSTANT(100000000000), LL_CONSTANT(92233720)},
	{LL_CONSTANT(1000000000000), LL_CONSTANT(9223372)},
	{LL_CONSTANT(10000000000000), LL_CONSTANT(922337)},
	{LL_CONSTANT(100000000000000), LL_CONSTANT(92233)},
	{LL_CONSTANT(1000000000000000), LL_CONSTANT(9223)},
	{LL_CONSTANT(10000000000000000), LL_CONSTANT(922)},
	{LL_CONSTANT(100000000000000000), LL_CONSTANT(92)},
	{LL_CONSTANT(1000000000000000000), LL_CONSTANT(9)},
#endif
};
static const int maxmod10 = 7;	/* (int) (maxdiv[0].maxval % 10) */

static ssize_t
numFromStr(const char *src, size_t *len, void **dst, int tp, bool external)
{
	const char *p = src;
	size_t sz = ATOMsize(tp);
#ifdef HAVE_HGE
	hge base = 0;
#else
	lng base = 0;
#endif
	int sign = 1;

	/* a valid number has the following syntax:
	 * [-+]?[0-9]+(_[0-9]+)*([eE][0-9]+(_[0-9]+)*)?(LL)? -- PCRE syntax, or in other words
	 * optional sign, one or more digits, optional exponent, optional LL
	 * the exponent has the following syntax:
	 * lower or upper case letter E, one or more digits
	 * embedded spaces are not allowed but embedded underscores are
	 * (but not more than one consecutively)
	 * the optional LL at the end are only allowed for lng and hge
	 * values */
	atommem(sz);

	if (strNil(src)) {
		memcpy(*dst, ATOMnilptr(tp), sz);
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (!GDKisdigit(*p)) {
		switch (*p) {
		case 'n':
			if (external) {
				memcpy(*dst, ATOMnilptr(tp), sz);
				if (p[1] == 'i' && p[2] == 'l') {
					p += 3;
					return (ssize_t) (p - src);
				}
			}
			GDKerror("'%s' not a number\n", src);
			goto bailout;
		case '-':
			sign = -1;
			p++;
			break;
		case '+':
			p++;
			break;
		}
		if (!GDKisdigit(*p)) {
			GDKerror("'%s' not a number\n", src);
			goto bailout;
		}
	}
	do {
		int dig = base10(*p);
		if (base > maxdiv[1].maxval ||
		    (base == maxdiv[1].maxval && dig > maxmod10)) {
			/* overflow */
			goto overflow;
		}
		base = 10 * base + dig;
		p++;
		if (*p == '_' && GDKisdigit(p[1]))
			p++;
	} while (GDKisdigit(*p));
	if ((*p == 'e' || *p == 'E') && GDKisdigit(p[1])) {
		p++;
		if (base == 0) {
			/* if base is 0, any exponent will do, the
			 * result is still 0 */
			while (GDKisdigit(*p))
				p++;
		} else {
			int exp = 0;
			do {
				/* this calculation cannot overflow */
				exp = exp * 10 + base10(*p);
				if (exp >= (int) (sizeof(maxdiv) / sizeof(maxdiv[0]))) {
					/* overflow */
					goto overflow;
				}
				p++;
				if (*p == '_' && GDKisdigit(p[1]))
					p++;
			} while (GDKisdigit(*p));
			if (base > maxdiv[exp].maxval) {
				/* overflow */
				goto overflow;
			}
			base *= maxdiv[exp].scale;
		}
	}
	base *= sign;
	switch (sz) {
	case 1: {
		bte **dstbte = (bte **) dst;
		if (base < GDK_bte_min || base > GDK_bte_max) {
			goto overflow;
		}
		**dstbte = (bte) base;
		break;
	}
	case 2: {
		sht **dstsht = (sht **) dst;
		if (base < GDK_sht_min || base > GDK_sht_max) {
			goto overflow;
		}
		**dstsht = (sht) base;
		break;
	}
	case 4: {
		int **dstint = (int **) dst;
		if (base < GDK_int_min || base > GDK_int_max) {
			goto overflow;
		}
		**dstint = (int) base;
		break;
	}
	case 8: {
		lng **dstlng = (lng **) dst;
#ifdef HAVE_HGE
		if (base < GDK_lng_min || base > GDK_lng_max) {
			goto overflow;
		}
#endif
		**dstlng = (lng) base;
		if (p[0] == 'L' && p[1] == 'L')
			p += 2;
		break;
	}
#ifdef HAVE_HGE
	case 16: {
		hge **dsthge = (hge **) dst;
		**dsthge = (hge) base;
		if (p[0] == 'L' && p[1] == 'L')
			p += 2;
		break;
	}
#endif
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);

  overflow:
	while (GDKisdigit(*p))
		p++;
	GDKerror("overflow: \"%.*s\" does not fit in %s\n",
		 (int) (p - src), src, ATOMname(tp));
  bailout:
	memcpy(*dst, ATOMnilptr(tp), sz);
	return -1;
}

ssize_t
bteFromStr(const char *src, size_t *len, bte **dst, bool external)
{
	return numFromStr(src, len, (void **) dst, TYPE_bte, external);
}

ssize_t
shtFromStr(const char *src, size_t *len, sht **dst, bool external)
{
	return numFromStr(src, len, (void **) dst, TYPE_sht, external);
}

ssize_t
intFromStr(const char *src, size_t *len, int **dst, bool external)
{
	return numFromStr(src, len, (void **) dst, TYPE_int, external);
}

ssize_t
lngFromStr(const char *src, size_t *len, lng **dst, bool external)
{
	return numFromStr(src, len, (void **) dst, TYPE_lng, external);
}

#ifdef HAVE_HGE
ssize_t
hgeFromStr(const char *src, size_t *len, hge **dst, bool external)
{
	return numFromStr(src, len, (void **) dst, TYPE_hge, external);
}
#endif

#define atom_io(TYPE, NAME, CAST)					\
static TYPE *								\
TYPE##Read(TYPE *A, size_t *dstlen, stream *s, size_t cnt)		\
{									\
	TYPE *a = A;							\
	if (a == NULL || *dstlen < cnt * sizeof(TYPE)) {		\
		if ((a = GDKrealloc(a, cnt * sizeof(TYPE))) == NULL)	\
			return NULL;					\
		*dstlen = cnt * sizeof(TYPE);				\
	}								\
	if (mnstr_read##NAME##Array(s, (CAST *) a, cnt) == 0 ||		\
	    mnstr_errnr(s) != MNSTR_NO__ERROR) {			\
		if (a != A)						\
			GDKfree(a);					\
		return NULL;						\
	}								\
	return a;							\
}									\
static gdk_return							\
TYPE##Write(const TYPE *a, stream *s, size_t cnt)			\
{									\
	return mnstr_write##NAME##Array(s, (const CAST *) a, cnt) ?	\
		GDK_SUCCEED : GDK_FAIL;					\
}

static gdk_return
mskWrite(const msk *a, stream *s, size_t cnt)
{
	if (cnt == 0)
		return GDK_SUCCEED;
	if (cnt == 1)
		return mnstr_writeBte(s, (int8_t) *a) ? GDK_SUCCEED : GDK_FAIL;
	return GDK_FAIL;
}

static void *
mskRead(msk *A, size_t *dstlen, stream *s, size_t cnt)
{
	int8_t v;
	msk *a = A;
	if (cnt != 1)
		return NULL;
	if (a == NULL || *dstlen == 0) {
		if ((a = GDKrealloc(a, 1)) == NULL)
			return NULL;
		*dstlen = 1;
	}
	if (mnstr_readBte(s, &v) != 1) {
		if (a != A)
			GDKfree(a);
		return NULL;
	}
	*a = v != 0;
	return a;
}

atom_io(bit, Bte, bte)

atomtostr(bte, "%hhd", )
atom_io(bte, Bte, bte)

atomtostr(sht, "%hd", )
atom_io(sht, Sht, sht)

atomtostr(int, "%d", )
atom_io(int, Int, int)

atomtostr(lng, LLFMT, )
atom_io(lng, Lng, lng)

#ifdef HAVE_HGE
#define HGE_LL018FMT "%018" PRId64
#define HGE_LL18DIGITS LL_CONSTANT(1000000000000000000)
#define HGE_ABS(a) (((a) < 0) ? -(a) : (a))
ssize_t
hgeToStr(char **dst, size_t *len, const hge *src, bool external)
{
	atommem(hgeStrlen);
	if (is_hge_nil(*src)) {
		if (external) {
			assert(*len >= strlen("nil") + 1);
			strcpy(*dst, "nil");
			return 3;
		}
		assert(*len >= strlen(str_nil) + 1);
		strcpy(*dst, str_nil);
		return 1;
	}
	if ((hge) GDK_lng_min <= *src && *src <= (hge) GDK_lng_max) {
		lng s = (lng) *src;
		return lngToStr(dst, len, &s, external);
	} else {
		hge s = *src / HGE_LL18DIGITS;
		ssize_t llen = hgeToStr(dst, len, &s, external);
		if (llen < 0)
			return llen;
		snprintf(*dst + llen, *len - llen, HGE_LL018FMT,
			 (lng) HGE_ABS(*src % HGE_LL18DIGITS));
		return strlen(*dst);
	}
}
atom_io(hge, Hge, hge)
#endif

ssize_t
ptrFromStr(const char *src, size_t *len, ptr **dst, bool external)
{
	size_t base = 0;
	const char *p = src;

	atommem(sizeof(ptr));

	**dst = ptr_nil;
	if (strNil(src))
		return 1;

	while (GDKisspace(*p))
		p++;
	if (external && strncmp(p, "nil", 3) == 0) {
		p += 3;
	} else {
		if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
			p += 2;
		}
		if (!GDKisxdigit(*p)) {
			GDKerror("'%s' not a number\n", src);
			return -1;
		}
		while (GDKisxdigit(*p)) {
			if (base >= ((size_t) 1 << (8 * sizeof(size_t) - 4))) {
				GDKerror("'%s' overflow\n", src);
				return -1;
			}
			base = mult16(base) + base16(*p);
			p++;
		}
		**dst = (ptr) base;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

#ifdef _MSC_VER
/* Windows doesn't put 0x in front whereas Linux does, so we do it ourselves */
atomtostr(ptr, "0x%p", )
#else
atomtostr(ptr, "%p", )
#endif

#if SIZEOF_VOID_P == SIZEOF_INT
atom_io(ptr, Int, int)
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
atom_io(ptr, Lng, lng)
#endif

ssize_t
dblFromStr(const char *src, size_t *len, dbl **dst, bool external)
{
	const char *p = src;
	ssize_t n = 0;
	double d;

	/* alloc memory */
	atommem(sizeof(dbl));

	if (strNil(src)) {
		**dst = dbl_nil;
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (external && strncmp(p, "nil", 3) == 0) {
		**dst = dbl_nil;
		p += 3;
		n = (ssize_t) (p - src);
	} else {
		/* on overflow, strtod returns HUGE_VAL and sets
		 * errno to ERANGE; on underflow, it returns a value
		 * whose magnitude is no greater than the smallest
		 * normalized double, and may or may not set errno to
		 * ERANGE.  We accept underflow, but not overflow. */
		char *pe;
		errno = 0;
		d = strtod(p, &pe);
		if (p == pe)
			p = src; /* nothing converted */
		else
			p = pe;
		n = (ssize_t) (p - src);
		if (n == 0 || (errno == ERANGE && (d < -1 || d > 1))
		    || !isfinite(d) /* no NaN or Infinite */
		    ) {
			GDKerror("'%s' overflow or not a number\n", src);
			return -1;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			**dst = (dbl) d;
		}
	}
	return n;
}

ssize_t
dblToStr(char **dst, size_t *len, const dbl *src, bool external)
{
	int l = 0;

	atommem(dblStrlen);
	if (is_dbl_nil(*src)) {
		if (external) {
			strcpy(*dst, "nil");
			return 3;
		}
		strcpy(*dst, str_nil);
		return 1;
	}
	if (*src <= (dbl) 999999999999999 &&
	    *src >= (dbl) -999999999999999 &&
	    (dbl) (int) *src == *src) {
		l = snprintf(*dst, *len, "%.0f", *src);
		if (strtod(*dst, NULL) == *src)
			return (ssize_t) l;
	}
	for (int i = 4; i < 18; i++) {
		l = snprintf(*dst, *len, "%.*g", i, *src);
		if (strtod(*dst, NULL) == *src)
			break;
	}
	return (ssize_t) l;
}

atom_io(dbl, Lng, lng)

ssize_t
fltFromStr(const char *src, size_t *len, flt **dst, bool external)
{
	const char *p = src;
	ssize_t n = 0;
	float f;

	/* alloc memory */
	atommem(sizeof(flt));

	if (strNil(src)) {
		**dst = flt_nil;
		return 1;
	}

	while (GDKisspace(*p))
		p++;
	if (external && strncmp(p, "nil", 3) == 0) {
		**dst = flt_nil;
		p += 3;
		n = (ssize_t) (p - src);
	} else {
		/* on overflow, strtof returns HUGE_VALF and sets
		 * errno to ERANGE; on underflow, it returns a value
		 * whose magnitude is no greater than the smallest
		 * normalized float, and may or may not set errno to
		 * ERANGE.  We accept underflow, but not overflow. */
		char *pe;
		errno = 0;
		f = strtof(p, &pe);
		if (p == pe)
			p = src; /* nothing converted */
		else
			p = pe;
		n = (ssize_t) (p - src);
		if (n == 0 || (errno == ERANGE && (f < -1 || f > 1))
		    || !isfinite(f) /* no NaN or infinite */) {
			GDKerror("'%s' overflow or not a number\n", src);
			return -1;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			if (f == -0)
				f = 0;
			**dst = (flt) f;
		}
	}
	return n;
}

ssize_t
fltToStr(char **dst, size_t *len, const flt *src, bool external)
{
	int l = 0;

	atommem(fltStrlen);
	if (is_flt_nil(*src)) {
		if (external) {
			strcpy(*dst, "nil");
			return 3;
		}
		strcpy(*dst, str_nil);
		return 1;
	}
	if (*src <= (flt) 9999999 &&
	    *src >= (flt) -9999999 &&
	    (flt) (int) *src == *src) {
		l = snprintf(*dst, *len, "%.0f", *src);
		if (strtof(*dst, NULL) == *src)
			return (ssize_t) l;
	}
	for (int i = 4; i < 10; i++) {
		l = snprintf(*dst, *len, "%.*g", i, *src);
		if (strtof(*dst, NULL) == *src)
			break;
	}
	return (ssize_t) l;
}

atom_io(flt, Int, int)


/*
 * String conversion routines.
 */
ssize_t
OIDfromStr(const char *src, size_t *len, oid **dst, bool external)
{
#if SIZEOF_OID == SIZEOF_INT
	int ui = 0, *uip = &ui;
#else
	lng ui = 0, *uip = &ui;
#endif
	size_t l = sizeof(ui);
	ssize_t pos = 0;
	const char *p = src;

	atommem(sizeof(oid));

	**dst = oid_nil;
	if (strNil(src))
		return 1;

	while (GDKisspace(*p))
		p++;

	if (external && strncmp(p, "nil", 3) == 0)
		return (ssize_t) (p - src) + 3;

	if (*p >= '0' && *p <= '9') {
#if SIZEOF_OID == SIZEOF_INT
		pos = intFromStr(p, &l, &uip, external);
#else
		pos = lngFromStr(p, &l, &uip, external);
#endif
		if (pos < 0)
			return pos;
		if (p[pos] == '@') {
			pos++;
			while (p[pos] >= '0' && p[pos] <= '9')
				pos++;
		}
		if (ui >= 0) {
			**dst = ui;
		}
		p += pos;
	} else {
		GDKerror("'%s' not an OID\n", src);
		return -1;
	}
	while (GDKisspace(*p))
		p++;
	return (ssize_t) (p - src);
}

ssize_t
OIDtoStr(char **dst, size_t *len, const oid *src, bool external)
{
	atommem(oidStrlen);

	if (is_oid_nil(*src)) {
		if (external) {
			strcpy(*dst, "nil");
			return 3;
		}
		strcpy(*dst, str_nil);
		return 1;
	}
	return snprintf(*dst, *len, OIDFMT "@0", *src);
}

static int
UUIDcompare(const void *L, const void *R)
{
	const uuid *l = L, *r = R;
	return memcmp(l->u, r->u, UUID_SIZE);
}

static ssize_t
UUIDfromString(const char *svalue, size_t *len, void **RETVAL, bool external)
{
	uuid **retval = (uuid **) RETVAL;
	const char *s = svalue;

	if (*len < UUID_SIZE || *retval == NULL) {
		GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_SIZE)) == NULL)
			return -1;
		*len = UUID_SIZE;
	}
	if (external && strcmp(svalue, "nil") == 0) {
		**retval = uuid_nil;
		return 3;
	}
	if (strNil(svalue)) {
		**retval = uuid_nil;
		return 1;
	}
	while (GDKisspace(*s))
		s++;
	/* we don't use uuid_parse since we accept UUIDs without hyphens */
	uuid u;
	for (int i = 0, j = 0; i < UUID_SIZE; i++) {
		/* on select locations we allow a '-' in the source string */
		if (j == 8 || j == 12 || j == 16 || j == 20) {
			if (*s == '-')
				s++;
		}
		if (*s >= '0' && *s <= '9')
			u.u[i] = *s - '0';
		else if ('a' <= *s && *s <= 'f')
			u.u[i] = *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			u.u[i] = *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
		u.u[i] <<= 4;
		if (*s >= '0' && *s <= '9')
			u.u[i] |= *s - '0';
		else if ('a' <= *s && *s <= 'f')
			u.u[i] |= *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			u.u[i] |= *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
	}
	while (GDKisspace(*s))
		s++;
	if (*s != 0)
		goto bailout;
	**retval = u;
	return (ssize_t) (s - svalue);

  bailout:
	**retval = uuid_nil;
	return -1;
}

static BUN
UUIDhash(const void *v)
{
	return mix_uuid((const uuid *) v);
}

static void *
UUIDread(void *U, size_t *dstlen, stream *s, size_t cnt)
{
	uuid *u = U;
	if (u == NULL || *dstlen < cnt * sizeof(uuid)) {
		if ((u = GDKrealloc(u, cnt * sizeof(uuid))) == NULL)
			return NULL;
		*dstlen = cnt * sizeof(uuid);
	}
	if (mnstr_read(s, u, UUID_SIZE, cnt) < (ssize_t) cnt) {
		if (u != U)
			GDKfree(u);
		return NULL;
	}
	return u;
}

static gdk_return
UUIDwrite(const void *u, stream *s, size_t cnt)
{
	return mnstr_write(s, u, UUID_SIZE, cnt) ? GDK_SUCCEED : GDK_FAIL;
}

static ssize_t
UUIDtoString(str *retval, size_t *len, const void *VALUE, bool external)
{
	const uuid *value = VALUE;
	if (*len <= UUID_STRLEN || *retval == NULL) {
		if (*retval)
			GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_STRLEN + 1)) == NULL)
			return -1;
		*len = UUID_STRLEN + 1;
	}
	if (is_uuid_nil(*value)) {
		if (external) {
			assert(*len >= strlen("nil") + 1);
			strcpy(*retval, "nil");
			return 3;
		}
		assert(*len >= strlen(str_nil) + 1);
		strcpy(*retval, str_nil);
		return 1;
	}
	snprintf(*retval, *len,
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x"
			 "-%02x%02x-%02x%02x%02x%02x%02x%02x",
			 value->u[0], value->u[1], value->u[2], value->u[3],
			 value->u[4], value->u[5], value->u[6], value->u[7],
			 value->u[8], value->u[9], value->u[10], value->u[11],
			 value->u[12], value->u[13], value->u[14], value->u[15]);
	assert(strlen(*retval) == UUID_STRLEN);
	return UUID_STRLEN;
}

static const blob blob_nil = {
	~(size_t) 0
};

size_t
blobsize(size_t nitems)
{
	if (nitems == ~(size_t) 0)
		nitems = 0;
	assert(offsetof(blob, data) + nitems <= VAR_MAX);
	return (size_t) (offsetof(blob, data) + nitems);
}

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
	size_t len = blobsize(a->nitems);

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
	size_t l = blobsize(p->nitems); /* 64bit: check for overflow */
	assert(l <= (size_t) GDK_int_max);
	return l;
}

static gdk_return
BLOBheap(Heap *heap, size_t capacity)
{
	return HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

static var_t
BLOBput(BAT *b, var_t *bun, const void *VAL)
{
	const blob *val = VAL;
	char *base = NULL;

	*bun = HEAP_malloc(b, blobsize(val->nitems));
	base = b->tvheap->base;
	if (*bun != (var_t) -1) {
		memcpy(&base[*bun], val, blobsize(val->nitems));
		b->tvheap->dirty = true;
	}
	return *bun;
}

static ssize_t
BLOBtostr(str *tostr, size_t *l, const void *P, bool external)
{
	static const char hexit[] = "0123456789ABCDEF";
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
		*s++ = hexit[(p->data[i] >> 4) & 15];
		*s++ = hexit[p->data[i] & 15];
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
	size_t nbytes;
	blob *result;
	const char *s = instr;

	if (strNil(instr) || (external && strncmp(instr, "nil", 3) == 0)) {
		nbytes = blobsize(0);
		if (*l < nbytes || *val == NULL) {
			GDKfree(*val);
			if ((*val = GDKmalloc(nbytes)) == NULL)
				return -1;
		}
		**val = blob_nil;
		return strNil(instr) ? 1 : 3;
	}

	/* count hexits and check for hexits/space */
	for (i = nitems = 0; instr[i]; i++) {
		if (GDKisxdigit(instr[i]))
			nitems++;
		else if (!GDKisspace(instr[i])) {
			GDKerror("Illegal char '%c' in blob\n", instr[i]);
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
		*l = nbytes;
	}
	result = *val;
	result->nitems = nitems;

	/*
	   // Read the values of the blob.
	 */
	for (i = 0; i < nitems; ++i) {
		int res = 0;

		for (;;) {
			if (*s >= '0' && *s <= '9') {
				res = *s - '0';
			} else if (*s >= 'A' && *s <= 'F') {
				res = 10 + *s - 'A';
			} else if (*s >= 'a' && *s <= 'f') {
				res = 10 + *s - 'a';
			} else {
				assert(GDKisspace(*s));
				s++;
				continue;
			}
			break;
		}
		s++;
		res <<= 4;
		for (;;) {
			if (*s >= '0' && *s <= '9') {
				res += *s - '0';
			} else if (*s >= 'A' && *s <= 'F') {
				res += 10 + *s - 'A';
			} else if (*s >= 'a' && *s <= 'f') {
				res += 10 + *s - 'a';
			} else {
				assert(GDKisspace(*s));
				s++;
				continue;
			}
			break;
		}
		s++;

		result->data[i] = (uint8_t) res;
	}
	while (GDKisspace(*s))
		s++;

	return (ssize_t) (s - instr);
}

atomDesc BATatoms[MAXATOMS] = {
	[TYPE_void] = {
		.name = "void",
		.storage = TYPE_void,
		.linear = true,
#if SIZEOF_OID == SIZEOF_INT
		.atomNull = (void *) &int_nil,
		.atomCmp = (int (*)(const void *, const void *)) intCmp,
		.atomHash = (BUN (*)(const void *)) intHash,
#else
		.atomNull = (void *) &lng_nil,
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
#endif
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) OIDfromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) OIDtoStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) voidRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) voidWrite,
	},
	[TYPE_bit] = {
		.name = "bit",
		.storage = TYPE_bte,
		.linear = true,
		.size = sizeof(bit),
		.atomNull = (void *) &bte_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) bitFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) bitToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) bitRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) bitWrite,
		.atomCmp = (int (*)(const void *, const void *)) bteCmp,
		.atomHash = (BUN (*)(const void *)) bteHash,
	},
	[TYPE_msk] = {
		.name = "msk",
		.storage = TYPE_msk,
		.linear = false,
		.size = 1,	/* really 1/8 */
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) mskFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) mskToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) mskRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) mskWrite,
		.atomCmp = (int (*)(const void *, const void *)) mskCmp,
	},
	[TYPE_bte] = {
		.name = "bte",
		.storage = TYPE_bte,
		.linear = true,
		.size = sizeof(bte),
		.atomNull = (void *) &bte_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) bteFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) bteToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) bteRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) bteWrite,
		.atomCmp = (int (*)(const void *, const void *)) bteCmp,
		.atomHash = (BUN (*)(const void *)) bteHash,
	},
	[TYPE_sht] = {
		.name = "sht",
		.storage = TYPE_sht,
		.linear = true,
		.size = sizeof(sht),
		.atomNull = (void *) &sht_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) shtFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) shtToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) shtRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) shtWrite,
		.atomCmp = (int (*)(const void *, const void *)) shtCmp,
		.atomHash = (BUN (*)(const void *)) shtHash,
	},
	[TYPE_int] = {
		.name = "int",
		.storage = TYPE_int,
		.linear = true,
		.size = sizeof(int),
		.atomNull = (void *) &int_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) intFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) intToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) intRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) intWrite,
		.atomCmp = (int (*)(const void *, const void *)) intCmp,
		.atomHash = (BUN (*)(const void *)) intHash,
	},
	[TYPE_oid] = {
		.name = "oid",
		.linear = true,
		.size = sizeof(oid),
#if SIZEOF_OID == SIZEOF_INT
		.storage = TYPE_int,
		.atomNull = (void *) &int_nil,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) intRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) intWrite,
		.atomCmp = (int (*)(const void *, const void *)) intCmp,
		.atomHash = (BUN (*)(const void *)) intHash,
#else
		.storage = TYPE_lng,
		.atomNull = (void *) &lng_nil,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) lngRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) lngWrite,
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
#endif
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) OIDfromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) OIDtoStr,
	},
	[TYPE_ptr] = {
		.name = "ptr",
		.storage = TYPE_ptr,
		.linear = true,
		.size = sizeof(void *),
		.atomNull = (void *) &ptr_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) ptrFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) ptrToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) ptrRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) ptrWrite,
#if SIZEOF_VOID_P == SIZEOF_INT
		.atomCmp = (int (*)(const void *, const void *)) intCmp,
		.atomHash = (BUN (*)(const void *)) intHash,
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
#endif
	},
	[TYPE_flt] = {
		.name = "flt",
		.storage = TYPE_flt,
		.linear = true,
		.size = sizeof(flt),
		.atomNull = (void *) &flt_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) fltFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) fltToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) fltRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) fltWrite,
		.atomCmp = (int (*)(const void *, const void *)) fltCmp,
		.atomHash = (BUN (*)(const void *)) fltHash,
	},
	[TYPE_dbl] = {
		.name = "dbl",
		.storage = TYPE_dbl,
		.linear = true,
		.size = sizeof(dbl),
		.atomNull = (void *) &dbl_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) dblFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) dblToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) dblRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) dblWrite,
		.atomCmp = (int (*)(const void *, const void *)) dblCmp,
		.atomHash = (BUN (*)(const void *)) dblHash,
	},
	[TYPE_lng] = {
		.name = "lng",
		.storage = TYPE_lng,
		.linear = true,
		.size = sizeof(lng),
		.atomNull = (void *) &lng_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) lngFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) lngToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) lngRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) lngWrite,
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
	},
#ifdef HAVE_HGE
	[TYPE_hge] = {
		.name = "hge",
		.storage = TYPE_hge,
		.linear = true,
		.size = sizeof(hge),
		.atomNull = (void *) &hge_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) hgeFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) hgeToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) hgeRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) hgeWrite,
		.atomCmp = (int (*)(const void *, const void *)) hgeCmp,
		.atomHash = (BUN (*)(const void *)) hgeHash,
	},
#endif
	[TYPE_date] = {
		.name = "date",
		.storage = TYPE_int,
		.linear = true,
		.size = sizeof(int),
		.atomNull = (void *) &int_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) date_fromstr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) date_tostr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) intRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) intWrite,
		.atomCmp = (int (*)(const void *, const void *)) intCmp,
		.atomHash = (BUN (*)(const void *)) intHash,
	},
	[TYPE_daytime] = {
		.name = "daytime",
		.storage = TYPE_lng,
		.linear = true,
		.size = sizeof(lng),
		.atomNull = (void *) &lng_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) daytime_tz_fromstr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) daytime_tostr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) lngRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) lngWrite,
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
	},
	[TYPE_timestamp] = {
		.name = "timestamp",
		.storage = TYPE_lng,
		.linear = true,
		.size = sizeof(lng),
		.atomNull = (void *) &lng_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) timestamp_fromstr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) timestamp_tostr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) lngRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) lngWrite,
		.atomCmp = (int (*)(const void *, const void *)) lngCmp,
		.atomHash = (BUN (*)(const void *)) lngHash,
	},
	[TYPE_uuid] = {
		.name = "uuid",
		.storage = TYPE_uuid,
		.linear = true,
		.size = sizeof(uuid),
		.atomNull = (void *) &uuid_nil,
		.atomFromStr = UUIDfromString,
		.atomToStr = UUIDtoString,
		.atomRead = UUIDread,
		.atomWrite = UUIDwrite,
		.atomCmp = UUIDcompare,
		.atomHash = UUIDhash,
	},
	[TYPE_str] = {
		.name = "str",
		.storage = TYPE_str,
		.linear = true,
		.size = sizeof(var_t),
		.atomNull = (void *) str_nil,
		.atomFromStr = (ssize_t (*)(const char *, size_t *, void **, bool)) strFromStr,
		.atomToStr = (ssize_t (*)(char **, size_t *, const void *, bool)) strToStr,
		.atomRead = (void *(*)(void *, size_t *, stream *, size_t)) strRead,
		.atomWrite = (gdk_return (*)(const void *, stream *, size_t)) strWrite,
		.atomCmp = (int (*)(const void *, const void *)) strCmp,
		.atomHash = (BUN (*)(const void *)) strHash,
		.atomPut = strPut,
		.atomLen = (size_t (*)(const void *)) strLen,
		.atomHeap = strHeap,
	},
	[TYPE_blob] = {
		.name = "blob",
		.storage = TYPE_blob,
		.linear = true,
		.size = sizeof(var_t),
		.atomNull = (void *) &blob_nil,
		.atomFromStr = BLOBfromstr,
		.atomToStr = BLOBtostr,
		.atomRead = BLOBread,
		.atomWrite = BLOBwrite,
		.atomCmp = BLOBcmp,
		.atomHash = BLOBhash,
		.atomPut = BLOBput,
		.atomDel = BLOBdel,
		.atomLen = BLOBlength,
		.atomHeap = BLOBheap,
	},
};

int GDKatomcnt = TYPE_blob + 1;

/*
 * Sometimes a bat descriptor is loaded before the dynamic module
 * defining the atom is loaded. To support this an extra set of
 * unknown atoms is kept.  These can be accessed via the ATOMunknown
 * interface. Finding an (negative) atom index can be done via
 * ATOMunknown_find, which simply adds the atom if it's not in the
 * unknown set. The index can be used to find the name of an unknown
 * ATOM via ATOMunknown_name.
 */
static str unknown[MAXATOMS] = { NULL };

int
ATOMunknown_find(const char *nme)
{
	int i, j = 0;

	/* first try to find the atom */
	MT_lock_set(&GDKatomLock);
	for (i = 1; i < MAXATOMS; i++) {
		if (unknown[i]) {
			if (strcmp(unknown[i], nme) == 0) {
				MT_lock_unset(&GDKatomLock);
				return -i;
			}
		} else if (j == 0)
			j = i;
	}
	if (j == 0) {
		/* no space for new atom (shouldn't happen) */
		MT_lock_unset(&GDKatomLock);
		return 0;
	}
	if ((unknown[j] = GDKstrdup(nme)) == NULL) {
		MT_lock_unset(&GDKatomLock);
		return 0;
	}
	MT_lock_unset(&GDKatomLock);
	return -j;
}

const char *
ATOMunknown_name(int i)
{
	assert(i < 0);
	assert(-i < MAXATOMS);
	assert(unknown[-i]);
	return unknown[-i];
}

void
ATOMunknown_clean(void)
{
	int i;

	MT_lock_set(&GDKatomLock);
	for (i = 1; i < MAXATOMS; i++) {
		if(unknown[i]) {
			GDKfree(unknown[i]);
			unknown[i] = NULL;
		} else {
			break;
		}
	}
	MT_lock_unset(&GDKatomLock);
}
