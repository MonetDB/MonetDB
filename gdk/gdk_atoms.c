/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include "gdk_private.h"
#include <math.h>		/* for isfinite macro */
#ifdef HAVE_IEEEFP_H
#include <ieeefp.h>		/* for Solaris */
#ifndef isfinite
#define isfinite(f)	finite(f)
#endif
#endif

static int
bteCmp(const bte *l, const bte *r)
{
	return simple_CMP(l, r, bte);
}

static int
shtCmp(const sht *l, const sht *r)
{
	return simple_CMP(l, r, sht);
}

static int
intCmp(const int *l, const int *r)
{
	return simple_CMP(l, r, int);
}

static int
fltCmp(const flt *l, const flt *r)
{
	return simple_CMP(l, r, flt);
}

static int
lngCmp(const lng *l, const lng *r)
{
	return simple_CMP(l, r, lng);
}

#ifdef HAVE_HGE
static int
hgeCmp(const hge *l, const hge *r)
{
	return simple_CMP(l, r, hge);
}
#endif

static int
dblCmp(const dbl *l, const dbl *r)
{
	return simple_CMP(l, r, dbl);
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

/*
 * @+ Standard Atoms
 */
static int
batFix(const bat *b)
{
	return BBPretain(*b);
}

static int
batUnfix(const bat *b)
{
	return BBPrelease(*b);
}

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
int
ATOMallocate(const char *id)
{
	int t;

	MT_lock_set(&GDKthreadLock);
	t = ATOMindex(id);

	if (t < 0) {
		t = -t;
		if (t == GDKatomcnt) {
			GDKatomcnt++;
		}
		if (GDKatomcnt == MAXATOMS)
			GDKfatal("ATOMallocate: too many types");
		if (strlen(id) >= IDLENGTH)
			GDKfatal("ATOMallocate: name too long");
		memset(BATatoms + t, 0, sizeof(atomDesc));
		snprintf(BATatoms[t].name, sizeof(BATatoms[t].name), "%s", id);
		BATatoms[t].size = sizeof(int);		/* default */
		BATatoms[t].align = sizeof(int);	/* default */
		BATatoms[t].linear = 1;			/* default */
		BATatoms[t].storage = t;		/* default */
	}
	MT_lock_unset(&GDKthreadLock);
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
	if (strcmp(nme, "bat") == 0) {
		return TYPE_bat;
	}
	return -j;
}

char *
ATOMname(int t)
{
	return t >= 0 && t < GDKatomcnt && *BATatoms[t].name ? BATatoms[t].name : "null";
}

int
ATOMisdescendant(int tpe, int parent)
{
	int cur = -1;

	while (cur != tpe) {
		cur = tpe;
		if (cur == parent)
			return TRUE;
		tpe = ATOMstorage(tpe);
	}
	return FALSE;
}


const bte bte_nil = GDK_bte_min;
const sht sht_nil = GDK_sht_min;
const int int_nil = GDK_int_min;
const flt flt_nil = GDK_flt_min;
const dbl dbl_nil = GDK_dbl_min;
const lng lng_nil = GDK_lng_min;
#ifdef HAVE_HGE
const hge hge_nil = GDK_hge_min;
#endif
const oid oid_nil = (oid) 1 << (sizeof(oid) * 8 - 1);
const char str_nil[2] = { '\200', 0 };
const ptr ptr_nil = NULL;

ptr
ATOMnil(int t)
{
	const void *src = ATOMnilptr(t);
	int len = ATOMlen(ATOMtype(t), src);
	ptr dst = GDKmalloc(len);

	if (dst)
		memcpy(dst, src, len);
	return dst;
}

/*
 * @- Atomic ADT functions
 */
int
ATOMlen(int t, const void *src)
{
	int (*l)(const void *) = BATatoms[t].atomLen;

	return l ? (*l) (src) : ATOMsize(t);
}

gdk_return
ATOMheap(int t, Heap *hp, size_t cap)
{
	void (*h) (Heap *, size_t) = BATatoms[t].atomHeap;

	if (h) {
		(*h) (hp, cap);
		if (hp->base == NULL)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

int
ATOMcmp(int t, const void *l, const void *r)
{
	switch (ATOMbasetype(t)) {
	case TYPE_bte:
		return simple_CMP(l, r, bte);
	case TYPE_sht:
		return simple_CMP(l, r, sht);
	case TYPE_int:
		return simple_CMP(l, r, int);
	case TYPE_flt:
		return simple_CMP(l, r, flt);
	case TYPE_lng:
		return simple_CMP(l, r, lng);
#ifdef HAVE_HGE
	case TYPE_hge:
		return simple_CMP(l, r, hge);
#endif
	case TYPE_dbl:
		return simple_CMP(l, r, dbl);
	default:
		return (l == r) ? 0 : atom_CMP(l, r, t);
	}
}

/*
 * Atom print avoids coercion to strings for built-in types.
 * The comparison against the NULL value is hard coded for speed.
 */
#define LINE_LEN	60

int
ATOMprint(int t, const void *p, stream *s)
{
	int (*tostr) (str *, int *, const void *);
	ssize_t res;

	if (p && t >= 0 && t < GDKatomcnt && (tostr = BATatoms[t].atomToStr)) {
		if (t != TYPE_bat && t < TYPE_str) {
			char buf[dblStrlen], *addr = buf;	/* use memory from stack */
			int sz = dblStrlen, l = (*tostr) (&addr, &sz, p);

			res = mnstr_write(s, buf, l, 1);
		} else {
			str buf = 0;
			int sz = 0, l = (*tostr) (&buf, &sz, p);

			res = mnstr_write(s, buf, l, 1);
			GDKfree(buf);
		}
	} else {
		res = mnstr_write(s, "nil", 1, 3);
	}
	if (res < 0)
		GDKsyserror("ATOMprint: write failure\n");
	return (int) res;
}


int
ATOMformat(int t, const void *p, char **buf)
{
	int (*tostr) (str *, int *, const void *);

	if (p && 0 <= t && t < GDKatomcnt && (tostr = BATatoms[t].atomToStr)) {
		int sz = 0;
		return (*tostr) (buf, &sz, p);
	}
	*buf = GDKstrdup("nil");
	if (*buf == NULL)
		return -1;
	return 3;		/* strlen(*buf) */
}

ptr
ATOMdup(int t, const void *p)
{
	int len = ATOMlen(t, p);
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
#define atommem(TYPE, size)					\
	do {							\
		if (*dst == NULL || *len < (int) (size)) {	\
			GDKfree(*dst);				\
			*len = (size);				\
			*dst = (TYPE *) GDKmalloc(*len);	\
			if (*dst == NULL)			\
				return -1;			\
		}						\
	} while (0)

#define atomtostr(TYPE, FMT, FMTCAST)			\
int							\
TYPE##ToStr(char **dst, int *len, const TYPE *src)	\
{							\
	atommem(char, TYPE##Strlen);			\
	if (*src == TYPE##_nil) {			\
		return snprintf(*dst, *len, "nil");	\
	}						\
	return snprintf(*dst, *len, FMT, FMTCAST *src);	\
}

#define num08(x)	((x) >= '0' && (x) <= '7')
#define num10(x)	GDKisdigit(x)
#define num16(x)	(GDKisdigit(x) || ((x)  >= 'a' && (x)  <= 'f') || ((x)  >= 'A' && (x)  <= 'F'))
#define base10(x)	((x) - '0')
#define base08(x)	((x) - '0')
#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult08(x)	((x) << 3)
#define mult16(x)	((x) << 4)

#if 0
int
voidFromStr(const char *src, int *len, void **dst)
{
	(void) src;
	(void) len;
	(void) dst;
	return 0;
}

int
voidToStr(str *dst, int *len, void *src)
{
	(void) src;

	atommem(char, 4);
	return snprintf(*dst, *len, "nil");
}
#endif

static void *
voidRead(void *a, stream *s, size_t cnt)
{
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
int
bitFromStr(const char *src, int *len, bit **dst)
{
	const char *p = src;

	atommem(bit, sizeof(bit));

	while (GDKisspace(*p))
		p++;
	**dst = bit_nil;
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
	} else if (strncasecmp(p, "nil",   3) == 0) {
		p += 3;
	} else {
		p = src;
	}
	while (GDKisspace(*p))
		p++;
	return (int) (p - src);
}

int
bitToStr(char **dst, int *len, const bit *src)
{
	atommem(char, 6);

	if (*src == bit_nil)
		return snprintf(*dst, *len, "nil");
	if (*src)
		return snprintf(*dst, *len, "true");
	return snprintf(*dst, *len, "false");
}

int
batFromStr(const char *src, int *len, bat **dst)
{
	char *s;
	const char *t, *r = src;
	int c;
	bat bid = 0;

	atommem(bat, sizeof(bat));

	while (GDKisspace(*r))
		r++;
	if (*r == '<')
		r++;
	t = r;
	while ((c = *t) && (c == '_' || GDKisalnum(c)))
		t++;

	if (strcmp(r, "nil") == 0) {
		**dst = 0;
		return (int) (t + (c == '>') - src);
	}

	s = GDKmalloc((unsigned) (1 + t - r));
	if (s != NULL) {
		strncpy(s, r, t - r);
		s[t - r] = 0;
		bid = BBPindex(s);
		GDKfree(s);
	}
	**dst = bid == 0 ? bat_nil : bid;
	return bid == 0 ? 0 : (int) (t + (c == '>') - src);
}

int
batToStr(char **dst, int *len, const bat *src)
{
	bat b = *src;
	int i;
	str s;

	if (b == bat_nil || (s = BBPname(b)) == NULL || *s == 0) {
		atommem(char, 4);
		return snprintf(*dst, *len, "nil");
	}
	i = (int) (strlen(s) + 4);
	atommem(char, i);
	return snprintf(*dst, *len, "<%s>", s);
}


/*
 * numFromStr parses the head of the string for a number, accepting an
 * optional sign. The code has been prepared to continue parsing by
 * returning the number of characters read.  Both overflow and
 * incorrect syntax (not a number) result in the function returning 0
 * and setting the destination to nil.
 */
static int
numFromStr(const char *src, int *len, void **dst, int tp)
{
	const char *p = src;
	int sz = ATOMsize(tp);
#ifdef HAVE_HGE
	hge base = 0;
	hge expbase = -1;
	const hge maxdiv10 = GDK_hge_max / 10;
#else
	lng base = 0;
	lng expbase = -1;
	const lng maxdiv10 = LL_CONSTANT(922337203685477580); /*7*/
#endif
	const int maxmod10 = 7;	/* max value % 10 */
	int sign = 1;

	atommem(void, sz);
	while (GDKisspace(*p))
		p++;
	if (!num10(*p)) {
		switch (*p) {
		case 'n':
			memcpy(*dst, ATOMnilptr(tp), sz);
			if (p[1] == 'i' && p[2] == 'l') {
				p += 3;
				return (int) (p - src);
			}
			/* not a number */
			return 0;
		case '-':
			sign = -1;
			p++;
			break;
		case '+':
			p++;
			break;
		}
		if (!num10(*p)) {
			/* still not a number */
			memcpy(*dst, ATOMnilptr(tp), sz);
			return 0;
		}
	}
	do {
		if (base > maxdiv10 ||
		    (base == maxdiv10 && base10(*p) > maxmod10)) {
			/* overflow */
			memcpy(*dst, ATOMnilptr(tp), sz);
			return 0;
		}
		base = 10 * base + base10(*p);
		p++;
		/* Special case: xEy = x*10^y handling part 1 */
		if (*p == 'E' || *p == 'e') {
			// if there is a second E in the string we give up
			if (expbase > -1) {
				memcpy(*dst, ATOMnilptr(tp), sz);
				return 0;
			}
			expbase = base;
			base = 0;
			p++;
		}
	} while (num10(*p));
	/* Special case: xEy = x*10^y handling part 2 */
	if (expbase > -1) {
#ifdef HAVE_HGE
		hge res = expbase;
#else
		lng res = expbase;
#endif
		while (base > 0) {
			if (res > maxdiv10) {
				memcpy(*dst, ATOMnilptr(tp), sz);
				return 0;
			}
			res *= 10L;
			base--;
		}
		base = res;
	}
	base *= sign;

	switch (sz) {
	case 1: {
		bte **dstbte = (bte **) dst;
		if (base <= GDK_bte_min || base > GDK_bte_max) {
			**dstbte = bte_nil;
			return 0;
		}
		**dstbte = (bte) base;
		break;
	}
	case 2: {
		sht **dstsht = (sht **) dst;
		if (base <= GDK_sht_min || base > GDK_sht_max) {
			**dstsht = sht_nil;
			return 0;
		}
		**dstsht = (sht) base;
		break;
	}
	case 4: {
		int **dstint = (int **) dst;
		if (base <= GDK_int_min || base > GDK_int_max) {
			**dstint = int_nil;
			return 0;
		}
		**dstint = (int) base;
		break;
	}
	case 8: {
		lng **dstlng = (lng **) dst;
#ifdef HAVE_HGE
		if (base <= GDK_lng_min || base > GDK_lng_max) {
			**dstlng = lng_nil;
			return 0;
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
	return (int) (p - src);
}

int
bteFromStr(const char *src, int *len, bte **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_bte);
}

int
shtFromStr(const char *src, int *len, sht **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_sht);
}

int
intFromStr(const char *src, int *len, int **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_int);
}

int
lngFromStr(const char *src, int *len, lng **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_lng);
}

#ifdef HAVE_HGE
int
hgeFromStr(const char *src, int *len, hge **dst)
{
	return numFromStr(src, len, (void **) dst, TYPE_hge);
}
#endif

#define atom_io(TYPE, NAME, CAST)					\
static TYPE *								\
TYPE##Read(TYPE *A, stream *s, size_t cnt)				\
{									\
	TYPE *a = A;							\
	if (a == NULL && (a = GDKmalloc(cnt * sizeof(TYPE))) == NULL)	\
		return NULL;						\
	if (mnstr_read##NAME##Array(s, (CAST *) a, cnt) == 0 ||		\
	    mnstr_errnr(s)) {						\
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

atom_io(bat, Int, int)
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
#ifdef WIN32
#define HGE_LL018FMT "%018I64d"
#else
#define HGE_LL018FMT "%018lld"
#endif
#define HGE_LL18DIGITS LL_CONSTANT(1000000000000000000)
#define HGE_ABS(a) (((a) < 0) ? -(a) : (a))
int
hgeToStr(char **dst, int *len, const hge *src)
{
	atommem(char, hgeStrlen);
	if (*src == hge_nil) {
		strncpy(*dst, "nil", *len);
		return 3;
	}
	if ((hge) GDK_lng_min < *src && *src <= (hge) GDK_lng_max) {
		lng s = (lng) *src;
		return lngToStr(dst, len, &s);
	} else {
		hge s = *src / HGE_LL18DIGITS;
		int l = hgeToStr(dst, len, &s);
		snprintf(*dst + l, *len - l, HGE_LL018FMT, (lng) HGE_ABS(*src % HGE_LL18DIGITS));
		return (int) strlen(*dst);
	}
}
atom_io(hge, Hge, hge)
#endif

int
ptrFromStr(const char *src, int *len, ptr **dst)
{
	size_t base = 0;
	const char *p = src;

	atommem(ptr, sizeof(ptr));

	while (GDKisspace(*p))
		p++;
	**dst = ptr_nil;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		p += 3;
	} else {
		if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
			p += 2;
		}
		if (!num16(*p)) {
			/* not a number */
			return 0;
		}
		while (num16(*p)) {
			if (base >= ((size_t) 1 << (8 * sizeof(size_t) - 4))) {
				/* overflow */
				return 0;
			}
			base = mult16(base) + base16(*p);
			p++;
		}
		**dst = (ptr) base;
	}
	while (GDKisspace(*p))
		p++;
	return (int) (p - src);
}

atomtostr(ptr, PTRFMT, PTRFMTCAST)

#if SIZEOF_VOID_P == SIZEOF_INT
atom_io(ptr, Int, int)
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
atom_io(ptr, Lng, lng)
#endif
#if defined(_MSC_VER) && !defined(isfinite)
/* with more recent Visual Studio, isfinite is defined */
#define isfinite(x)	_finite(x)
#endif

int
dblFromStr(const char *src, int *len, dbl **dst)
{
	const char *p = src;
	int n = 0;
	double d;

	/* alloc memory */
	atommem(dbl, sizeof(dbl));

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		**dst = dbl_nil;
		p += 3;
		n = (int) (p - src);
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
		n = (int) (p - src);
		if (n == 0 || (errno == ERANGE && (d < -1 || d > 1))
#ifdef isfinite
		    || !isfinite(d) /* no NaN or Infinte */
#endif
		    ) {
			**dst = dbl_nil; /* default return value is nil */
			n = 0;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			**dst = (dbl) d;
		}
	}
	return n;
}

int
dblToStr(char **dst, int *len, const dbl *src)
{
	int i;

	atommem(char, dblStrlen);
	if (*src == dbl_nil) {
		return snprintf(*dst, *len, "nil");
	}
	for (i = 4; i < 18; i++) {
		snprintf(*dst, *len, "%.*g", i, *src);
		if (strtod(*dst, NULL) == *src)
			break;
	}
	return (int) strlen(*dst);
}

atom_io(dbl, Lng, lng)

int
fltFromStr(const char *src, int *len, flt **dst)
{
	const char *p = src;
	int n = 0;
	float f;

	/* alloc memory */
	atommem(flt, sizeof(flt));

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		**dst = flt_nil;
		p += 3;
		n = (int) (p - src);
	} else {
#ifdef HAVE_STRTOF
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
		n = (int) (p - src);
		if (n == 0 || (errno == ERANGE && (f < -1 || f > 1))
#else /* no strtof, try sscanf */
		if (sscanf(src, "%f%n", &f, &n) <= 0 || n <= 0
#endif
#ifdef isfinite
		    || !isfinite(f) /* no NaN or infinite */
#endif
		    ) {
			**dst = flt_nil; /* default return value is nil */
			n = 0;
		} else {
			while (src[n] && GDKisspace(src[n]))
				n++;
			**dst = (flt) f;
		}
	}
	return n;
}

int
fltToStr(char **dst, int *len, const flt *src)
{
	int i;

	atommem(char, fltStrlen);
	if (*src == flt_nil) {
		return snprintf(*dst, *len, "nil");
	}
	for (i = 4; i < 10; i++) {
		snprintf(*dst, *len, "%.*g", i, *src);
#ifdef HAVE_STRTOF
		if (strtof(*dst, NULL) == *src)
			break;
#else
		if ((float) strtod(*dst, NULL) == *src)
			break;
#endif
	}
	return (int) strlen(*dst);
}

atom_io(flt, Int, int)


/* String Atom Implementation
 *
 * Strings are stored in two parts.  The first part is the normal tail
 * heap which contains a list of offsets.  The second part is the
 * theap which contains the actual strings.  The offsets in the tail
 * heap (a.k.a. offset heap) point into the theap (a.k.a. string
 * heap).  Strings are NULL-terminated and are stored without any
 * escape sequec=nces.  Strings are encoded using the UTF-8 encoding
 * of Unicode.  This means that individual "characters" (really,
 * Unicode code points) can be between one and four bytes long.
 *
 * Because in many typical situations there are lots of duplicated
 * string values that are being stored in a table, but also in many
 * (other) typical situations there are very few duplicated string
 * values stored, a scheme has been introduced to cater to both
 * situations.
 *
 * When the string heap is "small" (defined as less than 64KiB), the
 * string heap is fully duplicate eliminated.  When the string heap
 * grows beyond this size, the heap is not kept free of duplicate
 * strings, but there is then a heuristic that tries to limit the
 * number of duplicates.
 *
 * This is done by having a fixed sized hash table at the start of the
 * string heap, and allocating space for collision lists in the first
 * 64KiB of the string heap.  After the first 64KiB no extra space is
 * allocated for lists, so hash collisions cannot be resolved.
 */

int
strNil(const char *s)
{
	return GDK_STRNIL(s);
}

int
strLen(const char *s)
{
	return (int) GDK_STRLEN(s);
}

static int
strCmp(const char *l, const char *r)
{
	return GDK_STRCMP(l, r);
}

int
strCmpNoNil(const unsigned char *l, const unsigned char *r)
{
	while (*l == *r) {
		if (*l == 0)
			return 0;
		l++;
		r++;
	}
	return (*l < *r) ? -1 : 1;
}

static void
strHeap(Heap *d, size_t cap)
{
	size_t size;

	cap = MAX(cap, BATTINY);
	size = GDK_STRHASHTABLE * sizeof(stridx_t) + MIN(GDK_ELIMLIMIT, cap * GDK_VARALIGN);
	if (HEAPalloc(d, size, 1) == GDK_SUCCEED) {
		d->free = GDK_STRHASHTABLE * sizeof(stridx_t);
		d->dirty = 1;
		memset(d->base, 0, d->free);
		d->hashash = 0;
#ifndef NDEBUG
		/* fill should solve initialization problems within valgrind */
		memset(d->base + d->free, 0, d->size - d->free);
#endif
	}
}


BUN
strHash(const char *s)
{
	BUN res;

	GDK_STRHASH(s, res);
	return res;
}

void
strCleanHash(Heap *h, int rebuild)
{
	size_t pad, pos;
	const size_t extralen = h->hashash ? EXTRALEN : 0;
	stridx_t *bucket;
	BUN off, strhash;
	const char *s;

	(void) rebuild;
	if (!h->cleanhash)
		return;
	h->cleanhash = 0;
	/* rebuild hash table for double elimination
	 *
	 * If appending strings to the BAT was aborted, if the heap
	 * was memory mapped, the hash in the string heap may well be
	 * incorrect.  Therefore we don't trust it when we read in a
	 * string heap and we rebuild the complete table (it is small,
	 * so this won't take any time at all).
	 * Note that we will only do this the first time the heap is
	 * loaded, and only for heaps that existed when the server was
	 * started. */
	memset(h->base, 0, GDK_STRHASHSIZE);
	pos = GDK_STRHASHSIZE;
	while (pos < h->free && pos < GDK_ELIMLIMIT) {
		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		s = h->base + pos;
		if (h->hashash)
			strhash = ((const BUN *) s)[-1];
		else
			GDK_STRHASH(s, strhash);
		off = strhash & GDK_STRHASHMASK;
		bucket = ((stridx_t *) h->base) + off;
		*bucket = (stridx_t) (pos - extralen - sizeof(stridx_t));
		pos += GDK_STRLEN(s);
	}
#ifndef NDEBUG
	if (GDK_ELIMDOUBLES(h)) {
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad + extralen;
			s = h->base + pos;
			assert(strLocate(h, s) != 0);
			pos += GDK_STRLEN(s);
		}
	}
#endif
}

/*
 * The strPut routine. The routine strLocate can be used to identify
 * the location of a string in the heap if it exists. Otherwise it
 * returns zero.
 */
var_t
strLocate(Heap *h, const char *v)
{
	stridx_t *ref, *next;
	const size_t extralen = h->hashash ? EXTRALEN : 0;

	/* search hash-table, if double-elimination is still in place */
	BUN off;
	GDK_STRHASH(v, off);
	off &= GDK_STRHASHMASK;

	/* should only use strLocate iff fully double eliminated */
	assert(GDK_ELIMBASE(h->free) == 0);

	/* search the linked list */
	for (ref = ((stridx_t *) h->base) + off; *ref; ref = next) {
		next = (stridx_t *) (h->base + *ref);
		if (GDK_STRCMP(v, (str) (next + 1) + extralen) == 0)
			return (var_t) ((sizeof(stridx_t) + *ref + extralen));	/* found */
	}
	return 0;
}

static var_t
strPut(Heap *h, var_t *dst, const char *v)
{
	size_t elimbase = GDK_ELIMBASE(h->free);
	size_t pad;
	size_t pos, len = GDK_STRLEN(v);
	const size_t extralen = h->hashash ? EXTRALEN : 0;
	stridx_t *bucket;
	BUN off, strhash;

	GDK_STRHASH(v, off);
	strhash = off;
	off &= GDK_STRHASHMASK;
	bucket = ((stridx_t *) h->base) + off;

	if (*bucket) {
		/* the hash list is not empty */
		if (*bucket < GDK_ELIMLIMIT) {
			/* small string heap (<64KiB) -- fully double
			 * eliminated: search the linked list */
			const stridx_t *ref = bucket;

			do {
				pos = *ref + sizeof(stridx_t) + extralen;
				if (GDK_STRCMP(v, h->base + pos) == 0) {
					/* found */
					return *dst = (var_t) pos;
				}
				ref = (stridx_t *) (h->base + *ref);
			} while (*ref);
		} else {
			/* large string heap (>=64KiB) -- there is no
			 * linked list, so only look at single
			 * entry */
			pos = *bucket + extralen;
			if (GDK_STRCMP(v, h->base + pos) == 0) {
				/* already in heap: reuse */
				return *dst = (var_t) pos;
			}
		}
	}
	/* the string was not found in the heap, we need to enter it */

	pad = GDK_VARALIGN - (h->free & (GDK_VARALIGN - 1));
	if (elimbase == 0) {	/* i.e. h->free < GDK_ELIMLIMIT */
		if (pad < sizeof(stridx_t)) {
			/* make room for hash link */
			pad += GDK_VARALIGN;
		}
	} else if (extralen == 0) { /* i.e., h->hashash == FALSE */
		/* no VARSHIFT and no string hash value stored => no
		 * padding/alignment needed */
		pad = 0;
	} else {
		/* pad to align on VARALIGN for VARSHIFT and/or string
		 * hash value */
		pad &= (GDK_VARALIGN - 1);
	}

	/* check heap for space (limited to a certain maximum after
	 * which nils are inserted) */
	if (h->free + pad + len + extralen >= h->size) {
		size_t newsize = MAX(h->size, 4096);

		/* double the heap size until we have enough space */
		do {
			if (newsize < 4 * 1024 * 1024)
				newsize <<= 1;
			else
				newsize += 4 * 1024 * 1024;
		} while (newsize <= h->free + pad + len + extralen);

		assert(newsize);

		if (h->free + pad + len + extralen >= (size_t) VAR_MAX) {
			GDKerror("strPut: string heaps gets larger than " SZFMT "GiB.\n", (size_t) VAR_MAX >> 30);
			return 0;
		}
		HEAPDEBUG fprintf(stderr, "#HEAPextend in strPut %s " SZFMT " " SZFMT "\n", h->filename, h->size, newsize);
		if (HEAPextend(h, newsize, TRUE) != GDK_SUCCEED) {
			return 0;
		}
#ifndef NDEBUG
		/* fill should solve initialization problems within
		 * valgrind */
		memset(h->base + h->free, 0, h->size - h->free);
#endif

		/* make bucket point into the new heap */
		bucket = ((stridx_t *) h->base) + off;
	}

	/* insert string */
	pos = h->free + pad + extralen;
	*dst = (var_t) pos;
#ifndef NDEBUG
	/* just before inserting into the heap, make sure that the
	 * string is actually UTF-8 (if we encountered a return
	 * statement before this, the string was already in the heap,
	 * and hence already checked) */
	if (v[0] != '\200' || v[1] != '\0') {
		/* not str_nil, must be UTF-8 */
		size_t i;

		for (i = 0; v[i] != '\0'; i++) {
			/* check that v[i] is the start of a validly
			 * coded UTF-8 sequence: this involves
			 * checking that the first byte is a valid
			 * start byte and is followed by the correct
			 * number of follow-up bytes, but also that
			 * the sequence cannot be shorter */
			if ((v[i] & 0x80) == 0) {
				/* 0aaaaaaa */
				continue;
			} else if ((v[i] & 0xE0) == 0xC0) {
				/* 110bbbba 10aaaaaa
				 * one of the b's must be set*/
				assert(v[i] & 0x4D);
				i++;
				assert((v[i] & 0xC0) == 0x80);
			} else if ((v[i] & 0xF0) == 0xE0) {
				/* 1110cccc 10cbbbba 10aaaaaa
				 * one of the c's must be set*/
				assert(v[i] & 0x0F || v[i + 1] & 0x20);
				i++;
				assert((v[i] & 0xC0) == 0x80);
				i++;
				assert((v[i] & 0xC0) == 0x80);
			} else if ((v[i] & 0xF8) == 0xF0) {
				/* 11110ddd 10ddcccc 10cbbbba 10aaaaaa
				 * one of the d's must be set */
				assert(v[i] & 0x07 || v[i + 1] & 0x30);
				i++;
				assert((v[i] & 0xC0) == 0x80);
				i++;
				assert((v[i] & 0xC0) == 0x80);
				i++;
				assert((v[i] & 0xC0) == 0x80);
			} else {
				/* this will fail */
				assert((v[i] & 0x80) == 0);
			}
		}
	}
#endif
	memcpy(h->base + pos, v, len);
	if (h->hashash) {
		((BUN *) (h->base + pos))[-1] = strhash;
#if EXTRALEN > SIZEOF_BUN
		((BUN *) (h->base + pos))[-2] = (BUN) len;
#endif
	}
	h->free += pad + len + extralen;
	h->dirty = 1;

	/* maintain hash table */
	pos -= extralen;
	if (elimbase == 0) {	/* small string heap: link the next pointer */
		/* the stridx_t next pointer directly precedes the
		 * string and optional (depending on hashash) hash
		 * value */
		pos -= sizeof(stridx_t);
		*(stridx_t *) (h->base + pos) = *bucket;
	}
	*bucket = (stridx_t) pos;	/* set bucket to the new string */

	return *dst;
}

/*
 * Convert an "" separated string to a GDK string value, checking that
 * the input is correct UTF-8.
 */

/*
   UTF-8 encoding is as follows:
U-00000000 - U-0000007F: 0xxxxxxx
U-00000080 - U-000007FF: 110xxxxx 10xxxxxx
U-00000800 - U-0000FFFF: 1110xxxx 10xxxxxx 10xxxxxx
U-00010000 - U-001FFFFF: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
U-00200000 - U-03FFFFFF: 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
U-04000000 - U-7FFFFFFF: 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
*/
/* To be correctly coded UTF-8, the sequence should be the shortest
 * possible encoding of the value being encoded.  This means that for
 * an encoding of length n+1 (1 <= n <= 5), at least one of the bits
 * in utf8chkmsk[n] should be non-zero (else the encoding could be
 * shorter). */
static int utf8chkmsk[] = {
	0x0000007f,
	0x00000780,
	0x0000f800,
	0x001f0000,
	0x03e00000,
	0x7c000000,
};

ssize_t
GDKstrFromStr(unsigned char *dst, const unsigned char *src, ssize_t len)
{
	unsigned char *p = dst;
	const unsigned char *cur = src, *end = src + len;
	int escaped = FALSE, mask = 0, n, c, utf8char = 0;

	/* copy it in, while performing the correct escapes */
	/* n is the number of follow-on bytes left in a multi-byte
	 * UTF-8 sequence */
	for (cur = src, n = 0; cur < end || escaped; cur++) {
		/* first convert any \ escapes and store value in c */
		if (escaped) {
			switch (*cur) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
				/* \ with up to three octal digits */
				c = base08(*cur);
				if (num08(cur[1])) {
					cur++;
					c = mult08(c) + base08(*cur);
					if (num08(cur[1])) {
						if (c > 037) {
							/* octal
							 * escape
							 * sequence
							 * out or
							 * range */
							return -1;
						}
						cur++;
						c = mult08(c) + base08(*cur);
						assert(c >= 0 && c <= 0377);
					}
				}
				break;
			case 'x':
				/* \x with one or two hexadecimal digits */
				if (num16(cur[1])) {
					cur++;
					c = base16(*cur);
					if (num16(cur[1])) {
						cur++;
						c = mult16(c) + base16(*cur);
					}
				} else
					c = 'x';
				break;
			case 'a':
				c = '\a';
				break;
			case 'b':
				c = '\b';
				break;
			case 'f':
				c = '\f';
				break;
			case 'n':
				c = '\n';
				break;
			case 'r':
				c = '\r';
				break;
			case 't':
				c = '\t';
				break;
			case '\0':
				c = '\\';
				break;
			case '\'':
			case '\\':
				/* \' and \\ can be handled by the
				 * default case */
			default:
				/* unrecognized \ escape, just copy
				 * the backslashed character */
				c = *cur;
				break;
			}
			escaped = FALSE;
		} else if ((c = *cur) == '\\') {
			escaped = TRUE;
			continue;
		}

		if (n > 0) {
			/* we're still expecting follow-up bytes in a
			 * UTF-8 sequence */
			if ((c & 0xC0) != 0x80) {
				/* incorrect UTF-8 sequence: byte is
				 * not 10xxxxxx */
				return -1;
			}
			utf8char = (utf8char << 6) | (c & 0x3F);
			n--;
			if (n == 0) {
				/* this was the last byte in the sequence */
				if ((utf8char & mask) == 0) {
					/* incorrect UTF-8 sequence:
					 * not shortest possible */
					return -1;
				}
				if (utf8char > 0x10FFFF) {
					/* incorrect UTF-8 sequence:
					 * value too large */
					return -1;
				}
				if ((utf8char & 0x1FFF800) == 0xD800) {
					/* incorrect UTF-8 sequence:
					 * low or high surrogate
					 * encoded as UTF-8 */
					return -1;
				}
			}
		} else if (c >= 0x80) {
			int m;

			/* start of multi-byte UTF-8 character */
			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				;
			/* n now is number of 10xxxxxx bytes that
			 * should follow */
			if (n == 0 || n >= 4) {
				/* incorrect UTF-8 sequence */
				/* n==0: c == 10xxxxxx */
				/* n>=4: c == 11111xxx */
				return -1;
			}
			mask = utf8chkmsk[n];
			/* collect the Unicode code point in utf8char */
			utf8char = c & ~(0xFFC0 >> n);	/* remove non-x bits */
		}
		*p++ = c;
	}
	if (n > 0) {
		/* incomplete UTF-8 sequence */
		return -1;
	}
	*p++ = 0;
	return len;
}

int
strFromStr(const char *src, int *len, char **dst)
{
	unsigned char *p;
	const unsigned char *cur = (const unsigned char *) src, *start = NULL;
	ssize_t res;
	int l = 1, escaped = FALSE;

	while (GDKisspace(*cur))
		cur++;
	if (*cur != '"') {
		if (*dst != NULL && *dst != str_nil) {
			GDKfree(*dst);
		}
		*dst = GDKstrdup(str_nil);
		*len = 2;
		return strncmp((char *) cur, "nil", 3) ? 0 : (int) (((char *) cur + 3) - src);
	}

	/* scout the string to find out its length and whether it was
	 * properly quoted */
	for (start = ++cur; *cur != '"' || escaped; cur++) {
		if (*cur == 0) {
			goto error;
		} else if (*cur == '\\' && escaped == FALSE) {
			escaped = TRUE;
		} else {
			escaped = FALSE;
			l++;
		}
	}

	/* alloc new memory */
	p = (unsigned char *) *dst;
	if (p != NULL && (char *) p != str_nil && *len < l) {
		GDKfree(p);
		p = NULL;
		*dst = NULL;
	}
	if (p == NULL || (char *) p == str_nil)
		if ((p = GDKmalloc(*len = l)) == NULL)
			goto error;
	*dst = (char *) p;

	assert(cur - start <= INT_MAX);	/* 64bit */
	if ((res = GDKstrFromStr((unsigned char *) *dst, start, (ssize_t) (cur - start))) >= 0)
		return (int) res;

      error:
	if (*dst && *dst != str_nil)
		GDKfree(*dst);
	*dst = GDKstrdup(str_nil);
	*len = 2;
	return 0;
}

/*
 * Convert a GDK string value to something printable.
 */
/* all but control characters (in range 0 to 31) and DEL */
#ifdef ASCII_CHR
/* ASCII printable characters */
#define printable_chr(ch)	(' ' <= (ch) && (ch) <= '~')
#else
/* everything except ASCII control characters */
#define printable_chr(ch)	((' ' <= (ch) && (ch) <= '~') || ((ch) & 0x80) != 0)
#endif

int
escapedStrlen(const char *src, const char *sep1, const char *sep2, int quote)
{
	int end, sz = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (end = 0; src[end]; end++)
		if (src[end] == '\\' ||
		    src[end] == quote ||
		    (sep1len && strncmp(src + end, sep1, sep1len) == 0) ||
		    (sep2len && strncmp(src + end, sep2, sep2len) == 0)) {
			sz += 2;
#ifndef ASCII_CHR
		} else if (src[end] == (char) '\302' &&
			   0200 <= ((int) src[end + 1] & 0377) &&
			   ((int) src[end + 1] & 0377) <= 0237) {
			/* Unicode control character (code point range
			 * U-00000080 through U-0000009F encoded in
			 * UTF-8 */
			/* for the first one of the two UTF-8 bytes we
			 * count a width of 7 and for the second one
			 * 1, together that's 8, i.e. the width of two
			 * backslash-escaped octal coded characters */
			sz += 7;
#endif
		} else if (!printable_chr(src[end])) {
			sz += 4;
		} else {
			sz++;
		}
	return sz;
}

int
escapedStr(char *dst, const char *src, int dstlen, const char *sep1, const char *sep2, int quote)
{
	int cur = 0, l = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (; src[cur] && l < dstlen; cur++)
		if (!printable_chr(src[cur])
#ifndef ASCII_CHR
		    || (src[cur] == '\302' &&
			0200 <= (src[cur + 1] & 0377) &&
			((int) src[cur + 1] & 0377) <= 0237)
		    || (cur > 0 &&
			src[cur - 1] == '\302' &&
			0200 <= (src[cur] & 0377) &&
			(src[cur] & 0377) <= 0237)
#endif
			) {
			dst[l++] = '\\';
			switch (src[cur]) {
			case '\t':
				dst[l++] = 't';
				break;
			case '\n':
				dst[l++] = 'n';
				break;
			case '\r':
				dst[l++] = 'r';
				break;
			case '\f':
				dst[l++] = 'f';
				break;
			default:
				snprintf(dst + l, dstlen - l, "%03o", (unsigned char) src[cur]);
				l += 3;
				break;
			}
		} else if (src[cur] == '\\' ||
			   src[cur] == quote ||
			   (sep1len && strncmp(src + cur, sep1, sep1len) == 0) ||
			   (sep2len && strncmp(src + cur, sep2, sep2len) == 0)) {
			dst[l++] = '\\';
			dst[l++] = src[cur];
		} else {
			dst[l++] = src[cur];
		}
	assert(l < dstlen);
	dst[l] = 0;
	return l;
}

static int
strToStr(char **dst, int *len, const char *src)
{
	int l = 0;

	if (GDK_STRNIL((str) src)) {
		atommem(char, 4);

		return snprintf(*dst, *len, "nil");
	} else {
		int sz = escapedStrlen(src, NULL, NULL, '"');
		atommem(char, sz + 3);
		l = escapedStr((*dst) + 1, src, *len - 1, NULL, NULL, '"');
		l++;
		(*dst)[0] = (*dst)[l++] = '"';
		(*dst)[l] = 0;
	}
	return l;
}

static str
strRead(str a, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if ((a = GDKmalloc(len + 1)) == NULL)
		return NULL;
	if (len && mnstr_read(s, a, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	a[len] = 0;
	return a;
}

static gdk_return
strWrite(const char *a, stream *s, size_t cnt)
{
	size_t len = strlen(a);

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_writeInt(s, (int) len) && mnstr_write(s, a, len, 1) == 1)
		return GDK_SUCCEED;
	else
		return GDK_FAIL;
}

/*
 * String conversion routines.
 */
int
OIDfromStr(const char *src, int *len, oid **dst)
{
#if SIZEOF_OID == SIZEOF_INT
	int ui = 0, *uip = &ui;
#else
	lng ui = 0, *uip = &ui;
#endif
	int l = (int) sizeof(ui);
	int pos = 0;
	const char *p = src;

	atommem(oid, sizeof(oid));

	**dst = oid_nil;
	while (GDKisspace(*p))
		p++;
	if (GDKisdigit(*p)) {
#if SIZEOF_OID == SIZEOF_INT
		pos = intFromStr(p, &l, &uip);
#else
		pos = lngFromStr(p, &l, &uip);
#endif
		if (pos > 0 && p[pos] == '@') {
			pos++;
			while (GDKisdigit(p[pos]))
				pos++;
		}
		if (pos > 0 && ui >= 0) {
			**dst = ui;
		}
		p += pos;
	}
	while (GDKisspace(*p))
		p++;
	return (int) (p - src);
}

int
OIDtoStr(char **dst, int *len, const oid *src)
{
	atommem(char, oidStrlen);

	if (*src == oid_nil) {
		return snprintf(*dst, *len, "nil");
	}
	return snprintf(*dst, *len, OIDFMT "@0", *src);
}

atomDesc BATatoms[MAXATOMS] = {
	{"void",		/* name */
	 TYPE_void,		/* storage */
	 1,			/* linear */
	 0,			/* size */
	 0,			/* align */
#if SIZEOF_OID == SIZEOF_INT
	 (ptr) &int_nil,	/* atomNull */
#else
	 (ptr) &lng_nil,	/* atomNull */
#endif
	 (int (*)(const char *, int *, ptr *)) OIDfromStr,    /* atomFromStr */
	 (int (*)(str *, int *, const void *)) OIDtoStr,      /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) voidRead,      /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) voidWrite, /* atomWrite */
#if SIZEOF_OID == SIZEOF_INT
	 (int (*)(const void *, const void *)) intCmp,	      /* atomCmp */
	 (BUN (*)(const void *)) intHash,		      /* atomHash */
#else
	 (int (*)(const void *, const void *)) lngCmp,	      /* atomCmp */
	 (BUN (*)(const void *)) lngHash,		      /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"bit",			/* name */
	 TYPE_bte,		/* storage */
	 1,			/* linear */
	 sizeof(bit),		/* size */
	 sizeof(bit),		/* align */
	 (ptr) &bte_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) bitFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) bitToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) bitRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) bitWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) bteCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) bteHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"bte",			/* name */
	 TYPE_bte,		/* storage */
	 1,			/* linear */
	 sizeof(bte),		/* size */
	 sizeof(bte),		/* align */
	 (ptr) &bte_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) bteFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) bteToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) bteRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) bteWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) bteCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) bteHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"sht",			/* name */
	 TYPE_sht,		/* storage */
	 1,			/* linear */
	 sizeof(sht),		/* size */
	 sizeof(sht),		/* align */
	 (ptr) &sht_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) shtFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) shtToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) shtRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) shtWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) shtCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) shtHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"BAT",			/* name */
	 TYPE_int,		/* storage */
	 1,			/* linear */
	 sizeof(bat),		/* size */
	 sizeof(bat),		/* align */
	 (ptr) &int_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) batFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) batToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) batRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) batWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) intCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) intHash,		     /* atomHash */
	 (int (*)(const void *)) batFix,		     /* atomFix */
	 (int (*)(const void *)) batUnfix,		     /* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"int",			/* name */
	 TYPE_int,		/* storage */
	 1,			/* linear */
	 sizeof(int),		/* size */
	 sizeof(int),		/* align */
	 (ptr) &int_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) intFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) intToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) intRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) intWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) intCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) intHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"oid",			/* name */
#if SIZEOF_OID == SIZEOF_INT
	 TYPE_int,		/* storage */
#else
	 TYPE_lng,		/* storage */
#endif
	 1,			/* linear */
	 sizeof(oid),		/* size */
	 sizeof(oid),		/* align */
#if SIZEOF_OID == SIZEOF_INT
	 (ptr) &int_nil,	/* atomNull */
#else
	 (ptr) &lng_nil,	/* atomNull */
#endif
	 (int (*)(const char *, int *, ptr *)) OIDfromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) OIDtoStr,     /* atomToStr */
#if SIZEOF_OID == SIZEOF_INT
	 (void *(*)(void *, stream *, size_t)) intRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) intWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) intCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) intHash,		     /* atomHash */
#else
	 (void *(*)(void *, stream *, size_t)) lngRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) lngWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) lngCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) lngHash,		     /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"ptr",			/* name */
	 TYPE_ptr,		/* storage */
	 1,			/* linear */
	 sizeof(ptr),		/* size */
	 sizeof(ptr),		/* align */
	 (ptr) &ptr_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) ptrFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) ptrToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) ptrRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) ptrWrite, /* atomWrite */
#if SIZEOF_VOID_P == SIZEOF_INT
	 (int (*)(const void *, const void *)) intCmp,       /* atomCmp */
	 (BUN (*)(const void *)) intHash,		     /* atomHash */
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
	 (int (*)(const void *, const void *)) lngCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) lngHash,		     /* atomHash */
#endif
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"flt",			/* name */
	 TYPE_flt,		/* storage */
	 1,			/* linear */
	 sizeof(flt),		/* size */
	 sizeof(flt),		/* align */
	 (ptr) &flt_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) fltFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) fltToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) fltRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) fltWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) fltCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) intHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"dbl",			/* name */
	 TYPE_dbl,		/* storage */
	 1,			/* linear */
	 sizeof(dbl),		/* size */
	 sizeof(dbl),		/* align */
	 (ptr) &dbl_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) dblFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) dblToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) dblRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) dblWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) dblCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) lngHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
	{"lng",			/* name */
	 TYPE_lng,		/* storage */
	 1,			/* linear */
	 sizeof(lng),		/* size */
	 sizeof(lng),		/* align */
	 (ptr) &lng_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) lngFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) lngToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) lngRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) lngWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) lngCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) lngHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
#ifdef HAVE_HGE
	{"hge",			/* name */
	 TYPE_hge,		/* storage */
	 1,			/* linear */
	 sizeof(hge),		/* size */
	 sizeof(hge),		/* align */
	 (ptr) &hge_nil,	/* atomNull */
	 (int (*)(const char *, int *, ptr *)) hgeFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) hgeToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) hgeRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) hgeWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) hgeCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) hgeHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 0,			/* atomPut */
	 0,			/* atomDel */
	 0,			/* atomLen */
	 0,			/* atomHeap */
	},
#endif
	{"str",			/* name */
	 TYPE_str,		/* storage */
	 1,			/* linear */
	 sizeof(var_t),		/* size */
	 sizeof(var_t),		/* align */
	 (ptr) str_nil,		/* atomNull */
	 (int (*)(const char *, int *, ptr *)) strFromStr,   /* atomFromStr */
	 (int (*)(str *, int *, const void *)) strToStr,     /* atomToStr */
	 (void *(*)(void *, stream *, size_t)) strRead,	     /* atomRead */
	 (gdk_return (*)(const void *, stream *, size_t)) strWrite, /* atomWrite */
	 (int (*)(const void *, const void *)) strCmp,	     /* atomCmp */
	 (BUN (*)(const void *)) strHash,		     /* atomHash */
	 0,			/* atomFix */
	 0,			/* atomUnfix */
	 (var_t (*)(Heap *, var_t *, const void *)) strPut,  /* atomPut */
	 0,			/* atomDel */
	 (int (*)(const void *)) strLen,		     /* atomLen */
	 strHeap,		/* atomHeap */
	},
};

int GDKatomcnt = TYPE_str + 1;

/*
 * Sometimes a bat descriptor is loaded before the dynamic module
 * defining the atom is loaded. To support this an extra set of
 * unknown atoms is kept.  These can be accessed via the ATOMunknown
 * interface. Finding an (negative) atom index can be done via
 * ATOMunknown_find, which simply adds the atom if it's not in the
 * unknown set. The index van be used to find the name of an unknown
 * ATOM via ATOMunknown_name.
 */
static str unknown[MAXATOMS] = { NULL };

int
ATOMunknown_find(const char *nme)
{
	int i, j = 0;

	/* first try to find the atom */
	MT_lock_set(&GDKthreadLock);
	for (i = 1; i < MAXATOMS; i++) {
		if (unknown[i]) {
			if (strcmp(unknown[i], nme) == 0) {
				MT_lock_unset(&GDKthreadLock);
				return -i;
			}
		} else if (j == 0)
			j = i;
	}
	if (j == 0) {
		/* no space for new atom (shouldn't happen) */
		MT_lock_unset(&GDKthreadLock);
		return 0;
	}
	if ((unknown[j] = GDKstrdup(nme)) == NULL) {
		MT_lock_unset(&GDKthreadLock);
		return 0;
	}
	MT_lock_unset(&GDKthreadLock);
	return -j;
}

str
ATOMunknown_name(int i)
{
	assert(unknown[-i]);
	return unknown[-i];
}
