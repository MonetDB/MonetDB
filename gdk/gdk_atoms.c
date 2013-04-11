/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
#include <math.h>		/* for INFINITY and NAN */

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
	return (BUN) *(const unsigned char *) v;
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
	return (BUN) mix_int(((const unsigned int *) v)[0] ^ ((const unsigned int *) v)[1]);
}

/*
 * @+ Standard Atoms
 */
static inline void
shtConvert(sht *s)
{
	*s = short_int_SWAP(*s);
}

static inline void
intConvert(int *s)
{
	*s = normal_int_SWAP(*s);
}

static inline void
lngConvert(lng *s)
{
	*s = long_long_SWAP(*s);
}

static inline int
batFix(const bat *b)
{
	return BBPincref(*b, TRUE);
}

static inline int
batUnfix(const bat *b)
{
	return BBPdecref(*b, TRUE);
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
 * ATOMproperty(id, property, function, value).  The parameter id
 * denotes the type name; an entry is created if the type is so far
 * unknown. The property argument is a string identifying the type
 * description property to be updated. Valid property names are size,
 * tostr, fromstr, put, get, cmp, eq, del, hash, null, new, and heap.
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
static int
align(int n)
{
	if (n == 0)
		return 0;
	/* successively check bits from the bottom to see if one is set */
	if (n & 1)
		return 1;
	if (n & 2)
		return 2;
	if (n & 4)
		return 4;
	return 8;
}

void
ATOMproperty(str id, str property, GDKfcn arg, int val)
{
	int t;

	MT_lock_set(&GDKthreadLock, "ATOMproperty");
	t = ATOMindex(id);

	if (t < 0) {
		t = -t;
		if (t == GDKatomcnt) {
			GDKatomcnt++;
		}
		if (GDKatomcnt == MAXATOMS)
			GDKfatal("ATOMproperty: too many types");
		if (strlen(id) >= IDLENGTH)
			GDKfatal("ATOMproperty: name too long");
		memset(BATatoms + t, 0, sizeof(atomDesc));
		strncpy(BATatoms[t].name, id, IDLENGTH);
		BATatoms[t].size = sizeof(int);		/* default */
		assert_shift_width(ATOMelmshift(BATatoms[t].size), BATatoms[t].size);
		BATatoms[t].align = sizeof(int);	/* default */
		BATatoms[t].linear = 1;			/* default */
		BATatoms[t].storage = t;		/* default */
		BATatoms[t].deleting = 1;		/* not yet usable */
	}
	if (strcmp("size", property) == 0) {
		if (val) {
			assert(val <= SHRT_MAX);
			BATatoms[t].size = val;
			assert_shift_width(ATOMelmshift(BATatoms[t].size), BATatoms[t].size);
			BATatoms[t].varsized = 0;
			BATatoms[t].align = align(val);
		} else {
			BATatoms[t].size = sizeof(var_t);
			assert_shift_width(ATOMelmshift(BATatoms[t].size), BATatoms[t].size);
			BATatoms[t].varsized = 1;
			BATatoms[t].align = sizeof(var_t);
		}
	} else if (strcmp("linear", property) == 0) {
		BATatoms[t].linear = val;
	} else if (strcmp("align", property) == 0) {
		BATatoms[t].align = val;
	} else if (strcmp("storage", property) == 0) {
		BATatoms[t] = BATatoms[val];	/* copy from example */
		strncpy(BATatoms[t].name, id, IDLENGTH); /* restore name */
		BATatoms[t].name[IDLENGTH - 1] = 0;
	} else if (strcmp("fromstr", property) == 0) {
		BATatoms[t].atomFromStr = (int (*)(const char *, int *, void **)) arg;
	} else if (strcmp("tostr", property) == 0) {
		BATatoms[t].atomToStr = (int (*)(char **, int *, const void *)) arg;
	} else if (strcmp("read", property) == 0) {
		BATatoms[t].atomRead = (void *(*)(void *, stream *, size_t)) arg;
	} else if (strcmp("write", property) == 0) {
		BATatoms[t].atomWrite = (int (*)(const void *, stream *, size_t)) arg;
	} else if (strcmp("fix", property) == 0) {
		BATatoms[t].atomFix = (int (*)(const void *)) arg;
	} else if (strcmp("unfix", property) == 0) {
		BATatoms[t].atomUnfix = (int (*)(const void *)) arg;
	} else {
#define atomset(dst,val) oldval = (ptr) dst; if (val == NULL || dst == val) goto out; dst = val;
		ptr oldval = NULL;

		if (strcmp("heap", property) == 0) {
			BATatoms[t].size = sizeof(var_t);
			assert_shift_width(ATOMelmshift(BATatoms[t].size), BATatoms[t].size);
			BATatoms[t].varsized = 1;
			BATatoms[t].align = sizeof(var_t);
			atomset(BATatoms[t].atomHeap, (void (*)(Heap *, size_t)) arg);
		} else if (strcmp("heapconvert", property) == 0) {
			atomset(BATatoms[t].atomHeapConvert, (void (*)(Heap *, int)) arg);
		} else if (strcmp("check", property) == 0) {
			atomset(BATatoms[t].atomHeapCheck, (int (*)(Heap *, HeapRepair *)) arg);
		} else if (strcmp("del", property) == 0) {
			atomset(BATatoms[t].atomDel, (void (*)(Heap *, var_t *)) arg);
		} else if (strcmp("convert", property) == 0) {
			atomset(BATatoms[t].atomConvert, (void (*)(void *, int)) arg);
		} else if (strcmp("put", property) == 0) {
			atomset(BATatoms[t].atomPut, (var_t (*)(Heap *, var_t *, const void *)) arg);
		} else if (strcmp("null", property) == 0) {
			ptr atmnull = ((ptr (*)(void)) arg) ();

			atomset(BATatoms[t].atomNull, atmnull);
		}
		if (oldval)
			goto out;

		/* these ADT functions *must* be equal for overloaded types */
		if (strcmp("cmp", property) == 0) {
			atomset(BATatoms[t].atomCmp, (int (*)(const void *, const void *)) arg);
		} else if (strcmp("hash", property) == 0) {
			atomset(BATatoms[t].atomHash, (BUN (*)(const void *)) arg);
		} else if (strcmp("length", property) == 0) {
			atomset(BATatoms[t].atomLen, (int (*)(const void *)) arg);
		}
		if (BATatoms[t].storage != t)
			GDKerror("ATOMproperty: %s overload of %s violates "
				 "inheritance from %s.\n", ATOMname(t),
				 property, ATOMname(BATatoms[t].storage));
		BATatoms[t].storage = t;	/* critical redefine: undo remapping */
	}
      out:
	MT_lock_unset(&GDKthreadLock, "ATOMproperty");
}

int
ATOMindex(str nme)
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
		tpe = BATatoms[tpe].storage;
	}
	return FALSE;
}


const bte bte_nil = GDK_bte_min;
const sht sht_nil = GDK_sht_min;
const int int_nil = GDK_int_min;
const flt flt_nil = GDK_flt_min;
const dbl dbl_nil = GDK_dbl_min;
const lng lng_nil = GDK_lng_min;
const oid oid_nil = (oid) 1 << (sizeof(oid) * 8 - 1);
const wrd wrd_nil = GDK_wrd_min;
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

int
ATOMheap(int t, Heap *hp, size_t cap)
{
	void (*h) (Heap *, size_t) = BATatoms[t].atomHeap;

	if (h) {
		(*h) (hp, cap);
		if (hp->base == NULL)
			return -1;
	}
	return 0;
}

int
ATOMcmp(int t, const void *l, const void *r)
{
	switch (ATOMstorage(t)) {
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

	if (p && (t >= 0) && (t < GDKatomcnt) && (tostr = BATatoms[t].atomToStr)) {
		if (t != TYPE_bat && t < TYPE_str) {
			char buf[dblStrlen], *addr = buf;	/* use memory from stack */
			int sz = dblStrlen, l = (*tostr) (&addr, &sz, p);

			mnstr_write(s, buf, l, 1);
			return l;
		} else {
			str buf = 0;
			int sz = 0, l = (*tostr) (&buf, &sz, p);

			l = (int) mnstr_write(s, buf, l, 1);
			GDKfree(buf);
			return l;
		}
	}
	return (int) mnstr_write(s, "nil", 1, 3);
}


int
ATOMformat(int t, const void *p, char **buf)
{
	int (*tostr) (str *, int *, const void *);

	if (p && (t >= 0) && (t < GDKatomcnt) && (tostr = BATatoms[t].atomToStr)) {
		int sz = 0, l = (*tostr) (buf, &sz, p);

		return l;
	}
	*buf = GDKmalloc(4);
	if (*buf == NULL)
		return -1;
	strncpy(*buf, "nil", 4);
	return 3;
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
#define atommem(TYPE, size)						\
	do {								\
		if (!*dst) {						\
			*dst = (TYPE *) GDKmalloc(*len = (size));	\
		} else if (*len < (int) (size)) {			\
			GDKfree(*dst);					\
			*dst = (TYPE *) GDKmalloc(*len = (size));	\
		}							\
		if (!*dst)						\
			return -1;					\
	} while (0)

#define atomtostr(TYPE, FMT, FMTCAST)			\
int							\
TYPE##ToStr(char **dst, int *len, const TYPE *src)	\
{							\
	atommem(char, TYPE##Strlen);			\
	if (*src == TYPE##_nil) {			\
		strncpy(*dst, "nil", *len);		\
		return 3;				\
	}						\
	snprintf(*dst, *len, FMT, FMTCAST *src);	\
	return (int) strlen(*dst);			\
}

#define num08(x)	((x) >= '0' && (x) <= '7')
#define num10(x)	GDKisdigit(x)
#define num16(x)	(GDKisdigit(x) || ((x)  >= 'a' && (x)  <= 'f') || ((x)  >= 'A' && (x)  <= 'F'))
#define base10(x)	((x) - '0')
#define base08(x)	((x) - '0')
#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult08(x)	((x) << 3)
#define mult16(x)	((x) << 4)
#define mult10(x)	((x) + (x) + ((x) << 3))
#define mult7(x)	(((x) << 3) - (x))

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
	strncpy(*dst, "nil", *len);
	return 3;
}
#endif

static void *
voidRead(void *a, stream *s, size_t cnt)
{
	(void) s;
	(void) cnt;
	return a;
}

static int
voidWrite(const void *a, stream *s, size_t cnt)
{
	(void) a;
	(void) s;
	(void) cnt;
	return GDK_SUCCEED;
}

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
	} else if (p[0] == 't' && p[1] == 'r' && p[2] == 'u' && p[3] == 'e') {
		**dst = TRUE;
		p += 4;
	} else if (p[0] == 'f' && p[1] == 'a' && p[2] == 'l' && p[3] == 's' && p[4] == 'e') {
		**dst = FALSE;
		p += 5;
	} else if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		p += 3;
	} else {
		p = src;
	}
	return (int) (p - src);
}

int
bitToStr(char **dst, int *len, const bit *src)
{
	atommem(char, 6);

	if (*src == bit_nil) {
		strncpy(*dst, "nil", *len);
		return 3;
	} else if (*src) {
		strncpy(*dst, "true", *len);
		return 4;
	}
	strncpy(*dst, "false", *len);
	return 5;
}

static bit *
bitRead(bit *a, stream *s, size_t cnt)
{
	mnstr_read(s, (char *) a, 1, cnt);
	return mnstr_errnr(s) ? NULL : a;
}

static int
bitWrite(const bit *a, stream *s, size_t cnt)
{
	if (mnstr_write(s, (const char *) a, 1, cnt) == (ssize_t) cnt)
		return GDK_SUCCEED;
	else
		return GDK_FAIL;
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
		strncpy(*dst, "nil", *len);
		return 3;
	}
	i = (int) (strlen(s) + 4);
	atommem(char, i);
	snprintf(*dst, *len, "<%s>", s);
	return (int) strlen(*dst);
}

static bat *
batRead(bat *a, stream *s, size_t cnt)
{
	mnstr_readIntArray(s, (int *) a, cnt);	/* bat==int */
	return mnstr_errnr(s) ? NULL : a;
}

static int
batWrite(const bat *a, stream *s, size_t cnt)
{
	/* bat==int */
	return mnstr_writeIntArray(s, (const int *) a, cnt) ? GDK_SUCCEED : GDK_FAIL;
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
	lng base = 0;
	lng maxdiv10 = 0;	/* max value / 10 */
	int maxmod10 = 7;	/* max value % 10 */
	int sign = 1;

	atommem(void, sz);
	while (GDKisspace(*p))
		p++;
	memcpy(*dst, ATOMnilptr(tp), sz);
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		p += 3;
		return (int) (p - src);
	}
	if (*p == '-') {
		sign = -1;
		p++;
	} else if (*p == '+') {
		p++;
	}
	if (!num10(*p)) {
		/* not a number */
		return 0;
	}
	switch (sz) {
	case 1:
		maxdiv10 = 12/*7*/;
		break;
	case 2:
		maxdiv10 = 3276/*7*/;
		break;
	case 4:
		maxdiv10 = 214748364/*7*/;
		break;
	case 8:
		maxdiv10 = LL_CONSTANT(922337203685477580)/*7*/;
		break;
	}
	do {
		if (base > maxdiv10 ||
		    (base == maxdiv10 && base10(*p) > maxmod10)) {
			/* overflow */
			return 0;
		}
		base = 10 * base + base10(*p);
		p++;
	} while (num10(*p));
	base *= sign;
	switch (sz) {
	case 1: {
		bte **dstbte = (bte **) dst;
		**dstbte = (bte) base;
		break;
	}
	case 2: {
		sht **dstsht = (sht **) dst;
		**dstsht = (sht) base;
		break;
	}
	case 4: {
		int **dstint = (int **) dst;
		**dstint = (int) base;
		break;
	}
	case 8: {
		lng **dstlng = (lng **) dst;
		**dstlng = (lng) base;
		if (p[0] == 'L' && p[1] == 'L')
			p += 2;
		break;
	}
	}
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

#define atom_io(TYPE, NAME, CAST)					\
static TYPE *								\
TYPE##Read(TYPE *a, stream *s, size_t cnt)				\
{									\
	mnstr_read##NAME##Array(s, (CAST *) a, cnt);			\
	return mnstr_errnr(s) ? NULL : a;				\
}									\
static int								\
TYPE##Write(const TYPE *a, stream *s, size_t cnt)			\
{									\
	return mnstr_write##NAME##Array(s, (const CAST *) a, cnt) ?	\
		GDK_SUCCEED : GDK_FAIL;					\
}

atomtostr(bte, "%hhd", )
atom_io(bte, Bte, bte)

atomtostr(sht, "%hd", )
atom_io(sht, Sht, sht)

atomtostr(int, "%d", )
atom_io(int, Int, int)

atomtostr(lng, LLFMT, )

atom_io(lng, Lng, lng)

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
	return (int) (p - src);
}

atomtostr(ptr, PTRFMT, PTRFMTCAST)

#if SIZEOF_VOID_P == SIZEOF_INT
atom_io(ptr, Int, int)
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
atom_io(ptr, Lng, lng)
#endif

int
dblFromStr(const char *src, int *len, dbl **dst)
{
	const char *p = src;
	double d;

	/* alloc memory */
	atommem(dbl, sizeof(dbl));

	while (GDKisspace(*p))
		p++;
	if (p[0] == 'n' && p[1] == 'i' && p[2] == 'l') {
		**dst = dbl_nil;
		p += 3;
	} else {
		/* on overflow, strtod returns HUGE_VAL and sets
		 * errno to ERANGE; on underflow, it returns a value
		 * whose magnitude is no greater than the smallest
		 * normalized double, and may or may not set errno to
		 * ERANGE.  We accept underflow, but not overflow. */
		char *pe;
		errno = 0;
		d = strtod(src, &pe);
		p = pe;
		if (p == src || (errno == ERANGE && (d < -1 || d > 1))) {
			**dst = dbl_nil; /* default return value is nil */
			p = src;
		} else {
			**dst = (dbl) d;
		}
	}
	return (int) (p - src);
}

atomtostr(dbl, "%.17g", (double))
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
		f = strtof(src, &pe);
		p = pe;
		n = (int) (p - src);
		if (n == 0 || (errno == ERANGE && (f < -1 || f > 1))
#ifdef INFINITY
		    || f == INFINITY
#endif
#ifdef NAN
#ifndef __PGI
		    || f == NAN
#endif
#endif
		    )
#else /* no strtof, try sscanf */
		if (sscanf(src, "%f%n", &f, &n) <= 0 || n <= 0
#ifdef INFINITY
		    || f == INFINITY
#endif
#ifdef NAN
#ifndef __PGI
		    || f == NAN
#endif
#endif
		    )
#endif
		{
			**dst = flt_nil; /* default return value is nil */
			n = 0;
		} else
			**dst = (flt) f;
	}
	return n;
}

atomtostr(flt, "%.9g", (float))
atom_io(flt, Int, int)


/*
 * @+ String Atom Implementation
 * The Built-in type string is partly handled in an atom extension
 * library. The main reason is to limit the number of built-in types
 * in the BAT library kernel. Moreover, an extra indirection for a
 * string is less harmful than for manipulation of, e.g. an int.
 *
 * The internal representation of strings is without escape sequences.
 * When the string is printed we should add the escapes back into it.
 *
 * The current escape policy is that single- and double-quote can be
 * prepended by a backslash. Furthermore, the backslash may be
 * followed by three octal digits to denote a character.
 *
 * @- Automatic Double Elimination
 *
 * Because in many typical situations lots of double string values
 * occur in tables, the string insertion provides automatic double
 * elimination.  To do this, a GDK_STRHASHTABLE(=1024) bucket
 * hashtable is hidden in the first 4096 bytes of the string heap,
 * consisting of an offset to the first string hashing to that bucket
 * in the heap.  These offsets are made small (stridx_t is an unsigned
 * short) by exploiting the fact that the double elimination chunks
 * are (now) 64KB, hence a short suffices.
 *
 * In many other situations the cardinality of string columns is
 * large, or the string values might even be unique. In those cases,
 * our fixed-size hash table will start to overflow
 * quickly. Therefore, after the hash table is full (this is measured
 * very simplistically by looking whether the string heap exceeds a
 * heap size = GDK_ELIMLIMIT = 64KB) we flush the hash table. Even
 * more, from that moment on, we do not use a linked list, but a lossy
 * hash table that just contains the last position for each
 * bucket. Basically, after exceeding GDK_ELIMLIMIT, we get a
 * probabilistic/opportunistic duplicate elimination mechanism, that
 * only looks at the last GDK_ELIMLIMIT chunk in the heap, in a lossy
 * way.
 *
 * When comparing with the previous string implementation, the biggest
 * difference is that on 64-bits but with 32-bit oids, strings are
 * always 8-byte aligned and var_t numbers are multiplied by 8 to get
 * the true offset. The goal to do this is to allow 32-bits var_t on
 * 64-bits systems to address 32GB (using string alignment=8).  For
 * large database, the cost of padding (4 bytes avg) is offset by the
 * savings in var_t (allowing to go from 64- to 32-bits). Nothing lost
 * there, and 32-bits var_t also pay in smaller OIDs and smaller hash
 * tables, reducing memory pressure. For small duplicate eliminated
 * heaps, the short indices used in the hash table have now allowed
 * more buckets (2K instead of 1K) and average 2 bytes overhead for
 * the next pointers instead of 6-12. Therefore small heaps are now
 * more compact than before.
 *
 * The routine strElimDoubles() can be used to check whether all
 * strings are still being double-eliminated in the original
 * hash-table.  Only then we know that unequal offset-integers in the
 * BUN array means guaranteed different strings in the heap. This
 * optimization is made at some points in the GDK. Make sure you check
 * GDK_ELIMDOUBLES before assuming this!
 */
int
strElimDoubles(Heap *h)
{
	return GDK_ELIMDOUBLES(h);
}

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
	if (HEAPalloc(d, size, 1) >= 0) {
		d->free = GDK_STRHASHTABLE * sizeof(stridx_t);
		memset(d->base, 0, d->free);
		d->hashash = 1;	/* new string heaps get the hash value (and length) stored */
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
	(void) rebuild;
	if (!GDK_ELIMDOUBLES(h)) {
		/* flush hash table for security */
		memset(h->base, 0, GDK_STRHASHSIZE);
	}
}

/*
 * The strPut routine. The routine strLocate can be used to identify
 * the location of a string in the heap if it exists. Otherwise it
 * returns zero.
 */
/* if at least (2*SIZEOF_BUN), also store length (heaps are then
 * incompatible) */
#define EXTRALEN ((SIZEOF_BUN + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1))

var_t
strLocate(Heap *h, const char *v)
{
	stridx_t *ref, *next;
	size_t extralen = h->hashash ? EXTRALEN : 0;

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
			return (var_t) ((sizeof(stridx_t) + *ref + extralen) >> GDK_VARSHIFT);	/* found */
	}
	return 0;
}

static var_t
strPut(Heap *h, var_t *dst, const char *v)
{
	size_t elimbase = GDK_ELIMBASE(h->free);
	size_t pad = GDK_VARALIGN - (h->free & (GDK_VARALIGN - 1));
	size_t pos, len = GDK_STRLEN(v);
	size_t extralen = h->hashash ? EXTRALEN : 0;
	stridx_t *bucket, *ref, *next;
	BUN off, strhash;

	GDK_STRHASH(v, off);
	strhash = off;
	off &= GDK_STRHASHMASK;
	bucket = ((stridx_t *) h->base) + off;

	/* search hash-table, if double-elimination is still in place */
	if (elimbase == 0) {	/* small string heap (<64KB) -- fully double eliminated */
		for (ref = bucket; *ref; ref = next) {	/* search the linked list */
			next = (stridx_t *) (h->base + *ref);
			if (GDK_STRCMP(v, (str) (next + 1) + extralen) == 0) {	/* found */
				pos = sizeof(stridx_t) + *ref + extralen;
				return *dst = (var_t) (pos >> GDK_VARSHIFT);
			}
		}
		/* is there room for the next pointer in the padding space? */
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;	/* if not, pad more */
	} else if (*bucket) {
		/* large string heap (>=64KB) --
		 * opportunistic/probabilistic double elimination */
		pos = elimbase + *bucket + extralen;
		if (GDK_STRCMP(v, h->base + pos) == 0) {
			return *dst = (var_t) (pos >> GDK_VARSHIFT);	/* already in heap; do not insert! */
		}
#if SIZEOF_VAR_T >= SIZEOF_VOID_P /* in fact SIZEOF_VAR_T == SIZEOF_VOID_P */
		if (extralen == 0)
			/* i.e., h->hashash == FALSE */
			/* no VARSHIFT and no string hash value stored
			 * => no padding/alignment needed */
			pad = 0;
		else
#endif
			/* pad to align on VARALIGN for VARSHIFT
			 * and/or string hash value */
			pad &= (GDK_VARALIGN - 1);
	}

	/* check heap for space (limited to a certain maximum after
	 * which nils are inserted) */
	if (h->free + pad + len + extralen >= h->size) {
		size_t newsize = MAX(h->size, 4096);

		/* double the heap size until we have enough space */
		do {
			newsize <<= 1;
		} while (newsize <= h->free + pad + len + extralen);

		assert(newsize);

		if (h->free + pad + len + extralen >= (((size_t) VAR_MAX) << GDK_VARSHIFT)) {
			GDKerror("strPut: string heaps gets larger than " SZFMT "GB.\n", (((size_t) VAR_MAX) << GDK_VARSHIFT) >> 30);
			return 0;
		}
		if (h->free + pad + len + extralen < h->maxsize) {
			/* if there is reserved space, first use the
			 * reserved space */
			newsize = MIN(newsize, h->maxsize);
		}
		HEAPDEBUG fprintf(stderr, "#HEAPextend in strPut %s " SZFMT " " SZFMT "\n", h->filename, h->size, newsize);
		if (HEAPextend(h, newsize) < 0) {
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
	*dst = (var_t) (pos >> GDK_VARSHIFT);
	memcpy(h->base + pos, v, len);
	if (h->hashash) {
		((BUN *) (h->base + pos))[-1] = strhash;
#if EXTRALEN > SIZEOF_BUN
		((BUN *) (h->base + pos))[-2] = (BUN) len;
#endif
	}
	h->free += pad + len + extralen;

	/* maintain hash table */
	pos -= extralen;
	if (elimbase == 0) {	/* small string heap: link the next pointer */
		/* the stridx_t next pointer directly precedes the
		 * string and optional (depending on hashash) hash
		 * value */
		pos -= sizeof(stridx_t);
		*(stridx_t *) (h->base + pos) = *bucket;
	}
	*bucket = (stridx_t) (pos - elimbase);	/* set bucket to the new string */

	if (h->free >= elimbase + GDK_ELIMLIMIT) {
		memset(h->base, 0, GDK_STRHASHSIZE);	/* flush hash table */
	}
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
						cur++;
						c = mult08(c) + base08(*cur);
						/* if three digits, only look at lower 8 bits */
						c &= 0377;
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
			}
		} else if (c >= 0x80) {
			int m;

			/* start of multi-byte UTF-8 character */
			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				;
			/* n now is number of 10xxxxxx bytes that
			 * should follow */
			if (n == 0 || n >= 6) {
				/* incorrect UTF-8 sequence */
				/* n==0: c == 10xxxxxx */
				/* n>=6: c == 1111111x */
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
/*
#define printable_chr(ch) ((ch)==0 || GDKisgraph((ch)) || GDKisspace((ch)) || \
		         GDKisspecial((ch)) || GDKisupperl((ch)) || GDKislowerl((ch)))
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
escapedStrlen(const char *src)
{
	int end, sz = 0;

	for (end = 0; src[end]; end++)
		if (src[end] == '\t' ||
		    src[end] == '\n' ||
		    src[end] == '\\' ||
		    src[end] == '"') {
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
escapedStr(char *dst, const char *src, int dstlen)
{
	int cur = 0, l = 0;

	for (; src[cur] && l < dstlen; cur++)
		if (src[cur] == '\t') {
			dst[l++] = '\\';
			dst[l++] = 't';
		} else if (src[cur] == '\n') {
			dst[l++] = '\\';
			dst[l++] = 'n';
		} else if (src[cur] == '\\') {
			dst[l++] = '\\';
			dst[l++] = '\\';
		} else if (src[cur] == '"') {
			dst[l++] = '\\';
			dst[l++] = '"';
		} else if (!printable_chr(src[cur])
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
			snprintf(dst + l, dstlen - l, "\\%03o", (unsigned char) src[cur]);
			l += 4;
		} else {
			dst[l++] = src[cur];
		}
	assert(l < dstlen);
	dst[l] = 0;
	return l;
}

int
strToStr(char **dst, int *len, const char *src)
{
	int l = 0;

	if (GDK_STRNIL((str) src)) {
		atommem(char, 4);

		strncpy(*dst, "nil", *len);
		return 3;
	} else {
		int sz = escapedStrlen(src);
		atommem(char, sz + 3);
		l = escapedStr((*dst) + 1, src, *len - 1);
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
	if (!mnstr_readInt(s, &len))
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

static int
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
 * @+ Unique OIDs
 * The basic type OID represents unique values. Refinements should be
 * considered to link oids in time order.
 *
 * Values start from the "seqbase" (usually 0@@0). A nil seqbase makes
 * the entire column nil.  Monet's BUN access methods
 * BUNhead(b,p)/BUNtail(b,p) instantiate a value on-the-fly by looking
 * at the position p in BAT b.
 */
oid	GDKoid, GDKflushed;
/*
 * Init the shared array of oid bases.
 */
int
OIDinit(void)
{
	GDKflushed = GDKoid = 0;
	return 0;
}

/*
 * Make up some new OID for a specified database, based on the current
 * time.
 */
static oid
OIDrand(void)
{
	return 1000000;
}

/*
 * Initialize the current OID number to be starting at 'o'.
 */
oid
OIDbase(oid o)
{
	MT_lock_set(&MT_system_lock, "OIDbase");
	GDKoid = o;
	MT_lock_unset(&MT_system_lock, "OIDbase");
	return o;
}

static oid
OIDseed(oid o)
{
	oid t, p = GDKoid;

	MT_lock_set(&MT_system_lock, "OIDseed");
	t = OIDrand();
	if (o > t)
		t = o;
	if (p >= t)
		t = p;
	MT_lock_unset(&MT_system_lock, "OIDseed");
	return t;
}

/*
 * Initialize a sequence of OID seeds (for a sequence of database) as
 * stored in a string.
 */
oid
OIDread(str s)
{
	oid new = 0, *p = &new;
	int l = sizeof(oid);

	while (GDKisspace(*s))
		s++;
	while (GDKisdigit(*s)) {
		s += OIDfromStr(s, &l, &p);
		while (GDKisspace(*s))
			s++;
		new = OIDseed(new);
	}
	return new;
}

/*
 * Write the current sequence of OID seeds to a file in string format.
 */
int
OIDwrite(stream *s)
{
	int ret = 0;

	MT_lock_set(&MT_system_lock, "OIDwrite");
	if (GDKoid) {
		GDKflushed = GDKoid;
		ATOMprint(TYPE_oid, &GDKflushed, s);
		if (mnstr_errnr(s) ||
		    mnstr_write(s, " ", 1, 1) <= 0)
			ret = -1;
	}
	MT_lock_unset(&MT_system_lock, "OIDwrite");
	return ret;
}

int
OIDdirty(void)
{
	if (GDKoid && GDKoid > GDKflushed) {
		return TRUE;
	}
	return FALSE;
}

/*
 * Reserve a range of unique OIDs
 */
oid
OIDnew(oid inc)
{
	oid ret;

	MT_lock_set(&MT_system_lock, "OIDnew");
	if (!GDKoid)
		GDKoid = OIDrand();
	ret = GDKoid;
	GDKoid += inc;
	MT_lock_unset(&MT_system_lock, "OIDnew");
	return ret;
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
	return (int) (p - src);
}

int
OIDtoStr(char **dst, int *len, const oid *src)
{
	atommem(char, oidStrlen);

	if (*src == oid_nil) {
		strncpy(*dst, "nil", *len);
		return 3;
	}
	snprintf(*dst, *len, OIDFMT "@0", *src);
	return (int) strlen(*dst);
}

atomDesc BATatoms[MAXATOMS] = {
	{"void",
#if SIZEOF_OID == SIZEOF_INT
	 TYPE_void, 1, 0, /* sizeof(void) */ 0, 0, 1, (ptr) &oid_nil,
	 (int (*)(const char *, int *, ptr *)) OIDfromStr, (int (*)(str *, int *, const void *)) OIDtoStr,
	 (void *(*)(void *, stream *, size_t)) voidRead, (int (*)(const void *, stream *, size_t)) voidWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, 0,
#else
	 TYPE_void, 1, 0, /* sizeof(void) */ 0, 0, 1, (ptr) &oid_nil,
	 (int (*)(const char *, int *, ptr *)) OIDfromStr, (int (*)(str *, int *, const void *)) OIDtoStr,
	 (void *(*)(void *, stream *, size_t)) voidRead, (int (*)(const void *, stream *, size_t)) voidWrite,
	 (int (*)(const void *, const void *)) lngCmp,
	 (BUN (*)(const void *)) lngHash, 0,
#endif
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"bit", TYPE_bte, 1, sizeof(bit), sizeof(bit), 0, 0, (ptr) &bte_nil,
	 (int (*)(const char *, int *, ptr *)) bitFromStr, (int (*)(str *, int *, const void *)) bitToStr,
	 (void *(*)(void *, stream *, size_t)) bitRead, (int (*)(const void *, stream *, size_t)) bitWrite,
	 (int (*)(const void *, const void *)) bteCmp,
	 (BUN (*)(const void *)) bteHash, 0,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"bte", TYPE_bte, 1, sizeof(bte), sizeof(bte), 0, 0, (ptr) &bte_nil,
	 (int (*)(const char *, int *, ptr *)) bteFromStr, (int (*)(str *, int *, const void *)) bteToStr,
	 (void *(*)(void *, stream *, size_t)) bteRead, (int (*)(const void *, stream *, size_t)) bteWrite,
	 (int (*)(const void *, const void *)) bteCmp,
	 (BUN (*)(const void *)) bteHash, 0,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"sht", TYPE_sht, 1, sizeof(sht), sizeof(sht), 0, 0, (ptr) &sht_nil,
	 (int (*)(const char *, int *, ptr *)) shtFromStr, (int (*)(str *, int *, const void *)) shtToStr,
	 (void *(*)(void *, stream *, size_t)) shtRead, (int (*)(const void *, stream *, size_t)) shtWrite,
	 (int (*)(const void *, const void *)) shtCmp,
	 (BUN (*)(const void *)) shtHash, (void (*)(ptr, int)) shtConvert,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"BAT", TYPE_int, 1, sizeof(bat), sizeof(bat), 0, 0, (ptr) &int_nil,
	 (int (*)(const char *, int *, ptr *)) batFromStr, (int (*)(str *, int *, const void *)) batToStr,
	 (void *(*)(void *, stream *, size_t)) batRead, (int (*)(const void *, stream *, size_t)) batWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
	 (int (*)(const void *)) batFix, (int (*)(const void *)) batUnfix,
	 0, 0,
	 0, 0,
	 0, 0},
	{"int", TYPE_int, 1, sizeof(int), sizeof(int), 0, 0, (ptr) &int_nil,
	 (int (*)(const char *, int *, ptr *)) intFromStr, (int (*)(str *, int *, const void *)) intToStr,
	 (void *(*)(void *, stream *, size_t)) intRead, (int (*)(const void *, stream *, size_t)) intWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"oid",
#if SIZEOF_OID == SIZEOF_INT
	 TYPE_int, 1, sizeof(oid), sizeof(oid), 0, 0, (ptr) &oid_nil,
	 (int (*)(const char *, int *, ptr *)) OIDfromStr, (int (*)(str *, int *, const void *)) OIDtoStr,
	 (void *(*)(void *, stream *, size_t)) intRead, (int (*)(const void *, stream *, size_t)) intWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
#else
	 TYPE_lng, 1, sizeof(oid), sizeof(oid), 0, 0, (ptr) &oid_nil,
	 (int (*)(const char *, int *, ptr *)) OIDfromStr, (int (*)(str *, int *, const void *)) OIDtoStr,
	 (void *(*)(void *, stream *, size_t)) lngRead, (int (*)(const void *, stream *, size_t)) lngWrite,
	 (int (*)(const void *, const void *)) lngCmp,
	 (BUN (*)(const void *)) lngHash, (void (*)(ptr, int)) lngConvert,
#endif
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"wrd",
#if SIZEOF_WRD == SIZEOF_INT
	 TYPE_int, 1, sizeof(wrd), sizeof(wrd), 0, 0, (ptr) &int_nil,
	 (int (*)(const char *, int *, ptr *)) intFromStr, (int (*)(str *, int *, const void *)) intToStr,
	 (void *(*)(void *, stream *, size_t)) intRead, (int (*)(const void *, stream *, size_t)) intWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
#else
	 TYPE_lng, 1, sizeof(wrd), sizeof(wrd), 0, 0, (ptr) &lng_nil,
	 (int (*)(const char *, int *, ptr *)) lngFromStr, (int (*)(str *, int *, const void *)) lngToStr,
	 (void *(*)(void *, stream *, size_t)) lngRead, (int (*)(const void *, stream *, size_t)) lngWrite,
	 (int (*)(const void *, const void *)) lngCmp,
	 (BUN (*)(const void *)) lngHash, (void (*)(ptr, int)) lngConvert,
#endif
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"ptr",
#if SIZEOF_VOID_P == SIZEOF_INT
	 TYPE_ptr, 1, sizeof(ptr), sizeof(ptr), 0, 0, (ptr) &ptr_nil,
	 (int (*)(const char *, int *, ptr *)) ptrFromStr, (int (*)(str *, int *, const void *)) ptrToStr,
	 (void *(*)(void *, stream *, size_t)) ptrRead, (int (*)(const void *, stream *, size_t)) ptrWrite,
	 (int (*)(const void *, const void *)) intCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
	 TYPE_ptr, 1, sizeof(ptr), sizeof(ptr), 0, 0, (ptr) &ptr_nil,
	 (int (*)(const char *, int *, ptr *)) ptrFromStr, (int (*)(str *, int *, const void *)) ptrToStr,
	 (void *(*)(void *, stream *, size_t)) ptrRead, (int (*)(const void *, stream *, size_t)) ptrWrite,
	 (int (*)(const void *, const void *)) lngCmp,
	 (BUN (*)(const void *)) lngHash, (void (*)(ptr, int)) lngConvert,
#endif
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"flt", TYPE_flt, 1, sizeof(flt), sizeof(flt), 0, 0, (ptr) &flt_nil,
	 (int (*)(const char *, int *, ptr *)) fltFromStr, (int (*)(str *, int *, const void *)) fltToStr,
	 (void *(*)(void *, stream *, size_t)) fltRead, (int (*)(const void *, stream *, size_t)) fltWrite,
	 (int (*)(const void *, const void *)) fltCmp,
	 (BUN (*)(const void *)) intHash, (void (*)(ptr, int)) intConvert,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"dbl", TYPE_dbl, 1, sizeof(dbl), sizeof(dbl), 0, 0, (ptr) &dbl_nil,
	 (int (*)(const char *, int *, ptr *)) dblFromStr, (int (*)(str *, int *, const void *)) dblToStr,
	 (void *(*)(void *, stream *, size_t)) dblRead, (int (*)(const void *, stream *, size_t)) dblWrite,
	 (int (*)(const void *, const void *)) dblCmp,
	 (BUN (*)(const void *)) lngHash, (void (*)(ptr, int)) lngConvert,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"lng", TYPE_lng, 1, sizeof(lng), sizeof(lng), 0, 0, (ptr) &lng_nil,
	 (int (*)(const char *, int *, ptr *)) lngFromStr, (int (*)(str *, int *, const void *)) lngToStr,
	 (void *(*)(void *, stream *, size_t)) lngRead, (int (*)(const void *, stream *, size_t)) lngWrite,
	 (int (*)(const void *, const void *)) lngCmp,
	 (BUN (*)(const void *)) lngHash, (void (*)(ptr, int)) lngConvert,
	 0, 0,
	 0, 0,
	 0, 0,
	 0, 0},
	{"str", TYPE_str, 1, sizeof(var_t), sizeof(var_t), 0, 1, (ptr) str_nil,
	 (int (*)(const char *, int *, ptr *)) strFromStr, (int (*)(str *, int *, const void *)) strToStr,
	 (void *(*)(void *, stream *, size_t)) strRead, (int (*)(const void *, stream *, size_t)) strWrite,
	 (int (*)(const void *, const void *)) strCmp,
	 (BUN (*)(const void *)) strHash, 0,
	 0, 0,
	 (var_t (*)(Heap *, var_t *, const void *)) strPut, 0,
	 (int (*)(const void *)) strLen, strHeap,
	 (void (*)(Heap *, int)) 0, 0},
};

int GDKatomcnt = TYPE_str + 1;

/*
 * Sometimes a bat descriptor is loaded before the dynamic module
 * defining the atom is loaded. To support this an extra set of
 * unknown atoms is kept.  These can be accessed via the ATOMunknown
 * interface. Adding atoms to this set is done via the ATOMunknown_add
 * function. Finding an (negative) atom index can be done via
 * ATOMunknown_find, which simply adds the atom if it's not in the
 * unknown set. The index van be used to find the name of an unknown
 * ATOM via ATOMunknown_name. Once an atom becomes known, ie the
 * module defining it is loaded, it should be removed from the unknown
 * set using ATOMunknown_del.
 */
static str unknown[MAXATOMS] = { NULL };

int
ATOMunknown_add(const char *nme)
{
	int i = 1;

	for (; i < MAXATOMS; i++) {
		if (!unknown[i]) {
			unknown[i] = GDKstrdup(nme);
			return -i;
		}
	}
	assert(0);
	return 0;
}

int
ATOMunknown_del(int i)
{
	assert(unknown[-i]);
	GDKfree(unknown[-i]);
	unknown[-i] = NULL;
	return 0;
}

int
ATOMunknown_find(const char *nme)
{
	int i = 1;

	for (; i < MAXATOMS; i++) {
		if (unknown[i] && strcmp(unknown[i], nme) == 0) {
			return -i;
		}
	}
	return ATOMunknown_add(nme);
}

str
ATOMunknown_name(int i)
{
	assert(unknown[-i]);
	return unknown[-i];
}
