/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_ATOMS_H_
#define _GDK_ATOMS_H_

/* atomFromStr returns the number of bytes of the input string that
 * were processed.  atomToStr returns the length of the string
 * produced.  Both functions return -1 on (any kind of) failure.  If
 * *dst is not NULL, *len specifies the available space.  If there is
 * not enough space, or if *dst is NULL, *dst will be freed (if not
 * NULL) and a new buffer will be allocated and returned in *dst.
 * *len will be set to reflect the actual size allocated.  If
 * allocation fails, *dst will be NULL on return and *len is
 * undefined.  In any case, if the function returns, *buf is either
 * NULL or a valid pointer and then *len is the size of the area *buf
 * points to.
 *
 * atomCmp returns a value less than zero/equal to zero/greater than
 * zer if the first argument points to a values which is deemed
 * smaller/equal to/larger than the value pointed to by the second
 * argument.
 *
 * atomHash calculates a hash function for the value pointed to by the
 * argument.
 */

#define IDLENGTH	64	/* maximum BAT id length */

typedef struct {
	/* simple attributes */
	char name[IDLENGTH];
	uint8_t storage;	/* stored as another type? */
	bool linear;		/* atom can be ordered linearly */
	uint16_t size;		/* fixed size of atom */

	/* automatically generated fields */
	const void *atomNull;	/* global nil value */

	/* generic (fixed + varsized atom) ADT functions */
	ssize_t (*atomFromStr) (const char *src, size_t *len, void **dst, bool external);
	ssize_t (*atomToStr) (char **dst, size_t *len, const void *src, bool external);
	void *(*atomRead) (void *dst, size_t *dstlen, stream *s, size_t cnt);
	gdk_return (*atomWrite) (const void *src, stream *s, size_t cnt);
	int (*atomCmp) (const void *v1, const void *v2);
	BUN (*atomHash) (const void *v);
	/* optional functions */
	gdk_return (*atomFix) (const void *atom);
	gdk_return (*atomUnfix) (const void *atom);

	/* varsized atom-only ADT functions */
	var_t (*atomPut) (BAT *, var_t *off, const void *src);
	void (*atomDel) (Heap *, var_t *atom);
	size_t (*atomLen) (const void *atom);
	void (*atomHeap) (Heap *, size_t);
} atomDesc;

#define MAXATOMS	128

gdk_export atomDesc BATatoms[MAXATOMS];
gdk_export int GDKatomcnt;

gdk_export int ATOMallocate(const char *nme);
gdk_export int ATOMindex(const char *nme);

gdk_export str ATOMname(int id);
gdk_export size_t ATOMlen(int id, const void *v);
gdk_export void *ATOMnil(int id)
	__attribute__((__malloc__));
gdk_export int ATOMprint(int id, const void *val, stream *fd);
gdk_export char *ATOMformat(int id, const void *val);

gdk_export void *ATOMdup(int id, const void *val);

/*
 * @- maximum atomic string lengths
 */
#define bitStrlen	8
#define bteStrlen	8
#define shtStrlen	12
#define intStrlen	24
#if SIZEOF_OID == SIZEOF_INT
#define oidStrlen	24
#else
#define oidStrlen	48
#endif
#if SIZEOF_PTR == SIZEOF_INT
#define ptrStrlen	24
#else
#define ptrStrlen	48
#endif
#define lngStrlen	48
#ifdef HAVE_HGE
#define hgeStrlen	96
#endif
#define fltStrlen	48
#define dblStrlen	96

/*
 * The system comes with the traditional atomic types: int (4 bytes),
 * bool(1 byte) and str (variable). In addition, we support the notion
 * of an OID type, which ensures uniqueness of its members.  This
 * leads to the following type descriptor table.
 */

#ifdef HAVE_HGE
gdk_export ssize_t hgeFromStr(const char *src, size_t *len, hge **dst, bool external);
gdk_export ssize_t hgeToStr(str *dst, size_t *len, const hge *src, bool external);
#endif
gdk_export ssize_t lngFromStr(const char *src, size_t *len, lng **dst, bool external);
gdk_export ssize_t lngToStr(str *dst, size_t *len, const lng *src, bool external);
gdk_export ssize_t intFromStr(const char *src, size_t *len, int **dst, bool external);
gdk_export ssize_t intToStr(str *dst, size_t *len, const int *src, bool external);
gdk_export ssize_t batFromStr(const char *src, size_t *len, bat **dst, bool external);
gdk_export ssize_t batToStr(str *dst, size_t *len, const bat *src, bool external);
gdk_export ssize_t ptrFromStr(const char *src, size_t *len, ptr **dst, bool external);
gdk_export ssize_t ptrToStr(str *dst, size_t *len, const ptr *src, bool external);
gdk_export ssize_t bitFromStr(const char *src, size_t *len, bit **dst, bool external);
gdk_export ssize_t bitToStr(str *dst, size_t *len, const bit *src, bool external);
gdk_export ssize_t OIDfromStr(const char *src, size_t *len, oid **dst, bool external);
gdk_export ssize_t OIDtoStr(str *dst, size_t *len, const oid *src, bool external);
gdk_export ssize_t shtFromStr(const char *src, size_t *len, sht **dst, bool external);
gdk_export ssize_t shtToStr(str *dst, size_t *len, const sht *src, bool external);
gdk_export ssize_t bteFromStr(const char *src, size_t *len, bte **dst, bool external);
gdk_export ssize_t bteToStr(str *dst, size_t *len, const bte *src, bool external);
gdk_export ssize_t fltFromStr(const char *src, size_t *len, flt **dst, bool external);
gdk_export ssize_t fltToStr(str *dst, size_t *len, const flt *src, bool external);
gdk_export ssize_t dblFromStr(const char *src, size_t *len, dbl **dst, bool external);
gdk_export ssize_t dblToStr(str *dst, size_t *len, const dbl *src, bool external);
gdk_export ssize_t GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len);
gdk_export ssize_t strFromStr(const char *restrict src, size_t *restrict len, str *restrict dst, bool external);
gdk_export size_t escapedStrlen(const char *restrict src, const char *sep1, const char *sep2, int quote);
gdk_export size_t escapedStr(char *restrict dst, const char *restrict src, size_t dstlen, const char *sep1, const char *sep2, int quote);
/*
 * @- nil values
 * All types have a single value designated as a NIL value. It
 * designates a missing value and it is ignored (forbidden) in several
 * primitives.  The current policy is to use the smallest value in any
 * ordered domain.  The routine atomnil returns a pointer to the nil
 * value representation.
 */
#define GDK_bit_max ((bit) 1)
#define GDK_bit_min ((bit) 0)
#define GDK_bte_max ((bte) INT8_MAX)
#define GDK_bte_min ((bte) INT8_MIN+1)
#define GDK_sht_max ((sht) INT16_MAX)
#define GDK_sht_min ((sht) INT16_MIN+1)
#define GDK_int_max ((int) INT32_MAX)
#define GDK_int_min ((int) INT32_MIN+1)
#define GDK_lng_max ((lng) INT64_MAX)
#define GDK_lng_min ((lng) INT64_MIN+1)
#ifdef HAVE_HGE
#define GDK_hge_max ((((hge) 1) << 126) - 1 + (((hge) 1) << 126))
#define GDK_hge_min (-GDK_hge_max)
#endif
#define GDK_flt_max ((flt) FLT_MAX)
#define GDK_flt_min ((flt) -FLT_MAX)
#define GDK_dbl_max ((dbl) DBL_MAX)
#define GDK_dbl_min ((dbl) -DBL_MAX)
#define GDK_oid_max (((oid) 1 << ((8 * SIZEOF_OID) - 1)) - 1)
#define GDK_oid_min ((oid) 0)
/* representation of the nil */
gdk_export const bte bte_nil;
gdk_export const sht sht_nil;
gdk_export const int int_nil;
#ifdef NAN_CANNOT_BE_USED_AS_INITIALIZER
/* Definition of NAN is seriously broken on Intel compiler (at least
 * in some versions), so we work around it. */
union _flt_nil_t {
	uint32_t l;
	flt f;
};
gdk_export const union _flt_nil_t _flt_nil_;
#define flt_nil (_flt_nil_.f)
union _dbl_nil_t {
	uint64_t l;
	dbl d;
};
gdk_export const union _dbl_nil_t _dbl_nil_;
#define dbl_nil (_dbl_nil_.d)
#else
gdk_export const flt flt_nil;
gdk_export const dbl dbl_nil;
#endif
gdk_export const lng lng_nil;
#ifdef HAVE_HGE
gdk_export const hge hge_nil;
#endif
gdk_export const oid oid_nil;
gdk_export const char str_nil[2];
gdk_export const ptr ptr_nil;
gdk_export const uuid uuid_nil;

/* derived NIL values - OIDDEPEND */
#define bit_nil	((bit) bte_nil)
#define bat_nil	((bat) int_nil)

#define void_nil	oid_nil

#define is_bit_nil(v)	((v) == bit_nil)
#define is_bte_nil(v)	((v) == bte_nil)
#define is_sht_nil(v)	((v) == sht_nil)
#define is_int_nil(v)	((v) == int_nil)
#define is_lng_nil(v)	((v) == lng_nil)
#ifdef HAVE_HGE
#define is_hge_nil(v)	((v) == hge_nil)
#endif
#define is_oid_nil(v)	((v) == oid_nil)
#define is_flt_nil(v)	isnan(v)
#define is_dbl_nil(v)	isnan(v)
#define is_bat_nil(v)	(((v) & 0x7FFFFFFF) == 0) /* v == bat_nil || v == 0 */

#include <math.h>

#if defined(_MSC_VER) && !defined(__INTEL_COMPILER) && _MSC_VER < 1800
#include <float.h>
#define isnan(x)	_isnan(x)
#define isinf(x)	(_fpclass(x) & (_FPCLASS_NINF | _FPCLASS_PINF))
#define isfinite(x)	_finite(x)
#endif

#ifdef HAVE_UUID
#define is_uuid_nil(x)	uuid_is_null((x).u)
#else
#define is_uuid_nil(x)	(memcmp((x).u, uuid_nil.u, UUID_SIZE) == 0)
#endif

/*
 * @- Derived types
 * In all algorithms across GDK, you will find switches on the types
 * (bte, sht, int, flt, dbl, lng, hge, str). They respectively
 * represent an octet, a 16-bit int, a 32-bit int, a 32-bit float, a
 * 64-bit double, a 64-bit int, a 128-bit int, and a pointer-sized location
 * of a char-buffer (ended by a zero char).
 *
 * In contrast, the types (bit, ptr, bat, oid) are derived types. They
 * do not occur in the switches. The ATOMstorage macro maps them
 * respectively onto a @code{ bte}, @code{ int} (pointers are 32-bit),
 * @code{ int}, and @code{ int}. OIDs are 32-bit.
 *
 * This approach makes it tractable to switch to 64-bits OIDs, or to a
 * fully 64-bits OS easily. One only has to map the @code{ oid} and
 * @code{ ptr} types to @code{ lng} instead of @code{ int}.
 *
 * Derived types mimic their fathers in many ways. They inherit the
 * @code{ size}, @code{ linear}, and @code{ null}
 * properties of their father.  The same goes for the
 * ADT functions HASH, CMP, PUT, NULL, DEL, LEN, and HEAP. So, a
 * derived type differs in only two ways from its father:
 * @table @code
 * @item [string representation]
 * the only two ADT operations specific for a derived type are FROMSTR
 * and TOSTR.
 * @item [identity]
 * (a @code{ bit} is really of a different type than @code{ bte}). The
 * set of operations on derived type values or BATs of such types may
 * differ from the sets of operations on the father type.
 * @end table
 */
/* use "do ... while(0)" so that lhs can safely be used in if statements */
#define ATOMstorage(t)		BATatoms[t].storage
#define ATOMsize(t)		BATatoms[t].size
#define ATOMfromstr(t,s,l,src,ext)	BATatoms[t].atomFromStr(src,l,s,ext)
#define ATOMnilptr(t)		BATatoms[t].atomNull
#define ATOMcompare(t)		BATatoms[t].atomCmp
#define ATOMcmp(t,l,r)		((*ATOMcompare(t))(l, r))
#define ATOMhash(t,src)		BATatoms[t].atomHash(src)
#define ATOMdel(t,hp,src)	do if (BATatoms[t].atomDel) BATatoms[t].atomDel(hp,src); while (0)
#define ATOMvarsized(t)		(BATatoms[t].atomPut != NULL)
#define ATOMlinear(t)		BATatoms[t].linear
#define ATOMtype(t)		((t) == TYPE_void ? TYPE_oid : (t))
#define ATOMfix(t,v)		(BATatoms[t].atomFix ? BATatoms[t].atomFix(v) : GDK_SUCCEED)
#define ATOMunfix(t,v)		(BATatoms[t].atomUnfix ? BATatoms[t].atomUnfix(v) : GDK_SUCCEED)

/* The base type is the storage type if the comparison function, the
 * hash function, and the nil value are the same as those of the
 * storage type; otherwise it is the type itself. */
#define ATOMbasetype(t)	((t) != ATOMstorage(t) &&			\
			 ATOMnilptr(t) == ATOMnilptr(ATOMstorage(t)) && \
			 ATOMcompare(t) == ATOMcompare(ATOMstorage(t)) && \
			 BATatoms[t].atomHash == BATatoms[ATOMstorage(t)].atomHash ? \
			 ATOMstorage(t) : (t))

/*
 * In case that atoms are added to a bat, their logical reference
 * count should be incremented (and decremented if deleted). Notice
 * that BATs with atomic types that have logical references (e.g. BATs
 * of BATs but also BATs of ODMG odSet) can never be persistent, as
 * this would make the commit tremendously complicated.
 */

static inline gdk_return __attribute__((__warn_unused_result__))
ATOMputVAR(BAT *b, var_t *dst, const void *src)
{
	assert(BATatoms[b->ttype].atomPut != NULL);
	if ((*BATatoms[b->ttype].atomPut)(b, dst, src) == 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}


static inline gdk_return __attribute__((__warn_unused_result__))
ATOMputFIX(int type, void *dst, const void *src)
{
	gdk_return rc;

	assert(BATatoms[type].atomPut == NULL);
	rc = ATOMfix(type, src);
	if (rc != GDK_SUCCEED)
		return rc;
	switch (ATOMsize(type)) {
	case 0:		/* void */
		break;
	case 1:
		* (bte *) dst = * (bte *) src;
		break;
	case 2:
		* (sht *) dst = * (sht *) src;
		break;
	case 4:
		* (int *) dst = * (int *) src;
		break;
	case 8:
		* (lng *) dst = * (lng *) src;
		break;
	case 16:
#ifdef HAVE_HGE
		* (hge *) dst = * (hge *) src;
#else
		* (uuid *) dst = * (uuid *) src;
#endif
		break;
	default:
		memcpy(dst, src, ATOMsize(type));
		break;
	}
	return GDK_SUCCEED;
}

static inline gdk_return __attribute__((__warn_unused_result__))
ATOMreplaceVAR(BAT *b, var_t *dst, const void *src)
{
	var_t loc = *dst;
	int type = b->ttype;

	assert(BATatoms[type].atomPut != NULL);
	if ((*BATatoms[type].atomPut)(b, &loc, src) == 0)
		return GDK_FAIL;
	if (ATOMunfix(type, dst) != GDK_SUCCEED)
		return GDK_FAIL;
	ATOMdel(type, b->tvheap, dst);
	*dst = loc;
	return ATOMfix(type, src);
}

/* string heaps:
 * - strings are 8 byte aligned
 * - start with a 1024 bucket hash table
 * - heaps < 64KiB are fully duplicate eliminated with this hash tables
 * - heaps >= 64KiB are opportunistically (imperfect) duplicate
 *   eliminated as only the last 128KiB chunk is considered and there
 *   is no linked list
 * - buckets and next pointers are unsigned short "indices"
 * - indices should be multiplied by 8 and takes from ELIMBASE to get
 *   an offset
 * Note that a 64KiB chunk of the heap contains at most 8K 8-byte
 * aligned strings. The 1K bucket list means that in worst load, the
 * list length is 8 (OK).
 */
#define GDK_STRHASHTABLE	(1<<10)	/* 1024 */
#define GDK_STRHASHMASK		(GDK_STRHASHTABLE-1)
#define GDK_STRHASHSIZE		(GDK_STRHASHTABLE * sizeof(stridx_t))
#define GDK_ELIMPOWER		16	/* 64KiB is the threshold */
#define GDK_ELIMDOUBLES(h)	((h)->free < GDK_ELIMLIMIT)
#define GDK_ELIMLIMIT		(1<<GDK_ELIMPOWER)	/* equivalently: ELIMBASE == 0 */
#define GDK_ELIMBASE(x)		(((x) >> GDK_ELIMPOWER) << GDK_ELIMPOWER)
#define GDK_VAROFFSET		((var_t) GDK_STRHASHSIZE)

/*
 * @- String Comparison, NILs and UTF-8
 *
 * Using the char* type for strings is handy as this is the type of
 * any constant strings in a C/C++ program. Therefore, MonetDB uses
 * this definition for str.  However, different compilers and
 * platforms use either signed or unsigned characters for the char
 * type.  It is required that string ordering in MonetDB is consistent
 * over platforms though.
 *
 * As for the choice how strings should be ordered, our support for
 * UTF-8 actually imposes that it should follow 'unsigned char'
 * doctrine (like in the AIX native compiler). In this semantics,
 * though we have to take corrective action to ensure that str(nil) is
 * the smallest value of the domain.
 */
static inline bool __attribute__((__pure__))
strEQ(const char *l, const char *r)
{
	return strcmp(l, r) == 0;
}

static inline bool __attribute__((__pure__))
strNil(const char *s)
{
	return s == NULL || (s[0] == '\200' && s[1] == '\0');
}

static inline size_t __attribute__((__pure__))
strLen(const char *s)
{
	return strNil(s) ? 2 : strlen(s) + 1;
}

static inline int __attribute__((__pure__))
strCmp(const char *l, const char *r)
{
	return strNil(r)
		? !strNil(l)
		: strNil(l) ? -1 : strcmp(l, r);
}

static inline size_t
VarHeapVal(const void *b, BUN p, int w)
{
	switch (w) {
	case 1:
		return (size_t) ((const uint8_t *) b)[p] + GDK_VAROFFSET;
	case 2:
		return (size_t) ((const uint16_t *) b)[p] + GDK_VAROFFSET;
#if SIZEOF_VAR_T == 8
	case 4:
		return (size_t) ((const uint32_t *) b)[p];
#endif
	default:
		return (size_t) ((const var_t *) b)[p];
	}
}

static inline BUN __attribute__((__pure__))
strHash(const char *key)
{
	BUN y = 0;

	for (BUN i = 0; key[i]; i++) {
		y += key[i];
		y += (y << 10);
		y ^= (y >> 6);
	}
	y += (y << 3);
	y ^= (y >> 11);
	y += (y << 15);
	return y;
}

#endif /* _GDK_ATOMS_H_ */
