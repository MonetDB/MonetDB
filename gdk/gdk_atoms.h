/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _GDK_ATOMS_H_
#define _GDK_ATOMS_H_

#define MAXATOMS	128

/*
 * @- comparison macro's
 * In order to get maximum performance, we extensively use
 * out-factoring of type checks using CPP macros. To catch diverging
 * code in one CPP macro we use the following #defines for comparing
 * atoms:
 */
#define simple_CMP(x,y,tpe)	(simple_GT(x,y,tpe) - simple_LT(x,y,tpe))
#define simple_EQ(x,y,tpe)	((*(const tpe*) (x)) == (*(const tpe*) (y)))
#define simple_NE(x,y,tpe,nl)	((*(const tpe*)(y)) != nl && (*(const tpe*) (x)) != (*(const tpe*) (y)))
#define simple_LT(x,y,tpe)	((*(const tpe*) (x))  < (*(const tpe*) (y)))
#define simple_GT(x,y,tpe)	((*(const tpe*) (x))  > (*(const tpe*) (y)))
#define simple_LE(x,y,tpe)	((*(const tpe*) (x)) <= (*(const tpe*) (y)))
#define simple_GE(x,y,tpe)	((*(const tpe*) (x)) >= (*(const tpe*) (y)))
#define atom_CMP(x,y,id)	(*ATOMcompare(id))(x,y)
#define atom_EQ(x,y,id)		((*ATOMcompare(id))(x,y) == 0)
#define atom_NE(x,y,id,nl)	((*ATOMcompare(id))(y,ATOMnilptr(id)) != 0 && (*ATOMcompare(id))(x,y) != 0)
#define atom_LT(x,y,id)		((*ATOMcompare(id))(x,y) < 0)
#define atom_GT(x,y,id)		((*ATOMcompare(id))(x,y) > 0)
#define atom_LE(x,y,id)		((*ATOMcompare(id))(x,y) <= 0)
#define atom_GE(x,y,id)		((*ATOMcompare(id))(x,y) >= 0)
#define simple_HASH(v,tpe,dst)	((dst) *(const tpe *) (v))
#define atom_HASH(v,id,dst)	((dst) ATOMhash(id, v))

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
gdk_export int hgeFromStr(const char *src, int *len, hge **dst);
gdk_export int hgeToStr(str *dst, int *len, const hge *src);
#endif
gdk_export int lngFromStr(const char *src, int *len, lng **dst);
gdk_export int lngToStr(str *dst, int *len, const lng *src);
gdk_export int intFromStr(const char *src, int *len, int **dst);
gdk_export int intToStr(str *dst, int *len, const int *src);
gdk_export int batFromStr(const char *src, int *len, bat **dst);
gdk_export int batToStr(str *dst, int *len, const bat *src);
gdk_export int ptrFromStr(const char *src, int *len, ptr **dst);
gdk_export int ptrToStr(str *dst, int *len, const ptr *src);
gdk_export int bitFromStr(const char *src, int *len, bit **dst);
gdk_export int bitToStr(str *dst, int *len, const bit *src);
gdk_export int OIDfromStr(const char *src, int *len, oid **dst);
gdk_export int OIDtoStr(str *dst, int *len, const oid *src);
gdk_export int shtFromStr(const char *src, int *len, sht **dst);
gdk_export int shtToStr(str *dst, int *len, const sht *src);
gdk_export int bteFromStr(const char *src, int *len, bte **dst);
gdk_export int bteToStr(str *dst, int *len, const bte *src);
gdk_export int fltFromStr(const char *src, int *len, flt **dst);
gdk_export int fltToStr(str *dst, int *len, const flt *src);
gdk_export int dblFromStr(const char *src, int *len, dbl **dst);
gdk_export int dblToStr(str *dst, int *len, const dbl *src);
gdk_export ssize_t GDKstrFromStr(unsigned char *dst, const unsigned char *src, ssize_t len);
gdk_export int strFromStr(const char *src, int *len, str *dst);
gdk_export BUN strHash(const char *s);
gdk_export int strLen(const char *s);
gdk_export int strNil(const char *s);
gdk_export int escapedStrlen(const char *src, const char *sep1, const char *sep2, int quote);
gdk_export int escapedStr(char *dst, const char *src, int dstlen, const char *sep1, const char *sep2, int quote);
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
#define GDK_bte_max ((bte) SCHAR_MAX)
#define GDK_bte_min ((bte) SCHAR_MIN)
#define GDK_sht_max ((sht) SHRT_MAX)
#define GDK_sht_min ((sht) SHRT_MIN)
#define GDK_int_max INT_MAX
#define GDK_int_min INT_MIN
#define GDK_flt_max ((flt) FLT_MAX)
#define GDK_flt_min (-GDK_flt_max)
#define GDK_lng_max ((lng) LLONG_MAX)
#define GDK_lng_min ((lng) LLONG_MIN)
#ifdef HAVE_HGE
#define GDK_hge_max ((((hge) 1) << 126) - 1 + \
                     (((hge) 1) << 126))
#define GDK_hge_min (-GDK_hge_max-1)
#endif
#define GDK_dbl_max ((dbl) DBL_MAX)
#define GDK_dbl_min (-GDK_dbl_max)
/* GDK_oid_max see below */
#define GDK_oid_min ((oid) 0)
/* representation of the nil */
gdk_export const bte bte_nil;
gdk_export const sht sht_nil;
gdk_export const int int_nil;
gdk_export const flt flt_nil;
gdk_export const dbl dbl_nil;
gdk_export const lng lng_nil;
#ifdef HAVE_HGE
gdk_export const hge hge_nil;
#endif
gdk_export const oid oid_nil;
gdk_export const char str_nil[2];
gdk_export const ptr ptr_nil;

/* derived NIL values - OIDDEPEND */
#define bit_nil	((bit) bte_nil)
#define bat_nil	((bat) int_nil)
#if SIZEOF_OID == SIZEOF_INT
#define GDK_oid_max ((oid) GDK_int_max)
#else
#define GDK_oid_max ((oid) GDK_lng_max)
#endif

#define void_nil	oid_nil
/*
 * @- Derived types
 * In all algorithms across GDK, you will find switches on the types
 * (bte, sht, int, flt, dbl, lng, hge, str). They respectively
 * represent an octet, a 16-bit int, a 32-bit int, a 32-bit float, a
 * 64-bit double, a 64-bit int, and a pointer-sized location of a
 * char-buffer (ended by a zero char).
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
 * @code{ size}, @code{ linear}, @code{ null} and
 * @code{ align} properties of their father.  The same goes for the
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
#define ATOMalign(t)		BATatoms[t].align
#define ATOMfromstr(t,s,l,src)	BATatoms[t].atomFromStr(src,l,s)
#define ATOMnilptr(t)		BATatoms[t].atomNull
#define ATOMcompare(t)		BATatoms[t].atomCmp
#define ATOMhash(t,src)		BATatoms[t].atomHash(src)
#define ATOMdel(t,hp,src)	do if (BATatoms[t].atomDel) BATatoms[t].atomDel(hp,src); while (0)
#define ATOMvarsized(t)		(BATatoms[t].atomPut != NULL)
#define ATOMlinear(t)		BATatoms[t].linear
#define ATOMtype(t)		((t == TYPE_void)?TYPE_oid:t)
#define ATOMfix(t,v)		do if (BATatoms[t].atomFix) BATatoms[t].atomFix(v); while (0)
#define ATOMunfix(t,v)		do if (BATatoms[t].atomUnfix) BATatoms[t].atomUnfix(v); while (0)

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
#ifdef HAVE_HGE
#define ATOM_CASE_16_hge						\
		case 16:						\
			* (hge *) d_ = * (hge *) s_;			\
			break
#else
#define ATOM_CASE_16_hge
#endif

#define ATOMputVAR(type, heap, dst, src)				\
	do {								\
		assert(BATatoms[type].atomPut != NULL);			\
		if ((*BATatoms[type].atomPut)(heap, dst, src) == 0)	\
			goto bunins_failed;				\
	} while (0)
#define ATOMputFIX(type, dst, src)					\
	do {								\
		int t_ = (type);					\
		void *d_ = (dst);					\
		const void *s_ = (src);					\
									\
		assert(BATatoms[t_].atomPut == NULL);			\
		ATOMfix(t_, s_);					\
		switch (ATOMsize(t_)) {					\
		case 0:		/* void */				\
			break;						\
		case 1:							\
			* (bte *) d_ = * (bte *) s_;			\
			break;						\
		case 2:							\
			* (sht *) d_ = * (sht *) s_;			\
			break;						\
		case 4:							\
			* (int *) d_ = * (int *) s_;			\
			break;						\
		case 8:							\
			* (lng *) d_ = * (lng *) s_;			\
			break;						\
		ATOM_CASE_16_hge;					\
		default:						\
			memcpy(d_, s_, (size_t) ATOMsize(t_));		\
			break;						\
		}							\
	} while (0)

#define ATOMreplaceVAR(type, heap, dst, src)				\
	do {								\
		int t_ = (type);					\
		var_t *d_ = (var_t *) (dst);				\
		const void *s_ = (src);					\
		var_t loc_ = *d_;					\
		Heap *h_ = (heap);					\
									\
		assert(BATatoms[t_].atomPut != NULL);			\
		if ((*BATatoms[t_].atomPut)(h_, &loc_, s_) == 0)	\
			goto bunins_failed;				\
		ATOMunfix(t_, d_);					\
		ATOMdel(t_, h_, d_);					\
		*d_ = loc_;						\
		ATOMfix(t_, s_);					\
	} while (0)
#define ATOMreplaceFIX(type, dst, src)					\
	do {								\
		int t_ = (type);					\
		void *d_ = (dst);					\
		const void *s_ = (src);					\
									\
		assert(BATatoms[t_].atomPut == NULL);			\
		ATOMfix(t_, s_);					\
		ATOMunfix(t_, d_);					\
		switch (ATOMsize(t_)) {					\
		case 0:	     /* void */					\
			break;						\
		case 1:							\
			* (bte *) d_ = * (bte *) s_;			\
			break;						\
		case 2:							\
			* (sht *) d_ = * (sht *) s_;			\
			break;						\
		case 4:							\
			* (int *) d_ = * (int *) s_;			\
			break;						\
		case 8:							\
			* (lng *) d_ = * (lng *) s_;			\
			break;						\
		ATOM_CASE_16_hge;					\
		default:						\
			memcpy(d_, s_, (size_t) ATOMsize(t_));		\
			break;						\
		}							\
	} while (0)

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
#define GDK_STRNIL(s)    ((s) == NULL || *(char*) (s) == '\200')
#define GDK_STRLEN(s)    ((GDK_STRNIL(s)?1:strlen(s))+1)
#define GDK_STRCMP(l,r)  (GDK_STRNIL(l)?(GDK_STRNIL(r)?0:-1):GDK_STRNIL(r)?1: \
			  (*(const unsigned char*)(l) < *(const unsigned char*)(r))?-1: \
			  (*(const unsigned char*)(l) > *(const unsigned char*)(r))?1: \
			  strCmpNoNil((const unsigned char*)(l),(const unsigned char*)(r)))
/*
 * @- Hash Function
 * The string hash function is a very simple hash function that xors
 * and rotates all characters together. It is optimized to process 2
 * characters at a time (adding 16-bits to the hash value each
 * iteration).
 */
#define GDK_STRHASH(x,y)				\
	do {						\
		const char *_key = (const char *) (x);	\
		BUN _i;					\
		for (_i = y = 0; _key[_i]; _i++) {	\
		    y += _key[_i];			\
		    y += (y << 10);			\
		    y ^= (y >> 6);			\
		}					\
		y += (y << 3);				\
		y ^= (y >> 11);				\
		y += (y << 15);				\
	} while (0)

#endif /* _GDK_ATOMS_H_ */
