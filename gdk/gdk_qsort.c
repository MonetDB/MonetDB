/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

struct qsort_t {
	unsigned int hs;
	unsigned int ts;
	int (*cmp)(const void *, const void *);
	const char *base;
	const void *nil;
};

#define glue(a, b, c)		a ## b ## c
#define CONCAT2(a, b)		a ## b
#define CONCAT3(a, b, c)	glue(a, b, c)

/* nil is smallest value, i.e. first for ascending, last for descending */
#define fixltf(i, j, TPE)	(((TPE *) h)[i] < ((TPE *) h)[j])
#define fixlef(i, j, TPE)	(((TPE *) h)[i] <= ((TPE *) h)[j])
#define fixgtl(i, j, TPE)	(((TPE *) h)[i] > ((TPE *) h)[j])
#define fixgel(i, j, TPE)	(((TPE *) h)[i] >= ((TPE *) h)[j])

/* nil is largest value, i.e. last for ascending, first for descending */
#define fixltl(i, j, TPE)	(!fixnil(i, TPE) && (fixnil(j, TPE) || ((TPE *) h)[i] < ((TPE *) h)[j]))
#define fixlel(i, j, TPE)	(fixnil(j, TPE) || (!fixnil(i, TPE) && ((TPE *) h)[i] <= ((TPE *) h)[j]))
#define fixgtf(i, j, TPE)	(!fixnil(j, TPE) && (fixnil(i, TPE) || ((TPE *) h)[i] > ((TPE *) h)[j]))
#define fixgef(i, j, TPE)	(fixnil(i, TPE) || (!fixnil(j, TPE) && ((TPE *) h)[i] >= ((TPE *) h)[j]))

#define fixeq(i, j, TPE)	(((TPE *) h)[i] == ((TPE *) h)[j])
#define fixnil(i, TPE)		is_##TPE##_nil(((TPE *) h)[i])
#define fixswap(i, j, TPE)						\
	do {								\
		TPE _t = ((TPE *) h)[i];				\
		((TPE *) h)[i] = ((TPE *) h)[j];			\
		((TPE *) h)[j] = _t;					\
		if (t)							\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)

#define bteltf(i, j)		fixltf(i, j, bte)
#define btelef(i, j)		fixlef(i, j, bte)
#define bteltl(i, j)		fixltl(i, j, bte)
#define btelel(i, j)		fixlel(i, j, bte)
#define bteltl_rev(i, j)	fixgtl(i, j, bte)
#define btelel_rev(i, j)	fixgel(i, j, bte)
#define bteltf_rev(i, j)	fixgtf(i, j, bte)
#define btelef_rev(i, j)	fixgef(i, j, bte)
#define bteeq(i, j)		fixeq(i, j, bte)
#define btenil(i)		fixnil(i, bte)
#define bteswap(i, j)		fixswap(i, j, bte)

#define shtltf(i, j)		fixltf(i, j, sht)
#define shtlef(i, j)		fixlef(i, j, sht)
#define shtltl(i, j)		fixltl(i, j, sht)
#define shtlel(i, j)		fixlel(i, j, sht)
#define shtltl_rev(i, j)	fixgtl(i, j, sht)
#define shtlel_rev(i, j)	fixgel(i, j, sht)
#define shtltf_rev(i, j)	fixgtf(i, j, sht)
#define shtlef_rev(i, j)	fixgef(i, j, sht)
#define shteq(i, j)		fixeq(i, j, sht)
#define shtnil(i)		fixnil(i, sht)
#define shtswap(i, j)		fixswap(i, j, sht)

#define intltf(i, j)		fixltf(i, j, int)
#define intlef(i, j)		fixlef(i, j, int)
#define intltl(i, j)		fixltl(i, j, int)
#define intlel(i, j)		fixlel(i, j, int)
#define intltl_rev(i, j)	fixgtl(i, j, int)
#define intlel_rev(i, j)	fixgel(i, j, int)
#define intltf_rev(i, j)	fixgtf(i, j, int)
#define intlef_rev(i, j)	fixgef(i, j, int)
#define inteq(i, j)		fixeq(i, j, int)
#define intnil(i)		fixnil(i, int)
#define intswap(i, j)		fixswap(i, j, int)

#define lngltf(i, j)		fixltf(i, j, lng)
#define lnglef(i, j)		fixlef(i, j, lng)
#define lngltl(i, j)		fixltl(i, j, lng)
#define lnglel(i, j)		fixlel(i, j, lng)
#define lngltl_rev(i, j)	fixgtl(i, j, lng)
#define lnglel_rev(i, j)	fixgel(i, j, lng)
#define lngltf_rev(i, j)	fixgtf(i, j, lng)
#define lnglef_rev(i, j)	fixgef(i, j, lng)
#define lngeq(i, j)		fixeq(i, j, lng)
#define lngnil(i)		fixnil(i, lng)
#define lngswap(i, j)		fixswap(i, j, lng)

#define hgeltf(i, j)		fixltf(i, j, hge)
#define hgelef(i, j)		fixlef(i, j, hge)
#define hgeltl(i, j)		fixltl(i, j, hge)
#define hgelel(i, j)		fixlel(i, j, hge)
#define hgeltl_rev(i, j)	fixgtl(i, j, hge)
#define hgelel_rev(i, j)	fixgel(i, j, hge)
#define hgeltf_rev(i, j)	fixgtf(i, j, hge)
#define hgelef_rev(i, j)	fixgef(i, j, hge)
#define hgeeq(i, j)		fixeq(i, j, hge)
#define hgenil(i)		fixnil(i, hge)
#define hgeswap(i, j)		fixswap(i, j, hge)

#define fltltf(i, j)		(!fltnil(j) && (fltnil(i) || fixltf(i, j, flt)))
#define fltlef(i, j)		(fltnil(i) || (!fltnil(j) && fixlef(i, j, flt)))
#define fltltl(i, j)		fixltl(i, j, flt)
#define fltlel(i, j)		fixlel(i, j, flt)
#define fltltl_rev(i, j)	(!fltnil(i) && (fltnil(j) || fixgtl(i, j, flt)))
#define fltlel_rev(i, j)	(fltnil(j) || (!fltnil(i) && fixgel(i, j, flt)))
#define fltltf_rev(i, j)	fixgtf(i, j, flt)
#define fltlef_rev(i, j)	fixgef(i, j, flt)
#define flteq(i, j)		(fltnil(i) ? fltnil(j) : !fltnil(j) && fixeq(i, j, flt))
#define fltnil(i)		fixnil(i, flt)
#define fltswap(i, j)		fixswap(i, j, flt)

#define dblltf(i, j)		(!dblnil(j) && (dblnil(i) || fixltf(i, j, dbl)))
#define dbllef(i, j)		(dblnil(i) || (!dblnil(j) && fixlef(i, j, dbl)))
#define dblltl(i, j)		fixltl(i, j, dbl)
#define dbllel(i, j)		fixlel(i, j, dbl)
#define dblltl_rev(i, j)	(!dblnil(i) && (dblnil(j) || fixgtl(i, j, dbl)))
#define dbllel_rev(i, j)	(dblnil(j) || (!dblnil(i) && fixgel(i, j, dbl)))
#define dblltf_rev(i, j)	fixgtf(i, j, dbl)
#define dbllef_rev(i, j)	fixgef(i, j, dbl)
#define dbleq(i, j)		(dblnil(i) ? dblnil(j) : !dblnil(j) && fixeq(i, j, dbl))
#define dblnil(i)		fixnil(i, dbl)
#define dblswap(i, j)		fixswap(i, j, dbl)

#define anyCMP(i, j)		(*buf->cmp)(h + (i)*buf->hs, h + (j)*buf->hs)
#define anyltf(i, j)		(anyCMP(i, j) < 0)
#define anylef(i, j)		(anyCMP(i, j) <= 0)
#define anyltl(i, j)		(!anynil(i) && (anynil(j) || anyCMP(i, j) < 0))
#define anylel(i, j)		(anynil(j) || (!anynil(i) && anyCMP(i, j) <= 0))
#define anyltl_rev(i, j)	(anyCMP(i, j) > 0)
#define anylel_rev(i, j)	(anyCMP(i, j) >= 0)
#define anyltf_rev(i, j)	(!anynil(j) && (anynil(i) || anyCMP(i, j) > 0))
#define anylef_rev(i, j)	(anynil(i) || (!anynil(j) && anyCMP(i, j) >= 0))
#define anyeq(i, j)		(anyCMP(i, j) == 0)
#define anynil(i)		((*buf->cmp)(h + (i)*buf->hs, buf->nil) == 0)
#define anyswap(i, j)							\
	do {								\
		SWAP1((i) * buf->hs, (j) * buf->hs, h, buf->hs);	\
		if (t)							\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)

#define varOFF(i)		(buf->base + VarHeapVal(h, i, buf->hs))
#define varCMP(i, j)		(*buf->cmp)(varOFF(i), varOFF(j))
#define varltf(i, j)		(varCMP(i, j) < 0)
#define varlef(i, j)		(varCMP(i, j) <= 0)
#define varltl(i, j)		(!varnil(i) && (varnil(j) || varCMP(i, j) < 0))
#define varlel(i, j)		(varnil(j) || (!varnil(i) && varCMP(i, j) <= 0))
#define varltl_rev(i, j)	(varCMP(i, j) > 0)
#define varlel_rev(i, j)	(varCMP(i, j) >= 0)
#define varltf_rev(i, j)	(!varnil(j) && (varnil(i) || varCMP(i, j) > 0))
#define varlef_rev(i, j)	(varnil(i) || (!varnil(j) && varCMP(i, j) >= 0))
#define vareq(i, j)		(varCMP(i, j) == 0)
#define varnil(i)		((*buf->cmp)(varOFF(i), buf->nil) == 0)
#define varswap(i, j)		anyswap(i, j)

#define LE(i, j, TPE, SUFF)	CONCAT3(TPE, le, SUFF)(i, j)
#define LT(i, j, TPE, SUFF)	CONCAT3(TPE, lt, SUFF)(i, j)
#define EQ(i, j, TPE)		CONCAT2(TPE, eq)(i, j)
#define SWAP(i, j, TPE)		CONCAT2(TPE, swap)(i, j)

/* return index of middle value at indexes a, b, and c */
#define MED3(a, b, c, TPE, SUFF)	(LT(a, b, TPE, SUFF)		\
					 ? (LT(b, c, TPE, SUFF)		\
					    ? (b)			\
					    : (LT(a, c, TPE, SUFF)	\
					       ? (c)			\
					       : (a)))			\
					 : (LT(c, b, TPE, SUFF)		\
					    ? (b)			\
					    : (LT(a, c, TPE, SUFF)	\
					       ? (a)			\
					       : (c))))

/* generic swap: swap w bytes starting at indexes i and j with each
 * other from the array given by b */
#define SWAP1(i, j, b, w)						\
	do {								\
		for (size_t _z = (w), _i = (i), _j = (j); _z > 0; _z--) { \
			char _tmp = b[_i];				\
			b[_i++] = b[_j];				\
			b[_j++] = _tmp;					\
		}							\
	} while (0)
/* swap n items from both h and t arrays starting at indexes i and j */
#define multi_SWAP(i, j, n)						\
	do {								\
		SWAP1((i) * buf->hs, (j) * buf->hs, h, n * buf->hs);	\
		if (t)							\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, n * buf->ts); \
	} while (0)

/* From here we define and redefine tokens and include the
 * implementation file multiple times to get versions for different
 * types and to get both ascending and descending (reverse) sort.
 * Note that for reverse sort, the LE (less or equal) and LT (less
 * than) macros are in fact greater or equal and greater than.  */

#define TPE bte
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE sht
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE int
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE lng
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#ifdef HAVE_HGE
#define TPE hge
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE
#endif

#define TPE flt
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE dbl
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE any
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

#define TPE var
#define SUFF f
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF l_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#define SUFF f_rev
#include "gdk_qsort_impl.h"
#undef SUFF
#undef TPE

/* Sort the array `h' of `n' elements with size `hs' each and type
 * `ts' in ascending or descending (if `reverse' is true) order.  If
 * the type `tpe' indicates a variable-sized type, `h' contains
 * offsets into the `base' array which should be NULL otherwise.  The
 * array `t', if not NULL, contains `n' values of size `ts' each which
 * will be moved around together with the corresponding elements in
 * `h' (i.e. `t' is the payload).  If `nilslast' is true, nils sort at
 * the end, otherwise at the beginning of the result.
 *
 * This function uses a variant of quicksort and is thus not a stable
 * sort. */
void
GDKqsort(void *restrict h, void *restrict t, const void *restrict base,
	 size_t n, int hs, int ts, int tpe, bool reverse, bool nilslast)
{
	struct qsort_t buf;

	assert(hs > 0);
	assert(ts >= 0);
	assert(tpe != TYPE_void);
	assert((ts == 0) == (t == NULL));

	if (n <= 1)
		return;		/* nothing to do */

	buf.hs = (unsigned int) hs;
	buf.ts = (unsigned int) ts;
	buf.cmp = ATOMcompare(tpe);
	buf.base = base;
	buf.nil = ATOMnilptr(tpe);
	assert(ATOMvarsized(tpe) ? base != NULL : base == NULL);

	tpe = ATOMbasetype(tpe);

	if (reverse) {
		if (nilslast) {
			/* "normal" descending sort order, i.e. with
			 * NILs as smallest value, so they come
			 * last */
			if (ATOMvarsized(tpe)) {
				GDKqsort_impl_varl_rev(&buf, h, t, n);
				return;
			}
			switch (tpe) {
			case TYPE_bte:
				GDKqsort_impl_btel_rev(&buf, h, t, n);
				break;
			case TYPE_sht:
				GDKqsort_impl_shtl_rev(&buf, h, t, n);
				break;
			case TYPE_int:
				GDKqsort_impl_intl_rev(&buf, h, t, n);
				break;
			case TYPE_lng:
				GDKqsort_impl_lngl_rev(&buf, h, t, n);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				GDKqsort_impl_hgel_rev(&buf, h, t, n);
				break;
#endif
			case TYPE_flt:
				GDKqsort_impl_fltl_rev(&buf, h, t, n);
				break;
			case TYPE_dbl:
				GDKqsort_impl_dbll_rev(&buf, h, t, n);
				break;
			default:
				GDKqsort_impl_anyl_rev(&buf, h, t, n);
				break;
			}
		} else {
			if (ATOMvarsized(tpe)) {
				GDKqsort_impl_varf_rev(&buf, h, t, n);
				return;
			}
			switch (tpe) {
			case TYPE_bte:
				GDKqsort_impl_btef_rev(&buf, h, t, n);
				break;
			case TYPE_sht:
				GDKqsort_impl_shtf_rev(&buf, h, t, n);
				break;
			case TYPE_int:
				GDKqsort_impl_intf_rev(&buf, h, t, n);
				break;
			case TYPE_lng:
				GDKqsort_impl_lngf_rev(&buf, h, t, n);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				GDKqsort_impl_hgef_rev(&buf, h, t, n);
				break;
#endif
			case TYPE_flt:
				GDKqsort_impl_fltf_rev(&buf, h, t, n);
				break;
			case TYPE_dbl:
				GDKqsort_impl_dblf_rev(&buf, h, t, n);
				break;
			default:
				GDKqsort_impl_anyf_rev(&buf, h, t, n);
				break;
			}
		}
	} else {
		if (nilslast) {
			if (ATOMvarsized(tpe)) {
				GDKqsort_impl_varl(&buf, h, t, n);
				return;
			}
			switch (tpe) {
			case TYPE_bte:
				GDKqsort_impl_btel(&buf, h, t, n);
				break;
			case TYPE_sht:
				GDKqsort_impl_shtl(&buf, h, t, n);
				break;
			case TYPE_int:
				GDKqsort_impl_intl(&buf, h, t, n);
				break;
			case TYPE_lng:
				GDKqsort_impl_lngl(&buf, h, t, n);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				GDKqsort_impl_hgel(&buf, h, t, n);
				break;
#endif
			case TYPE_flt:
				GDKqsort_impl_fltl(&buf, h, t, n);
				break;
			case TYPE_dbl:
				GDKqsort_impl_dbll(&buf, h, t, n);
				break;
			default:
				GDKqsort_impl_anyl(&buf, h, t, n);
				break;
			}
		} else {
			/* "normal" ascending sort order, i.e. with
			 * NILs as smallest value, so they come
			 * first */
			if (ATOMvarsized(tpe)) {
				GDKqsort_impl_varf(&buf, h, t, n);
				return;
			}
			switch (tpe) {
			case TYPE_bte:
				GDKqsort_impl_btef(&buf, h, t, n);
				break;
			case TYPE_sht:
				GDKqsort_impl_shtf(&buf, h, t, n);
				break;
			case TYPE_int:
				GDKqsort_impl_intf(&buf, h, t, n);
				break;
			case TYPE_lng:
				GDKqsort_impl_lngf(&buf, h, t, n);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				GDKqsort_impl_hgef(&buf, h, t, n);
				break;
#endif
			case TYPE_flt:
				GDKqsort_impl_fltf(&buf, h, t, n);
				break;
			case TYPE_dbl:
				GDKqsort_impl_dblf(&buf, h, t, n);
				break;
			default:
				GDKqsort_impl_anyf(&buf, h, t, n);
				break;
			}
		}
	}
}
