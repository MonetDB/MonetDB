/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

struct qsort_t {
	unsigned int hs;
	unsigned int ts;
	int (*cmp)(const void *, const void *);
	const char *base;
};

/* return index of middle value at indexes a, b, and c */
#define MED3(a, b, c) (LT(a, b) ?					\
		       (LT(b, c) ? (b) : (LT(a, c) ? (c) : (a))) :	\
		       (LT(c, b) ? (b) : (LT(a, c) ? (a) : (c))))

/* generic swap: swap w bytes starting at indexes i and j with each
 * other from the array given by b */
#define SWAP1(i, j, b, w)			\
	do {					\
		char _tmp;			\
		size_t _z;			\
		size_t _i = (i), _j = (j);	\
		for (_z = (w); _z > 0; _z--) {	\
			_tmp = b[_i];		\
			b[_i++] = b[_j];	\
			b[_j++] = _tmp;		\
		}				\
	} while (0)
/* swap n items from both h and t arrays starting at indexes i and j */
#define multi_SWAP(i, j, n)						\
	do {								\
		SWAP1((i) * buf->hs, (j) * buf->hs, h, n * buf->hs);	\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, n * buf->ts); \
	} while (0)

/* From here we define and redefine tokens and include the
 * implementation file multiple times to get versions for different
 * types and to get both ascending and descending (reverse) sort.
 * Note that for reverse sort, the LE (less or equal) and LT (less
 * than) macros are in fact greater or equal and greater than.  */

#define SWAP(i, j)							\
	do {								\
		bte _t = ((bte *) h)[i];				\
		((bte *) h)[i] = ((bte *) h)[j];			\
		((bte *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_bte
#define EQ(i, j)	(((bte *) h)[i] == ((bte *) h)[j])
#define LE(i, j)	(((bte *) h)[i] <= ((bte *) h)[j])
#define LT(i, j)	(((bte *) h)[i] < ((bte *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_bte_rev
#define LE(i, j)	(((bte *) h)[i] >= ((bte *) h)[j])
#define LT(i, j)	(((bte *) h)[i] > ((bte *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#define SWAP(i, j)							\
	do {								\
		sht _t = ((sht *) h)[i];				\
		((sht *) h)[i] = ((sht *) h)[j];			\
		((sht *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_sht
#define EQ(i, j)	(((sht *) h)[i] == ((sht *) h)[j])
#define LE(i, j)	(((sht *) h)[i] <= ((sht *) h)[j])
#define LT(i, j)	(((sht *) h)[i] < ((sht *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_sht_rev
#define LE(i, j)	(((sht *) h)[i] >= ((sht *) h)[j])
#define LT(i, j)	(((sht *) h)[i] > ((sht *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#define SWAP(i, j)							\
	do {								\
		int _t = ((int *) h)[i];				\
		((int *) h)[i] = ((int *) h)[j];			\
		((int *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_int
#define EQ(i, j)	(((int *) h)[i] == ((int *) h)[j])
#define LE(i, j)	(((int *) h)[i] <= ((int *) h)[j])
#define LT(i, j)	(((int *) h)[i] < ((int *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_int_rev
#define LE(i, j)	(((int *) h)[i] >= ((int *) h)[j])
#define LT(i, j)	(((int *) h)[i] > ((int *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#define SWAP(i, j)							\
	do {								\
		lng _t = ((lng *) h)[i];				\
		((lng *) h)[i] = ((lng *) h)[j];			\
		((lng *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_lng
#define EQ(i, j)	(((lng *) h)[i] == ((lng *) h)[j])
#define LE(i, j)	(((lng *) h)[i] <= ((lng *) h)[j])
#define LT(i, j)	(((lng *) h)[i] < ((lng *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_lng_rev
#define LE(i, j)	(((lng *) h)[i] >= ((lng *) h)[j])
#define LT(i, j)	(((lng *) h)[i] > ((lng *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#ifdef HAVE_HGE
#define SWAP(i, j)							\
	do {								\
		hge _t = ((hge *) h)[i];				\
		((hge *) h)[i] = ((hge *) h)[j];			\
		((hge *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_hge
#define EQ(i, j)	(((hge *) h)[i] == ((hge *) h)[j])
#define LE(i, j)	(((hge *) h)[i] <= ((hge *) h)[j])
#define LT(i, j)	(((hge *) h)[i] < ((hge *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_hge_rev
#define LE(i, j)	(((hge *) h)[i] >= ((hge *) h)[j])
#define LT(i, j)	(((hge *) h)[i] > ((hge *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP
#endif

#define SWAP(i, j)							\
	do {								\
		flt _t = ((flt *) h)[i];				\
		((flt *) h)[i] = ((flt *) h)[j];			\
		((flt *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_flt
#define EQ(i, j)	(((flt *) h)[i] == ((flt *) h)[j])
#define LE(i, j)	(((flt *) h)[i] <= ((flt *) h)[j])
#define LT(i, j)	(((flt *) h)[i] < ((flt *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_flt_rev
#define LE(i, j)	(((flt *) h)[i] >= ((flt *) h)[j])
#define LT(i, j)	(((flt *) h)[i] > ((flt *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#define SWAP(i, j)							\
	do {								\
		dbl _t = ((dbl *) h)[i];				\
		((dbl *) h)[i] = ((dbl *) h)[j];			\
		((dbl *) h)[j] = _t;					\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)
#define GDKqsort_impl GDKqsort_impl_dbl
#define EQ(i, j)	(((dbl *) h)[i] == ((dbl *) h)[j])
#define LE(i, j)	(((dbl *) h)[i] <= ((dbl *) h)[j])
#define LT(i, j)	(((dbl *) h)[i] < ((dbl *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_dbl_rev
#define LE(i, j)	(((dbl *) h)[i] >= ((dbl *) h)[j])
#define LT(i, j)	(((dbl *) h)[i] > ((dbl *) h)[j])
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT
#undef EQ
#undef SWAP

#define SWAP(i, j)							\
	do {								\
		SWAP1((i) * buf->hs, (j) * buf->hs, h, buf->hs);	\
		if (t && buf->ts)					\
			SWAP1((i) * buf->ts, (j) * buf->ts, t, buf->ts); \
	} while (0)

#define GDKqsort_impl GDKqsort_impl_var
#define INITIALIZER	int z
#define OFF(i)		(buf->base + VarHeapVal(h, i, buf->hs))
#define CMP(i, j)	(*buf->cmp)(OFF(i), OFF(j))
/* EQ is only ever called directly after LE with the same arguments */
#define EQ(i, j)	(z == 0)
#define LE(i, j)	((z = CMP(i, j)) <= 0)
#define LT(i, j)	(CMP(i, j) < 0)
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_var_rev
#define LE(i, j)	((z = CMP(i, j)) >= 0)
#define LT(i, j)	(CMP(i, j) > 0)
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef OFF
#undef CMP
#undef EQ
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_any
#define INITIALIZER	int z
#if SIZEOF_VAR_T == 8
#define OFF(i)		(buf->base +					\
			 (buf->hs == 1 ? ((unsigned char *) h)[i] :	\
			  buf->hs == 2 ? ((unsigned short *) h)[i] :	\
			  buf->hs == 4 ? ((unsigned int *) h)[i] :	\
			  ((var_t *) h)[i]))
#else
#define OFF(i)		(buf->base +					\
			 (buf->hs == 1 ? ((unsigned char *) h)[i] :	\
			  buf->hs == 2 ? ((unsigned short *) h)[i] :	\
			  ((var_t *) h)[i]))
#endif
#define CMP(i, j)	(buf->base ?					\
			 (*buf->cmp)(OFF(i), OFF(j)) :			\
			 (*buf->cmp)(h + (i) * buf->hs, h + (j) * buf->hs))
/* EQ is only ever called directly after LE with the same arguments */
#define EQ(i, j)	(z == 0)
#define LE(i, j)	((z = CMP(i, j)) <= 0)
#define LT(i, j)	(CMP(i, j) < 0)
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef LE
#undef LT

#define GDKqsort_impl GDKqsort_impl_any_rev
#define LE(i, j)	((z = CMP(i, j)) >= 0)
#define LT(i, j)	(CMP(i, j) > 0)
#include "gdk_qsort_impl.h"
#undef GDKqsort_impl
#undef CMP
#undef EQ
#undef LE
#undef LT

#undef SWAP

/* the interface functions */
void
GDKqsort(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe)
{
	struct qsort_t buf;

	assert(hs > 0);
	assert(ts >= 0);
	assert(tpe != TYPE_void);

	buf.hs = (unsigned int) hs;
	buf.ts = (unsigned int) ts;
	buf.cmp = ATOMcompare(tpe);
	buf.base = base;

	if (ATOMvarsized(tpe)) {
		assert(base != NULL);
		GDKqsort_impl_var(&buf, h, t, n);
		return;
	}
	if (base)
		tpe = TYPE_str;	/* we need the default case */

	tpe = ATOMbasetype(tpe);

	switch (tpe) {
	case TYPE_bte:
		GDKqsort_impl_bte(&buf, h, t, n);
		break;
	case TYPE_sht:
		GDKqsort_impl_sht(&buf, h, t, n);
		break;
	case TYPE_int:
		GDKqsort_impl_int(&buf, h, t, n);
		break;
	case TYPE_lng:
		GDKqsort_impl_lng(&buf, h, t, n);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		GDKqsort_impl_hge(&buf, h, t, n);
		break;
#endif
	case TYPE_flt:
		GDKqsort_impl_flt(&buf, h, t, n);
		break;
	case TYPE_dbl:
		GDKqsort_impl_dbl(&buf, h, t, n);
		break;
	default:
		GDKqsort_impl_any(&buf, h, t, n);
		break;
	}
}

void
GDKqsort_rev(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe)
{
	struct qsort_t buf;

	assert(hs > 0);
	assert(ts >= 0);
	assert(tpe != TYPE_void);

	buf.hs = (unsigned int) hs;
	buf.ts = (unsigned int) ts;
	buf.cmp = ATOMcompare(tpe);
	buf.base = base;

	if (ATOMvarsized(tpe)) {
		assert(base != NULL);
		GDKqsort_impl_var_rev(&buf, h, t, n);
		return;
	}
	if (base)
		tpe = TYPE_str;	/* we need the default case */

	tpe = ATOMbasetype(tpe);

	switch (tpe) {
	case TYPE_bte:
		GDKqsort_impl_bte_rev(&buf, h, t, n);
		break;
	case TYPE_sht:
		GDKqsort_impl_sht_rev(&buf, h, t, n);
		break;
	case TYPE_int:
		GDKqsort_impl_int_rev(&buf, h, t, n);
		break;
	case TYPE_lng:
		GDKqsort_impl_lng_rev(&buf, h, t, n);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		GDKqsort_impl_hge_rev(&buf, h, t, n);
		break;
#endif
	case TYPE_flt:
		GDKqsort_impl_flt_rev(&buf, h, t, n);
		break;
	case TYPE_dbl:
		GDKqsort_impl_dbl_rev(&buf, h, t, n);
		break;
	default:
		GDKqsort_impl_any_rev(&buf, h, t, n);
		break;
	}
}
