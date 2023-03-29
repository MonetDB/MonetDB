/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* auxiliary functions and structs for imprints */
#include "gdk_imprints.h"

static inline oid *
buninsfix(BAT *bn, oid *a, BUN i, oid v, BUN g, BUN m)
{
	if (i == BATcapacity(bn)) {
		BATsetcount(bn, i);
		if (BATextend(bn, MIN(BATcapacity(bn) + g, m)) != GDK_SUCCEED)
			return NULL;
		a = (oid *) Tloc(bn, 0);
	}
	a[i] = v;
	return a;
}

BAT *
virtualize(BAT *bn)
{
	/* input must be a valid candidate list or NULL */
	if (bn == NULL)
		return NULL;
	if ((bn->ttype != TYPE_void && bn->ttype != TYPE_oid) || !bn->tkey || !bn->tsorted) {
		fprintf(stderr, "#bn type %d nil %d key %d sorted %d\n",
			bn->ttype, is_oid_nil(bn->tseqbase),
			bn->tkey, bn->tsorted);
		fflush(stderr);
	}
	assert(((bn->ttype == TYPE_void && !is_oid_nil(bn->tseqbase)) ||
		bn->ttype == TYPE_oid) &&
	       bn->tkey && bn->tsorted);
	assert(BBP_refs(bn->batCacheid) == 1);
	assert(BBP_lrefs(bn->batCacheid) == 0);
	/* since bn has unique and strictly ascending values, we can
	 * easily check whether the column is dense */
	if (bn->ttype == TYPE_oid &&
	    (BATcount(bn) <= 1 ||
	     * (const oid *) Tloc(bn, 0) + BATcount(bn) - 1 ==
	     * (const oid *) Tloc(bn, BATcount(bn) - 1))) {
		/* column is dense, replace by virtual oid */
		oid tseq;	/* work around bug in Intel compiler */
		if (BATcount(bn) == 0)
			tseq = 0;
		else
			tseq = * (const oid *) Tloc(bn, 0);
		TRC_DEBUG(ALGO, ALGOBATFMT ",seq=" OIDFMT "\n",
			  ALGOBATPAR(bn), tseq);
		bn->tseqbase = tseq;
		if (VIEWtparent(bn)) {
			Heap *h = GDKmalloc(sizeof(Heap));
			bat bid = VIEWtparent(bn);
			if (h == NULL) {
				BBPunfix(bn->batCacheid);
				return NULL;
			}
			*h = *bn->theap;
			settailname(h, BBP_physical(bn->batCacheid), TYPE_oid, 0);
			h->parentid = bn->batCacheid;
			h->base = NULL;
			ATOMIC_INIT(&h->refs, 1);
			HEAPdecref(bn->theap, false);
			bn->theap = h;
			BBPunshare(bid);
			BBPunfix(bid);
		} else {
			HEAPfree(bn->theap, true);
		}
		bn->theap->storage = bn->theap->newstorage = STORE_MEM;
		bn->theap->size = 0;
		bn->ttype = TYPE_void;
		bn->twidth = 0;
		bn->tshift = 0;
	}

	return bn;
}

#define HASHloop_bound(bi, h, hb, v, lo, hi)		\
	for (hb = HASHget(h, HASHprobe((h), v));	\
	     hb != BUN_NONE;				\
	     hb = HASHgetlink(h,hb))			\
		if (hb >= (lo) && hb < (hi) &&		\
		    (cmp == NULL ||			\
		     (*cmp)(v, BUNtail(bi, hb)) == 0))

static BAT *
hashselect(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	   const void *tl, BUN maximum, bool havehash, bool phash,
	   const char **algo)
{
	BUN i, cnt;
	oid o, *restrict dst;
	BUN l, h, d = 0;
	oid seq;
	int (*cmp)(const void *, const void *);

	size_t counter = 0;
	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	assert(bn->ttype == TYPE_oid);
	seq = bi->b->hseqbase;
	l = ci->seq - seq;
	h = canditer_last(ci) + 1 - seq;

	*algo = "hashselect";
	if (phash) {
		BAT *b2 = BBP_cache(VIEWtparent(bi->b));
		*algo = "hashselect on parent";
		TRC_DEBUG(ALGO, ALGOBATFMT
			  " using parent(" ALGOBATFMT ") "
			  "for hash\n",
			  ALGOBATPAR(bi->b),
			  ALGOBATPAR(b2));
		d = bi->baseoff - b2->tbaseoff;
		l += d;
		h += d;
		bat_iterator_end(bi);
		*bi = bat_iterator(b2);
	}

	if (!havehash) {
		if (BAThash(bi->b) != GDK_SUCCEED) {
			BBPreclaim(bn);
			return NULL;
		}
		MT_rwlock_rdlock(&bi->b->thashlock);
		if (bi->b->thash == NULL) {
			GDKerror("Hash destroyed before we could use it\n");
			goto bailout;
		}
	}
	switch (ATOMbasetype(bi->type)) {
	case TYPE_bte:
	case TYPE_sht:
		cmp = NULL;	/* no need to compare: "hash" is perfect */
		break;
	default:
		cmp = ATOMcompare(bi->type);
		break;
	}
	dst = (oid *) Tloc(bn, 0);
	cnt = 0;
	if (ci->tpe != cand_dense) {
		HASHloop_bound(*bi, bi->b->thash, i, tl, l, h) {
			GDK_CHECK_TIMEOUT(timeoffset, counter,
					  GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			o = (oid) (i + seq - d);
			if (canditer_contains(ci, o)) {
				dst = buninsfix(bn, dst, cnt, o,
						maximum - BATcapacity(bn),
						maximum);
				if (dst == NULL)
					goto bailout;
				cnt++;
			}
		}
	} else {
		HASHloop_bound(*bi, bi->b->thash, i, tl, l, h) {
			GDK_CHECK_TIMEOUT(timeoffset, counter,
					  GOTO_LABEL_TIMEOUT_HANDLER(bailout));
			o = (oid) (i + seq - d);
			dst = buninsfix(bn, dst, cnt, o,
					maximum - BATcapacity(bn),
					maximum);
			if (dst == NULL)
				goto bailout;
			cnt++;
		}
	}
	MT_rwlock_rdunlock(&bi->b->thashlock);
	BATsetcount(bn, cnt);
	bn->tkey = true;
	if (cnt > 1) {
		/* hash chains produce results in the order high to
		 * low, so we just need to reverse */
		for (l = 0, h = BATcount(bn) - 1; l < h; l++, h--) {
			o = dst[l];
			dst[l] = dst[h];
			dst[h] = o;
		}
	}
	bn->tsorted = true;
	bn->trevsorted = bn->batCount <= 1;
	bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? *dst : oid_nil;
	return bn;

  bailout:
	MT_rwlock_rdunlock(&bi->b->thashlock);
	BBPreclaim(bn);
	return NULL;
}

/* Imprints select code */

/* inner check, non-dense canditer */
#define impscheck(TEST,ADD)						\
	do {								\
		const oid e = (oid) (i+limit-pr_off+hseq);		\
		if (im[icnt] & mask) {					\
			if ((im[icnt] & ~innermask) == 0) {		\
				while (p < ncand && o < e) {		\
					v = src[o-hseq];		\
					if ((ADD) == NULL) {		\
						BBPreclaim(bn);		\
						return BUN_NONE;	\
					}				\
					cnt++;				\
					p++;				\
					o = canditer_next(ci);		\
				}					\
			} else {					\
				while (p < ncand && o < e) {		\
					v = src[o-hseq];		\
					if ((ADD) == NULL) {		\
						BBPreclaim(bn);		\
						return BUN_NONE;	\
					}				\
					cnt += (TEST) != 0;		\
					p++;				\
					o = canditer_next(ci);		\
				}					\
			}						\
		} else {						\
			while (p < ncand && o < e) {			\
				p++;					\
				o = canditer_next(ci);			\
			}						\
		}							\
	} while (false)

/* inner check, dense canditer */
#define impscheck_dense(TEST,ADD)					\
	do {								\
		const oid e = (oid) (i+limit-pr_off+hseq);		\
		if (im[icnt] & mask) {					\
			if ((im[icnt] & ~innermask) == 0) {		\
				while (p < ncand && o < e) {		\
					v = src[o-hseq];		\
					if ((ADD) == NULL) {		\
						BBPreclaim(bn);		\
						return BUN_NONE;	\
					}				\
					cnt++;				\
					p++;				\
					o = canditer_next_dense(ci);	\
				}					\
			} else {					\
				while (p < ncand && o < e) {		\
					v = src[o-hseq];		\
					if ((ADD) == NULL) {		\
						BBPreclaim(bn);		\
						return BUN_NONE;	\
					}				\
					cnt += (TEST) != 0;		\
					p++;				\
					o = canditer_next_dense(ci);	\
				}					\
			}						\
		} else {						\
			BUN skip_sz = MIN(ncand - p, e - o);		\
			p += skip_sz;					\
			o += skip_sz;					\
			ci->next += skip_sz;				\
		}							\
	} while (false)

/* main loop for imprints */
/*
 * icnt is the iterator for imprints
 * dcnt is the iterator for dictionary entries
 * i    is the iterator for the values in imprints
 */
#define impsloop(ISDENSE,TEST,ADD)					\
	do {								\
		BUN dcnt, icnt, limit, i;				\
		const cchdc_t *restrict d = (cchdc_t *) imprints->dict;	\
		const uint8_t rpp = ATOMelmshift(IMPS_PAGE >> bi->shift); \
		o = canditer_next(ci);					\
		for (i = 0, dcnt = 0, icnt = 0, p = 0;			\
		     dcnt < imprints->dictcnt && i <= w - hseq + pr_off && p < ncand; \
		     dcnt++) {						\
			GDK_CHECK_TIMEOUT(timeoffset, counter, GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
			limit = ((BUN) d[dcnt].cnt) << rpp;		\
			while (i + limit <= o - hseq + pr_off) {	\
				i += limit;				\
				icnt += d[dcnt].repeat ? 1 : d[dcnt].cnt; \
				dcnt++;					\
				limit = ((BUN) d[dcnt].cnt) << rpp;	\
			}						\
			if (!d[dcnt].repeat) {				\
				const BUN l = icnt + d[dcnt].cnt;	\
				limit = (BUN) 1 << rpp;			\
				while (i + limit <= o - hseq + pr_off) { \
					icnt++;				\
					i += limit;			\
				}					\
				for (;					\
				     icnt < l && i <= w - hseq + pr_off; \
				     icnt++) {				\
					impscheck##ISDENSE(TEST,ADD);	\
					i += limit;			\
				}					\
			}						\
			else {						\
				impscheck##ISDENSE(TEST,ADD);		\
				i += limit;				\
				icnt++;					\
			}						\
		}							\
	} while (false)

static inline oid *
quickins(oid *dst, BUN cnt, oid o, BAT *bn)
{
	(void) bn;
	assert(cnt < BATcapacity(bn));
	dst[cnt] = o;
	return dst;
}

/* construct the mask */
#define impsmask(ISDENSE,TEST,B)					\
	do {								\
		const uint##B##_t *restrict im = (uint##B##_t *) imprints->imps; \
		uint##B##_t mask = 0, innermask;			\
		const int tpe = ATOMbasetype(bi->type);			\
		const int lbin = IMPSgetbin(tpe, imprints->bits, imprints->bins, tl); \
		const int hbin = IMPSgetbin(tpe, imprints->bits, imprints->bins, th); \
		/* note: (1<<n)-1 gives a sequence of n one bits */	\
		/* to set bits hbin..lbin inclusive, we would do: */	\
		/* mask = ((1 << (hbin + 1)) - 1) - ((1 << lbin) - 1); */ \
		/* except ((1 << (hbin + 1)) - 1) is not defined if */	\
		/* hbin == sizeof(uint##B##_t)*8 - 1 */			\
		/* the following does work, however */			\
		mask = (((((uint##B##_t) 1 << hbin) - 1) << 1) | 1) - (((uint##B##_t) 1 << lbin) - 1); \
		innermask = mask;					\
		if (vl != minval)					\
			innermask = IMPSunsetBit(B, innermask, lbin);	\
		if (vh != maxval)					\
			innermask = IMPSunsetBit(B, innermask, hbin);	\
		if (anti) {						\
			uint##B##_t tmp = mask;				\
			mask = ~innermask;				\
			innermask = ~tmp;				\
		}							\
		/* if there are nils, we may need to check bin 0 */	\
		if (!bi->nonil)						\
			innermask = IMPSunsetBit(B, innermask, 0);	\
									\
		if (BATcapacity(bn) < maximum) {			\
			impsloop(ISDENSE, TEST,				\
				 dst = buninsfix(bn, dst, cnt, o,	\
						 (BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p) \
							* (dbl) (ncand-p) * 1.1 + 1024), \
						 maximum));		\
		} else {						\
			impsloop(ISDENSE, TEST, dst = quickins(dst, cnt, o, bn)); \
		}							\
	} while (false)

#define checkMINMAX(B, TYPE)						\
	do {								\
		const BUN *restrict imp_cnt = imprints->stats + 128;	\
		imp_min = imp_max = nil;				\
		for (BUN ii = 0; ii < B; ii++) {			\
			if (is_##TYPE##_nil(imp_min) && imp_cnt[ii]) {	\
				imp_min = basesrc[imprints->stats[ii]];	\
			}						\
			if (is_##TYPE##_nil(imp_max) && imp_cnt[B-1-ii]) { \
				imp_max = basesrc[imprints->stats[64+B-1-ii]]; \
			}						\
		}							\
		assert(!is_##TYPE##_nil(imp_min) &&			\
		       !is_##TYPE##_nil(imp_max));			\
		if (anti ?						\
		    vl < imp_min && vh > imp_max :			\
		    vl > imp_max || vh < imp_min) {			\
			return 0;					\
		}							\
	} while (false)

/* choose number of bits */
#define bitswitch(ISDENSE, TEST, TYPE)					\
	do {								\
		BUN ncand = ci->ncand;					\
		assert(imprints);					\
		*algo = parent ? "parent imprints select " #TEST " (canditer_next" #ISDENSE ")" : "imprints select " #TEST " (canditer_next" #ISDENSE ")"; \
		switch (imprints->bits) {				\
		case 8:							\
			checkMINMAX(8, TYPE);				\
			impsmask(ISDENSE,TEST,8);			\
			break;						\
		case 16:						\
			checkMINMAX(16, TYPE);				\
			impsmask(ISDENSE,TEST,16);			\
			break;						\
		case 32:						\
			checkMINMAX(32, TYPE);				\
			impsmask(ISDENSE,TEST,32);			\
			break;						\
		case 64:						\
			checkMINMAX(64, TYPE);				\
			impsmask(ISDENSE,TEST,64);			\
			break;						\
		default:						\
			MT_UNREACHABLE();				\
		}							\
	} while (false)

/* scan select without imprints */

/* core scan select loop with & without candidates */
#define scanloop(NAME,canditer_next,TEST)				\
	do {								\
		BUN ncand = ci->ncand;					\
		*algo = "select: " #NAME " " #TEST " (" #canditer_next ")"; \
		if (BATcapacity(bn) < maximum) {			\
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {	\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				if (TEST) {				\
					dst = buninsfix(bn, dst, cnt, o, \
						  (BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p) \
							 * (dbl) (ncand-p) * 1.1 + 1024), \
							maximum);	\
					if (dst == NULL) {		\
						BBPreclaim(bn);		\
						return BUN_NONE;	\
					}				\
					cnt++;				\
				}					\
			}						\
		} else {						\
			TIMEOUT_LOOP(ncand, timeoffset) {		\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				assert(cnt < BATcapacity(bn));		\
				dst[cnt] = o;				\
				cnt += (TEST) != 0;			\
			}						\
		}							\
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
	} while (false)

/* argument list for type-specific core scan select function call */
#define scanargs							\
	bi, ci, bn, tl, th, li, hi, equi, anti, lval, hval, lnil,	\
	cnt, bi->b->hseqbase, dst, maximum, imprints, algo

#define PREVVALUEbte(x)	((x) - 1)
#define PREVVALUEsht(x)	((x) - 1)
#define PREVVALUEint(x)	((x) - 1)
#define PREVVALUElng(x)	((x) - 1)
#ifdef HAVE_HGE
#define PREVVALUEhge(x)	((x) - 1)
#endif
#define PREVVALUEoid(x)	((x) - 1)
#define PREVVALUEflt(x)	nextafterf((x), -GDK_flt_max)
#define PREVVALUEdbl(x)	nextafter((x), -GDK_dbl_max)

#define NEXTVALUEbte(x)	((x) + 1)
#define NEXTVALUEsht(x)	((x) + 1)
#define NEXTVALUEint(x)	((x) + 1)
#define NEXTVALUElng(x)	((x) + 1)
#ifdef HAVE_HGE
#define NEXTVALUEhge(x)	((x) + 1)
#endif
#define NEXTVALUEoid(x)	((x) + 1)
#define NEXTVALUEflt(x)	nextafterf((x), GDK_flt_max)
#define NEXTVALUEdbl(x)	nextafter((x), GDK_dbl_max)

#define MINVALUEbte	GDK_bte_min
#define MINVALUEsht	GDK_sht_min
#define MINVALUEint	GDK_int_min
#define MINVALUElng	GDK_lng_min
#ifdef HAVE_HGE
#define MINVALUEhge	GDK_hge_min
#endif
#define MINVALUEoid	GDK_oid_min
#define MINVALUEflt	GDK_flt_min
#define MINVALUEdbl	GDK_dbl_min

#define MAXVALUEbte	GDK_bte_max
#define MAXVALUEsht	GDK_sht_max
#define MAXVALUEint	GDK_int_max
#define MAXVALUElng	GDK_lng_max
#ifdef HAVE_HGE
#define MAXVALUEhge	GDK_hge_max
#endif
#define MAXVALUEoid	GDK_oid_max
#define MAXVALUEflt	GDK_flt_max
#define MAXVALUEdbl	GDK_dbl_max

#define choose(NAME, ISDENSE, TEST, TYPE)				\
	do {								\
		if (imprints) {						\
			bitswitch(ISDENSE, TEST, TYPE);			\
		} else {						\
			scanloop(NAME, canditer_next##ISDENSE, TEST);	\
		}							\
	} while (false)

/* definition of type-specific core scan select function */
#define scanfunc(NAME, TYPE, ISDENSE)					\
static BUN								\
NAME##_##TYPE(BATiter *bi, struct canditer *restrict ci, BAT *bn,	\
	      const TYPE *tl, const TYPE *th, bool li, bool hi,		\
	      bool equi, bool anti, bool lval, bool hval,		\
	      bool lnil, BUN cnt, const oid hseq, oid *restrict dst,	\
	      BUN maximum, Imprints *imprints, const char **algo)	\
{									\
	TYPE vl = *tl;							\
	TYPE vh = *th;							\
	TYPE imp_min;							\
	TYPE imp_max;							\
	TYPE v;								\
	const TYPE nil = TYPE##_nil;					\
	const TYPE minval = MINVALUE##TYPE;				\
	const TYPE maxval = MAXVALUE##TYPE;				\
	const TYPE *src = (const TYPE *) bi->base;			\
	const TYPE *basesrc;						\
	oid o, w;							\
	BUN p;								\
	BUN pr_off = 0;							\
	bat parent = 0;							\
	(void) li;							\
	(void) hi;							\
	(void) lval;							\
	(void) hval;							\
	assert(li == !anti);						\
	assert(hi == !anti);						\
	assert(lval);							\
	assert(hval);							\
	size_t counter = 0;						\
	lng timeoffset = 0;						\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	if (qry_ctx != NULL) {						\
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0; \
	}								\
	if (imprints && imprints->imprints.parentid != bi->b->batCacheid) {	\
		parent = imprints->imprints.parentid;			\
		BAT *pbat = BBP_cache(parent);				\
		assert(pbat);						\
		basesrc = (const TYPE *) Tloc(pbat, 0);			\
		pr_off = (BUN) (src - basesrc);				\
	} else {							\
		basesrc = src;						\
	}								\
	w = canditer_last(ci);						\
	if (equi) {							\
		assert(imprints == NULL);				\
		if (lnil)						\
			scanloop(NAME, canditer_next##ISDENSE, is_##TYPE##_nil(v)); \
		else							\
			scanloop(NAME, canditer_next##ISDENSE, v == vl); \
	} else if (anti) {						\
		if (bi->nonil) {					\
			choose(NAME, ISDENSE, (v <= vl || v >= vh), TYPE); \
		} else {						\
			choose(NAME, ISDENSE, !is_##TYPE##_nil(v) && (v <= vl || v >= vh), TYPE); \
		}							\
	} else if (bi->nonil && vl == minval) {				\
		choose(NAME, ISDENSE, v <= vh, TYPE);			\
	} else if (vh == maxval) {					\
		choose(NAME, ISDENSE, v >= vl, TYPE);			\
	} else {							\
		choose(NAME, ISDENSE, v >= vl && v <= vh, TYPE);	\
	}								\
	return cnt;							\
  bailout:								\
	BBPreclaim(bn);							\
	return BUN_NONE;						\
}

static BUN
fullscan_any(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	     const void *tl, const void *th,
	     bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	     bool lnil, BUN cnt, const oid hseq, oid *restrict dst,
	     BUN maximum, Imprints *imprints, const char **algo)
{
	const void *v;
	const void *restrict nil = ATOMnilptr(bi->type);
	int (*cmp)(const void *, const void *) = ATOMcompare(bi->type);
	oid o;
	BUN p, ncand = ci->ncand;
	int c;

	(void) maximum;
	(void) imprints;
	(void) lnil;
	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	if (equi) {
		*algo = "select: fullscan equi";
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				v = BUNtail(*bi, o-hseq);
				if ((*cmp)(tl, v) == 0) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				v = BUNtail(*bi, o-hseq);
				if ((*cmp)(tl, v) == 0) {
					dst = buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ncand-p) * 1.1 + 1024),
						maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
	} else if (anti) {
		*algo = "select: fullscan anti";
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				v = BUNtail(*bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((lval &&
					((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
					(hval &&
					((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0))))) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				v = BUNtail(*bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((lval &&
					((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
					(hval &&
					((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0))))) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
	} else {
		*algo = "select: fullscan range";
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				v = BUNtail(*bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((!lval ||
					(c = cmp(tl, v)) < 0 ||
					(li && c == 0)) &&
					(!hval ||
					(c = cmp(th, v)) > 0 ||
					(hi && c == 0)))) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				v = BUNtail(*bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((!lval ||
					(c = cmp(tl, v)) < 0 ||
					(li && c == 0)) &&
					(!hval ||
					(c = cmp(th, v)) > 0 ||
					(hi && c == 0)))) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
	}
	TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
	return cnt;
  bailout:
	BBPreclaim(bn);
	return BUN_NONE;
}

static BUN
fullscan_str(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	     const char *tl, const char *th,
	     bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	     bool lnil, BUN cnt, const oid hseq, oid *restrict dst,
	     BUN maximum, Imprints *imprints, const char **algo)
{
	var_t pos;
	BUN p, ncand = ci->ncand;
	oid o;
	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	if (!equi || !GDK_ELIMDOUBLES(bi->vh))
		return fullscan_any(bi, ci, bn, tl, th, li, hi, equi, anti,
				    lval, hval, lnil, cnt, hseq, dst,
				    maximum, imprints, algo);
	if ((pos = strLocate(bi->vh, tl)) == (var_t) -2) {
		*algo = "select: fullscan equi strelim (nomatch)";
		return 0;
	}
	if (pos == (var_t) -1) {
		BBPreclaim(bn);
		return BUN_NONE;
	}
	*algo = "select: fullscan equi strelim";
	assert(pos >= GDK_VAROFFSET);
	switch (bi->width) {
	case 1: {
		const unsigned char *ptr = (const unsigned char *) bi->base;
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
		break;
	}
	case 2: {
		const unsigned short *ptr = (const unsigned short *) bi->base;
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
		break;
	}
#if SIZEOF_VAR_T == 8
	case 4: {
		const unsigned int *ptr = (const unsigned int *) bi->base;
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
		break;
	}
#endif
	default: {
		const var_t *ptr = (const var_t *) bi->base;
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX(p, ncand, timeoffset) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					dst = buninsfix(bn, dst, cnt, o,
							(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							       * (dbl) (ncand-p) * 1.1 + 1024),
							maximum);
					if (dst == NULL) {
						BBPreclaim(bn);
						return BUN_NONE;
					}
					cnt++;
				}
			}
		}
		break;
	}
	}
	TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
	return cnt;
  bailout:
	BBPreclaim(bn);
	return BUN_NONE;
}

/* scan select type switch */
#ifdef HAVE_HGE
#define scanfunc_hge(NAME, ISDENSE)		\
	scanfunc(NAME, hge, ISDENSE)
#else
#define scanfunc_hge(NAME, ISDENSE)
#endif
#define scan_sel(NAME, ISDENSE)			\
	scanfunc(NAME, bte, ISDENSE)		\
	scanfunc(NAME, sht, ISDENSE)		\
	scanfunc(NAME, int, ISDENSE)		\
	scanfunc(NAME, flt, ISDENSE)		\
	scanfunc(NAME, dbl, ISDENSE)		\
	scanfunc(NAME, lng, ISDENSE)		\
	scanfunc_hge(NAME, ISDENSE)

/* scan/imprints select */
scan_sel(fullscan, )
scan_sel(densescan, _dense)


static BAT *
scanselect(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	   const void *tl, const void *th,
	   bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	   bool lnil, BUN maximum, Imprints *imprints, const char **algo)
{
#ifndef NDEBUG
	int (*cmp)(const void *, const void *);
#endif
	int t;
	BUN cnt = 0;
	oid *restrict dst;

	assert(bi->b != NULL);
	assert(bn != NULL);
	assert(bn->ttype == TYPE_oid);
	assert(!lval || tl != NULL);
	assert(!hval || th != NULL);
	assert(!equi || (li && hi && !anti));
	assert(!anti || lval || hval);
	assert(bi->type != TYPE_void || equi || bi->nonil);

#ifndef NDEBUG
	cmp = ATOMcompare(bi->type);
#endif

	assert(!lval || !hval || (*cmp)(tl, th) <= 0);

	dst = (oid *) Tloc(bn, 0);

	t = ATOMbasetype(bi->type);

	/* call type-specific core scan select function */
	switch (t) {
	case TYPE_bte:
		if (ci->tpe == cand_dense)
			cnt = densescan_bte(scanargs);
		else
			cnt = fullscan_bte(scanargs);
		break;
	case TYPE_sht:
		if (ci->tpe == cand_dense)
			cnt = densescan_sht(scanargs);
		else
			cnt = fullscan_sht(scanargs);
		break;
	case TYPE_int:
		if (ci->tpe == cand_dense)
			cnt = densescan_int(scanargs);
		else
			cnt = fullscan_int(scanargs);
		break;
	case TYPE_flt:
		if (ci->tpe == cand_dense)
			cnt = densescan_flt(scanargs);
		else
			cnt = fullscan_flt(scanargs);
		break;
	case TYPE_dbl:
		if (ci->tpe == cand_dense)
			cnt = densescan_dbl(scanargs);
		else
			cnt = fullscan_dbl(scanargs);
		break;
	case TYPE_lng:
		if (ci->tpe == cand_dense)
			cnt = densescan_lng(scanargs);
		else
			cnt = fullscan_lng(scanargs);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		if (ci->tpe == cand_dense)
			cnt = densescan_hge(scanargs);
		else
			cnt = fullscan_hge(scanargs);
		break;
#endif
	case TYPE_str:
		cnt = fullscan_str(scanargs);
		break;
	default:
		cnt = fullscan_any(scanargs);
		break;
	}
	if (cnt == BUN_NONE) {
		return NULL;
	}
	assert(bn->batCapacity >= cnt);

	BATsetcount(bn, cnt);
	bn->tsorted = true;
	bn->trevsorted = bn->batCount <= 1;
	bn->tkey = true;
	bn->tseqbase = cnt == 0 ? 0 : cnt == 1 || cnt == bi->count ? bi->b->hseqbase : oid_nil;

	return bn;
}

/* Normalize the variables li, hi, lval, hval, possibly changing anti
 * in the process.  This works for all (and only) numeric types.
 *
 * Note that the expression x < v is equivalent to x <= v' where v' is
 * the next smaller value in the domain of v (similarly for x > v).
 * Also note that for floating point numbers there actually is such a
 * value.  In fact, there is a function in standard C that calculates
 * that value.
 *
 * The result of this macro is:
 * li == !anti, hi == !anti, lval == true, hval == true
 * This means that all ranges that we check for are closed ranges.  If
 * a range is one-sided, we fill in the minimum resp. maximum value in
 * the domain so that we create a closed range. */
#define NORMALIZE(TYPE)							\
	do {								\
		if (anti && li) {					\
			/* -inf < x < vl === -inf < x <= vl-1 */	\
			if (*(TYPE*)tl == MINVALUE##TYPE) {		\
				/* -inf < x < MIN || *th <[=] x < +inf */ \
				/* degenerates into half range */	\
				/* *th <[=] x < +inf */			\
				anti = false;				\
				tl = th;				\
				li = !hi;				\
				hval = false;				\
				/* further dealt with below */		\
			} else {					\
				vl.v_##TYPE = PREVVALUE##TYPE(*(TYPE*)tl); \
				tl = &vl.v_##TYPE;			\
				li = false;				\
			}						\
		}							\
		if (anti && hi) {					\
			/* vl < x < +inf === vl+1 <= x < +inf */	\
			if (*(TYPE*)th == MAXVALUE##TYPE) {		\
				/* -inf < x <[=] *tl || MAX > x > +inf */ \
				/* degenerates into half range */	\
				/* -inf < x <[=] *tl */			\
				anti = false;				\
				if (tl == &vl.v_##TYPE) {		\
					vh.v_##TYPE = vl.v_##TYPE;	\
					th = &vh.v_##TYPE;		\
				} else {				\
					th = tl;			\
				}					\
				hi = !li;				\
				lval = false;				\
				/* further dealt with below */		\
			} else {					\
				vh.v_##TYPE = NEXTVALUE##TYPE(*(TYPE*)th); \
				th = &vh.v_##TYPE;			\
				hi = false;				\
			}						\
		}							\
		if (!anti) {						\
			if (lval) {					\
				/* range bounded on left */		\
				if (!li) {				\
					/* open range on left */	\
					if (*(TYPE*)tl == MAXVALUE##TYPE) { \
						bat_iterator_end(&bi);	\
						return BATdense(0, 0, 0); \
					}				\
					/* vl < x === vl+1 <= x */	\
					vl.v_##TYPE = NEXTVALUE##TYPE(*(TYPE*)tl); \
					li = true;			\
					tl = &vl.v_##TYPE;		\
				}					\
			} else {					\
				/* -inf, i.e. smallest value */		\
				vl.v_##TYPE = MINVALUE##TYPE;		\
				li = true;				\
				tl = &vl.v_##TYPE;			\
				lval = true;				\
			}						\
			if (hval) {					\
				/* range bounded on right */		\
				if (!hi) {				\
					/* open range on right */	\
					if (*(TYPE*)th == MINVALUE##TYPE) { \
						bat_iterator_end(&bi);	\
						return BATdense(0, 0, 0); \
					}				\
					/* x < vh === x <= vh-1 */	\
					vh.v_##TYPE = PREVVALUE##TYPE(*(TYPE*)th); \
					hi = true;			\
					th = &vh.v_##TYPE;		\
				}					\
			} else {					\
				/* +inf, i.e. largest value */		\
				vh.v_##TYPE = MAXVALUE##TYPE;		\
				hi = true;				\
				th = &vh.v_##TYPE;			\
				hval = true;				\
			}						\
			if (*(TYPE*)tl > *(TYPE*)th) {			\
				bat_iterator_end(&bi);			\
				return BATdense(0, 0, 0);		\
			}						\
		}							\
		assert(lval);						\
		assert(hval);						\
		assert(li != anti);					\
		assert(hi != anti);					\
		/* if anti is set, we can now check */			\
		/* (x <= *tl || x >= *th) && x != nil */		\
		/* if equi==true, the check is x != *tl && x != nil */	\
		/* if anti is not set, we can check just */		\
		/* *tl <= x && x <= *th */				\
		/* if equi==true, the check is x == *tl */		\
		/* note that this includes the check for != nil */	\
		/* in the case where equi==true, the check is x == *tl */ \
	} while (false)

#if SIZEOF_BUN == SIZEOF_INT
#define CALC_ESTIMATE(TPE)						\
	do {								\
		if (*(TPE*)tl < 1) {					\
			if ((int) BUN_MAX + *(TPE*)tl >= *(TPE*)th)	\
				estimate = (BUN) ((int) *(TPE*)th - *(TPE*)tl); \
		} else {						\
			if (-(int) BUN_MAX + *(TPE*)tl <= *(TPE*)th)	\
				estimate = (BUN) ((int) *(TPE*)th - *(TPE*)tl); \
		}							\
	} while (0)
#else
#define CALC_ESTIMATE(TPE)						\
	do {								\
		if (*(TPE*)tl < 1) {					\
			if ((lng) BUN_MAX + *(TPE*)tl >= *(TPE*)th)	\
				estimate = (BUN) ((lng) *(TPE*)th - *(TPE*)tl); \
		} else {						\
			if (-(lng) BUN_MAX + *(TPE*)tl <= *(TPE*)th)	\
				estimate = (BUN) ((lng) *(TPE*)th - *(TPE*)tl); \
		}							\
	} while (0)
#endif

/* generic range select
 *
 * Return a BAT with the OID values of b for qualifying tuples.  The
 * return BAT is sorted (i.e. in the same order as the input BAT).
 *
 * If s is non-NULL, it is a list of candidates.  s must be sorted.
 *
 * tl may not be NULL, li, hi, and anti must be either 0 or 1.
 *
 * If th is NULL, hi is ignored.
 *
 * If anti is 0, qualifying tuples are those whose value is between tl
 * and th (as in x >[=] tl && x <[=] th, where equality depends on li
 * and hi--so if tl > th, nothing will be returned).  If li or hi is
 * 1, the respective boundary is inclusive, otherwise exclusive.  If
 * th is NULL it is taken to be equal to tl, turning this into an
 * equi- or point-select.  Note that for a point select to return
 * anything, li (and hi if th was not NULL) must be 1.  There is a
 * special case if tl is nil and th is NULL.  This is the only way to
 * select for nil values.
 *
 * If anti is 1, the result is the complement of what the result would
 * be if anti were 0, except that nils are filtered out.
 *
 * In brief:
 * - if tl==nil and th==NULL and anti==0, return all nils (only way to
 *   get nils);
 * - it tl==nil and th==nil, return all but nils;
 * - if tl==nil and th!=NULL, no lower bound;
 * - if th==NULL or tl==th, point (equi) select;
 * - if th==nil, no upper bound
 *
 * A complete breakdown of the various arguments follows.  Here, v, v1
 * and v2 are values from the appropriate domain, and
 * v != nil, v1 != nil, v2 != nil, v1 < v2.
 *	tl	th	li	hi	anti	result list of OIDs for values
 *	-----------------------------------------------------------------
 *	nil	NULL	true	ignored	false	x == nil (only way to get nil)
 *	nil	NULL	false	ignored	false	NOTHING
 *	nil	NULL	ignored	ignored	true	x != nil
 *	nil	nil	ignored	ignored	false	x != nil
 *	nil	nil	ignored	ignored	true	NOTHING
 *	nil	v	ignored	false	false	x < v
 *	nil	v	ignored	true	false	x <= v
 *	nil	v	ignored	false	true	x >= v
 *	nil	v	ignored	true	true	x > v
 *	v	nil	false	ignored	false	x > v
 *	v	nil	true	ignored	false	x >= v
 *	v	nil	false	ignored	true	x <= v
 *	v	nil	true	ignored	true	x < v
 *	v	NULL	false	ignored	false	NOTHING
 *	v	NULL	true	ignored	false	x == v
 *	v	NULL	false	ignored	true	x != nil
 *	v	NULL	true	ignored	true	x != v
 *	v	v	false	false	false	NOTHING
 *	v	v	true	false	false	NOTHING
 *	v	v	false	true	false	NOTHING
 *	v	v	true	true	false	x == v
 *	v	v	false	false	true	x != nil
 *	v	v	true	false	true	x != nil
 *	v	v	false	true	true	x != nil
 *	v	v	true	true	true	x != v
 *	v1	v2	false	false	false	v1 < x < v2
 *	v1	v2	true	false	false	v1 <= x < v2
 *	v1	v2	false	true	false	v1 < x <= v2
 *	v1	v2	true	true	false	v1 <= x <= v2
 *	v1	v2	false	false	true	x <= v1 or x >= v2
 *	v1	v2	true	false	true	x < v1 or x >= v2
 *	v1	v2	false	true	true	x <= v1 or x > v2
 *	v1	v2	true	true	true	x < v1 or x > v2
 *	v2	v1	ignored	ignored	false	NOTHING
 *	v2	v1	ignored	ignored	true	x != nil
 */
BAT *
BATselect(BAT *b, BAT *s, const void *tl, const void *th,
	     bool li, bool hi, bool anti)
{
	bool lval;		/* low value used for comparison */
	bool lnil;		/* low value is nil */
	bool hval;		/* high value used for comparison */
	bool equi;		/* select for single value (not range) */
	bool wanthash = false;	/* use hash (equi must be true) */
	bool havehash = false;	/* we have a hash (and the hashlock) */
	bool phash = false;	/* use hash on parent BAT (if view) */
	int t;			/* data type */
	bat parent;		/* b's parent bat (if b is a view) */
	const void *nil;
	BAT *bn;
	struct canditer ci;
	BUN estimate = BUN_NONE, maximum = BUN_NONE;
	oid vwl = 0, vwh = 0;
	lng vwo = 0;
	Heap *oidxh = NULL;
	const char *algo;
	union {
		bte v_bte;
		sht v_sht;
		int v_int;
		lng v_lng;
#ifdef HAVE_HGE
		hge v_hge;
#endif
		flt v_flt;
		dbl v_dbl;
		oid v_oid;
	} vl, vh;
	lng t0 = GDKusec();

	BATcheck(b, NULL);
	if (tl == NULL) {
		GDKerror("tl value required");
		return NULL;
	}

	if (s && s->ttype != TYPE_msk && !s->tsorted) {
		GDKerror("invalid argument: s must be sorted.\n");
		return NULL;
	}

	canditer_init(&ci, b, s);
	if (ci.ncand == 0) {
		/* trivially empty result */
		MT_thread_setalgorithm("select: trivially empty");
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
			  " (" LLFMT " usec): "
			  "trivially empty\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	BATiter bi = bat_iterator(b);

	t = bi.type;
	nil = ATOMnilptr(t);
	/* can we use the base type? */
	t = ATOMbasetype(t);
	lnil = nil && ATOMcmp(t, tl, nil) == 0; /* low value = nil? */

	if (!lnil && th != NULL && (!li || !hi) && !anti && ATOMcmp(t, tl, th) == 0) {
		/* upper and lower bound of range are equal and we
		 * want an interval that's open on at least one
		 * side */
		MT_thread_setalgorithm("select: empty interval");
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",li=%d,hi=%d,anti=%d -> "
			  ALGOOPTBATFMT " (" LLFMT " usec): "
			  "empty interval\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s),
			  li, hi, anti, ALGOOPTBATPAR(bn), GDKusec() - t0);
		bat_iterator_end(&bi);
		return bn;
	}

	lval = !lnil || th == NULL;	 /* low value used for comparison */
	equi = th == NULL || (lval && ATOMcmp(t, tl, th) == 0); /* point select? */
	if (equi) {
		assert(lval);
		if (th == NULL)
			hi = li;
		th = tl;
		hval = true;
	} else if (nil == NULL) {
		hval = true;
	} else {
		hval = ATOMcmp(t, th, nil) != 0;
	}
	if (anti) {
		if (lval != hval) {
			/* one of the end points is nil and the other
			 * isn't: swap sub-ranges */
			const void *tv;
			bool ti;
			assert(!equi);
			ti = li;
			li = !hi;
			hi = !ti;
			tv = tl;
			tl = th;
			th = tv;
			ti = lval;
			lval = hval;
			hval = ti;
			lnil = ATOMcmp(t, tl, nil) == 0;
			anti = false;
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",anti=%d "
				  "anti: switch ranges...\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  anti);
		} else if (!lval && !hval) {
			/* antiselect for nil-nil range: all non-nil
			 * values are in range; we must return all
			 * other non-nil values, i.e. nothing */
			MT_thread_setalgorithm("select: anti: nil-nil range, nonil");
			bn = BATdense(0, 0, 0);
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",anti=%d -> "
				  ALGOOPTBATFMT " (" LLFMT " usec): "
				  "anti: nil-nil range, nonil\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  anti, ALGOOPTBATPAR(bn), GDKusec() - t0);
			bat_iterator_end(&bi);
			return bn;
		} else if (equi && lnil) {
			/* antiselect for nil value: turn into range
			 * select for nil-nil range (i.e. everything
			 * but nil) */
			equi = false;
			anti = false;
			lval = false;
			hval = false;
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",anti=0 "
				  "anti-nil...\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		} else if (equi) {
			equi = false;
			if (!(li && hi)) {
				/* antiselect for nothing: turn into
				 * range select for nil-nil range
				 * (i.e. everything but nil) */
				anti = false;
				lval = false;
				hval = false;
				TRC_DEBUG(ALGO, "b="
					  ALGOBATFMT ",s="
					  ALGOOPTBATFMT ",anti=0 "
					  "anti-nothing...\n",
					  ALGOBATPAR(b),
					  ALGOOPTBATPAR(s));
			}
		} else if (ATOMcmp(t, tl, th) > 0) {
			/* empty range: turn into range select for
			 * nil-nil range (i.e. everything but nil) */
			equi = false;
			anti = false;
			lval = false;
			hval = false;
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",anti=0 "
				  "anti-nil...\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s));
		}
	}

	/* if equi set, then so are both lval and hval */
	assert(!equi || (lval && hval));

	if (hval && (equi ? !li || !hi : ATOMcmp(t, tl, th) > 0)) {
		/* empty range */
		MT_thread_setalgorithm("select: empty range");
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
			  " (" LLFMT " usec) "
			  "empty range\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		bat_iterator_end(&bi);
		return bn;
	}
	if (equi && lnil && bi.nonil) {
		/* return all nils, but there aren't any */
		MT_thread_setalgorithm("select: equi-nil, nonil");
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
			  " (" LLFMT " usec): "
			  "equi-nil, nonil\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		bat_iterator_end(&bi);
		return bn;
	}

	if (!equi && !lval && !hval && lnil && bi.nonil) {
		/* return all non-nils from a BAT that doesn't have
		 * any: i.e. return everything */
		MT_thread_setalgorithm("select: everything, nonil");
		bn = canditer_slice(&ci, 0, ci.ncand);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
			  " (" LLFMT " usec): "
			  "everything, nonil\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		bat_iterator_end(&bi);
		return bn;
	}

	/* figure out how the searched for range compares with the known
	 * minimum and maximum values */
	if (anti) {
		int c;

		if (bi.minpos != BUN_NONE) {
			c = ATOMcmp(t, tl, BUNtail(bi, bi.minpos));
			if (c < 0 || (li && c == 0)) {
				if (bi.maxpos != BUN_NONE) {
					c = ATOMcmp(t, th, BUNtail(bi, bi.maxpos));
					if (c > 0 || (hi && c == 0)) {
						/* tl..th range fully
						 * inside MIN..MAX
						 * range of values in
						 * BAT, so nothing
						 * left over for
						 * anti */
						MT_thread_setalgorithm("select: nothing, out of range");
						bn = BATdense(0, 0, 0);
						TRC_DEBUG(ALGO, "b=" ALGOBATFMT
							  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
							  " (" LLFMT " usec): "
							  "nothing, out of range\n",
							  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti, ALGOOPTBATPAR(bn), GDKusec() - t0);
						bat_iterator_end(&bi);
						return bn;
					}
				}
			}
		}
	} else if (!equi || !lnil) {
		int c;

		if (hval) {
			if (bi.minpos != BUN_NONE) {
				c = ATOMcmp(t, th, BUNtail(bi, bi.minpos));
				if (c < 0 || (!hi && c == 0)) {
					/* smallest value in BAT larger than
					 * what we're looking for */
					MT_thread_setalgorithm("select: nothing, out of range");
					bn = BATdense(0, 0, 0);
					TRC_DEBUG(ALGO, "b="
						  ALGOBATFMT ",s="
						  ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
						  " (" LLFMT " usec): "
						  "nothing, out of range\n",
						  ALGOBATPAR(b),
						  ALGOOPTBATPAR(s), anti,
						  ALGOOPTBATPAR(bn), GDKusec() - t0);
					bat_iterator_end(&bi);
					return bn;
				}
			}
		}
		if (lval) {
			if (bi.maxpos != BUN_NONE) {
				c = ATOMcmp(t, tl, BUNtail(bi, bi.maxpos));
				if (c > 0 || (!li && c == 0)) {
					/* largest value in BAT smaller than
					 * what we're looking for */
					MT_thread_setalgorithm("select: nothing, out of range");
					bn = BATdense(0, 0, 0);
					TRC_DEBUG(ALGO, "b="
						  ALGOBATFMT ",s="
						  ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
						  " (" LLFMT " usec): "
						  "nothing, out of range\n",
						  ALGOBATPAR(b),
						  ALGOOPTBATPAR(s), anti,
						  ALGOOPTBATPAR(bn), GDKusec() - t0);
					bat_iterator_end(&bi);
					return bn;
				}
			}
		}
	}

	if (ATOMtype(bi.type) == TYPE_oid) {
		NORMALIZE(oid);
	} else {
		switch (t) {
		case TYPE_bte:
			NORMALIZE(bte);
			break;
		case TYPE_sht:
			NORMALIZE(sht);
			break;
		case TYPE_int:
			NORMALIZE(int);
			break;
		case TYPE_lng:
			NORMALIZE(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			NORMALIZE(hge);
			break;
#endif
		case TYPE_flt:
			NORMALIZE(flt);
			break;
		case TYPE_dbl:
			NORMALIZE(dbl);
			break;
		}
	}

	parent = VIEWtparent(b);
	assert(parent >= 0);
	BAT *pb;
	BATiter pbi;
	if (parent > 0)
		pb = BBP_cache(parent);
	else
		pb = NULL;
	pbi = bat_iterator(pb);
	/* use hash only for equi-join, and then only if b or its
	 * parent already has a hash, or if b or its parent is
	 * persistent and the total size wouldn't be too large; check
	 * for existence of hash last since that may involve I/O */
	if (equi) {
		double cost = joincost(b, 1, &ci, &havehash, &phash, NULL);
		if (cost > 0 && cost < ci.ncand) {
			wanthash = true;
			if (havehash) {
				if (phash) {
					MT_rwlock_rdlock(&pb->thashlock);
					if (pb->thash == NULL) {
						MT_rwlock_rdunlock(&pb->thashlock);
						havehash = false;
					}
				} else {
					MT_rwlock_rdlock(&b->thashlock);
					if (b->thash == NULL) {
						MT_rwlock_rdunlock(&b->thashlock);
						havehash = false;
					}
				}
			}
			if (wanthash && !havehash) {
				MT_lock_set(&b->theaplock);
				if (++b->selcnt > 1000)
					b->selcnt = 1000; /* limit value */
				else
					wanthash = false;
				MT_lock_unset(&b->theaplock);
			}
		} else {
			wanthash = havehash = false;
		}
	}

	/* at this point, if havehash is set, we have the hash lock
	 * the lock is on the parent if phash is set, on b itself if not
	 * also, if havehash is set, then so is wanthash (but not v.v.) */

	if (!havehash) {
		/* make sure tsorted and trevsorted flags are set, but
		 * we only need to know if we're not yet sure that we're
		 * going for the hash (i.e. it already exists) */
		(void) BATordered(b);
		(void) BATordered_rev(b);
		/* reinitialize iterator since properties may have changed */
		bat_iterator_end(&bi);
		bi = bat_iterator(b);
	}

	/* If there is an order index or it is a view and the parent
	 * has an ordered index, and the bat is not tsorted or
	 * trevstorted then use the order index.  And there is no cand
	 * list or if there is one, it is dense.
	 * TODO: we do not support anti-select with order index */
	bool poidx = false;
	if (!anti &&
	    !havehash &&
	    !bi.sorted &&
	    !bi.revsorted &&
	    ci.tpe == cand_dense) {
		BAT *view = NULL;
		(void) BATcheckorderidx(b);
		MT_lock_set(&b->batIdxLock);
		if ((oidxh = b->torderidx) != NULL)
			HEAPincref(oidxh);
		MT_lock_unset(&b->batIdxLock);
		if (oidxh == NULL && pb != NULL) {
			(void) BATcheckorderidx(pb);
			MT_lock_set(&pb->batIdxLock);
			if ((oidxh = pb->torderidx) != NULL) {
				HEAPincref(oidxh);
				view = b;
				b = pb;
			}
			MT_lock_unset(&pb->batIdxLock);
		}
		if (oidxh) {
			/* Is query selective enough to use the ordered index ? */
			/* TODO: Test if this heuristic works in practice */
			/*if ((ORDERfnd(b, th) - ORDERfnd(b, tl)) < ((BUN)1000 < bi.count/1000 ? (BUN)1000: bi.count/1000))*/
			if ((ORDERfnd(b, oidxh, th) - ORDERfnd(b, oidxh, tl)) < bi.count/3) {
				if (view) {
					bat_iterator_end(&bi);
					bi = bat_iterator(b);
					poidx = true; /* using parent oidx */
					vwo = (lng) (view->tbaseoff - bi.baseoff);
					vwl = b->hseqbase + (oid) vwo + ci.seq - view->hseqbase;
					vwh = vwl + canditer_last(&ci) - ci.seq;
					vwo = (lng) view->hseqbase - (lng) b->hseqbase - vwo;
					TRC_DEBUG(ALGO, "Switch from " ALGOBATFMT " to " ALGOBATFMT " " OIDFMT "-" OIDFMT " hseq " LLFMT "\n", ALGOBATPAR(view), ALGOBATPAR(b), vwl, vwh, vwo);
				} else {
					vwl = ci.seq;
					vwh = canditer_last(&ci);
				}
			} else {
				if (view) {
					b = view;
					view = NULL;
				}
				HEAPdecref(oidxh, false);
				oidxh = NULL;
			}
		}
	}

	if (!havehash && (bi.sorted || bi.revsorted || oidxh != NULL)) {
		BUN low = 0;
		BUN high = bi.count;

		if (BATtdensebi(&bi)) {
			/* positional */
			/* we expect nonil to be set, in which case we
			 * already know that we're not dealing with a
			 * nil equiselect (dealt with above) */
			oid h, l;
			assert(bi.nonil);
			assert(bi.sorted);
			assert(oidxh == NULL);
			algo = "select: dense";
			h = * (oid *) th + hi;
			if (h > bi.tseq)
				h -= bi.tseq;
			else
				h = 0;
			if ((BUN) h < high)
				high = (BUN) h;

			l = *(oid *) tl + !li;
			if (l > bi.tseq)
				l -= bi.tseq;
			else
				l = 0;
			if ((BUN) l > low)
				low = (BUN) l;
			if (low > high)
				low = high;
		} else if (bi.sorted) {
			assert(oidxh == NULL);
			algo = "select: sorted";
			if (lval) {
				if (li)
					low = SORTfndfirst(b, tl);
				else
					low = SORTfndlast(b, tl);
			} else {
				/* skip over nils at start of column */
				low = SORTfndlast(b, nil);
			}
			if (hval) {
				if (hi)
					high = SORTfndlast(b, th);
				else
					high = SORTfndfirst(b, th);
			}
		} else if (bi.revsorted) {
			assert(oidxh == NULL);
			algo = "select: reverse sorted";
			if (lval) {
				if (li)
					high = SORTfndlast(b, tl);
				else
					high = SORTfndfirst(b, tl);
			} else {
				/* skip over nils at end of column */
				high = SORTfndfirst(b, nil);
			}
			if (hval) {
				if (hi)
					low = SORTfndfirst(b, th);
				else
					low = SORTfndlast(b, th);
			}
		} else {
			assert(oidxh != NULL);
			algo = poidx ? "select: parent orderidx" : "select: orderidx";
			if (lval) {
				if (li)
					low = ORDERfndfirst(b, oidxh, tl);
				else
					low = ORDERfndlast(b, oidxh, tl);
			} else {
				/* skip over nils at start of column */
				low = ORDERfndlast(b, oidxh, nil);
			}
			if (hval) {
				if (hi)
					high = ORDERfndlast(b, oidxh, th);
				else
					high = ORDERfndfirst(b, oidxh, th);
			}
		}
		if (anti) {
			assert(oidxh == NULL);
			if (bi.sorted) {
				BUN first = SORTfndlast(b, nil);
				/* match: [first..low) + [high..last) */
				bn = canditer_slice2val(&ci,
							first + b->hseqbase,
							low + b->hseqbase,
							high + b->hseqbase,
							oid_nil);
			} else {
				BUN last = SORTfndfirst(b, nil);
				/* match: [first..low) + [high..last) */
				bn = canditer_slice2val(&ci,
							oid_nil,
							low + b->hseqbase,
							high + b->hseqbase,
							last + b->hseqbase);
			}
		} else {
			if (bi.sorted || bi.revsorted) {
				assert(oidxh == NULL);
				/* match: [low..high) */
				bn = canditer_sliceval(&ci,
						       low + b->hseqbase,
						       high + b->hseqbase);
			} else {
				BUN i;
				BUN cnt = 0;
				const oid *rs;
				oid *rbn;

				rs = (const oid *) oidxh->base + ORDERIDXOFF;
				rs += low;
				bn = COLnew(0, TYPE_oid, high-low, TRANSIENT);
				if (bn == NULL) {
					HEAPdecref(oidxh, false);
					bat_iterator_end(&bi);
					bat_iterator_end(&pbi);
					return NULL;
				}

				rbn = (oid *) Tloc((bn), 0);

				for (i = low; i < high; i++) {
					if (vwl <= *rs && *rs <= vwh) {
						*rbn++ = (oid) ((lng) *rs + vwo);
						cnt++;
					}
					rs++;
				}
				HEAPdecref(oidxh, false);
				BATsetcount(bn, cnt);

				/* output must be sorted */
				GDKqsort(Tloc(bn, 0), NULL, NULL, (size_t) bn->batCount, sizeof(oid), 0, TYPE_oid, false, false);
				bn->tsorted = true;
				bn->trevsorted = bn->batCount <= 1;
				bn->tkey = true;
				bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? * (oid *) Tloc(bn, 0) : oid_nil;
				bn->tnil = false;
				bn->tnonil = true;
				if (s) {
					s = BATintersectcand(bn, s);
					BBPunfix(bn->batCacheid);
					bn = s;
				}
			}
		}

		bn = virtualize(bn);
		MT_thread_setalgorithm(algo);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",anti=%s -> "
			  ALGOOPTBATFMT " %s (" LLFMT " usec)\n",
			  ALGOBATPAR(b), anti ? "true" : "false",
			  ALGOOPTBATPAR(bn), algo,
			  GDKusec() - t0);

		bat_iterator_end(&bi);
		bat_iterator_end(&pbi);
		return bn;
	}

	assert(oidxh == NULL);
	/* upper limit for result size */
	maximum = ci.ncand;
	if (equi && havehash) {
		/* we can look in the hash struct to see whether all
		 * values are distinct and set estimate accordingly */
		if (phash) {
			if (pb->thash->nunique == pbi.count)
				estimate = 1;
		} else if (b->thash->nunique == bi.count)
			estimate = 1;
	}
	if (estimate == BUN_NONE && (bi.key || (pb != NULL && pbi.key))) {
		/* exact result size in special cases */
		if (equi) {
			estimate = 1;
		} else if (!anti && lval && hval) {
			switch (t) {
			case TYPE_bte:
				estimate = (BUN) (*(bte *) th - *(bte *) tl);
				break;
			case TYPE_sht:
				estimate = (BUN) (*(sht *) th - *(sht *) tl);
				break;
			case TYPE_int:
				CALC_ESTIMATE(int);
				break;
			case TYPE_lng:
				CALC_ESTIMATE(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				CALC_ESTIMATE(hge);
				break;
#endif
			}
			if (estimate == BUN_NONE)
				estimate += li + hi - 1;
		}
	}
	/* refine upper limit by exact size (if known) */
	maximum = MIN(maximum, estimate);
	if (wanthash && !havehash && estimate == BUN_NONE) {
		/* no exact result size, but we need estimate to
		 * choose between hash- & scan-select (if we already
		 * have a hash, it's a no-brainer: we use it) */
		if (ci.ncand <= 10000) {
			/* "small" input: don't bother about more accurate
			 * estimate */
			estimate = maximum;
		} else {
			/* layman's quick "pseudo-sample" of 1000 tuples,
			 * i.e., 333 from begin, middle & end of BAT */
			BUN smpl_cnt = 0, slct_cnt = 0, pos, skip, delta;
			BAT *smpl, *slct;

			delta = 1000 / 3 / 2;
			skip = (bi.count - (2 * delta)) / 2;
			for (pos = delta; pos < bi.count; pos += skip) {
				smpl = BATslice(b, pos - delta, pos + delta);
				if (smpl) {
					slct = BATselect(smpl, NULL, tl,
							    th, li, hi, anti);
					if (slct) {
						smpl_cnt += BATcount(smpl);
						slct_cnt += BATcount(slct);
						BBPreclaim(slct);
					}
					BBPreclaim(smpl);
				}
			}
			if (smpl_cnt > 0 && slct_cnt > 0) {
				/* linear extrapolation plus 10% margin */
				estimate = (BUN) ((dbl) slct_cnt / (dbl) smpl_cnt
						  * (dbl) bi.count * 1.1);
			} else if (smpl_cnt > 0 && slct_cnt == 0) {
				/* estimate low enough to trigger hash select */
				estimate = (ci.ncand / 100) - 1;
			}
		}
		wanthash = estimate < ci.ncand / 100;
	}
	if (estimate == BUN_NONE) {
		/* no better estimate possible/required:
		 * (pre-)allocate 1M tuples, i.e., avoid/delay extend
		 * without too much overallocation */
		estimate = 1000000;
	}
	/* limit estimation by upper limit */
	estimate = MIN(estimate, maximum);

	bn = COLnew(0, TYPE_oid, estimate, TRANSIENT);
	if (bn == NULL) {
		if (havehash) {
			if (phash)
				MT_rwlock_rdunlock(&pb->thashlock);
			else
				MT_rwlock_rdunlock(&b->thashlock);
		}
		bat_iterator_end(&bi);
		bat_iterator_end(&pbi);
		return NULL;
	}

	if (wanthash) {
		/* hashselect unlocks the hash lock */
		bn = hashselect(&bi, &ci, bn, tl, maximum, havehash, phash, &algo);
	} else {
		assert(!havehash);
		/* use imprints if
		 *   i) bat is persistent, or parent is persistent
		 *  ii) it is not an equi-select, and
		 * iii) imprints are supported.
		 */
		Imprints *imprints = NULL;
		if (!equi &&
		    /* DISABLES CODE */ (0) && imprintable(bi.type) &&
		    (!bi.transient ||
		     (pb != NULL && !pbi.transient)) &&
		    BATimprints(b) == GDK_SUCCEED) {
			if (pb != NULL) {
				MT_lock_set(&pb->batIdxLock);
				imprints = pb->timprints;
				if (imprints != NULL)
					IMPSincref(imprints);
				else
					imprints = NULL;
				MT_lock_unset(&pb->batIdxLock);
			} else {
				MT_lock_set(&b->batIdxLock);
				imprints = b->timprints;
				if (imprints != NULL)
					IMPSincref(imprints);
				else
					imprints = NULL;
				MT_lock_unset(&b->batIdxLock);
			}
		}
		GDKclrerr();
		bn = scanselect(&bi, &ci, bn, tl, th, li, hi, equi, anti,
				lval, hval, lnil, maximum, imprints, &algo);
		if (imprints)
			IMPSdecref(imprints, false);
	}
	bat_iterator_end(&bi);
	bat_iterator_end(&pbi);

	bn = virtualize(bn);
	MT_thread_setalgorithm(algo);
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT",anti=%s -> " ALGOOPTBATFMT
		  " %s (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  anti ? "true" : "false",
		  ALGOOPTBATPAR(bn), algo,
		  GDKusec() - t0);

	return bn;
}

/* theta select
 *
 * Returns a BAT with the OID values of b for qualifying tuples.  The
 * return BAT is sorted (i.e. in the same order as the input BAT).
 *
 * If s is not NULL, it is a list of candidates.  s must be sorted.
 *
 * Theta select returns all values from b which are less/greater than
 * or (not) equal to the provided value depending on the value of op.
 * Op is a string with one of the values: "=", "==", "<", "<=", ">",
 * ">=", "<>", "!=" (the first two are equivalent and the last two are
 * equivalent).  Theta select never returns nils.
 *
 * If value is nil, the result is empty.
 */
BAT *
BATthetaselect(BAT *b, BAT *s, const void *val, const char *op)
{
	const void *nil;

	BATcheck(b, NULL);
	BATcheck(val, NULL);
	BATcheck(op, NULL);

	nil = ATOMnilptr(b->ttype);
	if (ATOMcmp(b->ttype, val, nil) == 0)
		return BATdense(0, 0, 0);
	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[1] == 0)) {
		/* "=" or "==" */
		return BATselect(b, s, val, NULL, true, true, false);
	}
	if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
		/* "!=" (equivalent to "<>") */
		return BATselect(b, s, val, NULL, true, true, true);
	}
	if (op[0] == '<') {
		if (op[1] == 0) {
			/* "<" */
			return BATselect(b, s, nil, val, false, false, false);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* "<=" */
			return BATselect(b, s, nil, val, false, true, false);
		}
		if (op[1] == '>' && op[2] == 0) {
			/* "<>" (equivalent to "!=") */
			return BATselect(b, s, val, NULL, true, true, true);
		}
	}
	if (op[0] == '>') {
		if (op[1] == 0) {
			/* ">" */
			return BATselect(b, s, val, nil, false, false, false);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* ">=" */
			return BATselect(b, s, val, nil, true, false, false);
		}
	}
	GDKerror("unknown operator.\n");
	return NULL;
}

#define VALUE(s, x)	(s##vars ?					\
			 s##vars + VarHeapVal(s##vals, (x), s##width) : \
			 s##vals + ((x) * s##width))
#define FVALUE(s, x)	(s##vals + ((x) * s##width))

#define LTany(a,b)	((*cmp)(a, b) < 0)
#define EQany(a,b)	((*cmp)(a, b) == 0)
#define is_any_nil(v)	((v) == NULL || (*cmp)((v), nil) == 0)

#define less3(a,b,i,t)	(is_##t##_nil(a) || is_##t##_nil(b) ? bit_nil : LT##t(a, b) || (i && EQ##t(a, b)))
#define grtr3(a,b,i,t)	(is_##t##_nil(a) || is_##t##_nil(b) ? bit_nil : LT##t(b, a) || (i && EQ##t(a, b)))
#define or3(a,b)	((a) == 1 || (b) == 1 ? 1 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 0)
#define and3(a,b)	((a) == 0 || (b) == 0 ? 0 : is_bit_nil(a) || is_bit_nil(b) ? bit_nil : 1)
#define not3(a)		(is_bit_nil(a) ? bit_nil : !(a))

#define between3(v, lo, linc, hi, hinc, TYPE)				\
	and3(grtr3(v, lo, linc, TYPE), less3(v, hi, hinc, TYPE))

#define BETWEEN(v, lo, linc, hi, hinc, TYPE)				\
	(is_##TYPE##_nil(v)						\
	 ? bit_nil							\
	 : (bit) (anti							\
		  ? (symmetric						\
		     ? not3(or3(between3(v, lo, linc, hi, hinc, TYPE),	\
				between3(v, hi, hinc, lo, linc, TYPE)))	\
		     : not3(between3(v, lo, linc, hi, hinc, TYPE)))	\
		  : (symmetric						\
		     ? or3(between3(v, lo, linc, hi, hinc, TYPE),	\
			   between3(v, hi, hinc, lo, linc, TYPE))	\
		     : between3(v, lo, linc, hi, hinc, TYPE))))

gdk_return
rangejoin(BAT *r1, BAT *r2, BAT *l, BAT *rl, BAT *rh,
	  struct canditer *lci, struct canditer *rci,
	  bool linc, bool hinc, bool anti, bool symmetric, BUN maxsize)
{
	if (!anti && !symmetric) {
		/* we'll need these */
		(void) BATordered(l);
		(void) BATordered_rev(l);
	}
	BATiter li = bat_iterator(l);
	BATiter rli = bat_iterator(rl);
	BATiter rhi = bat_iterator(rh);
	const char *rlvals, *rhvals;
	const char *lvars, *rlvars, *rhvars;
	int rlwidth, rhwidth;
	int lwidth;
	const void *nil = ATOMnilptr(li.type);
	int (*cmp)(const void *, const void *) = ATOMcompare(li.type);
	int t;
	BUN cnt, ncnt, lncand = lci->ncand, rncand = rci->ncand;
	oid *restrict dst1, *restrict dst2;
	const void *vrl, *vrh;
	oid ro;
	oid rlval = oid_nil, rhval = oid_nil;
	int sorted = 0;		/* which output column is sorted */
	BAT *tmp = NULL;
	const char *algo = NULL;
	Heap *oidxh = NULL;

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	assert(ATOMtype(li.type) == ATOMtype(rli.type));
	assert(ATOMtype(li.type) == ATOMtype(rhi.type));
	assert(BATcount(rl) == BATcount(rh));
	assert(rl->hseqbase == rh->hseqbase);
	assert(r1->ttype == TYPE_oid);
	assert(r2 == NULL || r2->ttype == TYPE_oid);
	assert(r2 == NULL || BATcount(r1) == BATcount(r2));
	assert(li.type != TYPE_void || !is_oid_nil(l->tseqbase));
	assert(rli.type != TYPE_void || !is_oid_nil(rl->tseqbase));
	assert(rhi.type != TYPE_void || !is_oid_nil(rh->tseqbase));

	TRC_DEBUG(ALGO, "l=" ALGOBATFMT ","
		  "rl=" ALGOBATFMT ",rh=" ALGOBATFMT ","
		  "sl=" ALGOOPTBATFMT ",sr=" ALGOOPTBATFMT ","
		  "anti=%s,symmetric=%s\n",
		  ALGOBATPAR(l),
		  ALGOBATPAR(rl),
		  ALGOBATPAR(rh),
		  ALGOOPTBATPAR(lci->s),
		  ALGOOPTBATPAR(rci->s),
		  anti ? "true" : "false",
		  symmetric ? "true" : "false");

	rlvals = rli.type == TYPE_void ? NULL : (const char *) rli.base;
	rhvals = rhi.type == TYPE_void ? NULL : (const char *) rhi.base;
	lwidth = li.width;
	rlwidth = rli.width;
	rhwidth = rhi.width;
	dst1 = (oid *) Tloc(r1, 0);
	dst2 = r2 ? (oid *) Tloc(r2, 0) : NULL;

	t = ATOMtype(li.type);
	t = ATOMbasetype(t);

	if (li.vh && li.type) {
		assert(rli.vh && rli.type);
		assert(rhi.vh && rhi.type);
		lvars = li.vh->base;
		rlvars = rli.vh->base;
		rhvars = rhi.vh->base;
	} else {
		assert(rli.vh == NULL);
		assert(rhi.vh == NULL);
		lvars = rlvars = rhvars = NULL;
	}

	if (!anti && !symmetric && !li.sorted && !li.revsorted) {
		(void) BATcheckorderidx(l);
		MT_lock_set(&l->batIdxLock);
		if ((oidxh = l->torderidx) != NULL)
			HEAPincref(oidxh);
		MT_lock_unset(&l->batIdxLock);
#if 0 /* needs checking */
		if (oidxh == NULL && VIEWtparent(l)) {
			BAT *pb = BBP_cache(VIEWtparent(l));
			(void) BATcheckorderidx(pb);
			MT_lock_set(&pb->batIdxLock);
			if ((oidxh = pb->torderidx) != NULL) {
				HEAPincref(oidxh);
				l = pb;
			}
			MT_lock_unset(&pb->batIdxLock);
		}
#endif
	}

	vrl = &rlval;
	vrh = &rhval;
	if (!anti && !symmetric && (li.sorted || li.revsorted || oidxh)) {
		/* left column is sorted, use binary search */
		sorted = 2;
		TIMEOUT_LOOP(rncand, timeoffset) {
			BUN low, high;

			ro = canditer_next(rci);
			if (rlvals) {
				vrl = VALUE(rl, ro - rl->hseqbase);
			} else {
				/* TYPE_void */
				rlval = ro - rl->hseqbase + rl->tseqbase;
			}
			if (rhvals) {
				vrh = VALUE(rh, ro - rh->hseqbase);
			} else {
				/* TYPE_void */
				rhval = ro - rh->hseqbase + rh->tseqbase;
			}
			if (cmp(vrl, nil) == 0 || cmp(vrh, nil) == 0)
				continue;
			if (li.sorted) {
				if (linc)
					low = SORTfndfirst(l, vrl);
				else
					low = SORTfndlast(l, vrl);
				if (hinc)
					high = SORTfndlast(l, vrh);
				else
					high = SORTfndfirst(l, vrh);
			} else  if (li.revsorted) {
				if (hinc)
					low = SORTfndfirst(l, vrh);
				else
					low = SORTfndlast(l, vrh);
				if (linc)
					high = SORTfndlast(l, vrl);
				else
					high = SORTfndfirst(l, vrl);
			} else {
				assert(oidxh);
				if (linc)
					low = ORDERfndfirst(l, oidxh, vrl);
				else
					low = ORDERfndlast(l, oidxh, vrl);
				if (hinc)
					high = ORDERfndlast(l, oidxh, vrh);
				else
					high = ORDERfndfirst(l, oidxh, vrh);
			}
			if (high <= low)
				continue;
			if (li.sorted || li.revsorted) {
				low = canditer_search(lci, low + l->hseqbase, true);
				high = canditer_search(lci, high + l->hseqbase, true);
				assert(high >= low);

				if (BATcapacity(r1) < BATcount(r1) + high - low) {
					cnt = BATcount(r1) + high - low + 1024;
					if (cnt > maxsize)
						cnt = maxsize;
					BATsetcount(r1, BATcount(r1));
					if (BATextend(r1, cnt) != GDK_SUCCEED)
						goto bailout;
					dst1 = (oid *) Tloc(r1, 0);
					if (r2) {
						BATsetcount(r2, BATcount(r2));
						if (BATextend(r2, cnt) != GDK_SUCCEED)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
						dst2 = (oid *) Tloc(r2, 0);
					}
				}
				canditer_setidx(lci, low);
				while (low < high) {
					dst1[r1->batCount++] = canditer_next(lci);
					if (r2) {
						dst2[r2->batCount++] = ro;
					}
					low++;
				}
			} else {
				const oid *ord;

				assert(oidxh);
				ord = (const oid *) oidxh->base + ORDERIDXOFF;

				if (BATcapacity(r1) < BATcount(r1) + high - low) {
					cnt = BATcount(r1) + high - low + 1024;
					if (cnt > maxsize)
						cnt = maxsize;
					BATsetcount(r1, BATcount(r1));
					if (BATextend(r1, cnt) != GDK_SUCCEED)
						goto bailout;
					dst1 = (oid *) Tloc(r1, 0);
					if (r2) {
						BATsetcount(r2, BATcount(r2));
						if (BATextend(r2, cnt) != GDK_SUCCEED)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
						dst2 = (oid *) Tloc(r2, 0);
					}
				}

				while (low < high) {
					if (canditer_contains(lci, ord[low])) {
						dst1[r1->batCount++] = ord[low];
						if (r2) {
							dst2[r2->batCount++] = ro;
						}
					}
					low++;
				}
			}
		}
		if (oidxh)
			HEAPdecref(oidxh, false);
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		cnt = BATcount(r1);
		assert(r2 == NULL || BATcount(r1) == BATcount(r2));
	} else if (!anti && !symmetric &&
		   /* DISABLES CODE */ (0) && imprintable(li.type) &&
		   (BATcount(rl) > 2 ||
		    !li.transient ||
		    (VIEWtparent(l) != 0 &&
		     (tmp = BBP_cache(VIEWtparent(l))) != NULL &&
		     /* batTransient access needs to be protected */
		     !tmp->batTransient) ||
		    BATcheckimprints(l)) &&
		   BATimprints(l) == GDK_SUCCEED) {
		/* implementation using imprints on left column
		 *
		 * we use imprints if we can (the type is right for
		 * imprints) and either the left bat is persistent or
		 * already has imprints, or the right bats are long
		 * enough (for creating imprints being worth it) */
		Imprints *imprints = NULL;

		if (tmp) {
			MT_lock_set(&tmp->batIdxLock);
			imprints = tmp->timprints;
			if (imprints != NULL)
				IMPSincref(imprints);
			else
				imprints = NULL;
			MT_lock_unset(&tmp->batIdxLock);
		} else {
			MT_lock_set(&l->batIdxLock);
			imprints = l->timprints;
			if (imprints != NULL)
				IMPSincref(imprints);
			else
				imprints = NULL;
			MT_lock_unset(&l->batIdxLock);
		}
		/* in the unlikely case that the imprints were removed
		 * before we could get at them, take the slow, nested
		 * loop implementation */
		if (imprints == NULL)
			goto nestedloop;

		sorted = 2;
		cnt = 0;
		TIMEOUT_LOOP_IDX_DECL(i, rncand, timeoffset) {
			maxsize = cnt + (rncand - i) * lncand;
			ro = canditer_next(rci);
			if (rlvals) {
				vrl = FVALUE(rl, ro - rl->hseqbase);
			} else {
				/* TYPE_void */
				rlval = ro - rl->hseqbase + rl->tseqbase;
			}
			if (rhvals) {
				vrh = FVALUE(rh, ro - rh->hseqbase);
			} else {
				/* TYPE_void */
				rhval = ro - rl->hseqbase + rl->tseqbase;
			}
			dst1 = (oid *) Tloc(r1, 0);
			canditer_reset(lci);
			switch (t) {
			case TYPE_bte: {
				bte vl, vh;
				if (is_bte_nil((vl = *(bte *) vrl)))
					continue;
				if (is_bte_nil((vh = *(bte *) vrh)))
					continue;
				if (!linc) {
					if (vl == MAXVALUEbte)
						continue;
					vl = NEXTVALUEbte(vl);
				}
				if (!hinc) {
					if (vh == MINVALUEbte)
						continue;
					vh = PREVVALUEbte(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_bte(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
			case TYPE_sht: {
				sht vl, vh;
				if (is_sht_nil((vl = *(sht *) vrl)))
					continue;
				if (is_sht_nil((vh = *(sht *) vrh)))
					continue;
				if (!linc) {
					if (vl == MAXVALUEsht)
						continue;
					vl = NEXTVALUEsht(vl);
				}
				if (!hinc) {
					if (vh == MINVALUEsht)
						continue;
					vh = PREVVALUEsht(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_sht(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
			case TYPE_int:
#if SIZEOF_OID == SIZEOF_INT
			case TYPE_oid:
#endif
			{
				int vl, vh;
				if (is_int_nil((vl = *(int *) vrl)))
					continue;
				if (is_int_nil((vh = *(int *) vrh)))
					continue;
				if (!linc) {
					if (vl == MAXVALUEint)
						continue;
					vl = NEXTVALUEint(vl);
				}
				if (!hinc) {
#if SIZEOF_OID == SIZEOF_INT
					if (t == TYPE_oid) {
						if (vh == MINVALUEoid)
							continue;
						vh = PREVVALUEoid(vh);
					} else
#endif
					{
						if (vh == MINVALUEint)
							continue;
						vh = PREVVALUEint(vh);
					}
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_int(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
			case TYPE_lng:
#if SIZEOF_OID == SIZEOF_LNG
			case TYPE_oid:
#endif
			{
				lng vl, vh;
				if (is_lng_nil((vl = *(lng *) vrl)))
					continue;
				if (is_lng_nil((vh = *(lng *) vrh)))
					continue;
				if (!linc) {
					if (vl == MAXVALUElng)
						continue;
					vl = NEXTVALUElng(vl);
				}
				if (!hinc) {
#if SIZEOF_OID == SIZEOF_LNG
					if (t == TYPE_oid) {
						if (vh == MINVALUEoid)
							continue;
						vh = PREVVALUEoid(vh);
					} else
#endif
					{
						if (vh == MINVALUElng)
							continue;
						vh = PREVVALUElng(vh);
					}
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_lng(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
#ifdef HAVE_HGE
			case TYPE_hge: {
				hge vl, vh;
				if (is_hge_nil((vl = *(hge *) vrl)))
					continue;
				if (is_hge_nil((vh = *(hge *) vrh)))
					continue;
				if (!linc) {
					if (vl == MAXVALUEhge)
						continue;
					vl = NEXTVALUEhge(vl);
				}
				if (!hinc) {
					if (vh == MINVALUEhge)
						continue;
					vh = PREVVALUEhge(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_hge(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
#endif
			case TYPE_flt: {
				flt vl, vh;
				vl = *(flt *) vrl;
				if (is_flt_nil(vl))
					continue;
				vh = *(flt *) vrh;
				if (is_flt_nil(vh))
					continue;
				if (!linc) {
					if (vl == MAXVALUEflt)
						continue;
					vl = NEXTVALUEflt(vl);
				}
				if (!hinc) {
					if (vh == MINVALUEflt)
						continue;
					vh = PREVVALUEflt(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_flt(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
			case TYPE_dbl: {
				dbl vl, vh;
				vl = *(dbl *) vrl;
				if (is_dbl_nil(vl))
					continue;
				vh = *(dbl *) vrh;
				if (is_dbl_nil(vh))
					continue;
				if (!linc) {
					if (vl == MAXVALUEdbl)
						continue;
					vl = NEXTVALUEdbl(vl);
				}
				if (!hinc) {
					if (vh == MINVALUEdbl)
						continue;
					vh = PREVVALUEdbl(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_dbl(&li, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    imprints, &algo);
				break;
			}
			default:
				ncnt = BUN_NONE;
				GDKerror("unsupported type\n");
				MT_UNREACHABLE();
			}
			if (ncnt == BUN_NONE) {
				IMPSdecref(imprints, false);
				goto bailout;
			}
			assert(ncnt >= cnt || ncnt == 0);
			if (ncnt == cnt || ncnt == 0)
				continue;
			if (r2) {
				if (BATcapacity(r2) < ncnt) {
					BATsetcount(r2, cnt);
					if (BATextend(r2, BATcapacity(r1)) != GDK_SUCCEED) {
						IMPSdecref(imprints, false);
						goto bailout;
					}
					dst2 = (oid *) Tloc(r2, 0);
				}
				while (cnt < ncnt)
					dst2[cnt++] = ro;
			} else {
				cnt = ncnt;
			}
		}
		IMPSdecref(imprints, false);
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
	} else {
	  nestedloop:;
		/* nested loop implementation */
		const void *vl;
		const char *lvals;
		oid lval;

		GDKclrerr();	/* not interested in BATimprints errors */
		sorted = 1;
		lvals = li.type == TYPE_void ? NULL : (const char *) li.base;
		vl = &lval;
		TIMEOUT_LOOP(lncand, timeoffset) {
			oid lo;

			lo = canditer_next(lci);
			if (lvals) {
				vl = VALUE(l, lo - l->hseqbase);
				if (cmp(vl, nil) == 0)
					continue;
			} else {
				lval = lo - l->hseqbase + l->tseqbase;
			}
			canditer_reset(rci);
			for (BUN j = 0; j < rncand; j++) {
				ro = canditer_next(rci);
				if (rlvals) {
					vrl = VALUE(rl, ro - rl->hseqbase);
				} else {
					/* TYPE_void */
					rlval = ro - rl->hseqbase + rl->tseqbase;
				}
				if (rhvals) {
					vrh = VALUE(rh, ro - rh->hseqbase);
				} else {
					/* TYPE_void */
					rhval = ro - rh->hseqbase + rh->tseqbase;
				}
				if (BETWEEN(vl, vrl, linc, vrh, hinc, any) != 1)
					continue;
				if (BATcount(r1) == BATcapacity(r1)) {
					BUN newcap = BATgrows(r1);
					if (newcap > maxsize)
						newcap = maxsize;
					BATsetcount(r1, BATcount(r1));
					if (BATextend(r1, newcap) != GDK_SUCCEED)
						goto bailout;
					dst1 = (oid *) Tloc(r1, 0);
					if (r2) {
						BATsetcount(r2, BATcount(r2));
						if (BATextend(r2, newcap) != GDK_SUCCEED)
							goto bailout;
						assert(BATcapacity(r1) == BATcapacity(r2));
						dst2 = (oid *) Tloc(r2, 0);
					}
				}
				dst1[r1->batCount++] = lo;
				if (r2) {
					dst2[r2->batCount++] = ro;
				}
			}
		}
		TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		cnt = BATcount(r1);
		assert(r2 == NULL || BATcount(r1) == BATcount(r2));
	}

	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, cnt);

	/* set properties using an extra scan (usually not complete) */
	dst1 = (oid *) Tloc(r1, 0);
	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	r1->tseqbase = 0;
	r1->tnil = false;
	r1->tnonil = true;
	for (ncnt = 1; ncnt < cnt; ncnt++) {
		if (dst1[ncnt - 1] == dst1[ncnt]) {
			r1->tseqbase = oid_nil;
			r1->tkey = false;
		} else if (dst1[ncnt - 1] < dst1[ncnt]) {
			r1->trevsorted = false;
			if (dst1[ncnt - 1] + 1 != dst1[ncnt])
				r1->tseqbase = oid_nil;
		} else {
			assert(sorted != 1);
			r1->tsorted = false;
			r1->tseqbase = oid_nil;
			r1->tkey = false;
		}
		if (!(r1->trevsorted | BATtdense(r1) | r1->tkey | ((sorted != 1) & r1->tsorted)))
			break;
	}
	if (BATtdense(r1))
		r1->tseqbase = cnt > 0 ? dst1[0] : 0;
	if (r2) {
		BATsetcount(r2, cnt);
		dst2 = (oid *) Tloc(r2, 0);
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
		r2->tseqbase = 0;
		r2->tnil = false;
		r2->tnonil = true;
		for (ncnt = 1; ncnt < cnt; ncnt++) {
			if (dst2[ncnt - 1] == dst2[ncnt]) {
				r2->tseqbase = oid_nil;
				r2->tkey = false;
			} else if (dst2[ncnt - 1] < dst2[ncnt]) {
				r2->trevsorted = false;
				if (dst2[ncnt - 1] + 1 != dst2[ncnt])
					r2->tseqbase = oid_nil;
			} else {
				assert(sorted != 2);
				r2->tsorted = false;
				r2->tseqbase = oid_nil;
				r2->tkey = false;
			}
			if (!(r2->trevsorted | BATtdense(r2) | r2->tkey | ((sorted != 2) & r2->tsorted)))
				break;
		}
		if (BATtdense(r2))
			r2->tseqbase = cnt > 0 ? dst2[0] : 0;
	}
	TRC_DEBUG(ALGO, "l=%s,rl=%s,rh=%s -> "
		  "(" ALGOBATFMT "," ALGOOPTBATFMT ")\n",
		  BATgetId(l), BATgetId(rl), BATgetId(rh),
		  ALGOBATPAR(r1), ALGOOPTBATPAR(r2));
	bat_iterator_end(&li);
	bat_iterator_end(&rli);
	bat_iterator_end(&rhi);
	return GDK_SUCCEED;

  bailout:
	bat_iterator_end(&li);
	bat_iterator_end(&rli);
	bat_iterator_end(&rhi);
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}
