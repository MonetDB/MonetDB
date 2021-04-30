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

/* auxiliary functions and structs for imprints */
#include "gdk_imprints.h"

#define buninsfix(B,A,I,V,G,M,R)					\
	do {								\
		if ((I) == BATcapacity(B)) {				\
			BATsetcount((B), (I));				\
			if (BATextend((B),				\
				      MIN(BATcapacity(B) + (G),		\
					  (M))) != GDK_SUCCEED) {	\
				BBPreclaim(B);				\
				return (R);				\
			}						\
			A = (oid *) Tloc((B), 0);			\
		}							\
		A[(I)] = (V);						\
	} while (false)

BAT *
virtualize(BAT *bn)
{
	/* input must be a valid candidate list or NULL */
	if(bn && ((bn->ttype != TYPE_void && bn->ttype != TYPE_oid) || !bn->tkey || !bn->tsorted)) {
		fprintf(stderr, "#bn type %d nil %d key %d sorted %d\n",
				bn->ttype, is_oid_nil(bn->tseqbase),
				bn->tkey, bn->tsorted);
		fflush(stderr);
	}
	assert(bn == NULL ||
	       (((bn->ttype == TYPE_void && !is_oid_nil(bn->tseqbase)) ||
		 bn->ttype == TYPE_oid) &&
		bn->tkey && bn->tsorted));
	/* since bn has unique and strictly ascending values, we can
	 * easily check whether the column is dense */
	if (bn && bn->ttype == TYPE_oid &&
	    (BATcount(bn) <= 1 ||
	     * (const oid *) Tloc(bn, 0) + BATcount(bn) - 1 ==
	     * (const oid *) Tloc(bn, BUNlast(bn) - 1))) {
		/* column is dense, replace by virtual oid */
		TRC_DEBUG(ALGO, ALGOBATFMT ",seq=" OIDFMT "\n",
			  ALGOBATPAR(bn),
			  BATcount(bn) > 0 ? * (const oid *) Tloc(bn, 0) : 0);
		if (BATcount(bn) == 0)
			bn->tseqbase = 0;
		else
			bn->tseqbase = * (const oid *) Tloc(bn, 0);
		if (VIEWtparent(bn)) {
			Heap *h = GDKmalloc(sizeof(Heap));
			bat bid = VIEWtparent(bn);
			if (h == NULL) {
				BBPunfix(bn->batCacheid);
				return NULL;
			}
			*h = *bn->theap;
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
		bn->tvarsized = true;
		bn->twidth = 0;
		bn->tshift = 0;
	}

	return bn;
}

#define HASHloop_bound(bi, h, hb, v, lo, hi)		\
	for (hb = HASHget(h, HASHprobe((h), v));	\
	     hb != HASHnil(h);				\
	     hb = HASHgetlink(h,hb))			\
		if (hb >= (lo) && hb < (hi) &&		\
		    (cmp == NULL ||			\
		     (*cmp)(v, BUNtail(bi, hb)) == 0))

static BAT *
hashselect(BAT *b, struct canditer *restrict ci, BAT *bn,
	   const void *tl, BUN maximum, bool phash, const char **algo)
{
	BATiter bi;
	BUN i, cnt;
	oid o, *restrict dst;
	BUN l, h, d = 0;
	oid seq;
	int (*cmp)(const void *, const void *);

	assert(bn->ttype == TYPE_oid);
	seq = b->hseqbase;
	l = ci->seq - seq;
	h = canditer_last(ci) + 1 - seq;

	*algo = "hashselect";
	if (phash) {
		BAT *b2 = BBPdescriptor(VIEWtparent(b));
		*algo = "hashselect on parent";
		TRC_DEBUG(ALGO, ALGOBATFMT
			  " using parent(" ALGOBATFMT ") "
			  "for hash\n",
			  ALGOBATPAR(b),
			  ALGOBATPAR(b2));
		d = b->tbaseoff - b2->tbaseoff;
		l += d;
		h += d;
		b = b2;
	}

	if (BAThash(b) != GDK_SUCCEED) {
		BBPreclaim(bn);
		return NULL;
	}
	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
		cmp = NULL;	/* no need to compare: "hash" is perfect */
		break;
	default:
		cmp = ATOMcompare(b->ttype);
		break;
	}
	bi = bat_iterator(b);
	dst = (oid *) Tloc(bn, 0);
	cnt = 0;
	MT_rwlock_rdlock(&b->thashlock);
	if (ci->tpe != cand_dense) {
		HASHloop_bound(bi, b->thash, i, tl, l, h) {
			o = (oid) (i + seq - d);
			if (canditer_contains(ci, o)) {
				buninsfix(bn, dst, cnt, o,
					  maximum - BATcapacity(bn),
					  maximum, NULL);
				cnt++;
			}
		}
	} else {
		HASHloop_bound(bi, b->thash, i, tl, l, h) {
			o = (oid) (i + seq - d);
			buninsfix(bn, dst, cnt, o,
				  maximum - BATcapacity(bn),
				  maximum, NULL);
			cnt++;
		}
	}
	MT_rwlock_rdunlock(&b->thashlock);
	BATsetcount(bn, cnt);
	bn->tkey = true;
	if (cnt > 1) {
		/* hash chains produce results in the order high to
		 * low, so we just need to reverse */
		for (l = 0, h = BUNlast(bn) - 1; l < h; l++, h--) {
			o = dst[l];
			dst[l] = dst[h];
			dst[h] = o;
		}
	}
	bn->tsorted = true;
	bn->trevsorted = bn->batCount <= 1;
	bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? *dst : oid_nil;
	return bn;
}

/* Imprints select code */

/* inner check, non-dense canditer */
#define impscheck(TEST,ADD)						\
	do {								\
		const oid e = (oid) (i+limit-pr_off+hseq);		\
		if (im[icnt] & mask) {					\
			if ((im[icnt] & ~innermask) == 0) {		\
				while (p < ci->ncand && o < e) {	\
					v = src[o-hseq];		\
					ADD;				\
					cnt++;				\
					p++;				\
					o = canditer_next(ci);		\
				}					\
			} else {					\
				while (p < ci->ncand && o < e) {	\
					v = src[o-hseq];		\
					ADD;				\
					cnt += (TEST) != 0;		\
					p++;				\
					o = canditer_next(ci);		\
				}					\
			}						\
		} else {						\
			while (p < ci->ncand && o < e) {		\
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
				while (p < ci->ncand && o < e) {	\
					v = src[o-hseq];		\
					ADD;				\
					cnt++;				\
					p++;				\
					o = canditer_next_dense(ci);	\
				}					\
			} else {					\
				while (p < ci->ncand && o < e) {	\
					v = src[o-hseq];		\
					ADD;				\
					cnt += (TEST) != 0;		\
					p++;				\
					o = canditer_next_dense(ci);	\
				}					\
			}						\
		} else {						\
			BUN skip_sz = MIN(ci->ncand - p, e - o);	\
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
		const uint8_t rpp = ATOMelmshift(IMPS_PAGE >> b->tshift); \
		o = canditer_next(ci);					\
		for (i = 0, dcnt = 0, icnt = 0, p = 0;			\
		     dcnt < imprints->dictcnt && i <= w - hseq + pr_off && p < ci->ncand; \
		     dcnt++) {						\
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

#define quickins(dst, cnt, o, bn)			\
	do {						\
		assert((cnt) < BATcapacity(bn));	\
		dst[cnt] = (o);				\
	} while (false)

/* construct the mask */
#define impsmask(ISDENSE,TEST,B)					\
	do {								\
		const uint##B##_t *restrict im = (uint##B##_t *) imprints->imps; \
		uint##B##_t mask = 0, innermask;			\
		const int tpe = ATOMbasetype(b->ttype);			\
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
		if (!b->tnonil)						\
			innermask = IMPSunsetBit(B, innermask, 0);	\
									\
		if (BATcapacity(bn) < maximum) {			\
			impsloop(ISDENSE, TEST,				\
				 buninsfix(bn, dst, cnt, o,		\
					   (BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p) \
						  * (dbl) (ci->ncand-p) * 1.1 + 1024), \
					   maximum, BUN_NONE)); \
		} else {						\
			impsloop(ISDENSE, TEST, quickins(dst, cnt, o, bn)); \
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
		default: assert(0); break;				\
		}							\
	} while (false)

/* scan select without imprints */

/* core scan select loop with & without candidates */
#define scanloop(NAME,canditer_next,TEST)				\
	do {								\
		*algo = "select: " #NAME " " #TEST " (" #canditer_next ")"; \
		if (BATcapacity(bn) < maximum) {			\
			for (p = 0; p < ci->ncand; p++) {		\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				if (TEST) {				\
					buninsfix(bn, dst, cnt, o,	\
						  (BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p) \
							 * (dbl) (ci->ncand-p) * 1.1 + 1024), \
						  maximum, BUN_NONE); \
					cnt++;				\
				}					\
			}						\
		} else {						\
			for (p = 0; p < ci->ncand; p++) {		\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				assert(cnt < BATcapacity(bn));		\
				dst[cnt] = o;				\
				cnt += (TEST) != 0;			\
			}						\
		}							\
	} while (false)

/* argument list for type-specific core scan select function call */
#define scanargs							\
	b, ci, bn, tl, th, li, hi, equi, anti, lval, hval, lnil,	\
	cnt, b->hseqbase, dst, maximum, use_imprints, algo

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
		if (use_imprints) {					\
			bitswitch(ISDENSE, TEST, TYPE);			\
		} else {						\
			scanloop(NAME, canditer_next##ISDENSE, TEST);	\
		}							\
	} while (false)

/* definition of type-specific core scan select function */
#define scanfunc(NAME, TYPE, ISDENSE)					\
static BUN								\
NAME##_##TYPE(BAT *b, struct canditer *restrict ci, BAT *bn,		\
	      const TYPE *tl, const TYPE *th, bool li, bool hi,		\
	      bool equi, bool anti, bool lval, bool hval,		\
	      bool lnil, BUN cnt, const oid hseq, oid *restrict dst,	\
	      BUN maximum, bool use_imprints, const char **algo)	\
{									\
	TYPE vl = *tl;							\
	TYPE vh = *th;							\
	TYPE imp_min;							\
	TYPE imp_max;							\
	TYPE v;								\
	const TYPE nil = TYPE##_nil;					\
	const TYPE minval = MINVALUE##TYPE;				\
	const TYPE maxval = MAXVALUE##TYPE;				\
	const TYPE *src = (const TYPE *) Tloc(b, 0);			\
	const TYPE *basesrc;						\
	oid o, w;							\
	BUN p;								\
	BUN pr_off = 0;							\
	Imprints *imprints;						\
	bat parent = 0;							\
	(void) li;							\
	(void) hi;							\
	(void) lval;							\
	(void) hval;							\
	assert(li == !anti);						\
	assert(hi == !anti);						\
	assert(lval);							\
	assert(hval);							\
	if (use_imprints && /* DISABLES CODE */ (0) && (parent = VIEWtparent(b))) {		\
		BAT *pbat = BBPdescriptor(parent);			\
		assert(pbat);						\
		basesrc = (const TYPE *) Tloc(pbat, 0);			\
		imprints = pbat->timprints;				\
		pr_off = (BUN) (src - basesrc);				\
	} else {							\
		imprints = b->timprints;				\
		basesrc = src;						\
	}								\
	w = canditer_last(ci);						\
	if (equi) {							\
		assert(!use_imprints);					\
		if (lnil)						\
			scanloop(NAME, canditer_next##ISDENSE, is_##TYPE##_nil(v)); \
		else							\
			scanloop(NAME, canditer_next##ISDENSE, v == vl); \
	} else if (anti) {						\
		if (b->tnonil) {					\
			choose(NAME, ISDENSE, (v <= vl || v >= vh), TYPE); \
		} else {						\
			choose(NAME, ISDENSE, !is_##TYPE##_nil(v) && (v <= vl || v >= vh), TYPE); \
		}							\
	} else if (b->tnonil && vl == minval) {				\
		choose(NAME, ISDENSE, v <= vh, TYPE);			\
	} else if (vh == maxval) {					\
		choose(NAME, ISDENSE, v >= vl, TYPE);			\
	} else {							\
		choose(NAME, ISDENSE, v >= vl && v <= vh, TYPE);	\
	}								\
	return cnt;							\
}

static BUN
fullscan_any(BAT *b, struct canditer *restrict ci, BAT *bn,
	     const void *tl, const void *th,
	     bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	     bool lnil, BUN cnt, const oid hseq, oid *restrict dst,
	     BUN maximum, bool use_imprints, const char **algo)
{
	const void *v;
	const void *restrict nil = ATOMnilptr(b->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(b->ttype);
	BATiter bi = bat_iterator(b);
	oid o;
	BUN p;
	int c;

	(void) maximum;
	(void) use_imprints;
	(void) lnil;

	if (equi) {
		*algo = "select: fullscan equi";
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				v = BUNtail(bi, o-hseq);
				if ((*cmp)(tl, v) == 0) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				v = BUNtail(bi, o-hseq);
				if ((*cmp)(tl, v) == 0) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
	} else if (anti) {
		*algo = "select: fullscan anti";
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				v = BUNtail(bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((lval &&
					((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
					(hval &&
					((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0))))) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				v = BUNtail(bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((lval &&
					((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
					(hval &&
					((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0))))) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
	} else {
		*algo = "select: fullscan range";
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				v = BUNtail(bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((!lval ||
					(c = cmp(tl, v)) < 0 ||
					(li && c == 0)) &&
					(!hval ||
					(c = cmp(th, v)) > 0 ||
					(hi && c == 0)))) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				v = BUNtail(bi, o-hseq);
				if ((nil == NULL || (*cmp)(v, nil) != 0) &&
					((!lval ||
					(c = cmp(tl, v)) < 0 ||
					(li && c == 0)) &&
					(!hval ||
					(c = cmp(th, v)) > 0 ||
					(hi && c == 0)))) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
	}
	return cnt;
}

static BUN
fullscan_str(BAT *b, struct canditer *restrict ci, BAT *bn,
	     const char *tl, const char *th,
	     bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	     bool lnil, BUN cnt, const oid hseq, oid *restrict dst,
	     BUN maximum, bool use_imprints, const char **algo)
{
	var_t pos;
	BUN p;
	oid o;

	if (!equi || !GDK_ELIMDOUBLES(b->tvheap))
		return fullscan_any(b, ci, bn, tl, th, li, hi, equi, anti,
				    lval, hval, lnil, cnt, hseq, dst,
				    maximum, use_imprints, algo);
	if ((pos = strLocate(b->tvheap, tl)) == 0) {
		*algo = "select: fullscan equi strelim (nomatch)";
		return 0;
	}
	*algo = "select: fullscan equi strelim";
	assert(pos >= GDK_VAROFFSET);
	switch (b->twidth) {
	case 1: {
		const unsigned char *ptr = (const unsigned char *) Tloc(b, 0);
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
		break;
	}
	case 2: {
		const unsigned short *ptr = (const unsigned short *) Tloc(b, 0);
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
		break;
	}
#if SIZEOF_VAR_T == 8
	case 4: {
		const unsigned int *ptr = (const unsigned int *) Tloc(b, 0);
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
		break;
	}
#endif
	default: {
		const var_t *ptr = (const var_t *) Tloc(b, 0);
		if (ci->tpe == cand_dense) {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next_dense(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		} else {
			for (p = 0; p < ci->ncand; p++) {
				o = canditer_next(ci);
				if (ptr[o - hseq] == pos) {
					buninsfix(bn, dst, cnt, o,
						(BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p)
							* (dbl) (ci->ncand-p) * 1.1 + 1024),
						maximum, BUN_NONE);
					cnt++;
				}
			}
		}
		break;
	}
	}
	return cnt;
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
scanselect(BAT *b, struct canditer *restrict ci, BAT *bn,
	   const void *tl, const void *th,
	   bool li, bool hi, bool equi, bool anti, bool lval, bool hval,
	   bool lnil, BUN maximum, bool use_imprints, const char **algo)
{
#ifndef NDEBUG
	int (*cmp)(const void *, const void *);
#endif
	int t;
	BUN cnt = 0;
	oid *restrict dst;

	assert(b != NULL);
	assert(bn != NULL);
	assert(bn->ttype == TYPE_oid);
	assert(!lval || tl != NULL);
	assert(!hval || th != NULL);
	assert(!equi || (li && hi && !anti));
	assert(!anti || lval || hval);
	assert( anti || lval || hval || !b->tnonil);
	assert(b->ttype != TYPE_void || equi || b->tnonil);

#ifndef NDEBUG
	cmp = ATOMcompare(b->ttype);
#endif

	assert(!lval || !hval || (*cmp)(tl, th) <= 0);

	/* build imprints if they do not exist */
	if (use_imprints && (BATimprints(b) != GDK_SUCCEED)) {
		GDKclrerr();	/* not interested in BATimprints errors */
		use_imprints = false;
	}

	dst = (oid *) Tloc(bn, 0);

	t = ATOMbasetype(b->ttype);

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
	bn->tseqbase = cnt == 0 ? 0 : cnt == 1 || cnt == b->batCount ? b->hseqbase : oid_nil;

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
					if (*(TYPE*)tl == MAXVALUE##TYPE) \
						return BATdense(0, 0, 0); \
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
					if (*(TYPE*)th == MINVALUE##TYPE) \
						return BATdense(0, 0, 0); \
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
			if (*(TYPE*)tl > *(TYPE*)th)			\
				return BATdense(0, 0, 0);		\
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
	bool hash;		/* use hash (equi must be true) */
	bool phash = false;	/* use hash on parent BAT (if view) */
	int t;			/* data type */
	bat parent;		/* b's parent bat (if b is a view) */
	const void *nil;
	BAT *bn, *tmp;
	struct canditer ci;
	BUN estimate = BUN_NONE, maximum = BUN_NONE;
	oid vwl = 0, vwh = 0;
	lng vwo = 0;
	bool use_orderidx = false;
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

	if (s && s->ttype != TYPE_msk && !BATtordered(s)) {
		GDKerror("invalid argument: s must be sorted.\n");
		return NULL;
	}

	if (canditer_init(&ci, b, s) == 0) {
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

	t = b->ttype;
	nil = ATOMnilptr(t);
	/* can we use the base type? */
	t = ATOMbasetype(t);
	lnil = ATOMcmp(t, tl, nil) == 0; /* low value = nil? */

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
		return bn;
	}
	if (equi && lnil && b->tnonil) {
		/* return all nils, but there aren't any */
		MT_thread_setalgorithm("select: equi-nil, nonil");
		bn = BATdense(0, 0, 0);
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT
			  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
			  " (" LLFMT " usec): "
			  "equi-nil, nonil\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if (!equi && !lval && !hval && lnil && b->tnonil) {
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
		return bn;
	}

	if (anti) {
		const ValRecord *prop;
		int c;

		if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL) {
			c = ATOMcmp(t, tl, VALptr(prop));
			if (c < 0 || (li && c == 0)) {
				if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL) {
					c = ATOMcmp(t, th, VALptr(prop));
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
						return bn;
					}
				}
			}
		}
	} else if (!equi || !lnil) {
		const ValRecord *prop;
		int c;

		if (hval && (prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL) {
			c = ATOMcmp(t, th, VALptr(prop));
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
				return bn;
			}
		}
		if (lval && (prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL) {
			c = ATOMcmp(t, tl, VALptr(prop));
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
				return bn;
			}
		}
	}

	if (ATOMtype(b->ttype) == TYPE_oid) {
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
	/* use hash only for equi-join, and then only if b or its
	 * parent already has a hash, or if b or its parent is
	 * persistent and the total size wouldn't be too large; check
	 * for existence of hash last since that may involve I/O */
	hash = equi &&
		(BATcheckhash(b) ||
		 (!b->batTransient &&
		  ATOMsize(b->ttype) >= sizeof(BUN) / 4 &&
		  BATcount(b) * (ATOMsize(b->ttype) + 2 * sizeof(BUN)) < GDK_mem_maxsize / 2));
	if (equi && !hash && parent != 0) {
		/* use parent hash if it already exists and if either
		 * a quick check shows the value we're looking for
		 * does not occur, or if it is cheaper to check the
		 * candidate list for each value in the hash chain
		 * than to scan (cost for probe is average length of
		 * hash chain (count divided by #slots) times the cost
		 * to do a binary search on the candidate list (or 1
		 * if no need for search)) */
		tmp = BBPquickdesc(parent, false);
		hash = phash = tmp && BATcheckhash(tmp) &&
			(BATcount(tmp) == BATcount(b) ||
			 BATcount(tmp) / tmp->thash->nheads * (ci.tpe != cand_dense ? ilog2(BATcount(s)) : 1) < (s ? BATcount(s) : BATcount(b)) ||
			 HASHget(tmp->thash, HASHprobe(tmp->thash, tl)) == HASHnil(tmp->thash));
		/* create a hash on the parent bat (and use it) if it is
		 * the same size as the view and it is persistent */
		if (!phash &&
		    !tmp->batTransient &&
		    BATcount(tmp) == BATcount(b) &&
		    BAThash(tmp) == GDK_SUCCEED)
			hash = phash = true;
	}

	if (hash && (phash || b->thash)) {
		/* make sure tsorted and trevsorted flags are set, but
		 * we only need to know if we're not yet sure that we're
		 * going for the hash (i.e. it already exists) */
		(void) BATordered(b);
		(void) BATordered_rev(b);
	}

	/* If there is an order index or it is a view and the parent
	 * has an ordered index, and the bat is not tsorted or
	 * trevstorted then use the order index.  And there is no cand
	 * list or if there is one, it is dense.
	 * TODO: we do not support anti-select with order index */
	bool poidx = false;
	if (!anti &&
	    !(hash && (phash || b->thash)) &&
	    !(b->tsorted || b->trevsorted) &&
	    ci.tpe == cand_dense &&
	    (BATcheckorderidx(b) ||
	     (/* DISABLES CODE */ (0) &&
	      VIEWtparent(b) &&
	      BATcheckorderidx(BBPquickdesc(VIEWtparent(b), false))))) {
		BAT *view = NULL;
		if (/* DISABLES CODE */ (0) && VIEWtparent(b) && !BATcheckorderidx(b)) {
			view = b;
			b = BBPdescriptor(VIEWtparent(b));
		}
		/* Is query selective enough to use the ordered index ? */
		/* TODO: Test if this heuristic works in practice */
		/*if ((ORDERfnd(b, th) - ORDERfnd(b, tl)) < ((BUN)1000 < b->batCount/1000 ? (BUN)1000: b->batCount/1000))*/
		if ((ORDERfnd(b, th) - ORDERfnd(b, tl)) < b->batCount/3) {
			use_orderidx = true;
			if (view) {
				poidx = true; /* using parent oidx */
				vwo = (lng) (view->tbaseoff - b->tbaseoff);
				vwl = b->hseqbase + (oid) vwo + ci.seq - view->hseqbase;
				vwh = vwl + canditer_last(&ci) - ci.seq;
				vwo = (lng) view->hseqbase - (lng) b->hseqbase - vwo;
			} else {
				vwl = ci.seq;
				vwh = canditer_last(&ci);
			}
		} else if (view) {
			b = view;
			view = NULL;
		}
		if( view)
			TRC_DEBUG(ALGO, "Switch from " ALGOBATFMT " to " ALGOBATFMT " " OIDFMT "-" OIDFMT " hseq " LLFMT "\n", ALGOBATPAR(view), ALGOBATPAR(b), vwl, vwh, vwo);
	}

	if (!(hash && (phash || b->thash)) &&
	    (b->tsorted || b->trevsorted || use_orderidx)) {
		BUN low = 0;
		BUN high = b->batCount;

		if (BATtdense(b)) {
			/* positional */
			/* we expect nonil to be set, in which case we
			 * already know that we're not dealing with a
			 * nil equiselect (dealt with above) */
			oid h, l;
			assert(b->tnonil);
			assert(b->tsorted);
			algo = "select: dense";
			h = * (oid *) th + hi;
			if (h > b->tseqbase)
				h -= b->tseqbase;
			else
				h = 0;
			if ((BUN) h < high)
				high = (BUN) h;

			l = *(oid *) tl + !li;
			if (l > b->tseqbase)
				l -= b->tseqbase;
			else
				l = 0;
			if ((BUN) l > low)
				low = (BUN) l;
			if (low > high)
				low = high;
		} else if (b->tsorted) {
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
		} else if (b->trevsorted) {
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
			assert(use_orderidx);
			algo = poidx ? "select: parent orderidx" : "select: orderidx";
			if (lval) {
				if (li)
					low = ORDERfndfirst(b, tl);
				else
					low = ORDERfndlast(b, tl);
			} else {
				/* skip over nils at start of column */
				low = ORDERfndlast(b, nil);
			}
			if (hval) {
				if (hi)
					high = ORDERfndlast(b, th);
				else
					high = ORDERfndfirst(b, th);
			}
		}
		if (anti) {
			if (b->tsorted) {
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
			if (b->tsorted || b->trevsorted) {
				/* match: [low..high) */
				bn = canditer_sliceval(&ci,
						       low + b->hseqbase,
						       high + b->hseqbase);
			} else {
				BUN i;
				BUN cnt = 0;
				const oid *rs;
				oid *rbn;

				rs = (const oid *) b->torderidx->base + ORDERIDXOFF;
				rs += low;
				bn = COLnew(0, TYPE_oid, high-low, TRANSIENT);
				if (bn == NULL)
					return NULL;

				rbn = (oid *) Tloc((bn), 0);

				for (i = low; i < high; i++) {
					if (vwl <= *rs && *rs <= vwh) {
						*rbn++ = (oid) ((lng) *rs + vwo);
						cnt++;
					}
					rs++;
				}
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

		return bn;
	}

	/* upper limit for result size */
	maximum = ci.ncand;
	if (b->tkey) {
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
				estimate = (BUN) (*(int *) th - *(int *) tl);
				break;
			case TYPE_lng:
				estimate = (BUN) (*(lng *) th - *(lng *) tl);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				if (*(hge *) th - *(hge *) tl < (hge) BUN_MAX)
					estimate = (BUN) (*(hge *) th - *(hge *) tl);
				break;
#endif
			}
			if (estimate != BUN_NONE)
				estimate += li + hi - 1;
		}
	}
	/* refine upper limit by exact size (if known) */
	maximum = MIN(maximum, estimate);
	if (hash &&
	    !phash && /* phash implies there is a hash table already */
	    estimate == BUN_NONE &&
	    !b->thash) {
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
			skip = (BATcount(b) - (2 * delta)) / 2;
			for (pos = delta; pos < BATcount(b); pos += skip) {
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
						  * (dbl) BATcount(b) * 1.1);
			} else if (smpl_cnt > 0 && slct_cnt == 0) {
				/* estimate low enough to trigger hash select */
				estimate = (ci.ncand / 100) - 1;
			}
		}
		hash = estimate < ci.ncand / 100;
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
	if (bn == NULL)
		return NULL;

	if (hash) {
		bn = hashselect(b, &ci, bn, tl, maximum, phash, &algo);
	} else {
		/* use imprints if
		 *   i) bat is persistent, or parent is persistent
		 *  ii) it is not an equi-select, and
		 * iii) is not var-sized.
		 */
		bool use_imprints = !equi &&
			!b->tvarsized &&
			(!b->batTransient ||
			 (/* DISABLES CODE */ (0) &&
			  parent != 0 &&
			  (tmp = BBPquickdesc(parent, false)) != NULL &&
			  !tmp->batTransient));
		bn = scanselect(b, &ci, bn, tl, th, li, hi, equi, anti,
				lval, hval, lnil, maximum, use_imprints, &algo);
	}

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

#define between3(v, lo, linc, hi, hinc, TYPE)	\
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
	  bool li, bool hi, bool anti, bool symmetric, BUN maxsize)
{
	const char *rlvals, *rhvals;
	const char *lvars, *rlvars, *rhvars;
	int rlwidth, rhwidth;
	int lwidth;
	const void *nil = ATOMnilptr(l->ttype);
	int (*cmp)(const void *, const void *) = ATOMcompare(l->ttype);
	int t;
	BUN cnt, ncnt;
	oid *restrict dst1, *restrict dst2;
	const void *vrl, *vrh;
	oid ro;
	oid rlval = oid_nil, rhval = oid_nil;
	int sorted = 0;		/* which output column is sorted */
	BAT *tmp = NULL;
	bool use_orderidx = false;
	const char *algo = NULL;

	assert(ATOMtype(l->ttype) == ATOMtype(rl->ttype));
	assert(ATOMtype(l->ttype) == ATOMtype(rh->ttype));
	assert(BATcount(rl) == BATcount(rh));
	assert(rl->hseqbase == rh->hseqbase);
	assert(r1->ttype == TYPE_oid);
	assert(r2 == NULL || r2->ttype == TYPE_oid);
	assert(r2 == NULL || BATcount(r1) == BATcount(r2));
	assert(l->ttype != TYPE_void || !is_oid_nil(l->tseqbase));
	assert(rl->ttype != TYPE_void || !is_oid_nil(rl->tseqbase));
	assert(rh->ttype != TYPE_void || !is_oid_nil(rh->tseqbase));

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

	rlvals = rl->ttype == TYPE_void ? NULL : (const char *) Tloc(rl, 0);
	rhvals = rh->ttype == TYPE_void ? NULL : (const char *) Tloc(rh, 0);
	lwidth = l->twidth;
	rlwidth = rl->twidth;
	rhwidth = rh->twidth;
	dst1 = (oid *) Tloc(r1, 0);
	dst2 = r2 ? (oid *) Tloc(r2, 0) : NULL;

	t = ATOMtype(l->ttype);
	t = ATOMbasetype(t);

	if (l->tvarsized && l->ttype) {
		assert(rl->tvarsized && rl->ttype);
		assert(rh->tvarsized && rh->ttype);
		lvars = l->tvheap->base;
		rlvars = rl->tvheap->base;
		rhvars = rh->tvheap->base;
	} else {
		assert(!rl->tvarsized || !rl->ttype);
		assert(!rh->tvarsized || !rh->ttype);
		lvars = rlvars = rhvars = NULL;
	}

	if (!BATordered(l) && !BATordered_rev(l) &&
	    (BATcheckorderidx(l) || (/* DISABLES CODE */ (0) && VIEWtparent(l) && BATcheckorderidx(BBPquickdesc(VIEWtparent(l), false))))) {
		use_orderidx = true;
		if (/* DISABLES CODE */ (0) && VIEWtparent(l) && !BATcheckorderidx(l)) {
			l = BBPdescriptor(VIEWtparent(l));
		}
	}

	vrl = &rlval;
	vrh = &rhval;
	if (!anti && !symmetric && (BATordered(l) || BATordered_rev(l) || use_orderidx)) {
		/* left column is sorted, use binary search */
		sorted = 2;
		for (BUN i = 0; i < rci->ncand; i++) {
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
			if (l->tsorted) {
				if (li)
					low = SORTfndfirst(l, vrl);
				else
					low = SORTfndlast(l, vrl);
				if (hi)
					high = SORTfndlast(l, vrh);
				else
					high = SORTfndfirst(l, vrh);
			} else  if (l->trevsorted) {
				if (hi)
					low = SORTfndfirst(l, vrh);
				else
					low = SORTfndlast(l, vrh);
				if (li)
					high = SORTfndlast(l, vrl);
				else
					high = SORTfndfirst(l, vrl);
			} else {
				assert(use_orderidx);
				if (li)
					low = ORDERfndfirst(l, vrl);
				else
					low = ORDERfndlast(l, vrl);
				if (hi)
					high = ORDERfndlast(l, vrh);
				else
					high = ORDERfndfirst(l, vrh);
			}
			if (high <= low)
				continue;
			if (l->tsorted || l->trevsorted) {
				low = canditer_search(lci, low + l->hseqbase, true);
				high = canditer_search(lci, high + l->hseqbase, true);
				assert(high >= low);

				if (BATcapacity(r1) < BUNlast(r1) + high - low) {
					cnt = BUNlast(r1) + high - low + 1024;
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

				assert(use_orderidx);
				ord = (const oid *) l->torderidx->base + ORDERIDXOFF;

				if (BATcapacity(r1) < BUNlast(r1) + high - low) {
					cnt = BUNlast(r1) + high - low + 1024;
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
		cnt = BATcount(r1);
		assert(r2 == NULL || BATcount(r1) == BATcount(r2));
	} else if (!anti && !symmetric &&
		   (BATcount(rl) > 2 ||
		    !l->batTransient ||
		    (/* DISABLES CODE */ (0) &&
		     VIEWtparent(l) != 0 &&
		     (tmp = BBPquickdesc(VIEWtparent(l), false)) != NULL &&
		     !tmp->batTransient) ||
		    BATcheckimprints(l)) &&
		   BATimprints(l) == GDK_SUCCEED) {
		(void) tmp;	/* void cast because of disabled code */
		/* implementation using imprints on left column
		 *
		 * we use imprints if we can (the type is right for
		 * imprints) and either the left bat is persistent or
		 * already has imprints, or the right bats are long
		 * enough (for creating imprints being worth it) */

		sorted = 2;
		cnt = 0;
		for (BUN i = 0; i < rci->ncand; i++) {
			maxsize = cnt + (rci->ncand - i) * lci->ncand;
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
				if (!li) {
					if (vl == MAXVALUEbte)
						continue;
					vl = NEXTVALUEbte(vl);
				}
				if (!hi) {
					if (vh == MINVALUEbte)
						continue;
					vh = PREVVALUEbte(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_bte(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
				break;
			}
			case TYPE_sht: {
				sht vl, vh;
				if (is_sht_nil((vl = *(sht *) vrl)))
					continue;
				if (is_sht_nil((vh = *(sht *) vrh)))
					continue;
				if (!li) {
					if (vl == MAXVALUEsht)
						continue;
					vl = NEXTVALUEsht(vl);
				}
				if (!hi) {
					if (vh == MINVALUEsht)
						continue;
					vh = PREVVALUEsht(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_sht(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
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
				if (!li) {
					if (vl == MAXVALUEint)
						continue;
					vl = NEXTVALUEint(vl);
				}
				if (!hi) {
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
				ncnt = fullscan_int(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
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
				if (!li) {
					if (vl == MAXVALUElng)
						continue;
					vl = NEXTVALUElng(vl);
				}
				if (!hi) {
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
				ncnt = fullscan_lng(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
				break;
			}
#ifdef HAVE_HGE
			case TYPE_hge: {
				hge vl, vh;
				if (is_hge_nil((vl = *(hge *) vrl)))
					continue;
				if (is_hge_nil((vh = *(hge *) vrh)))
					continue;
				if (!li) {
					if (vl == MAXVALUEhge)
						continue;
					vl = NEXTVALUEhge(vl);
				}
				if (!hi) {
					if (vh == MINVALUEhge)
						continue;
					vh = PREVVALUEhge(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_hge(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
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
				if (!li) {
					if (vl == MAXVALUEflt)
						continue;
					vl = NEXTVALUEflt(vl);
				}
				if (!hi) {
					if (vh == MINVALUEflt)
						continue;
					vh = PREVVALUEflt(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_flt(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
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
				if (!li) {
					if (vl == MAXVALUEdbl)
						continue;
					vl = NEXTVALUEdbl(vl);
				}
				if (!hi) {
					if (vh == MINVALUEdbl)
						continue;
					vh = PREVVALUEdbl(vh);
				}
				if (vl > vh)
					continue;
				ncnt = fullscan_dbl(l, lci, r1, &vl, &vh,
						    true, true, false,
						    false, true, true,
						    false, cnt,
						    l->hseqbase, dst1,
						    maxsize,
						    true, &algo);
				break;
			}
			default:
				ncnt = BUN_NONE;
				GDKerror("unsupported type\n");
				assert(0);
			}
			if (ncnt == BUN_NONE)
				goto bailout;
			assert(ncnt >= cnt || ncnt == 0);
			if (ncnt == cnt || ncnt == 0)
				continue;
			if (r2) {
				if (BATcapacity(r2) < ncnt) {
					BATsetcount(r2, cnt);
					if (BATextend(r2, BATcapacity(r1)) != GDK_SUCCEED)
						goto bailout;
					dst2 = (oid *) Tloc(r2, 0);
				}
				while (cnt < ncnt)
					dst2[cnt++] = ro;
			} else {
				cnt = ncnt;
			}
		}
	} else {
		/* nested loop implementation */
		const void *vl;
		const char *lvals;
		oid lval;

		GDKclrerr();	/* not interested in BATimprints errors */
		sorted = 1;
		lvals = l->ttype == TYPE_void ? NULL : (const char *) Tloc(l, 0);
		vl = &lval;
		for (BUN i = 0; i < lci->ncand; i++) {
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
			for (BUN j = 0; j < rci->ncand; j++) {
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
				if (BETWEEN(vl, vrl, li, vrh, hi, any) != 1)
					continue;
				if (BUNlast(r1) == BATcapacity(r1)) {
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
	return GDK_SUCCEED;

  bailout:
	BBPreclaim(r1);
	BBPreclaim(r2);
	return GDK_FAIL;
}
