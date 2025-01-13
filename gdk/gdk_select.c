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

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

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
			if (h == NULL) {
				BBPunfix(bn->batCacheid);
				return NULL;
			}
			*h = *bn->theap;
			settailname(h, BBP_physical(bn->batCacheid), TYPE_oid, 0);
			h->parentid = bn->batCacheid;
			h->base = NULL;
			h->hasfile = false;
			ATOMIC_INIT(&h->refs, 1);
			if (bn->theap->parentid != bn->batCacheid)
				BBPrelease(bn->theap->parentid);
			HEAPdecref(bn->theap, false);
			bn->theap = h;
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
	BAT *b2 = NULL;
	BATiter pbi = {0};

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	assert(bn->ttype == TYPE_oid);
	seq = bi->b->hseqbase;
	l = ci->seq - seq;
	h = canditer_last(ci) + 1 - seq;

	*algo = "hashselect";
	if (phash && (b2 = BATdescriptor(VIEWtparent(bi->b))) != NULL) {
		*algo = "hashselect on parent";
		TRC_DEBUG(ALGO, ALGOBATFMT
			  " using parent(" ALGOBATFMT ") "
			  "for hash\n",
			  ALGOBATPAR(bi->b),
			  ALGOBATPAR(b2));
		d = bi->baseoff - b2->tbaseoff;
		l += d;
		h += d;
		pbi = bat_iterator(b2);
		bi = &pbi;
	} else {
		phash = false;
	}

	if (!havehash) {
		if (BAThash(bi->b) != GDK_SUCCEED) {
			BBPreclaim(bn);
			BBPreclaim(b2);
			if (phash)
				bat_iterator_end(&pbi);
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
			GDK_CHECK_TIMEOUT(qry_ctx, counter,
					  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
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
			GDK_CHECK_TIMEOUT(qry_ctx, counter,
					  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
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
	BBPreclaim(b2);
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
	if (phash)
		bat_iterator_end(&pbi);
	return bn;

  bailout:
	MT_rwlock_rdunlock(&bi->b->thashlock);
	if (phash)
		bat_iterator_end(&pbi);
	BBPreclaim(b2);
	BBPreclaim(bn);
	return NULL;
}

/* core scan select loop with & without candidates */
#define scanloop(NAME,canditer_next,TEST)				\
	do {								\
		BUN ncand = ci->ncand;					\
		*algo = "select: " #NAME " " #TEST " (" #canditer_next ")"; \
		if (BATcapacity(bn) < maximum) {			\
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {		\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				if (TEST) {				\
					dst = buninsfix(bn, dst, cnt, o, \
						  (BUN) ((dbl) cnt / (dbl) (p == 0 ? 1 : p) \
							 * (dbl) (ncand-p) * 1.1 + 1024), \
							maximum);	\
					if (dst == NULL) {		\
						goto bailout;		\
					}				\
					cnt++;				\
				}					\
			}						\
		} else {						\
			TIMEOUT_LOOP(ncand, qry_ctx) {			\
				o = canditer_next(ci);			\
				v = src[o-hseq];			\
				assert(cnt < BATcapacity(bn));		\
				dst[cnt] = o;				\
				cnt += (TEST) != 0;			\
			}						\
		}							\
		TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
	} while (false)

/* argument list for type-specific core scan select function call */
#define scanargs							\
	bi, ci, bn, tl, th, li, hi, equi, anti, nil_matches, lval, hval, \
	lnil, cnt, bi->b->hseqbase, dst, maximum, algo

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

/* definition of type-specific core scan select function */
#define scanfunc(NAME, TYPE, ISDENSE)					\
static BUN								\
NAME##_##TYPE(BATiter *bi, struct canditer *restrict ci, BAT *bn,	\
	      const TYPE *tl, const TYPE *th, bool li, bool hi,		\
	      bool equi, bool anti, bool nil_matches, bool lval,	\
	      bool hval, bool lnil, BUN cnt, const oid hseq,		\
	      oid *restrict dst, BUN maximum, const char **algo)	\
{									\
	TYPE vl = *tl;							\
	TYPE vh = *th;							\
	TYPE v;								\
	const TYPE minval = MINVALUE##TYPE;				\
	const TYPE maxval = MAXVALUE##TYPE;				\
	const TYPE *src = (const TYPE *) bi->base;			\
	oid o;								\
	BUN p;								\
	(void) li;							\
	(void) hi;							\
	(void) lval;							\
	(void) hval;							\
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();			\
	/* Normalize the variables li, hi, lval, hval, possibly */	\
	/* changing anti in the process.  This works for all */		\
	/* (and only) numeric types. */					\
									\
	/* Note that the expression x < v is equivalent to x <= */	\
	/* v' where v' is the next smaller value in the domain */	\
	/* of v (similarly for x > v).  Also note that for */		\
	/* floating point numbers there actually is such a */		\
	/* value.  In fact, there is a function in standard C */	\
	/* that calculates that value. */				\
									\
	/* The result is: */						\
	/* li == !anti, hi == !anti, lval == true, hval == true */	\
	/* This means that all ranges that we check for are */		\
	/* closed ranges.  If a range is one-sided, we fill in */	\
	/* the minimum resp. maximum value in the domain so that */	\
	/* we create a closed range. */					\
	if (anti && li) {						\
		/* -inf < x < vl === -inf < x <= vl-1 */		\
		if (vl == MINVALUE##TYPE) {				\
			/* -inf < x < MIN || *th <[=] x < +inf */	\
			/* degenerates into half range */		\
			/* *th <[=] x < +inf */				\
			anti = false;					\
			vl = vh;					\
			li = !hi;					\
			hval = false;					\
			/* further dealt with below */			\
		} else {						\
			vl = PREVVALUE##TYPE(vl);			\
			li = false;					\
		}							\
	}								\
	if (anti && hi) {						\
		/* vl < x < +inf === vl+1 <= x < +inf */		\
		if (vh == MAXVALUE##TYPE) {				\
			/* -inf < x <[=] *tl || MAX > x > +inf */	\
			/* degenerates into half range */		\
			/* -inf < x <[=] *tl */				\
			anti = false;					\
			vh = vl;					\
			hi = !li;					\
			lval = false;					\
			/* further dealt with below */			\
		} else {						\
			vh = NEXTVALUE##TYPE(vh);			\
			hi = false;					\
		}							\
	}								\
	if (!anti) {							\
		if (lval) {						\
			/* range bounded on left */			\
			if (!li) {					\
				/* open range on left */		\
				if (vl == MAXVALUE##TYPE) {		\
					*algo = "select: empty range";	\
					return 0;			\
				}					\
				/* vl < x === vl+1 <= x */		\
				vl = NEXTVALUE##TYPE(vl);		\
				li = true;				\
			}						\
		} else {						\
			/* -inf, i.e. smallest value */			\
			vl = MINVALUE##TYPE;				\
			li = true;					\
			lval = true;					\
		}							\
		if (hval) {						\
			/* range bounded on right */			\
			if (!hi) {					\
				/* open range on right */		\
				if (vh == MINVALUE##TYPE) {		\
					*algo = "select: empty range";	\
					return 0;			\
				}					\
				/* x < vh === x <= vh-1 */		\
				vh = PREVVALUE##TYPE(vh);		\
				hi = true;				\
			}						\
		} else {						\
			/* +inf, i.e. largest value */			\
			vh = MAXVALUE##TYPE;				\
			hi = true;					\
			hval = true;					\
		}							\
		if (vl > vh) {						\
			*algo = "select: empty range";			\
			return 0;					\
		}							\
	}								\
	/* if anti is set, we can now check */				\
	/* (x <= vl || x >= vh) && x != nil */				\
	/* if anti is not set, we can check just */			\
	/* vl <= x && x <= vh */					\
	/* if equi==true, the check is x == vl */			\
	/* note that this includes the check for != nil */		\
	assert(li == !anti);						\
	assert(hi == !anti);						\
	assert(lval);							\
	assert(hval);							\
	if (equi) {							\
		if (lnil)						\
			scanloop(NAME, canditer_next##ISDENSE, is_##TYPE##_nil(v)); \
		else							\
			scanloop(NAME, canditer_next##ISDENSE, v == vl); \
	} else if (anti) {						\
		if (bi->nonil) {					\
			scanloop(NAME, canditer_next##ISDENSE, (v <= vl || v >= vh)); \
		} else if (nil_matches) {				\
			scanloop(NAME, canditer_next##ISDENSE, is_##TYPE##_nil(v) || v <= vl || v >= vh); \
		} else {						\
			scanloop(NAME, canditer_next##ISDENSE, !is_##TYPE##_nil(v) && (v <= vl || v >= vh)); \
		}							\
	} else if (bi->nonil && vl == minval) {				\
		scanloop(NAME, canditer_next##ISDENSE, v <= vh);			\
	} else if (vh == maxval) {					\
		scanloop(NAME, canditer_next##ISDENSE, v >= vl);			\
	} else {							\
		scanloop(NAME, canditer_next##ISDENSE, v >= vl && v <= vh);	\
	}								\
	return cnt;							\
  bailout:								\
	BBPreclaim(bn);							\
	return BUN_NONE;						\
}

static BUN
fullscan_any(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	     const void *tl, const void *th,
	     bool li, bool hi, bool equi, bool anti, bool nil_matches,
	     bool lval, bool hval, bool lnil, BUN cnt, const oid hseq,
	     oid *restrict dst, BUN maximum, const char **algo)
{
	const void *v;
	const void *restrict nil = ATOMnilptr(bi->type);
	int (*cmp)(const void *, const void *) = ATOMcompare(bi->type);
	oid o;
	BUN p, ncand = ci->ncand;
	int c;

	(void) maximum;
	(void) lnil;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if (equi) {
		*algo = "select: fullscan equi";
		if (ci->tpe == cand_dense) {
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
				o = canditer_next_dense(ci);
				v = BUNtail(*bi, o-hseq);
				bool isnil = nil != NULL && (*cmp)(v, nil) == 0;
				if ((nil_matches && isnil) ||
				    (!isnil &&
				     ((lval &&
				       ((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
				      (hval &&
				       ((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0)))))) {
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
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
				o = canditer_next(ci);
				v = BUNtail(*bi, o-hseq);
				bool isnil = nil != NULL && (*cmp)(v, nil) == 0;
				if ((nil_matches && isnil) ||
				    (!isnil &&
				     ((lval &&
				       ((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
				      (hval &&
				       ((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0)))))) {
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
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	return cnt;
  bailout:
	BBPreclaim(bn);
	return BUN_NONE;
}

static BUN
fullscan_str(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	     const char *tl, const char *th,
	     bool li, bool hi, bool equi, bool anti, bool nil_matches,
	     bool lval, bool hval, bool lnil, BUN cnt, const oid hseq,
	     oid *restrict dst, BUN maximum, const char **algo)
{
	var_t pos;
	BUN p, ncand = ci->ncand;
	oid o;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	if (anti && tl == th && !bi->nonil && GDK_ELIMDOUBLES(bi->vh) &&
	    strcmp(tl, str_nil) != 0 &&
	    strLocate(bi->vh, str_nil) == (var_t) -2) {
		/* anti-equi select for non-nil value, and there are no
		 * nils, so we can use fast path; trigger by setting
		 * nonil */
		bi->nonil = true;
	}
	if (!((equi ||
	       (anti && tl == th && (bi->nonil || strcmp(tl, str_nil) == 0))) &&
	      GDK_ELIMDOUBLES(bi->vh)))
		return fullscan_any(bi, ci, bn, tl, th, li, hi, equi, anti,
				    nil_matches, lval, hval, lnil, cnt, hseq,
				    dst, maximum, algo);
	if ((pos = strLocate(bi->vh, tl)) == (var_t) -2) {
		if (anti) {
			/* return the whole shebang */
			*algo = "select: fullscan anti-equi strelim (all)";
			if (BATextend(bn, ncand) != GDK_SUCCEED) {
				BBPreclaim(bn);
				return BUN_NONE;
			}
			dst = Tloc(bn, 0);
			TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
				dst[p] = canditer_next(ci);
			}
			TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
			return ncand;
		}
		*algo = "select: fullscan equi strelim (nomatch)";
		return 0;
	}
	if (pos == (var_t) -1) {
		*algo = NULL;
		BBPreclaim(bn);
		return BUN_NONE;
	}
	*algo = anti ? "select: fullscan anti-equi strelim" : "select: fullscan equi strelim";
	assert(pos >= GDK_VAROFFSET);
	switch (bi->width) {
	case 1: {
		const unsigned char *ptr = (const unsigned char *) bi->base;
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next_dense(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			}
		} else {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
		}
		break;
	}
	case 2: {
		const unsigned short *ptr = (const unsigned short *) bi->base;
		pos -= GDK_VAROFFSET;
		if (ci->tpe == cand_dense) {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next_dense(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			}
		} else {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
		}
		break;
	}
#if SIZEOF_VAR_T == 8
	case 4: {
		const unsigned int *ptr = (const unsigned int *) bi->base;
		if (ci->tpe == cand_dense) {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next_dense(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			}
		} else {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
		}
		break;
	}
#endif
	default: {
		const var_t *ptr = (const var_t *) bi->base;
		if (ci->tpe == cand_dense) {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next_dense(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
			}
		} else {
			if (anti) {
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
					o = canditer_next(ci);
					if (ptr[o - hseq] != pos) {
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
				TIMEOUT_LOOP_IDX(p, ncand, qry_ctx) {
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
		}
		break;
	}
	}
	TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
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

/* scan select */
scan_sel(fullscan, )
scan_sel(densescan, _dense)

#if 0
/* some programs that produce editor tags files don't recognize the
 * scanselect function because before it are the scan_del macro
 * calls that don't look like function definitions or variable
 * declarations, hence we have this hidden away function to realign the
 * tags program */
void
realign_tags(void)
{
}
#endif

static BAT *
scanselect(BATiter *bi, struct canditer *restrict ci, BAT *bn,
	   const void *tl, const void *th,
	   bool li, bool hi, bool equi, bool anti, bool nil_matches,
	   bool lval, bool hval, bool lnil,
	   BUN maximum, const char **algo)
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

	assert(!lval || !hval || tl == th || (*cmp)(tl, th) <= 0);

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

static enum range_comp_t
BATrange(BATiter *bi, const void *tl, const void *th, bool li, bool hi)
{
	enum range_comp_t range;
	const ValRecord *minprop = NULL, *maxprop = NULL;
	const void *minval = NULL, *maxval = NULL;
	bool maxincl = true;
	BAT *pb = NULL;
	int c;
	int (*atomcmp) (const void *, const void *) = ATOMcompare(bi->type);
	BATiter bi2 = *bi;

	if (tl && (*atomcmp)(tl, ATOMnilptr(bi->type)) == 0)
		tl = NULL;
	if (th && (*atomcmp)(th, ATOMnilptr(bi->type)) == 0)
		th = NULL;
	if (tl == NULL && th == NULL)
		return range_contains; /* looking for everything */

	if (VIEWtparent(bi->b))
		pb = BATdescriptor(VIEWtparent(bi->b));

	/* keep locked while we look at the property values */
	MT_lock_set(&bi->b->theaplock);
	if (bi->sorted && (bi->nonil || atomcmp(BUNtail(*bi, 0), ATOMnilptr(bi->type)) != 0))
		minval = BUNtail(*bi, 0);
	else if (bi->revsorted && (bi->nonil || atomcmp(BUNtail(*bi, bi->count - 1), ATOMnilptr(bi->type)) != 0))
		minval = BUNtail(*bi, bi->count - 1);
	else if (bi->minpos != BUN_NONE)
		minval = BUNtail(*bi, bi->minpos);
	else if ((minprop = BATgetprop_nolock(bi->b, GDK_MIN_BOUND)) != NULL)
		minval = VALptr(minprop);
	if (bi->sorted && (bi->nonil || atomcmp(BUNtail(bi2, bi->count - 1), ATOMnilptr(bi->type)) != 0)) {
		maxval = BUNtail(bi2, bi->count - 1);
		maxincl = true;
	} else if (bi->revsorted && (bi->nonil || atomcmp(BUNtail(bi2, 0), ATOMnilptr(bi->type)) != 0)) {
		maxval = BUNtail(bi2, 0);
		maxincl = true;
	} else if (bi->maxpos != BUN_NONE) {
		maxval = BUNtail(bi2, bi->maxpos);
		maxincl = true;
	} else if ((maxprop = BATgetprop_nolock(bi->b, GDK_MAX_BOUND)) != NULL) {
		maxval = VALptr(maxprop);
		maxincl = false;
	}
	bool keep = false;	/* keep lock on parent bat? */
	if (minval == NULL || maxval == NULL) {
		if (pb != NULL) {
			MT_lock_set(&pb->theaplock);
			if (minval == NULL && (minprop = BATgetprop_nolock(pb, GDK_MIN_BOUND)) != NULL) {
				keep = true;
				minval = VALptr(minprop);
			}
			if (maxval == NULL && (maxprop = BATgetprop_nolock(pb, GDK_MAX_BOUND)) != NULL) {
				keep = true;
				maxval = VALptr(maxprop);
				maxincl = true;
			}
			if (!keep) {
				MT_lock_unset(&pb->theaplock);
			}
		}
	}

	if (minval == NULL && maxval == NULL) {
		range = range_inside; /* strictly: unknown */
	} else if (maxval &&
		   tl &&
		   ((c = atomcmp(tl, maxval)) > 0 ||
		    ((!maxincl || !li) && c == 0))) {
		range = range_after;
	} else if (minval &&
		   th &&
		   ((c = atomcmp(th, minval)) < 0 ||
		    (!hi && c == 0))) {
		range = range_before;
	} else if (tl == NULL) {
		if (minval == NULL) {
			c = atomcmp(th, maxval);
			if (c < 0 || ((maxincl || !hi) && c == 0))
				range = range_atstart;
			else
				range = range_contains;
		} else {
			c = atomcmp(th, minval);
			if (c < 0 || (!hi && c == 0))
				range = range_before;
			else if (maxval == NULL)
				range = range_atstart;
			else {
				c = atomcmp(th, maxval);
				if (c < 0 || ((maxincl || !hi) && c == 0))
					range = range_atstart;
				else
					range = range_contains;
			}
		}
	} else if (th == NULL) {
		if (maxval == NULL) {
			c = atomcmp(tl, minval);
			if (c >= 0)
				range = range_atend;
			else
				range = range_contains;
		} else {
			c = atomcmp(tl, maxval);
			if (c > 0 || ((!maxincl || !li) && c == 0))
				range = range_after;
			else if (minval == NULL)
				range = range_atend;
			else {
				c = atomcmp(tl, minval);
				if (c >= 0)
					range = range_atend;
				else
					range = range_contains;
			}
		}
	} else if (minval == NULL) {
		c = atomcmp(th, maxval);
		if (c < 0 || ((maxincl || !hi) && c == 0))
			range = range_inside;
		else
			range = range_atend;
	} else if (maxval == NULL) {
		c = atomcmp(tl, minval);
		if (c >= 0)
			range = range_inside;
		else
			range = range_atstart;
	} else {
		c = atomcmp(tl, minval);
		if (c >= 0) {
			c = atomcmp(th, maxval);
			if (c < 0 || ((maxincl || !hi) && c == 0))
				range = range_inside;
			else
				range = range_atend;
		} else {
			c = atomcmp(th, maxval);
			if (c < 0 || ((maxincl || !hi) && c == 0))
				range = range_atstart;
			else
				range = range_contains;
		}
	}

	MT_lock_unset(&bi->b->theaplock);
	if (pb) {
		if (keep)
			MT_lock_unset(&pb->theaplock);
		BBPreclaim(pb);
	}

	return range;
}

/* generic range select
 *
 * Return a BAT with the OID values of b for qualifying tuples.  The
 * return BAT is sorted (i.e. in the same order as the input BAT).
 *
 * If s is non-NULL, it is a list of candidates.  s must be sorted.
 *
 * tl may not be NULL, li, hi, and anti must be either false or true.
 *
 * If th is NULL, hi is ignored.
 *
 * If anti is false, qualifying tuples are those whose value is between
 * tl and th (as in x >[=] tl && x <[=] th, where equality depends on li
 * and hi--so if tl > th, nothing will be returned).  If li or hi is
 * true, the respective boundary is inclusive, otherwise exclusive.  If
 * th is NULL it is taken to be equal to tl, turning this into an equi-
 * or point-select.  Note that for a point select to return anything, li
 * (and hi if th was not NULL) must be true.  There is a special case if
 * tl is nil and th is NULL.  This is the only way to select for nil
 * values.
 *
 * If anti is true, the result is the complement of what the result
 * would be if anti were 0, except that nils are filtered out if
 * nil_matches is false.
 *
 * If nil_matches is true, NIL is considered an ordinary value that
 * can match, else NIL must be considered to never match.
 *
 * In brief:
 * - if tl==nil and th==NULL and anti==false, return all nils (only way
 *   to get nils);
 * - it tl==nil and th==nil, return all but nils;
 * - if tl==nil and th!=NULL, no lower bound;
 * - if th==NULL or tl==th, point (equi) select;
 * - if th==nil, no upper bound
 *
 * A complete breakdown of the various arguments follows.  Here, v, v1
 * and v2 are values from the appropriate domain, and
 * v != nil, v1 != nil, v2 != nil, v1 < v2.
 * Note that if nil_matches is true, all the "x != nil" conditions fall
 * away and for the "equi" or "point" selects, i.e. when tl is nil and
 * th is either NULL or nil, there is no special handling of nil (so
 * look at the rows with tl == v and th == v or NULL).
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
 *	v	NULL	true	ignored	false	x == v
 *	v	NULL	false	ignored	false	NOTHING
 *	v	NULL	true	ignored	true	x != v && x != nil
 *	v	NULL	false	ignored	true	x != nil
 *	v	nil	true	ignored	false	x >= v
 *	v	nil	false	ignored	false	x > v
 *	v	nil	true	ignored	true	x < v
 *	v	nil	false	ignored	true	x <= v
 *	v	v	true	true	false	x == v
 *	v	v	false	true	false	NOTHING
 *	v	v	true	false	false	NOTHING
 *	v	v	false	false	false	NOTHING
 *	v	v	true	true	true	x != v && x != nil
 *	v	v	false	true	true	x != nil
 *	v	v	true	false	true	x != nil
 *	v	v	false	false	true	x != nil
 *	v1	v2	true	true	false	v1 <= x <= v2
 *	v1	v2	false	true	false	v1 < x <= v2
 *	v1	v2	true	false	false	v1 <= x < v2
 *	v1	v2	false	false	false	v1 < x < v2
 *	v1	v2	true	true	true	x < v1 or x > v2
 *	v1	v2	false	true	true	x <= v1 or x > v2
 *	v1	v2	true	false	true	x < v1 or x >= v2
 *	v1	v2	false	false	true	x <= v1 or x >= v2
 *	v2	v1	ignored	ignored	false	NOTHING
 *	v2	v1	ignored	ignored	true	x != nil
 */
BAT *
BATselect(BAT *b, BAT *s, const void *tl, const void *th,
	  bool li, bool hi, bool anti, bool nil_matches)
{
	bool lval;		/* low value used for comparison */
	bool lnil;		/* low value is nil */
	bool hval;		/* high value used for comparison */
	bool equi;		/* select for single value (not range) */
	bool antiequi = false;	/* select for all but single value */
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
	enum range_comp_t range;
	const bool notnull = BATgetprop(b, GDK_NOT_NULL) != NULL;
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
	if (nil == NULL)
		nil_matches = false;
	/* can we use the base type? */
	t = ATOMbasetype(t);
	lnil = nil && ATOMcmp(t, tl, nil) == 0; /* low value == nil? */
	lval = !lnil || th == NULL;	 /* low value used for comparison */
	equi = th == NULL || (lval && ATOMcmp(t, tl, th) == 0); /* point select? */
	if (lnil && nil_matches && (th == NULL || ATOMcmp(t, th, nil) == 0)) {
		/* if nil_matches is set, tl==th==nil is just an equi select */
		equi = true;
		lval = true;
	}

	if (equi) {
		assert(lval);
		if (th == NULL)
			hi = li;
		th = tl;
		hval = true;
		if (!anti && (!li || !hi)) {
			/* upper and lower bound of range are equal (or
			 * upper is NULL) and we want an interval that's
			 * open on at least one side (v <= x < v or v <
			 * x <= v or v < x < v all result in nothing) */
			MT_thread_setalgorithm("select: empty interval");
			bn = BATdense(0, 0, 0);
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",li=%d,hi=%d,anti=%d -> "
				  ALGOOPTBATFMT " (" LLFMT " usec): "
				  "empty interval\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  li, hi, anti, ALGOOPTBATPAR(bn),
				  GDKusec() - t0);
			bat_iterator_end(&bi);
			return bn;
		}
	} else {
		/* range select: we only care about nil_matches in
		 * (anti-)equi-select */
		nil_matches = false;
		if (nil == NULL) {
			assert(th != NULL);
			hval = true;
		} else {
			hval = ATOMcmp(t, th, nil) != 0;
		}
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
		} else if ((equi && (lnil || !(li && hi))) || ATOMcmp(t, tl, th) > 0) {
			/* various ways to select for everything except nil */
			if (equi && !lnil && nil_matches && !(li && hi)) {
				/* nil is not special, so return
				 * everything */
				bn = canditer_slice(&ci, 0, ci.ncand);
				TRC_DEBUG(ALGO, "b=" ALGOBATFMT
					  ",s=" ALGOOPTBATFMT ",anti=%d -> " ALGOOPTBATFMT
					  " (" LLFMT " usec): "
					  "anti, equi, open, nil_matches\n",
					  ALGOBATPAR(b), ALGOOPTBATPAR(s), anti,
					  ALGOOPTBATPAR(bn), GDKusec() - t0);
				bat_iterator_end(&bi);
				return bn;
			}
			bn = BATselect(b, s, nil, NULL, true, true, false, false);
			if (bn == NULL) {
				bat_iterator_end(&bi);
				return NULL;
			}
			BAT *bn2;
			if (s) {
				bn2 = BATdiffcand(s, bn);
			} else {
				bn2 = BATnegcands(ci.seq, bi.count, bn);
			}
			bat_iterator_end(&bi);
			BBPreclaim(bn);
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",s=" ALGOOPTBATFMT ",anti=1,equi=%d -> "
				  ALGOOPTBATFMT " (" LLFMT " usec): "
				  "everything except nil\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(s),
				  equi, ALGOOPTBATPAR(bn2), GDKusec() - t0);
			return bn2; /* also if NULL */
		} else {
			antiequi = equi;
			equi = false;
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
	if (equi && lnil && (notnull || bi.nonil)) {
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

	if (!equi && !lval && !hval && lnil && (notnull || bi.nonil)) {
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
	range = BATrange(&bi, lval ? tl : NULL, hval ? th : NULL, li, hi);
	if (anti) {
		switch (range) {
		case range_contains:
			/* MIN..MAX range of values in BAT fully inside
			 * tl..th range, so nothing left over for
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
		case range_before:
		case range_after:
			if (notnull || b->tnonil || nil_matches) {
				/* search range does not overlap with
				 * BAT range, and there are no nils (or
				 * we want to include nils), so we can
				 * return everything */
				MT_thread_setalgorithm("select: everything, anti, nonil");
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
			break;
		default:
			break;
		}
	} else if (!equi || !lnil) {
		switch (range) {
		case range_before:
		case range_after:
			/* range we're looking for either completely
			 * before or complete after the range of values
			 * in the BAT */
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
		case range_contains:
			if (notnull || b->tnonil) {
				/* search range contains BAT range, and
				 * there are no nils, so we can return
				 * everything */
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
			break;
		default:
			break;
		}
	}

	parent = VIEWtparent(b);
	assert(parent >= 0);
	BAT *pb;
	BATiter pbi;
	if (parent > 0)
		pb = BATdescriptor(parent);
	else
		pb = NULL;
	pbi = bat_iterator(pb);
	/* use hash only for equi-join if the bat is not sorted, but
	 * only if b or its parent already has a hash, or if b or its
	 * parent is persistent and the total size wouldn't be too
	 * large; check for existence of hash last since that may
	 * involve I/O */
	if ((equi || antiequi) && !bi.sorted && !bi.revsorted) {
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
			if (wanthash && !havehash && b->batRole != PERSISTENT) {
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
			/* Is query selective enough to use the ordered
			 * index?  Finding the boundaries is 2*log(n)
			 * where n is the size of the bat, sorting is
			 * N*log(N) where N is the number of results.
			 * If the sum is less than n (cost of scan),
			 * it's cheaper.  However, to find out how large
			 * N is, we'd have to do the two boundary
			 * searches.  If we do that, we might as well do
			 * it all. */
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
		}
	}

	if (bi.sorted || bi.revsorted || (!havehash && oidxh != NULL)) {
		BUN low = 0;
		BUN high = bi.count;

		if (BATtdensebi(&bi)) {
			/* positional */
			/* we expect nonil to be set, in which case we
			 * already know that we're not dealing with a
			 * nil equiselect (dealt with above) */
			assert(bi.nonil);
			assert(bi.sorted);
			assert(oidxh == NULL);
			algo = "select: dense";
			if (hval) {
				oid h = * (oid *) th + hi;
				if (h > bi.tseq)
					h -= bi.tseq;
				else
					h = 0;
				if ((BUN) h < high)
					high = (BUN) h;
			}

			if (lval) {
				oid l = *(oid *) tl + !li;
				if (l > bi.tseq)
					l -= bi.tseq;
				else
					l = 0;
				if ((BUN) l > low)
					low = (BUN) l;
				if (low > high)
					low = high;
			}
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
				BUN first = nil_matches ? 0 : SORTfndlast(b, nil);
				/* match: [first..low) + [high..last) */
				bn = canditer_slice2val(&ci,
							first + b->hseqbase,
							low + b->hseqbase,
							high + b->hseqbase,
							oid_nil);
			} else {
				BUN last = nil_matches ? bi.count : SORTfndfirst(b, nil);
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
					BBPreclaim(pb);
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
		BBPreclaim(pb);
		return bn;
	}

	assert(oidxh == NULL);
	/* upper limit for result size */
	maximum = ci.ncand;
	if ((equi || antiequi) && havehash) {
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
		if (equi || (antiequi && wanthash)) {
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
							 th, li, hi, anti, nil_matches);
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
		BBPreclaim(pb);
		return NULL;
	}

	if (wanthash) {
		/* hashselect unlocks the hash lock */
		bn = hashselect(&bi, &ci, bn, tl, maximum, havehash, phash, &algo);
		if (bn && antiequi) {
			BAT *bn2;
			if (s) {
				bn2 = BATdiffcand(s, bn);
			} else {
				bn2 = BATnegcands(ci.seq, bi.count, bn);
			}
			BBPreclaim(bn);
			bn = bn2;
			if (!bi.nonil) {
				bn2 = BATselect(b, s, nil, NULL, true, true, false, false);
				if (bn2 == NULL) {
					BBPreclaim(bn);
					return NULL;
				}
				BAT *bn3 = BATdiffcand(bn, bn2);
				BBPreclaim(bn2);
				BBPreclaim(bn);
				bn = bn3;
			}
		}
	} else {
		assert(!havehash);
		bn = scanselect(&bi, &ci, bn, tl, th, li, hi, equi, anti,
				nil_matches, lval, hval, lnil, maximum,
				&algo);
	}
	bat_iterator_end(&bi);
	bat_iterator_end(&pbi);
	BBPreclaim(pb);

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
 * If value is nil, the result is empty, except when using eq/ne as
 * operator.
 */
BAT *
BATthetaselect(BAT *b, BAT *s, const void *val, const char *op)
{
	const void *nil;

	BATcheck(b, NULL);
	BATcheck(val, NULL);
	BATcheck(op, NULL);

	/* eq/ne are can be used for "is" nil-handling */
	if (strcmp(op, "eq") == 0)
		return BATselect(b, s, val, NULL, true, true, false, true);
	if (strcmp(op, "ne") == 0)
		return BATselect(b, s, val, NULL, true, true, true, true);

	nil = ATOMnilptr(b->ttype);
	if (ATOMcmp(b->ttype, val, nil) == 0)
		return BATdense(0, 0, 0);
	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[1] == 0)) {
		/* "=" or "==" */
		return BATselect(b, s, val, NULL, true, true, false, false);
	}
	if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
		/* "!=" (equivalent to "<>") */
		return BATselect(b, s, val, NULL, true, true, true, false);
	}
	if (op[0] == '<') {
		if (op[1] == 0) {
			/* "<" */
			return BATselect(b, s, nil, val, false, false, false, false);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* "<=" */
			return BATselect(b, s, nil, val, false, true, false, false);
		}
		if (op[1] == '>' && op[2] == 0) {
			/* "<>" (equivalent to "!=") */
			return BATselect(b, s, val, NULL, true, true, true, false);
		}
	}
	if (op[0] == '>') {
		if (op[1] == 0) {
			/* ">" */
			return BATselect(b, s, val, nil, false, false, false, false);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* ">=" */
			return BATselect(b, s, val, nil, true, false, false, false);
		}
	}
	GDKerror("unknown operator.\n");
	return NULL;
}
