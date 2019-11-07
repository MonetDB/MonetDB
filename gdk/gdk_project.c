/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * BATproject returns a BAT aligned with the left input whose values
 * are the values from the right input that were referred to by the
 * OIDs in the left input.
 */

#define project_loop(TYPE)						\
static gdk_return							\
project_##TYPE(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r, bool nilcheck) \
{									\
	BUN lo, hi;							\
	const TYPE *restrict rt;					\
	TYPE *restrict bt;						\
	TYPE v;								\
	oid rseq, rend;							\
	bool hasnil = false;						\
									\
	rt = (const TYPE *) Tloc(r, 0);					\
	bt = (TYPE *) Tloc(bn, 0);					\
	rseq = r->hseqbase;						\
	rend = rseq + BATcount(r);					\
	if (ci) {							\
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {		\
			oid o = canditer_next(ci);			\
			if (o < rseq || o >= rend) {			\
				GDKerror("BATproject: does not match always\n"); \
				return GDK_FAIL;			\
			}						\
			v = rt[o - rseq];				\
			bt[lo] = v;					\
			hasnil |= is_##TYPE##_nil(v);			\
		}							\
	} else {							\
		const oid *restrict o = (const oid *) Tloc(l, 0);	\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			if (is_oid_nil(o[lo])) {			\
				assert(nilcheck);			\
				bt[lo] = TYPE##_nil;			\
				hasnil = true;				\
			} else if (o[lo] < rseq || o[lo] >= rend) {	\
				GDKerror("BATproject: does not match always\n"); \
				return GDK_FAIL;			\
			} else {					\
				v = rt[o[lo] - rseq];			\
				bt[lo] = v;				\
				hasnil |= is_##TYPE##_nil(v);		\
			}						\
		}							\
	}								\
	if (nilcheck && hasnil) {					\
		bn->tnonil = false;					\
		bn->tnil = true;					\
	}								\
	BATsetcount(bn, lo);						\
	return GDK_SUCCEED;						\
}


/* project type switch */
project_loop(bte)
project_loop(sht)
project_loop(int)
project_loop(flt)
project_loop(dbl)
project_loop(lng)
#ifdef HAVE_HGE
project_loop(hge)
#endif

static gdk_return
project_void(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r)
{
	BUN lo, hi;
	oid *restrict bt;
	oid rseq, rend;

	assert(BATtdense(r));
	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	bt = (oid *) Tloc(bn, 0);
	bn->tsorted = l->tsorted;
	bn->trevsorted = l->trevsorted;
	bn->tkey = l->tkey;
	bn->tnonil = true;
	bn->tnil = false;
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				return GDK_FAIL;
			}
			bt[lo] = o - rseq + r->tseqbase;
		}
	} else {
		const oid *o = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			if (o[lo] < rseq || o[lo] >= rend) {
				if (is_oid_nil(o[lo])) {
					bt[lo] = oid_nil;
					bn->tnonil = false;
					bn->tnil = true;
				} else {
					GDKerror("BATproject: does not match always\n");
					return GDK_FAIL;
				}
			} else {
				bt[lo] = o[lo] - rseq + r->tseqbase;
			}
		}
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
}

static gdk_return
project_cand(BAT *bn, BAT *l, struct canditer *restrict lci, BAT *r)
{
	BUN lo, hi;
	oid *restrict bt;
	oid rseq, rend;
	struct canditer rci;

	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	canditer_init(&rci, NULL, r);
	bt = (oid *) Tloc(bn, 0);
	bn->tsorted = l->tsorted;
	bn->trevsorted = l->trevsorted;
	bn->tkey = l->tkey;
	bn->tnonil = true;
	bn->tnil = false;
	if (lci) {
		for (lo = 0, hi = lci->ncand; lo < hi; lo++) {
			oid o = canditer_next(lci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				return GDK_FAIL;
			}
			bt[lo] = canditer_idx(&rci, o - rseq);
		}
	} else {
		const oid *o = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			if (o[lo] < rseq || o[lo] >= rend) {
				if (is_oid_nil(o[lo])) {
					bt[lo] = oid_nil;
					bn->tnonil = false;
					bn->tnil = true;
				} else {
					GDKerror("BATproject: does not match always\n");
					return GDK_FAIL;
				}
			} else {
				bt[lo] = canditer_idx(&rci, o[lo] - rseq);
			}
		}
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
}

static gdk_return
project_any(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r, bool nilcheck)
{
	BUN lo, hi;
	BATiter ri;
	int (*cmp)(const void *, const void *) = ATOMcompare(r->ttype);
	const void *nil = ATOMnilptr(r->ttype);
	const void *v;
	oid rseq, rend;

	ri = bat_iterator(r);
	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				goto bunins_failed;
			}
			v = BUNtail(ri, o - rseq);
			tfastins_nocheck(bn, lo, v, Tsize(bn));
			if (nilcheck && bn->tnonil && cmp(v, nil) == 0) {
				bn->tnonil = false;
				bn->tnil = true;
			}
		}
	} else {
		const oid *restrict o = (const oid *) Tloc(l, 0);

		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			if (is_oid_nil(o[lo])) {
				tfastins_nocheck(bn, lo, nil, Tsize(bn));
				bn->tnonil = false;
				bn->tnil = true;
			} else if (o[lo] < rseq || o[lo] >= rend) {
				GDKerror("BATproject: does not match always\n");
				goto bunins_failed;
			} else {
				v = BUNtail(ri, o[lo] - rseq);
				tfastins_nocheck(bn, lo, v, Tsize(bn));
				if (nilcheck && bn->tnonil && cmp(v, nil) == 0) {
					bn->tnonil = false;
					bn->tnil = true;
				}
			}
		}
	}
	BATsetcount(bn, lo);
	bn->theap.dirty = true;
	return GDK_SUCCEED;
bunins_failed:
	return GDK_FAIL;
}

BAT *
BATproject(BAT *l, BAT *r)
{
	BAT *bn;
	oid lo, hi;
	gdk_return res;
	int tpe = ATOMtype(r->ttype);
	bool nilcheck = true, stringtrick = false;
	BUN lcount = BATcount(l), rcount = BATcount(r);
	struct canditer ci, *lci = NULL;
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	ALGODEBUG fprintf(stderr, "%s: %s(l=" ALGOBATFMT ","
			  "r=" ALGOBATFMT ")\n",
			  MT_thread_getname(), __func__,
			  ALGOBATPAR(l), ALGOBATPAR(r));

	assert(ATOMtype(l->ttype) == TYPE_oid);

	if (BATtdense(l) && lcount > 0) {
		lo = l->tseqbase;
		hi = l->tseqbase + lcount;
		if (lo < r->hseqbase || hi > r->hseqbase + rcount) {
			GDKerror("BATproject: does not match always\n");
			return NULL;
		}
		bn = BATslice(r, lo - r->hseqbase, hi - r->hseqbase);
		BAThseqbase(bn, l->hseqbase);
		ALGODEBUG fprintf(stderr, "%s: %s(l=%s,r=%s)=" ALGOOPTBATFMT " (slice)\n",
				  MT_thread_getname(), __func__,
				  BATgetId(l), BATgetId(r),  ALGOOPTBATPAR(bn));
		return bn;
	}
	if (l->ttype == TYPE_void && l->tvheap != NULL) {
		/* l is candidate list with exceptions */
		lcount = canditer_init(&ci, NULL, l);
		lci = &ci;
	}
	/* if l has type void, it is either empty or not dense (i.e. nil) */
	if (lcount == 0 || (l->ttype == TYPE_void && lci == NULL) ||
	    (r->ttype == TYPE_void && is_oid_nil(r->tseqbase))) {
		/* trivial: all values are nil (includes no entries at all) */
		const void *nil = ATOMnilptr(r->ttype);

		bn = BATconstant(l->hseqbase, r->ttype == TYPE_oid ? TYPE_void : r->ttype,
				 nil, lcount, TRANSIENT);
		if (bn != NULL &&
		    ATOMtype(bn->ttype) == TYPE_oid &&
		    BATcount(bn) == 0) {
			BATtseqbase(bn, 0);
		}
		ALGODEBUG fprintf(stderr, "%s: %s(l=%s,r=%s)=" ALGOOPTBATFMT " (constant)\n",
				  MT_thread_getname(), __func__,
				  BATgetId(l), BATgetId(r), ALGOOPTBATPAR(bn));
		return bn;
	}

	if (ATOMstorage(tpe) == TYPE_str &&
	    l->tnonil &&
	    (rcount == 0 ||
	     lcount > (rcount >> 3) ||
	     r->batRestricted == BAT_READ)) {
		/* insert strings as ints, we need to copy the string
		 * heap whole sale; we can't do this if there are nils
		 * in the left column, and we won't do it if the left
		 * is much smaller than the right and the right is
		 * writable (meaning we have to actually copy the
		 * right string heap) */
		tpe = r->twidth == 1 ? TYPE_bte : (r->twidth == 2 ? TYPE_sht : (r->twidth == 4 ? TYPE_int : TYPE_lng));
		/* int's nil representation is a valid offset, so
		 * don't check for nils */
		nilcheck = false;
		stringtrick = true;
	}
	bn = COLnew(l->hseqbase, tpe, lcount, TRANSIENT);
	if (bn == NULL) {
		ALGODEBUG fprintf(stderr, "%s: %s(l=%s,r=%s)=0\n",
				  MT_thread_getname(), __func__,
				  BATgetId(l), BATgetId(r));
		return NULL;
	}
	if (stringtrick) {
		/* "string type" */
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tnonil = false;
	} else {
		/* be optimistic, we'll clear these if necessary later */
		bn->tnonil = true;
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		if (l->tnonil && r->tnonil)
			nilcheck = false; /* don't bother checking: no nils */
		if (tpe != TYPE_oid &&
		    tpe != ATOMstorage(tpe) &&
		    !ATOMvarsized(tpe) &&
		    ATOMcompare(tpe) == ATOMcompare(ATOMstorage(tpe)) &&
		    (!nilcheck ||
		     ATOMnilptr(tpe) == ATOMnilptr(ATOMstorage(tpe)))) {
			/* use base type if we can:
			 * only fixed sized (no advantage for variable sized),
			 * compare function identical (for sorted check),
			 * either no nils, or nil representation identical,
			 * not oid (separate case for those) */
			tpe = ATOMstorage(tpe);
		}
	}
	bn->tnil = false;

	switch (tpe) {
	case TYPE_bte:
		res = project_bte(bn, l, lci, r, nilcheck);
		break;
	case TYPE_sht:
		res = project_sht(bn, l, lci, r, nilcheck);
		break;
	case TYPE_int:
		res = project_int(bn, l, lci, r, nilcheck);
		break;
	case TYPE_flt:
		res = project_flt(bn, l, lci, r, nilcheck);
		break;
	case TYPE_dbl:
		res = project_dbl(bn, l, lci, r, nilcheck);
		break;
	case TYPE_lng:
		res = project_lng(bn, l, lci, r, nilcheck);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		res = project_hge(bn, l, lci, r, nilcheck);
		break;
#endif
	case TYPE_oid:
		if (BATtdense(r)) {
			res = project_void(bn, l, lci, r);
		} else if (r->ttype == TYPE_void) {
			assert(r->tvheap != NULL);
			res = project_cand(bn, l, lci, r);
		} else {
#if SIZEOF_OID == SIZEOF_INT
			res = project_int(bn, l, lci, r, nilcheck);
#else
			res = project_lng(bn, l, lci, r, nilcheck);
#endif
		}
		break;
	default:
		res = project_any(bn, l, lci, r, nilcheck);
		break;
	}

	if (res != GDK_SUCCEED)
		goto bailout;

	/* handle string trick */
	if (stringtrick) {
		if (r->batRestricted == BAT_READ) {
			/* really share string heap */
			assert(r->tvheap->parentid > 0);
			BBPshare(r->tvheap->parentid);
			bn->tvheap = r->tvheap;
		} else {
			/* make copy of string heap */
			bn->tvheap = (Heap *) GDKzalloc(sizeof(Heap));
			if (bn->tvheap == NULL)
				goto bailout;
			bn->tvheap->parentid = bn->batCacheid;
			bn->tvheap->farmid = BBPselectfarm(bn->batRole, TYPE_str, varheap);
			strconcat_len(bn->tvheap->filename,
				      sizeof(bn->tvheap->filename),
				      BBP_physical(bn->batCacheid), ".theap",
				      NULL);
			if (HEAPcopy(bn->tvheap, r->tvheap) != GDK_SUCCEED)
				goto bailout;
		}
		bn->ttype = r->ttype;
		bn->tvarsized = true;
		bn->twidth = r->twidth;
		bn->tshift = r->tshift;

		bn->tnil = false; /* we don't know */
	}
	/* some properties follow from certain combinations of input
	 * properties */
	if (BATcount(bn) <= 1) {
		bn->tkey = true;
		bn->tsorted = true;
		bn->trevsorted = true;
	} else {
		bn->tkey = l->tkey && r->tkey;
		bn->tsorted = (l->tsorted & r->tsorted) | (l->trevsorted & r->trevsorted);
		bn->trevsorted = (l->tsorted & r->trevsorted) | (l->trevsorted & r->tsorted);
	}
	bn->tnonil |= l->tnonil & r->tnonil;

	if (!BATtdense(r))
		BATtseqbase(bn, oid_nil);
	ALGODEBUG fprintf(stderr, "%s: %s(l=%s,r=%s)=" ALGOBATFMT "%s " LLFMT "us\n",
			  MT_thread_getname(), __func__,
			  BATgetId(l), BATgetId(r), ALGOBATPAR(bn),
			  bn->ttype == TYPE_str && bn->tvheap == r->tvheap ? " shared string heap" : "",
			  GDKusec() - t0);
	return bn;

  bailout:
	BBPreclaim(bn);
	return NULL;
}

/* Calculate a chain of BATproject calls.
 * The argument is a NULL-terminated array of BAT pointers.
 * This function is equivalent (apart from reference counting) to a
 * sequence of calls
 * bn = BATproject(bats[0], bats[1]);
 * bn = BATproject(bn, bats[2]);
 * ...
 * bn = BATproject(bn, bats[n-1]);
 * return bn;
 * where none of the intermediates are actually produced (and bats[n]==NULL).
 * Note that all BATs except the last must have type oid/void.
 */
BAT *
BATprojectchain(BAT **bats)
{
	struct ba {
		BAT *b;
		oid hlo;
		BUN cnt;
		oid *t;
		struct canditer ci; /* used if .ci.s != NULL */
	} *ba;
	int n;
	BAT *b = NULL, *bn;
	bool allnil = false;
	bool issorted = true;
	bool nonil = true;
	bool stringtrick = false;
	const void *nil;
	int tpe;
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	/* count number of participating BATs and allocate some
	 * temporary work space */
	for (n = 0; bats[n]; n++) {
		b = bats[n];
		ALGODEBUG fprintf(stderr, "#%s: %s arg %d: " ALGOBATFMT "\n",
				  MT_thread_getname(), __func__, n + 1,
				  ALGOBATPAR(b));
	}
	if (n == 0) {
		GDKerror("%s: must have BAT arguments\n", __func__);
		return NULL;
	}
	if (n == 1) {
		bn = COLcopy(b, b->ttype, true, TRANSIENT);
		ALGODEBUG fprintf(stderr, "#%s: %s with 1 bat: copy: "
				  ALGOOPTBATFMT " (" LLFMT " usec)\n",
				  MT_thread_getname(), __func__,
				  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	ba = GDKmalloc(sizeof(*ba) * n);
	if (ba == NULL)
		return NULL;

	for (n = 0; bats[n]; n++) {
		b = bats[n];
		ba[n] = (struct ba) {
			.b = b,
			.hlo = b->hseqbase,
			.cnt = b->batCount,
			.t = (oid *) b->theap.base,
		};
		allnil |= b->ttype == TYPE_void && is_oid_nil(b->tseqbase);
		issorted &= b->tsorted;
		nonil &= b->tnonil;
		if (b->tnonil && b->tkey && b->tsorted &&
		    ATOMtype(b->ttype) == TYPE_oid) {
			canditer_init(&ba[n].ci, NULL, b);
		}
	}
	/* b is last BAT in bats array */
	tpe = ATOMtype(b->ttype);
	nil = ATOMnilptr(tpe);
	if (allnil || ba[0].cnt == 0) {
		bn = BATconstant(ba[0].hlo, tpe == TYPE_oid ? TYPE_void : tpe,
				 nil, ba[0].cnt, TRANSIENT);
		GDKfree(ba);
		ALGODEBUG fprintf(stderr, "#%s: %s with %d bats: nil/empty: "
				  ALGOOPTBATFMT " (" LLFMT " usec)\n",
				  MT_thread_getname(), __func__, n,
				  ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	if (nonil && ATOMstorage(tpe) == TYPE_str && b->batRestricted == BAT_READ) {
		stringtrick = true;
		tpe = b->twidth == 1 ? TYPE_bte : (b->twidth == 2 ? TYPE_sht : (b->twidth == 4 ? TYPE_int : TYPE_lng));
	}

	bn = COLnew(ba[0].hlo, tpe, ba[0].cnt, TRANSIENT);
	if (bn == NULL) {
		GDKfree(ba);
		return NULL;
	}

	if (ATOMtype(b->ttype) == TYPE_oid) {
		/* oid all the way */
		oid *d = (oid *) Tloc(bn, 0);
		assert(!stringtrick);
		for (BUN p = 0; p < ba[0].cnt; p++) {
			oid o = ba[0].ci.s ? canditer_next(&ba[0].ci) : ba[0].t[p];
			for (int i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					break;
				}
				if (o < ba[i].hlo || o >= ba[i].hlo + ba[i].cnt) {
					GDKerror("%s: does not match always\n",
						 __func__);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ? canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			bunfastappTYPE(oid, bn, &o);
			ATOMputFIX(bn->ttype, d, &o);
			d++;
		}
	} else if (!ATOMvarsized(tpe)) {
		const void *v;
		char *d = Tloc(bn, 0);

		bn->tnil = false;
		n--;	/* stop one before the end, also ba[n] is last */
		for (BUN p = 0; p < ba[0].cnt; p++) {
			oid o = ba[0].ci.s ? canditer_next(&ba[0].ci) : ba[0].t[p];

			for (int i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					break;
				}
				if (o < ba[i].hlo || o >= ba[i].hlo + ba[i].cnt) {
					GDKerror("%s: does not match always\n",
						 __func__);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ? canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			if (is_oid_nil(o)) {
				assert(!stringtrick);
				bn->tnil = true;
				v = nil;
			} else if (o < ba[n].hlo || o >= ba[n].hlo + ba[n].cnt) {
				GDKerror("%s: does not match always\n",
					 __func__);
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = Tloc(b, o);
			}
			ATOMputFIX(tpe, d, v);
			d += b->twidth;
		}
		if (stringtrick) {
			bn->tnil = false;
			bn->tnonil = nonil;
			bn->tkey = false;
			BBPshare(b->tvheap->parentid);
			bn->tvheap = b->tvheap;
			bn->ttype = b->ttype;
			bn->tvarsized = true;
			assert(bn->twidth == b->twidth);
			assert(bn->tshift == b->tshift);
		}
		n++;		/* undo for debug print */
	} else {
		BATiter bi = bat_iterator(b);
		const void *v;

		assert(!stringtrick);
		bn->tnil = false;
		n--;	/* stop one before the end, also ba[n] is last */
		for (BUN p = 0; p < ba[0].cnt; p++) {
			oid o = ba[0].ci.s ? canditer_next(&ba[0].ci) : ba[0].t[p];
			for (int i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					break;
				}
				if (o < ba[i].hlo || o >= ba[i].hlo + ba[i].cnt) {
					GDKerror("%s: does not match always\n",
						 __func__);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ? canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			if (is_oid_nil(o)) {
				bn->tnil = true;
				v = nil;
			} else if (o < ba[n].hlo || o >= ba[n].hlo + ba[n].cnt) {
				GDKerror("%s: does not match always\n",
					 __func__);
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = BUNtail(bi, o);
			}
			bunfastapp(bn, v);
		}
		n++;		/* undo for debug print */
	}
	BATsetcount(bn, ba[0].cnt);
	bn->tsorted = (ba[0].cnt <= 1) | issorted;
	bn->trevsorted = ba[0].cnt <= 1;
	bn->tnonil = nonil;
	bn->tseqbase = oid_nil;
	GDKfree(ba);
	ALGODEBUG fprintf(stderr, "#%s: %s with %d bats: "
			  ALGOOPTBATFMT " (" LLFMT " usec)\n",
			  MT_thread_getname(), __func__, n,
			  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bunins_failed:
	GDKfree(ba);
	BBPreclaim(bn);
	ALGODEBUG fprintf(stderr, "#%s: %s failed\n",
			  MT_thread_getname(), __func__);
	return NULL;
}
