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

/*
 * BATproject returns a BAT aligned with the left input whose values
 * are the values from the right input that were referred to by the
 * OIDs in the left input.
 *
 * BATproject2 is similar, except instead of a single right input
 * there are two of which the second's hseqbase is equal to the first
 * hseqbase + its batCount.
 */

#define project_loop(TYPE)						\
static gdk_return							\
project_##TYPE(BAT *restrict bn, BAT *restrict l,			\
	       struct canditer *restrict ci,				\
	       BAT *restrict r1, BAT *restrict r2)			\
{									\
	BUN lo, hi;							\
	const TYPE *restrict r1t;					\
	const TYPE *restrict r2t;					\
	TYPE *restrict bt;						\
	TYPE v;								\
	oid r1seq, r1end;						\
	oid r2seq, r2end;						\
									\
	MT_thread_setalgorithm(__func__);				\
	r1t = (const TYPE *) Tloc(r1, 0);				\
	r2t = r2 ? (const TYPE *) Tloc(r2, 0) : NULL;			\
	bt = (TYPE *) Tloc(bn, 0);					\
	r1seq = r1->hseqbase;						\
	r1end = r1seq + BATcount(r1);					\
	if (r2) {							\
		r2seq = r2->hseqbase;					\
		r2end = r2seq + BATcount(r2);				\
	} else {							\
		r2seq = r2end = r1end;					\
	}								\
	if (ci) {							\
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {		\
			oid o = canditer_next(ci);			\
			if (o < r1seq || o >= r2end) {			\
				GDKerror("does not match always\n");	\
				return GDK_FAIL;			\
			}						\
			if (o < r1end)					\
				v = r1t[o - r1seq];			\
			else						\
				v = r2t[o - r2seq];			\
			bt[lo] = v;					\
		}							\
	} else if (BATtdense(l)) {					\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			oid o = l->tseqbase + lo;			\
			if (o < r1seq || o >= r2end) {			\
				GDKerror("does not match always\n");	\
				return GDK_FAIL;			\
			}						\
			if (o < r1end)					\
				v = r1t[o - r1seq];			\
			else						\
				v = r2t[o - r2seq];			\
			bt[lo] = v;					\
		}							\
	} else {							\
		const oid *restrict ot = (const oid *) Tloc(l, 0);	\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			oid o = ot[lo];					\
			if (is_oid_nil(o)) {				\
				bt[lo] = v = TYPE##_nil;		\
				bn->tnil = true;			\
			} else if (o < r1seq || o >= r2end) {		\
				GDKerror("does not match always\n");	\
				return GDK_FAIL;			\
			} else if (o < r1end) {				\
				v = r1t[o - r1seq];			\
				bt[lo] = v;				\
			} else {					\
				v = r2t[o - r2seq];			\
				bt[lo] = v;				\
			}						\
		}							\
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
project_oid(BAT *restrict bn, BAT *restrict l, struct canditer *restrict lci,
	    BAT *restrict r1, BAT *restrict r2)
{
	BUN lo, hi;
	oid *restrict bt;
	oid r1seq, r1end;
	oid r2seq, r2end;
	const oid *restrict r1t = NULL;
	const oid *restrict r2t = NULL;
	struct canditer r1ci = {0}, r2ci = {0};

	MT_thread_setalgorithm(__func__);
	if (r1->ttype == TYPE_void && r1->tvheap != NULL)
		canditer_init(&r1ci, NULL, r1);
	else if (!BATtdense(r1))
		r1t = (const oid *) Tloc(r1, 0);
	r1seq = r1->hseqbase;
	r1end = r1seq + BATcount(r1);
	if (r2) {
		if (r2->ttype == TYPE_void && r2->tvheap != NULL)
			canditer_init(&r2ci, NULL, r2);
		else if (!BATtdense(r2))
			r2t = (const oid *) Tloc(r2, 0);
		r2seq = r2->hseqbase;
		r2end = r2seq + BATcount(r2);
	} else {
		r2seq = r2end = r1end;
	}
	bt = (oid *) Tloc(bn, 0);
	if (lci) {
		for (lo = 0, hi = lci->ncand; lo < hi; lo++) {
			oid o = canditer_next(lci);
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			}
			if (o < r1end) {
				if (r1ci.s)
					bt[lo] = canditer_idx(&r1ci, o - r1seq);
				else if (r1t)
					bt[lo] = r1t[o - r1seq];
				else
					bt[lo] = o - r1seq + r1->tseqbase;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2->tseqbase;
			}
		}
	} else if (BATtdense(l)) {
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = l->tseqbase + lo;
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			}
			if (o < r1end) {
				if (r1ci.s)
					bt[lo] = canditer_idx(&r1ci, o - r1seq);
				else if (r1t)
					bt[lo] = r1t[o - r1seq];
				else
					bt[lo] = o - r1seq + r1->tseqbase;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2->tseqbase;
			}
		}
	} else {
		const oid *ot = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				bt[lo] = oid_nil;
				bn->tnonil = false;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			} else if (o < r1end) {
				if (r1ci.s)
					bt[lo] = canditer_idx(&r1ci, o - r1seq);
				else if (r1t)
					bt[lo] = r1t[o - r1seq];
				else
					bt[lo] = o - r1seq + r1->tseqbase;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2->tseqbase;
			}
		}
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
}

static gdk_return
project_any(BAT *restrict bn, BAT *restrict l, struct canditer *restrict ci,
	    BAT *restrict r1, BAT *restrict r2)
{
	BUN lo, hi;
	BATiter r1i, r2i;
	const void *nil = ATOMnilptr(r1->ttype);
	const void *v;
	oid r1seq, r1end;
	oid r2seq, r2end;

	MT_thread_setalgorithm(__func__);
	r1i = bat_iterator(r1);
	r1seq = r1->hseqbase;
	r1end = r1seq + BATcount(r1);
	r2i = bat_iterator(r2);
	if (r2) {
		r2seq = r2->hseqbase;
		r2end = r2seq + BATcount(r2);
	} else {
		r2seq = r2end = r1end;
	}
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			}
			if (o < r1end)
				v = BUNtail(r1i, o - r1seq);
			else
				v = BUNtail(r2i, o - r2seq);
			if (tfastins_nocheck(bn, lo, v, Tsize(bn)) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	} else if (BATtdense(l)) {
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = l->tseqbase + lo;
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			}
			if (o < r1end)
				v = BUNtail(r1i, o - r1seq);
			else
				v = BUNtail(r2i, o - r2seq);
			if (tfastins_nocheck(bn, lo, v, Tsize(bn)) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	} else {
		const oid *restrict ot = (const oid *) Tloc(l, 0);

		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				v = nil;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			} else if (o < r1end) {
				v = BUNtail(r1i, o - r1seq);
			} else {
				v = BUNtail(r2i, o - r2seq);
			}
			if (tfastins_nocheck(bn, lo, v, Tsize(bn)) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}
	BATsetcount(bn, lo);
	bn->theap.dirty = true;
	return GDK_SUCCEED;
}

static BAT *
project_str(BAT *restrict l, struct canditer *restrict ci,
	    BAT *restrict r1, BAT *restrict r2, lng t0)
{
	BAT *bn;
	BUN lo, hi;
	oid r1seq, r1end;
	oid r2seq, r2end;
	BUN h1off;
	BAT *r;
	BUN off;
	oid seq;
	var_t v;

	if ((bn = COLnew(l->hseqbase, TYPE_str, ci ? ci->ncand : BATcount(l),
			 TRANSIENT)) == NULL)
		return NULL;

	v = (var_t) r1->tvheap->free;
	if (r1->tvheap == r2->tvheap) {
		h1off = 0;
		BBPshare(r1->tvheap->parentid);
		HEAPfree(bn->tvheap, true);
		GDKfree(bn->tvheap);
		bn->tvheap = r1->tvheap;
	} else {
		v = (v + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		h1off = (BUN) v;
		v += ((var_t) r2->tvheap->free + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		if (HEAPextend(bn->tvheap, v, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			return NULL;
		}
		memcpy(bn->tvheap->base, r1->tvheap->base, r1->tvheap->free);
#ifndef NDEBUG
		if (h1off > r1->tvheap->free)
			memset(bn->tvheap->base + r1->tvheap->free, 0, h1off - r1->tvheap->free);
#endif
		memcpy(bn->tvheap->base + h1off, r2->tvheap->base, r2->tvheap->free);
		bn->tvheap->free = h1off + r2->tvheap->free;
	}

	if (v >= ((var_t) 1 << (8 * bn->twidth)) &&
	    GDKupgradevarheap(bn, v, false, false) != GDK_SUCCEED) {
		BBPreclaim(bn);
		return NULL;
	}

	r1seq = r1->hseqbase;
	r1end = r1seq + BATcount(r1);
	r2seq = r2->hseqbase;
	r2end = r2seq + BATcount(r2);
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				BBPreclaim(bn);
				return NULL;
			}
			if (o < r1end) {
				r = r1;
				off = 0;
				seq = r1seq;
			} else {
				r = r2;
				off = h1off;
				seq = r2seq;
			}
			switch (r->twidth) {
			case 1:
				v = (var_t) ((uint8_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) r->theap.base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) r->theap.base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap.base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap.base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap.base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap.base)[lo] = (uint64_t) v;
				break;
			}
		}
	} else if (BATtdense(l)) {
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = l->tseqbase + lo;
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				BBPreclaim(bn);
				return NULL;
			}
			if (o < r1end) {
				r = r1;
				off = 0;
				seq = r1seq;
			} else {
				r = r2;
				off = h1off;
				seq = r2seq;
			}
			switch (r->twidth) {
			case 1:
				v = (var_t) ((uint8_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) r->theap.base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) r->theap.base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap.base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap.base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap.base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap.base)[lo] = (uint64_t) v;
				break;
			}
		}
	} else {
		const oid *restrict ot = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				BBPreclaim(bn);
				return NULL;
			}
			if (o < r1end) {
				r = r1;
				off = 0;
				seq = r1seq;
			} else {
				r = r2;
				off = h1off;
				seq = r2seq;
			}
			switch (r->twidth) {
			case 1:
				v = (var_t) ((uint8_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) r->theap.base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) r->theap.base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) r->theap.base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap.base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap.base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap.base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap.base)[lo] = (uint64_t) v;
				break;
			}
		}
	}
	BATsetcount(bn, lo);
	bn->tsorted = bn->trevsorted = false;
	bn->tnil = false;
	bn->tnonil = r1->tnonil & r2->tnonil;
	bn->tkey = false;
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT " r1=" ALGOBATFMT " r2=" ALGOBATFMT
		  " -> " ALGOBATFMT "%s " LLFMT "us\n",
		  ALGOBATPAR(l), ALGOBATPAR(r1), ALGOBATPAR(r2),
		  ALGOBATPAR(bn),
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1->tvheap ? " sharing string heap" : "",
		  GDKusec() - t0);
	return bn;
}

BAT *
BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2)
{
	BAT *bn;
	oid lo, hi;
	gdk_return res;
	int tpe = ATOMtype(r1->ttype);
	bool stringtrick = false;
	BUN lcount = BATcount(l);
	struct canditer ci, *lci = NULL;
	const char *msg = "";
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(ATOMtype(l->ttype) == TYPE_oid);
	assert(r2 == NULL || ATOMtype(r1->ttype) == ATOMtype(r2->ttype));
	assert(r2 == NULL || r1->hseqbase + r1->batCount == r2->hseqbase);

	if (BATtdense(l) && lcount > 0) {
		lo = l->tseqbase;
		hi = l->tseqbase + lcount;
		if (lo >= r1->hseqbase && hi <= r1->hseqbase + r1->batCount) {
			bn = BATslice(r1, lo - r1->hseqbase, hi - r1->hseqbase);
			BAThseqbase(bn, l->hseqbase);
			msg = " (slice)";
			goto doreturn;
		}
		if (lo < r1->hseqbase || r2 == NULL || hi > r2->hseqbase + r2->batCount) {
			GDKerror("does not match always\n");
			return NULL;
		}
		if (lo >= r2->hseqbase) {
			bn = BATslice(r2, lo - r2->hseqbase, hi - r2->hseqbase);
			BAThseqbase(bn, l->hseqbase);
			msg = " (slice2)";
			goto doreturn;
		}
	}
	if (l->ttype == TYPE_void && l->tvheap != NULL) {
		/* l is candidate list with exceptions */
		assert(!is_oid_nil(l->tseqbase));
		lcount = canditer_init(&ci, NULL, l);
		lci = &ci;
	}
	if (lcount == 0 ||
	    (l->ttype == TYPE_void && is_oid_nil(l->tseqbase)) ||
	    (r1->ttype == TYPE_void && is_oid_nil(r1->tseqbase) &&
	     (r2 == NULL ||
	      (r2->ttype == TYPE_void && is_oid_nil(r2->tseqbase))))) {
		/* trivial: all values are nil (includes no entries at all) */
		const void *nil = ATOMnilptr(r1->ttype);

		bn = BATconstant(l->hseqbase, r1->ttype == TYPE_oid ? TYPE_void : r1->ttype,
				 nil, lcount, TRANSIENT);
		if (bn != NULL &&
		    ATOMtype(bn->ttype) == TYPE_oid &&
		    BATcount(bn) == 0) {
			BATtseqbase(bn, 0);
		}
		msg = " (constant)";
		goto doreturn;
	}

	if (ATOMstorage(tpe) == TYPE_str) {
		if (l->tnonil &&
		    r2 == NULL &&
		    (r1->batCount == 0 ||
		     lcount > (r1->batCount >> 3) ||
		     r1->batRestricted == BAT_READ)) {
			/* insert strings as ints, we need to copy the
			 * string heap whole sale; we can't do this if
			 * there are nils in the left column, and we
			 * won't do it if the left is much smaller than
			 * the right and the right is writable (meaning
			 * we have to actually copy the right string
			 * heap) */
			tpe = r1->twidth == 1 ? TYPE_bte : (r1->twidth == 2 ? TYPE_sht : (r1->twidth == 4 ? TYPE_int : TYPE_lng));
			stringtrick = true;
		} else if (l->tnonil &&
			   r2 != NULL &&
			   (r1->tvheap == r2->tvheap ||
			    (!GDK_ELIMDOUBLES(r1->tvheap) /* && size tests */))) {
			/* r1 and r2 may explicitly share their vheap,
			 * if they do, the result will also share the
			 * vheap; this also means that for this case we
			 * don't care about duplicate elimination: it
			 * will remain the same */
			return project_str(l, lci, r1, r2, t0);
		}
	}
	bn = COLnew(l->hseqbase, tpe, lcount, TRANSIENT);
	if (bn == NULL) {
		goto doreturn;
	}
	bn->tnil = false;
	if (r2) {
		bn->tnonil = l->tnonil & r1->tnonil & r2->tnonil;
		bn->tsorted = l->batCount <= 1;
		bn->trevsorted = l->batCount <= 1;
		bn->tkey = l->batCount <= 1;
	} else {
		bn->tnonil = l->tnonil & r1->tnonil;
		bn->tsorted = l->batCount <= 1
			|| (l->tsorted & r1->tsorted)
			|| (l->trevsorted & r1->trevsorted);
		bn->trevsorted = l->batCount <= 1
			|| (l->tsorted & r1->trevsorted)
			|| (l->trevsorted & r1->tsorted);
		bn->tkey = l->batCount <= 1 || (l->tkey & r1->tkey);
	}

	if (!stringtrick && tpe != TYPE_oid)
		tpe = ATOMbasetype(tpe);
	switch (tpe) {
	case TYPE_bte:
		res = project_bte(bn, l, lci, r1, r2);
		break;
	case TYPE_sht:
		res = project_sht(bn, l, lci, r1, r2);
		break;
	case TYPE_int:
		res = project_int(bn, l, lci, r1, r2);
		break;
	case TYPE_flt:
		res = project_flt(bn, l, lci, r1, r2);
		break;
	case TYPE_dbl:
		res = project_dbl(bn, l, lci, r1, r2);
		break;
	case TYPE_lng:
		res = project_lng(bn, l, lci, r1, r2);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		res = project_hge(bn, l, lci, r1, r2);
		break;
#endif
	case TYPE_oid:
		res = project_oid(bn, l, lci, r1, r2);
		break;
	default:
		res = project_any(bn, l, lci, r1, r2);
		break;
	}

	if (res != GDK_SUCCEED)
		goto bailout;

	/* handle string trick */
	if (stringtrick) {
		assert(r1->tvheap);
		if (r1->batRestricted == BAT_READ) {
			/* really share string heap */
			assert(r1->tvheap->parentid > 0);
			BBPshare(r1->tvheap->parentid);
			bn->tvheap = r1->tvheap;
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
			if (HEAPcopy(bn->tvheap, r1->tvheap) != GDK_SUCCEED)
				goto bailout;
		}
		bn->ttype = r1->ttype;
		bn->tvarsized = true;
		bn->twidth = r1->twidth;
		bn->tshift = r1->tshift;
	}

	if (!BATtdense(r1) || (r2 && !BATtdense(r2)))
		BATtseqbase(bn, oid_nil);

  doreturn:
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT " r1=" ALGOBATFMT " r2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT "%s%s " LLFMT "us\n",
		  ALGOBATPAR(l), ALGOBATPAR(r1), ALGOOPTBATPAR(r2),
		  ALGOOPTBATPAR(bn),
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1->tvheap ? " sharing string heap" : "",
		  msg, GDKusec() - t0);
	return bn;

  bailout:
	BBPreclaim(bn);
	bn = NULL;
	goto doreturn;
}

BAT *
BATproject(BAT *restrict l, BAT *restrict r)
{
	return BATproject2(l, r, NULL);
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

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	/* count number of participating BATs and allocate some
	 * temporary work space */
	for (n = 0; bats[n]; n++) {
		b = bats[n];
		TRC_DEBUG(ALGO, "arg %d: " ALGOBATFMT "\n",
			  n + 1, ALGOBATPAR(b));
	}
	if (n == 0) {
		GDKerror("must have BAT arguments\n");
		return NULL;
	}
	if (n == 1) {
		bn = COLcopy(b, b->ttype, true, TRANSIENT);
		TRC_DEBUG(ALGO, "single bat: copy -> " ALGOOPTBATFMT
			  " " LLFMT " usec\n",
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
		if (bats[n + 1])
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
		TRC_DEBUG(ALGO, "with %d bats: nil/empty -> " ALGOOPTBATFMT
			  " " LLFMT " usec\n",
			  n, ALGOOPTBATPAR(bn), GDKusec() - t0);
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
					GDKerror("does not match always\n");
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ? canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			*d++ = o;
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
					GDKerror("does not match always\n");
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
				GDKerror("does not match always\n");
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = Tloc(b, o);
			}
			if (ATOMputFIX(tpe, d, v) != GDK_SUCCEED)
				goto bunins_failed;
			d += b->twidth;
		}
		if (stringtrick) {
			bn->tnil = false;
			bn->tnonil = b->tnonil;
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
					GDKerror("does not match always\n");
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ? canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			if (is_oid_nil(o)) {
				bn->tnil = true;
				v = nil;
			} else if (o < ba[n].hlo || o >= ba[n].hlo + ba[n].cnt) {
				GDKerror("does not match always\n");
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = BUNtail(bi, o);
			}
			if (bunfastapp(bn, v) != GDK_SUCCEED)
				goto bunins_failed;
		}
		n++;		/* undo for debug print */
	}
	BATsetcount(bn, ba[0].cnt);
	bn->tsorted = (ba[0].cnt <= 1) | issorted;
	bn->trevsorted = ba[0].cnt <= 1;
	bn->tnonil = nonil & b->tnonil;
	bn->tseqbase = oid_nil;
	GDKfree(ba);
	TRC_DEBUG(ALGO, "with %d bats: " ALGOOPTBATFMT " " LLFMT " usec\n",
		  n, ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bunins_failed:
	GDKfree(ba);
	BBPreclaim(bn);
	TRC_DEBUG(ALGO, "failed " LLFMT "usec\n", GDKusec() - t0);
	return NULL;
}
