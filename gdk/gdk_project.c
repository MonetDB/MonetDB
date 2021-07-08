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

#define project1_loop(TYPE)						\
static gdk_return							\
project1_##TYPE(BAT *restrict bn, BAT *restrict l, BAT *restrict r1,	\
		BATiter *restrict r1i)					\
{									\
	BUN lo, hi;							\
	const TYPE *restrict r1t;					\
	TYPE *restrict bt;						\
	oid r1seq, r1end;						\
									\
	MT_thread_setalgorithm(__func__);				\
	r1t = (const TYPE *) r1i->base;					\
	bt = (TYPE *) Tloc(bn, 0);					\
	r1seq = r1->hseqbase;						\
	r1end = r1seq + BATcount(r1);					\
	if (BATtdense(l)) {						\
		if (l->tseqbase < r1seq ||				\
		   (l->tseqbase+BATcount(l)) >= r1end) {		\
			GDKerror("does not match always\n");		\
			return GDK_FAIL;				\
		}							\
		oid off = l->tseqbase - r1seq;				\
		r1t += off;						\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) 		\
			bt[lo] = r1t[lo];				\
	} else {							\
		assert(l->ttype);					\
		BATiter li = bat_iterator(l);				\
		const oid *restrict ot = (const oid *) li.base;		\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			oid o = ot[lo];					\
			if (o < r1seq || o >= r1end) {			\
				GDKerror("does not match always\n");	\
				bat_iterator_end(&li);			\
				return GDK_FAIL;			\
			}						\
			bt[lo] = r1t[o - r1seq];			\
		}							\
		bat_iterator_end(&li);					\
	}								\
	BATsetcount(bn, lo);						\
	return GDK_SUCCEED;						\
}

/* project type switch */
project1_loop(bte)
project1_loop(sht)
project1_loop(int)
project1_loop(flt)
project1_loop(dbl)
project1_loop(lng)
#ifdef HAVE_HGE
project1_loop(hge)
#endif
project1_loop(uuid)

#define project_loop(TYPE)						\
static gdk_return							\
project_##TYPE(BAT *restrict bn, BAT *restrict l,			\
	       struct canditer *restrict ci,				\
	       BAT *restrict r1, BAT *restrict r2,			\
	       BATiter *restrict r1i, BATiter *restrict r2i)		\
{									\
	BUN lo, hi;							\
	const TYPE *restrict r1t;					\
	const TYPE *restrict r2t;					\
	TYPE *restrict bt;						\
	TYPE v;								\
	oid r1seq, r1end;						\
	oid r2seq, r2end;						\
									\
	if (r2 == NULL &&						\
	    (!ci || (ci->tpe == cand_dense && BATtdense(l))) &&		\
	    l->tnonil && r1->ttype && !BATtdense(r1))			\
		return project1_##TYPE(bn, l, r1, r1i);			\
	MT_thread_setalgorithm(__func__);				\
	r1t = (const TYPE *) r1i->base;					\
	r2t = (const TYPE *) r2i->base;	/* may be NULL if r2 == NULL */	\
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
		BATiter li = bat_iterator(l);				\
		const oid *restrict ot = (const oid *) li.base;		\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			oid o = ot[lo];					\
			if (is_oid_nil(o)) {				\
				bt[lo] = v = TYPE##_nil;		\
				bn->tnil = true;			\
			} else if (o < r1seq || o >= r2end) {		\
				GDKerror("does not match always\n");	\
				bat_iterator_end(&li);			\
				return GDK_FAIL;			\
			} else if (o < r1end) {				\
				v = r1t[o - r1seq];			\
				bt[lo] = v;				\
			} else {					\
				v = r2t[o - r2seq];			\
				bt[lo] = v;				\
			}						\
		}							\
		bat_iterator_end(&li);					\
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
project_loop(uuid)

static gdk_return
project_oid(BAT *restrict bn, BAT *restrict l, struct canditer *restrict lci,
	    BAT *restrict r1, BAT *restrict r2,
	    BATiter *restrict r1i, BATiter *restrict r2i)
{
	BUN lo, hi;
	oid *restrict bt;
	oid r1seq, r1end;
	oid r2seq, r2end;
	const oid *restrict r1t = NULL;
	const oid *restrict r2t = NULL;
	struct canditer r1ci = {0}, r2ci = {0};

	if ((!lci || (lci->tpe == cand_dense && BATtdense(l))) && r1->ttype && !BATtdense(r1) && !r2 &&	l->tnonil) {
		if (sizeof(oid) == sizeof(lng))
			return project1_lng(bn, l, r1, r1i);
		else
			return project1_int(bn, l, r1, r1i);
	}
	MT_thread_setalgorithm(__func__);
	if (complex_cand(r1))
		canditer_init(&r1ci, NULL, r1);
	else if (!BATtdense(r1))
		r1t = (const oid *) r1i->base;
	r1seq = r1->hseqbase;
	r1end = r1seq + r1i->count;
	if (r2) {
		if (complex_cand(r2))
			canditer_init(&r2ci, NULL, r2);
		else if (!BATtdense(r2))
			r2t = (const oid *) r2i->base;
		r2seq = r2->hseqbase;
		r2end = r2seq + r2i->count;
	} else {
		r2seq = r2end = r1end;
	}
	bt = (oid *) Tloc(bn, 0);
	if (lci) {
		for (lo = 0, hi = lci->ncand; lo < hi; lo++) {
			oid o = canditer_next(lci);
			if (o < r1seq || o >= r2end) {
				goto nomatch;
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
				goto nomatch;
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
		BATiter li = bat_iterator(l);
		const oid *ot = (const oid *) li.base;
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				bt[lo] = oid_nil;
				bn->tnonil = false;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				bat_iterator_end(&li);
				goto nomatch;
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
		bat_iterator_end(&li);
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
  nomatch:
	GDKerror("does not match always\n");
	return GDK_FAIL;
}

static gdk_return
project_any(BAT *restrict bn, BAT *restrict l, struct canditer *restrict ci,
	    BAT *restrict r1, BAT *restrict r2,
	    BATiter *restrict r1i, BATiter *restrict r2i)
{
	BUN lo, hi;
	const void *nil = ATOMnilptr(r1->ttype);
	const void *v;
	oid r1seq, r1end;
	oid r2seq, r2end;

	MT_thread_setalgorithm(__func__);
	r1seq = r1->hseqbase;
	r1end = r1seq + BATcount(r1);
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
				v = BUNtail(*r1i, o - r1seq);
			else
				v = BUNtail(*r2i, o - r2seq);
			if (tfastins_nocheck(bn, lo, v) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
	} else if (BATtdense(l)) {
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = l->tseqbase + lo;
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			}
			if (o < r1end)
				v = BUNtail(*r1i, o - r1seq);
			else
				v = BUNtail(*r2i, o - r2seq);
			if (tfastins_nocheck(bn, lo, v) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
	} else {
		BATiter li = bat_iterator(l);
		const oid *restrict ot = (const oid *) li.base;

		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				v = nil;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				bat_iterator_end(&li);
				return GDK_FAIL;
			} else if (o < r1end) {
				v = BUNtail(*r1i, o - r1seq);
			} else {
				v = BUNtail(*r2i, o - r2seq);
			}
			if (tfastins_nocheck(bn, lo, v) != GDK_SUCCEED) {
				bat_iterator_end(&li);
				return GDK_FAIL;
			}
		}
		bat_iterator_end(&li);
	}
	BATsetcount(bn, lo);
	bn->theap->dirty = true;
	return GDK_SUCCEED;
}

static BAT *
project_str(BAT *restrict l, struct canditer *restrict ci,
	    BAT *restrict r1, BAT *restrict r2,
	    BATiter *restrict r1i, BATiter *restrict r2i,
	    lng t0)
{
	BAT *bn;
	BUN lo, hi;
	oid r1seq, r1end;
	oid r2seq, r2end;
	BUN h1off;
	BUN off;
	oid seq;
	var_t v;
	BATiter *ri;

	if ((bn = COLnew(l->hseqbase, TYPE_str, ci ? ci->ncand : BATcount(l),
			 TRANSIENT)) == NULL)
		return NULL;

	v = (var_t) r1i->vh->free;
	if (r1i->vh == r2i->vh) {
		h1off = 0;
		BBPshare(r1i->vh->parentid);
		HEAPdecref(bn->tvheap, true);
		HEAPincref(r1i->vh);
		bn->tvheap = r1i->vh;
	} else {
		v = (v + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		h1off = (BUN) v;
		v += ((var_t) r2i->vh->free + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		if (HEAPextend(bn->tvheap, v, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			return NULL;
		}
		memcpy(bn->tvheap->base, r1i->vh->base, r1i->vh->free);
#ifndef NDEBUG
		if (h1off > r1i->vh->free)
			memset(bn->tvheap->base + r1i->vh->free, 0, h1off - r1i->vh->free);
#endif
		memcpy(bn->tvheap->base + h1off, r2i->vh->base, r2i->vh->free);
		bn->tvheap->free = h1off + r2i->vh->free;
	}

	if (v >= ((var_t) 1 << (8 << bn->tshift)) &&
	    GDKupgradevarheap(bn, v, false, 0) != GDK_SUCCEED) {
		BBPreclaim(bn);
		return NULL;
	}

	r1seq = r1->hseqbase;
	r1end = r1seq + r1i->count;
	r2seq = r2->hseqbase;
	r2end = r2seq + r2i->count;
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				BBPreclaim(bn);
				return NULL;
			}
			if (o < r1end) {
				ri = r1i;
				off = 0;
				seq = r1seq;
			} else {
				ri = r2i;
				off = h1off;
				seq = r2seq;
			}
			switch (ri->width) {
			case 1:
				v = (var_t) ((uint8_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) ri->base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) ri->base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap->base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap->base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap->base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap->base)[lo] = (uint64_t) v;
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
				ri = r1i;
				off = 0;
				seq = r1seq;
			} else {
				ri = r2i;
				off = h1off;
				seq = r2seq;
			}
			switch (ri->width) {
			case 1:
				v = (var_t) ((uint8_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) ri->base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) ri->base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap->base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap->base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap->base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap->base)[lo] = (uint64_t) v;
				break;
			}
		}
	} else {
		BATiter li = bat_iterator(l);
		const oid *restrict ot = (const oid *) li.base;
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			oid o = ot[lo];
			if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				BBPreclaim(bn);
				bat_iterator_end(&li);
				return NULL;
			}
			if (o < r1end) {
				ri = r1i;
				off = 0;
				seq = r1seq;
			} else {
				ri = r2i;
				off = h1off;
				seq = r2seq;
			}
			switch (ri->width) {
			case 1:
				v = (var_t) ((uint8_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) ((uint16_t *) ri->base)[o - seq] + GDK_VAROFFSET;
				break;
			case 4:
				v = (var_t) ((uint32_t *) ri->base)[o - seq];
				break;
			case 8:
				v = (var_t) ((uint64_t *) ri->base)[o - seq];
				break;
			}
			v += off;
			switch (bn->twidth) {
			case 1:
				((uint8_t *) bn->theap->base)[lo] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) bn->theap->base)[lo] = (uint16_t) (v - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) bn->theap->base)[lo] = (uint32_t) v;
				break;
			case 8:
				((uint64_t *) bn->theap->base)[lo] = (uint64_t) v;
				break;
			}
		}
		bat_iterator_end(&li);
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
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1i->vh ? " sharing string heap" : "",
		  GDKusec() - t0);
	return bn;
}

BAT *
BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2)
{
	BAT *bn = NULL;
	BAT *or1 = r1, *or2 = r2, *ol = l;
	oid lo, hi;
	gdk_return res;
	int tpe = ATOMtype(r1->ttype);
	bool stringtrick = false;
	BUN lcount = BATcount(l);
	struct canditer ci, *lci = NULL;
	const char *msg = "";
	lng t0 = 0;
	BATiter r1i = bat_iterator(r1);
	BATiter r2i = bat_iterator(r2);

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(ATOMtype(l->ttype) == TYPE_oid || l->ttype == TYPE_msk);
	assert(r2 == NULL || tpe == ATOMtype(r2->ttype));
	assert(r2 == NULL || r1->hseqbase + r1i.count == r2->hseqbase);

	if (BATtdense(l) && lcount > 0) {
		lo = l->tseqbase;
		hi = l->tseqbase + lcount;
		if (lo >= r1->hseqbase && hi <= r1->hseqbase + r1i.count) {
			bn = BATslice(r1, lo - r1->hseqbase, hi - r1->hseqbase);
			BAThseqbase(bn, l->hseqbase);
			msg = " (slice)";
			goto doreturn;
		}
		if (lo < r1->hseqbase || r2 == NULL || hi > r2->hseqbase + r2i.count) {
			GDKerror("does not match always\n");
			bat_iterator_end(&r1i);
			bat_iterator_end(&r2i);
			return NULL;
		}
		if (lo >= r2->hseqbase) {
			bn = BATslice(r2, lo - r2->hseqbase, hi - r2->hseqbase);
			BAThseqbase(bn, l->hseqbase);
			msg = " (slice2)";
			goto doreturn;
		}
	}
	if (complex_cand(l)) {
		/* l is candidate list with exceptions or is a bitmask */
		assert(l->ttype == TYPE_msk || !is_oid_nil(l->tseqbase));
		lcount = canditer_init(&ci, NULL, l);
		lci = &ci;
	} else if (l->ttype == TYPE_msk) {
		l = BATunmask(l);
		if (l == NULL)
			goto doreturn;
	}
	if (lcount == 0 ||
	    (l->ttype == TYPE_void && is_oid_nil(l->tseqbase)) ||
	    (r1->ttype == TYPE_void && is_oid_nil(r1->tseqbase) &&
	     (r2 == NULL ||
	      (r2->ttype == TYPE_void && is_oid_nil(r2->tseqbase))))) {
		/* trivial: all values are nil (includes no entries at all) */
		const void *nil = r1->ttype == TYPE_msk ? &oid_nil : ATOMnilptr(r1->ttype);

		bn = BATconstant(l->hseqbase, r1->ttype == TYPE_oid || r1->ttype == TYPE_msk ? TYPE_void : r1->ttype,
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
		    (r1i.count == 0 ||
		     lcount > (r1i.count >> 3) ||
		     r1->batRestricted == BAT_READ)) {
			/* insert strings as ints, we need to copy the
			 * string heap whole sale; we can't do this if
			 * there are nils in the left column, and we
			 * won't do it if the left is much smaller than
			 * the right and the right is writable (meaning
			 * we have to actually copy the right string
			 * heap) */
			tpe = r1i.width == 1 ? TYPE_bte : (r1i.width == 2 ? TYPE_sht : (r1i.width == 4 ? TYPE_int : TYPE_lng));
			stringtrick = true;
		} else if (l->tnonil &&
			   r2 != NULL &&
			   (r1i.vh == r2i.vh ||
			    (!GDK_ELIMDOUBLES(r1i.vh) /* && size tests */))) {
			/* r1 and r2 may explicitly share their vheap,
			 * if they do, the result will also share the
			 * vheap; this also means that for this case we
			 * don't care about duplicate elimination: it
			 * will remain the same */
			bn = project_str(l, lci, r1, r2, &r1i, &r2i, t0);
			bat_iterator_end(&r1i);
			bat_iterator_end(&r2i);
			return bn;
		}
	} else if (tpe == TYPE_msk || mask_cand(r1)) {
		r1 = BATunmask(r1);
		if (r1 == NULL)
			goto doreturn;
		if (r2) {
			r2 = BATunmask(r2);
			if (r2 == NULL)
				goto doreturn;
		}
		tpe = TYPE_oid;
		bat_iterator_end(&r1i);
		bat_iterator_end(&r2i);
		r1i = bat_iterator(r1);
		r2i = bat_iterator(r2);
	}
	bn = COLnew_intern(l->hseqbase, ATOMtype(r1->ttype), lcount, TRANSIENT, stringtrick ? r1i.width : 0);
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
		res = project_bte(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_sht:
		res = project_sht(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_int:
		res = project_int(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_flt:
		res = project_flt(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_dbl:
		res = project_dbl(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_lng:
		res = project_lng(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		res = project_hge(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
#endif
	case TYPE_oid:
		res = project_oid(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	case TYPE_uuid:
		res = project_uuid(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	default:
		res = project_any(bn, l, lci, r1, r2, &r1i, &r2i);
		break;
	}

	if (res != GDK_SUCCEED)
		goto bailout;

	/* handle string trick */
	if (stringtrick) {
		assert(r1i.vh);
		if (r1->batRestricted == BAT_READ) {
			/* really share string heap */
			assert(r1i.vh->parentid > 0);
			BBPshare(r1i.vh->parentid);
			/* there is no file, so we don't need to remove it */
			HEAPdecref(bn->tvheap, false);
			bn->tvheap = r1i.vh;
			HEAPincref(r1i.vh);
		} else {
			/* make copy of string heap */
			bn->tvheap->parentid = bn->batCacheid;
			bn->tvheap->farmid = BBPselectfarm(bn->batRole, TYPE_str, varheap);
			strconcat_len(bn->tvheap->filename,
				      sizeof(bn->tvheap->filename),
				      BBP_physical(bn->batCacheid), ".theap",
				      NULL);
			if (HEAPcopy(bn->tvheap, r1i.vh, 0) != GDK_SUCCEED)
				goto bailout;
		}
		bn->ttype = r1->ttype;
		bn->tvarsized = true;
		bn->twidth = r1i.width;
		bn->tshift = r1i.shift;
	}

	if (!BATtdense(r1) || (r2 && !BATtdense(r2)))
		BATtseqbase(bn, oid_nil);

  doreturn:
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT " r1=" ALGOBATFMT " r2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT "%s%s " LLFMT "us\n",
		  ALGOBATPAR(l), ALGOBATPAR(or1), ALGOOPTBATPAR(or2),
		  ALGOOPTBATPAR(bn),
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1i.vh ? " sharing string heap" : "",
		  msg, GDKusec() - t0);
	bat_iterator_end(&r1i);
	bat_iterator_end(&r2i);
	if (l != ol)
		BBPreclaim(l);
	if (r1 != or1)
		BBPreclaim(r1);
	if (r2 != or2)
		BBPreclaim(r2);
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
 * Note that all BATs except the last must have type oid/void or msk.
 *
 * We assume that all but the last BAT in the chain is temporary and
 * therefore there is no chance that another thread will modify it while
 * we're busy.  This is not necessarily the case for that last BAT, so
 * it uses a BAT iterator.
 */
BAT *
BATprojectchain(BAT **bats)
{
	struct ba {
		BAT *b;
		oid hlo;
		oid hhi;
		BUN cnt;
		oid *t;
		struct canditer ci; /* used if .ci.s != NULL */
	} *ba;
	BAT **tobedeleted = NULL;
	int ndelete = 0;
	int n, i;
	BAT *b = NULL, *bn = NULL;
	BATiter bi;
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
		ndelete += (b->ttype == TYPE_msk || mask_cand(b));
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

	if (ndelete > 0 &&
	    (tobedeleted = GDKmalloc(sizeof(BAT *) * ndelete)) == NULL)
		return NULL;
	ba = GDKmalloc(sizeof(*ba) * n);
	if (ba == NULL) {
		GDKfree(tobedeleted);
		return NULL;
	}

	ndelete = 0;
	for (n = 0, i = 0; bats[n]; n++) {
		b = bats[n];
		if (b->ttype == TYPE_msk || mask_cand(b)) {
			if ((b = BATunmask(b)) == NULL) {
				goto bunins_failed;
			}
			tobedeleted[ndelete++] = b;
		}
		if (bats[n+1] && BATtdense(b) && b->hseqbase == b->tseqbase && b->tseqbase == bats[n+1]->hseqbase && BATcount(b) == BATcount(bats[n+1]))
			continue; /* skip dense bat */
		ba[i] = (struct ba) {
			.b = b,
			.hlo = b->hseqbase,
			.hhi = b->hseqbase + b->batCount,
			.cnt = b->batCount,
			.t = (oid *) Tloc(b, 0),
		};
		allnil |= b->ttype == TYPE_void && is_oid_nil(b->tseqbase);
		issorted &= b->tsorted;
		if (bats[n + 1])
			nonil &= b->tnonil;
		if (b->tnonil && b->tkey && b->tsorted &&
		    ATOMtype(b->ttype) == TYPE_oid) {
			canditer_init(&ba[i].ci, NULL, b);
		}
		i++;
	}
	n = i;
	if (i<=2) {
		if (i == 1) {
			bn = ba[0].b;
			BBPfix(bn->batCacheid);
		} else {
			bn = BATproject(ba[0].b, ba[1].b);
		}
		while (ndelete-- > 0)
			BBPunfix(tobedeleted[ndelete]->batCacheid);
		GDKfree(tobedeleted);
		GDKfree(ba);
		return bn;
	}
	/* b is last BAT in bats array */
	tpe = ATOMtype(b->ttype);
	nil = ATOMnilptr(tpe);
	if (allnil || ba[0].cnt == 0) {
		bn = BATconstant(ba[0].hlo, tpe == TYPE_oid ? TYPE_void : tpe,
				 nil, ba[0].cnt, TRANSIENT);
		while (ndelete-- > 0)
			BBPreclaim(tobedeleted[ndelete]);
		GDKfree(tobedeleted);
		GDKfree(ba);
		TRC_DEBUG(ALGO, "with %d bats: nil/empty -> " ALGOOPTBATFMT
			  " " LLFMT " usec\n",
			  n, ALGOOPTBATPAR(bn), GDKusec() - t0);
		return bn;
	}

	bi = bat_iterator(b);
	if (nonil && ATOMstorage(tpe) == TYPE_str && b->batRestricted == BAT_READ) {
		stringtrick = true;
		tpe = bi.width == 1 ? TYPE_bte : (bi.width == 2 ? TYPE_sht : (bi.width == 4 ? TYPE_int : TYPE_lng));
	}

	bn = COLnew(ba[0].hlo, tpe, ba[0].cnt, TRANSIENT);
	if (bn == NULL) {
		bat_iterator_end(&bi);
		goto bunins_failed;
	}

	assert(ba[n - 1].b == b);
	ba[n - 1].t = bi.base;
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
				if (o < ba[i].hlo || o >= ba[i].hhi) {
					GDKerror("does not match always\n");
					bat_iterator_end(&bi);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ?
				    (ba[i].ci.tpe == cand_dense) ?
					canditer_idx_dense(&ba[i].ci, o) :
					canditer_idx(&ba[i].ci, o) : ba[i].t[o];
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
				if (o < ba[i].hlo || o >= ba[i].hhi) {
					GDKerror("does not match always\n");
					bat_iterator_end(&bi);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ?
				    (ba[i].ci.tpe == cand_dense) ?
					canditer_idx_dense(&ba[i].ci, o) :
					canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			if (is_oid_nil(o)) {
				assert(!stringtrick);
				bn->tnil = true;
				v = nil;
			} else if (o < ba[n].hlo || o >= ba[n].hhi) {
				GDKerror("does not match always\n");
				bat_iterator_end(&bi);
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = (const char *) bi.base + (o << bi.shift);
			}
			if (ATOMputFIX(tpe, d, v) != GDK_SUCCEED) {
				bat_iterator_end(&bi);
				goto bunins_failed;
			}
			d += bi.width;
		}
		if (stringtrick) {
			bn->tnil = false;
			bn->tnonil = b->tnonil;
			bn->tkey = false;
			BBPshare(bi.vh->parentid);
			assert(bn->tvheap == NULL);
			bn->tvheap = bi.vh;
			HEAPincref(bi.vh);
			bn->ttype = b->ttype;
			bn->tvarsized = true;
			assert(bn->twidth == bi.width);
			assert(bn->tshift == bi.shift);
		}
		n++;		/* undo for debug print */
	} else {
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
				if (o < ba[i].hlo || o >= ba[i].hhi) {
					GDKerror("does not match always\n");
					bat_iterator_end(&bi);
					goto bunins_failed;
				}
				o -= ba[i].hlo;
				o = ba[i].ci.s ?
				    (ba[i].ci.tpe == cand_dense) ?
					canditer_idx_dense(&ba[i].ci, o) :
					canditer_idx(&ba[i].ci, o) : ba[i].t[o];
			}
			if (is_oid_nil(o)) {
				bn->tnil = true;
				v = nil;
			} else if (o < ba[n].hlo || o >= ba[n].hhi) {
				GDKerror("does not match always\n");
				bat_iterator_end(&bi);
				goto bunins_failed;
			} else {
				o -= ba[n].hlo;
				v = BUNtail(bi, o);
			}
			if (bunfastapp(bn, v) != GDK_SUCCEED) {
				bat_iterator_end(&bi);
				goto bunins_failed;
			}
		}
		n++;		/* undo for debug print */
	}
	bat_iterator_end(&bi);
	BATsetcount(bn, ba[0].cnt);
	bn->tsorted = (ba[0].cnt <= 1) | issorted;
	bn->trevsorted = ba[0].cnt <= 1;
	bn->tnonil = nonil & b->tnonil;
	bn->tseqbase = oid_nil;
	/* note, b may point to one of the bats in tobedeleted, so
	 * reclaim after the last use of b */
	while (ndelete-- > 0)
		BBPreclaim(tobedeleted[ndelete]);
	GDKfree(tobedeleted);
	GDKfree(ba);
	TRC_DEBUG(ALGO, "with %d bats: " ALGOOPTBATFMT " " LLFMT " usec\n",
		  n, ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bunins_failed:
	while (ndelete-- > 0)
		BBPreclaim(tobedeleted[ndelete]);
	GDKfree(tobedeleted);
	GDKfree(ba);
	BBPreclaim(bn);
	TRC_DEBUG(ALGO, "failed " LLFMT "usec\n", GDKusec() - t0);
	return NULL;
}
