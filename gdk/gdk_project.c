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
project1_##TYPE(BAT *restrict bn, BATiter *restrict li,			\
		BATiter *restrict r1i, lng timeoffset)			\
{									\
	BUN lo;								\
	const TYPE *restrict r1t;					\
	TYPE *restrict bt;						\
	oid r1seq, r1end;						\
									\
	MT_thread_setalgorithm(__func__);				\
	r1t = (const TYPE *) r1i->base;					\
	bt = (TYPE *) Tloc(bn, 0);					\
	r1seq = r1i->b->hseqbase;					\
	r1end = r1seq + r1i->count;					\
	if (BATtdensebi(li)) {						\
		if (li->tseq < r1seq ||					\
		    (li->tseq + li->count) >= r1end) {			\
			GDKerror("does not match always\n");		\
			return GDK_FAIL;				\
		}							\
		oid off = li->tseq - r1seq;				\
		r1t += off;						\
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset)		\
			bt[lo] = r1t[lo];				\
	} else {							\
		assert(li->type);					\
		const oid *restrict ot = (const oid *) li->base;	\
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {		\
			oid o = ot[lo];					\
			if (o < r1seq || o >= r1end) {			\
				GDKerror("does not match always\n");	\
				return GDK_FAIL;			\
			}						\
			bt[lo] = r1t[o - r1seq];			\
		}							\
	}								\
	TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(GDK_FAIL));		\
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
project_##TYPE(BAT *restrict bn, BATiter *restrict li,			\
	       struct canditer *restrict ci,				\
	       BATiter *restrict r1i, BATiter *restrict r2i,		\
	       lng timeoffset)						\
{									\
	BUN lo;								\
	const TYPE *restrict r1t;					\
	const TYPE *restrict r2t;					\
	TYPE *restrict bt;						\
	TYPE v;								\
	oid r1seq, r1end;						\
	oid r2seq, r2end;						\
									\
	if (r2i == NULL &&						\
	    (ci == NULL || (ci->tpe == cand_dense && BATtdensebi(li))) && \
	    li->nonil && r1i->type && !BATtdensebi(r1i))		\
		return project1_##TYPE(bn, li, r1i, timeoffset);	\
	MT_thread_setalgorithm(__func__);				\
	r1t = (const TYPE *) r1i->base;					\
	bt = (TYPE *) Tloc(bn, 0);					\
	r1seq = r1i->b->hseqbase;					\
	r1end = r1seq + r1i->count;					\
	if (r2i) {							\
		r2t = (const TYPE *) r2i->base;				\
		r2seq = r2i->b->hseqbase;				\
		r2end = r2seq + r2i->count;				\
	} else {							\
		r2t = NULL;						\
		r2seq = r2end = r1end;					\
	}								\
	if (ci) {							\
		TIMEOUT_LOOP_IDX(lo, ci->ncand, timeoffset) {		\
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
	} else if (BATtdensebi(li)) {					\
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {		\
			oid o = li->tseq + lo;				\
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
		const oid *restrict ot = (const oid *) li->base;	\
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {		\
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
	TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(GDK_FAIL));		\
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
project_oid(BAT *restrict bn, BATiter *restrict li,
	    struct canditer *restrict lci,
	    BATiter *restrict r1i, BATiter *restrict r2i, lng timeoffset)
{
	BUN lo;
	oid *restrict bt;
	oid r1seq, r1end;
	oid r2seq, r2end;
	const oid *restrict r1t = NULL;
	const oid *restrict r2t = NULL;
	struct canditer r1ci = {0}, r2ci = {0};

	if ((!lci || (lci->tpe == cand_dense && BATtdensebi(li))) && r1i->type && !BATtdensebi(r1i) && !r2i && li->nonil) {
		if (sizeof(oid) == sizeof(lng))
			return project1_lng(bn, li, r1i, timeoffset);
		else
			return project1_int(bn, li, r1i, timeoffset);
	}
	MT_thread_setalgorithm(__func__);
	if (complex_cand(r1i->b))
		canditer_init(&r1ci, NULL, r1i->b);
	else if (!BATtdensebi(r1i))
		r1t = (const oid *) r1i->base;
	r1seq = r1i->b->hseqbase;
	r1end = r1seq + r1i->count;
	if (r2i) {
		if (complex_cand(r2i->b))
			canditer_init(&r2ci, NULL, r2i->b);
		else if (!BATtdensebi(r2i))
			r2t = (const oid *) r2i->base;
		r2seq = r2i->b->hseqbase;
		r2end = r2seq + r2i->count;
	} else {
		r2seq = r2end = r1end;
	}
	bt = (oid *) Tloc(bn, 0);
	if (lci) {
		TIMEOUT_LOOP_IDX(lo, lci->ncand, timeoffset) {
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
					bt[lo] = o - r1seq + r1i->tseq;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2i->tseq;
			}
		}
	} else if (BATtdensebi(li)) {
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = li->tseq + lo;
			if (o < r1seq || o >= r2end) {
				goto nomatch;
			}
			if (o < r1end) {
				if (r1ci.s)
					bt[lo] = canditer_idx(&r1ci, o - r1seq);
				else if (r1t)
					bt[lo] = r1t[o - r1seq];
				else
					bt[lo] = o - r1seq + r1i->tseq;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2i->tseq;
			}
		}
	} else {
		const oid *ot = (const oid *) li->base;
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				bt[lo] = oid_nil;
				bn->tnonil = false;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				goto nomatch;
			} else if (o < r1end) {
				if (r1ci.s)
					bt[lo] = canditer_idx(&r1ci, o - r1seq);
				else if (r1t)
					bt[lo] = r1t[o - r1seq];
				else
					bt[lo] = o - r1seq + r1i->tseq;
			} else {
				if (r2ci.s)
					bt[lo] = canditer_idx(&r2ci, o - r2seq);
				else if (r2t)
					bt[lo] = r2t[o - r2seq];
				else
					bt[lo] = o - r2seq + r2i->tseq;
			}
		}
	}
	TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(GDK_FAIL));
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
  nomatch:
	GDKerror("does not match always\n");
	return GDK_FAIL;
}

static gdk_return
project_any(BAT *restrict bn, BATiter *restrict li,
	    struct canditer *restrict ci,
	    BATiter *restrict r1i, BATiter *restrict r2i, lng timeoffset)
{
	BUN lo;
	const void *nil = ATOMnilptr(r1i->type);
	const void *v;
	oid r1seq, r1end;
	oid r2seq, r2end;

	MT_thread_setalgorithm(__func__);
	r1seq = r1i->b->hseqbase;
	r1end = r1seq + r1i->count;
	if (r2i) {
		r2seq = r2i->b->hseqbase;
		r2end = r2seq + r2i->count;
	} else {
		r2seq = r2end = r1end;
	}
	if (ci) {
		TIMEOUT_LOOP_IDX(lo, ci->ncand, timeoffset) {
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
	} else if (BATtdensebi(li)) {
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = li->tseq + lo;
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
		const oid *restrict ot = (const oid *) li->base;

		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = ot[lo];
			if (is_oid_nil(o)) {
				v = nil;
				bn->tnil = true;
			} else if (o < r1seq || o >= r2end) {
				GDKerror("does not match always\n");
				return GDK_FAIL;
			} else if (o < r1end) {
				v = BUNtail(*r1i, o - r1seq);
			} else {
				v = BUNtail(*r2i, o - r2seq);
			}
			if (tfastins_nocheck(bn, lo, v) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
	}
	TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(GDK_FAIL));
	BATsetcount(bn, lo);
	bn->theap->dirty = true;
	return GDK_SUCCEED;
}

static BAT *
project_str(BATiter *restrict li, struct canditer *restrict ci, int tpe,
	    BATiter *restrict r1i, BATiter *restrict r2i,
	    lng timeoffset, lng t0)
{
	BAT *bn;
	BUN lo;
	oid r1seq, r1end;
	oid r2seq, r2end;
	BUN h1off;
	BUN off;
	oid seq;
	var_t v;
	BATiter *ri;

	if ((bn = COLnew(li->b->hseqbase, tpe, ci ? ci->ncand : li->count,
			 TRANSIENT)) == NULL)
		return NULL;

	v = (var_t) r1i->vhfree;
	if (r1i->vh == r2i->vh) {
		h1off = 0;
		BBPshare(r1i->vh->parentid);
		HEAPdecref(bn->tvheap, true);
		HEAPincref(r1i->vh);
		bn->tvheap = r1i->vh;
	} else {
		v = (v + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		h1off = (BUN) v;
		v += ((var_t) r2i->vhfree + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
		if (HEAPextend(bn->tvheap, v, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			return NULL;
		}
		memcpy(bn->tvheap->base, r1i->vh->base, r1i->vhfree);
#ifndef NDEBUG
		if (h1off > r1i->vhfree)
			memset(bn->tvheap->base + r1i->vhfree, 0, h1off - r1i->vhfree);
#endif
		memcpy(bn->tvheap->base + h1off, r2i->vh->base, r2i->vhfree);
		bn->tvheap->free = h1off + r2i->vhfree;
		bn->tvheap->dirty = true;
	}

	if (v >= ((var_t) 1 << (8 << bn->tshift)) &&
	    GDKupgradevarheap(bn, v, false, 0) != GDK_SUCCEED) {
		BBPreclaim(bn);
		return NULL;
	}

	r1seq = r1i->b->hseqbase;
	r1end = r1seq + r1i->count;
	r2seq = r2i->b->hseqbase;
	r2end = r2seq + r2i->count;
	if (ci) {
		TIMEOUT_LOOP_IDX(lo, ci->ncand, timeoffset) {
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
	} else if (BATtdensebi(li)) {
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = li->tseq + lo;
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
		const oid *restrict ot = (const oid *) li->base;
		TIMEOUT_LOOP_IDX(lo, li->count, timeoffset) {
			oid o = ot[lo];
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
	}
	TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
	BATsetcount(bn, lo);
	bn->tsorted = bn->trevsorted = false;
	bn->tnil = false;
	bn->tnonil = r1i->nonil & r2i->nonil;
	bn->tkey = false;
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT " r1=" ALGOBATFMT " r2=" ALGOBATFMT
		  " -> " ALGOBATFMT "%s " LLFMT "us\n",
		  ALGOBATPAR(li->b), ALGOBATPAR(r1i->b), ALGOBATPAR(r2i->b),
		  ALGOBATPAR(bn),
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1i->vh ? " sharing string heap" : "",
		  GDKusec() - t0);
	return bn;
  bailout:
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2)
{
	BAT *bn = NULL;
	BAT *or1 = r1, *or2 = r2, *ol = l;
	oid lo, hi;
	gdk_return res;
	int tpe = ATOMtype(r1->ttype), otpe = tpe;
	bool stringtrick = false;
	struct canditer ci, *lci = NULL;
	const char *msg = "";
	lng t0 = 0;
	BATiter li = bat_iterator(l);
	BATiter r1i = bat_iterator(r1);
	BATiter r2i = bat_iterator(r2);
	BUN lcount = li.count;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(ATOMtype(li.type) == TYPE_oid || li.type == TYPE_msk);
	assert(r2 == NULL || tpe == ATOMtype(r2i.type));
	assert(r2 == NULL || r1->hseqbase + r1i.count == r2->hseqbase);

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	if (r2 && r1i.count == 0) {
		/* unlikely special case: r1 is empty, so we just have r2 */
		r1 = r2;
		r2 = NULL;
		bat_iterator_end(&r1i);
		r1i = r2i;
		r2i = bat_iterator(NULL);
	}

	if (BATtdensebi(&li) && lcount > 0) {
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
			bat_iterator_end(&li);
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
		assert(li.type == TYPE_msk || !is_oid_nil(l->tseqbase));
		canditer_init(&ci, NULL, l);
		lcount = ci.ncand;
		lci = &ci;
	} else if (li.type == TYPE_msk) {
		l = BATunmask(l);
		if (l == NULL)
			goto doreturn;
		if (complex_cand(l)) {
			canditer_init(&ci, NULL, l);
			lcount = ci.ncand;
			lci = &ci;
		}
	}
	if (lcount == 0 ||
	    (li.type == TYPE_void && is_oid_nil(l->tseqbase)) ||
	    (r1i.type == TYPE_void && is_oid_nil(r1->tseqbase) &&
	     (r2 == NULL ||
	      (r2i.type == TYPE_void && is_oid_nil(r2->tseqbase))))) {
		/* trivial: all values are nil (includes no entries at all) */
		const void *nil = r1i.type == TYPE_msk ? &oid_nil : ATOMnilptr(r1i.type);

		bn = BATconstant(l->hseqbase, r1i.type == TYPE_oid || r1i.type == TYPE_msk ? TYPE_void : r1i.type,
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
		if (li.nonil &&
		    r2 == NULL &&
		    (r1i.count == 0 ||
		     lcount > (r1i.count >> 3) ||
		     r1i.restricted == BAT_READ)) {
			/* insert strings as ints, we need to copy the
			 * string heap whole sale; we can't do this if
			 * there are nils in the left column, and we
			 * won't do it if the left is much smaller than
			 * the right and the right is writable (meaning
			 * we have to actually copy the right string
			 * heap) */
			tpe = r1i.width == 1 ? TYPE_bte : (r1i.width == 2 ? TYPE_sht : (r1i.width == 4 ? TYPE_int : TYPE_lng));
			stringtrick = true;
		} else if (li.nonil &&
			   r2 != NULL &&
			   (r1i.vh == r2i.vh ||
			    (!GDK_ELIMDOUBLES(r1i.vh) /* && size tests */))) {
			/* r1 and r2 may explicitly share their vheap,
			 * if they do, the result will also share the
			 * vheap; this also means that for this case we
			 * don't care about duplicate elimination: it
			 * will remain the same */
			bn = project_str(&li, lci, tpe, &r1i, &r2i, timeoffset, t0);
			bat_iterator_end(&li);
			bat_iterator_end(&r1i);
			bat_iterator_end(&r2i);
			return bn;
		}
	} else if (ATOMvarsized(tpe) &&
		   li.nonil &&
		   r2 == NULL &&
		   (r1i.count == 0 ||
		    lcount > (r1i.count >> 3) ||
		    r1i.restricted == BAT_READ)) {
		tpe = r1i.width == 4 ? TYPE_int : TYPE_lng;
		stringtrick = true;
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
	bn = COLnew2(l->hseqbase, ATOMtype(r1i.type), lcount, TRANSIENT, stringtrick ? r1i.width : 0);
	if (bn == NULL) {
		goto doreturn;
	}
	bn->tnil = false;
	if (r2) {
		bn->tnonil = li.nonil & r1i.nonil & r2i.nonil;
		bn->tsorted = li.count <= 1;
		bn->trevsorted = li.count <= 1;
		bn->tkey = li.count <= 1;
	} else {
		bn->tnonil = li.nonil & r1i.nonil;
		bn->tsorted = li.count <= 1
			|| (li.sorted & r1i.sorted)
			|| (li.revsorted & r1i.revsorted)
			|| r1i.count <= 1;
		bn->trevsorted = li.count <= 1
			|| (li.sorted & r1i.revsorted)
			|| (li.revsorted & r1i.sorted)
			|| r1i.count <= 1;
		bn->tkey = li.count <= 1 || (li.key & r1i.key);
	}

	if (!stringtrick && tpe != TYPE_oid)
		tpe = ATOMbasetype(tpe);
	switch (tpe) {
	case TYPE_bte:
		res = project_bte(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_sht:
		res = project_sht(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_int:
		res = project_int(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_flt:
		res = project_flt(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_dbl:
		res = project_dbl(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_lng:
		res = project_lng(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		res = project_hge(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
#endif
	case TYPE_oid:
		res = project_oid(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	case TYPE_uuid:
		res = project_uuid(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	default:
		res = project_any(bn, &li, lci, &r1i, r2 ? &r2i : NULL, timeoffset);
		break;
	}

	if (res != GDK_SUCCEED)
		goto bailout;

	/* handle string trick */
	if (stringtrick) {
		assert(r1i.vh);
		if (r1i.restricted == BAT_READ || VIEWvtparent(r1)) {
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
			bn->tvheap->farmid = BBPselectfarm(bn->batRole, otpe, varheap);
			strconcat_len(bn->tvheap->filename,
				      sizeof(bn->tvheap->filename),
				      BBP_physical(bn->batCacheid), ".theap",
				      NULL);
			if (HEAPcopy(bn->tvheap, r1i.vh, 0) != GDK_SUCCEED)
				goto bailout;
		}
		bn->ttype = r1i.type;
		bn->twidth = r1i.width;
		bn->tshift = r1i.shift;
	}

	if (!BATtdensebi(&r1i) || (r2 && !BATtdensebi(&r2i)))
		BATtseqbase(bn, oid_nil);

  doreturn:
	TRC_DEBUG(ALGO, "l=" ALGOBATFMT " r1=" ALGOBATFMT " r2=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT "%s%s " LLFMT "us\n",
		  ALGOBATPAR(l), ALGOBATPAR(or1), ALGOOPTBATPAR(or2),
		  ALGOOPTBATPAR(bn),
		  bn && bn->ttype == TYPE_str && bn->tvheap == r1i.vh ? " sharing string heap" : "",
		  msg, GDKusec() - t0);
	bat_iterator_end(&li);
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

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

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
	if (nonil && ATOMstorage(tpe) == TYPE_str && bi.restricted == BAT_READ) {
		stringtrick = true;
		bn = COLnew2(ba[0].hlo, tpe, ba[0].cnt, TRANSIENT, bi.width);
		if (bn && bn->tvheap) {
			/* no need to remove any files since they were
			 * never created for this bat */
			HEAPdecref(bn->tvheap, false);
			bn->tvheap = NULL;
		}
		tpe = bi.width == 1 ? TYPE_bte : (bi.width == 2 ? TYPE_sht : (bi.width == 4 ? TYPE_int : TYPE_lng));
	} else {
		bn = COLnew(ba[0].hlo, tpe, ba[0].cnt, TRANSIENT);
	}
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
		TIMEOUT_LOOP_IDX_DECL(p, ba[0].cnt, timeoffset) {
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
		TIMEOUT_LOOP_IDX_DECL(p, ba[0].cnt, timeoffset) {
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
			assert(bn->ttype == b->ttype);
			assert(bn->twidth == bi.width);
			assert(bn->tshift == bi.shift);
		}
		n++;		/* undo for debug print */
	} else {
		const void *v;

		assert(!stringtrick);
		bn->tnil = false;
		n--;	/* stop one before the end, also ba[n] is last */
		TIMEOUT_LOOP_IDX_DECL(p, ba[0].cnt, timeoffset) {
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
	TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bunins_failed));
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
