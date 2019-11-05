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
#include "gdk_cand.h"

/* create a new, dense candidate list with values from `first' up to,
 * but not including, `last' */
static inline BAT *
newdensecand(oid first, oid last)
{
	if (last <= first)
		first = last = 0; /* empty range */
	return BATdense(0, first, last - first);
}

/* merge two candidate lists and produce a new one
 *
 * candidate lists are VOID-headed BATs with an OID tail which is
 * sorted and unique.
 */
BAT *
BATmergecand(BAT *a, BAT *b)
{
	BAT *bn;
	oid *restrict p, i;
	struct canditer cia, cib;

	BATcheck(a, "BATmergecand", NULL);
	BATcheck(b, "BATmergecand", NULL);

	canditer_init(&cia, NULL, a);
	canditer_init(&cib, NULL, b);

	/* we can return a if b is empty (and v.v.) */
	if (cia.ncand == 0) {
		return canditer_slice(&cib, 0, cib.ncand);
	}
	if (cib.ncand == 0) {
		return canditer_slice(&cia, 0, cia.ncand);
	}
	/* we can return a if a fully covers b (and v.v) */
	if (cia.tpe == cand_dense && cib.tpe == cand_dense) {
		/* both are dense */
		if (cia.seq <= cib.seq && cib.seq <= cia.seq + cia.ncand) {
			/* partial overlap starting with a, or b is
			 * smack bang after a */
			return newdensecand(cia.seq, cia.seq + cia.ncand < cib.seq + cib.ncand ? cib.seq + cib.ncand : cia.seq + cia.ncand);
		}
		if (cib.seq <= cia.seq && cia.seq <= cib.seq + cib.ncand) {
			/* partial overlap starting with b, or a is
			 * smack bang after b */
			return newdensecand(cib.seq, cia.seq + cia.ncand < cib.seq + cib.ncand ? cib.seq + cib.ncand : cia.seq + cia.ncand);
		}
	}
	if (cia.tpe == cand_dense
	    && cia.seq <= cib.seq
	    && canditer_last(&cia) >= canditer_last(&cib)) {
		return canditer_slice(&cia, 0, cia.ncand);
	}
	if (cib.tpe == cand_dense
	    && cib.seq <= cia.seq
	    && canditer_last(&cib) >= canditer_last(&cia)) {
		return canditer_slice(&cib, 0, cib.ncand);
	}

	bn = COLnew(0, TYPE_oid, cia.ncand + cib.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (cia.tpe == cand_dense && cib.tpe == cand_dense) {
		/* both lists are dense */
		if (cia.seq > cib.seq) {
			struct canditer ci;
			ci = cia;
			cia = cib;
			cib = ci;
		}
		/* cia completely before cib */
		assert(cia.seq + cia.ncand < cib.seq);
		for (i = cia.seq; i < cia.seq + cia.ncand; i++)
			*p++ = i;
		/* there is a gap */
		for (i = cib.seq; i < cib.seq + cib.ncand; i++)
			*p++ = i;
	} else if (cia.tpe == cand_dense || cib.tpe == cand_dense) {
		if (cib.tpe == cand_dense) {
			struct canditer ci;
			ci = cia;
			cia = cib;
			cib = ci;
		}
		/* only cia is dense */

		/* copy part of cib that's before the start of cia */
		while (canditer_peek(&cib) < cia.seq) {
			*p++ = canditer_next(&cib);
		}
		/* copy all of cia */
		for (i = cia.seq; i < cia.seq + cia.ncand; i++)
			*p++ = i;
		/* skip over part of cib that overlaps with cia */
		canditer_setidx(&cib, canditer_search(&cib, cia.seq + cia.ncand, true));
		/* copy rest of cib */
		while (cib.next < cib.ncand) {
			*p++ = canditer_next(&cib);
		}
	} else {
		/* a and b are both not dense */
		oid ao = canditer_next(&cia);
		oid bo = canditer_next(&cib);
		while (!is_oid_nil(ao) && !is_oid_nil(bo)) {
			if (ao < bo) {
				*p++ = ao;
				ao = canditer_next(&cia);
			} else if (bo < ao) {
				*p++ = bo;
				bo = canditer_next(&cib);
			} else {
				*p++ = ao;
				ao = canditer_next(&cia);
				bo = canditer_next(&cib);
			}
		}
		while (!is_oid_nil(ao)) {
			*p++ = ao;
			ao = canditer_next(&cia);
		}
		while (!is_oid_nil(bo)) {
			*p++ = bo;
			bo = canditer_next(&cib);
		}
	}

	/* properties */
	BATsetcount(bn, (BUN) (p - (oid *) Tloc(bn, 0)));
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tsorted = true;
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
	return virtualize(bn);
}

/* intersect two candidate lists and produce a new one
 *
 * candidate lists are VOID-headed BATs with an OID tail which is
 * sorted and unique.
 */
BAT *
BATintersectcand(BAT *a, BAT *b)
{
	BAT *bn;
	oid *restrict p;
	struct canditer cia, cib;

	BATcheck(a, "BATintersectcand", NULL);
	BATcheck(b, "BATintersectcand", NULL);

	canditer_init(&cia, NULL, a);
	canditer_init(&cib, NULL, b);

	if (cia.ncand == 0 || cib.ncand == 0) {
		return BATdense(0, 0, 0);
	}

	if (cia.tpe == cand_dense && cib.tpe == cand_dense) {
		/* both lists are dense */
		return newdensecand(MAX(cia.seq, cib.seq), MIN(cia.seq + cia.ncand, cib.seq + cib.ncand));
	}

	bn = COLnew(0, TYPE_oid, MIN(cia.ncand, cib.ncand), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (cia.tpe == cand_dense || cib.tpe == cand_dense) {
		if (cib.tpe == cand_dense) {
			struct canditer ci;
			ci = cia;
			cia = cib;
			cib = ci;
		}
		/* only cia is dense */

		/* search for first value in cib that is contained in cia */
		canditer_setidx(&cib, canditer_search(&cib, cia.seq, true));
		oid bo;
		while (!is_oid_nil(bo = canditer_next(&cib)) && bo < cia.seq + cia.ncand)
			*p++ = bo;
	} else {
		/* a and b are both not dense */
		oid ao = canditer_next(&cia);
		oid bo = canditer_next(&cib);
		while (!is_oid_nil(ao) && !is_oid_nil(bo)) {
			if (ao < bo)
				ao = canditer_next(&cia);
			else if (bo < ao)
				bo = canditer_next(&cib);
			else {
				*p++ = ao;
				ao = canditer_next(&cia);
				bo = canditer_next(&cib);
			}
		}
	}

	/* properties */
	BATsetcount(bn, (BUN) (p - (oid *) Tloc(bn, 0)));
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tsorted = true;
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
	return virtualize(bn);
}

/* calculate the difference of two candidate lists and produce a new one
 */
BAT *
BATdiffcand(BAT *a, BAT *b)
{
	BAT *bn;
	oid *restrict p;
	struct canditer cia, cib;

	BATcheck(a, "BATdiffcand", NULL);
	BATcheck(b, "BATdiffcand", NULL);

	canditer_init(&cia, NULL, a);
	canditer_init(&cib, NULL, b);

	if (cia.ncand == 0)
		return BATdense(0, 0, 0);
	if (cia.ncand == 0)
		return canditer_slice(&cia, 0, cia.ncand);

	if (cib.seq > canditer_last(&cia) || canditer_last(&cib) < cia.seq) {
		/* no overlap, return a */
		return canditer_slice(&cia, 0, cia.ncand);
	}

	if (cia.tpe == cand_dense && cib.tpe == cand_dense) {
		/* both a and b are dense */
		if (cia.seq < cib.seq) {
			/* a starts before b */
			if (cia.seq + cia.ncand <= cib.seq + cib.ncand) {
				/* b overlaps with end of a */
				return canditer_slice(&cia, 0, cib.seq - cia.seq);
			}
			/* b is subset of a */
			return canditer_slice2(&cia, 0, cib.seq - cia.seq,
					       cib.seq + cib.ncand - cia.seq,
					       cia.ncand);
		} else {
			/* cia.seq >= cib.seq */
			if (cia.seq + cia.ncand > cib.seq + cib.ncand) {
				/* b overlaps with beginning of a */
				return canditer_slice(&cia, cib.seq + cib.ncand - cia.seq, cia.ncand);
			}
			/* a is subset f b */
			return BATdense(0, 0, 0);
		}
	}
	if (cib.tpe == cand_dense) {
		/* b is dense and a is not: we can copy the part of a
		 * that is before the start of b and the part of a
		 * that is after the end of b */
		return canditer_slice2(&cia, 0,
				       canditer_search(&cia, cib.seq, true),
				       canditer_search(&cia, cib.seq + cib.ncand, true),
				       cia.ncand);
	}

	/* b is not dense */
	bn = COLnew(0, TYPE_oid, BATcount(a), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = Tloc(bn, 0);
	/* find first position in b that is in range of a */
	canditer_setidx(&cib, canditer_search(&cib, cia.seq, true));
	oid ob = canditer_next(&cib);
	for (BUN i = 0; i < cia.ncand; i++) {
		oid oa = canditer_next(&cia);
		while (!is_oid_nil(ob) && ob < oa) {
			ob = canditer_next(&cib);
		}
		if (!is_oid_nil(ob) && oa < ob)
			*p++ = oa;
	}

	/* properties */
	BATsetcount(bn, (BUN) (p - (oid *) Tloc(bn, 0)));
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tsorted = true;
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
	return virtualize(bn);
}

/* return offset of first value in cand that is >= o */
static inline BUN
binsearchcand(const oid *cand, BUN hi, oid o)
{
	BUN lo = 0;

	if (o <= cand[lo])
		return 0;
	if (o > cand[hi])
		return hi + 1;
	/* loop invariant: cand[lo] < o <= cand[hi] */
	while (hi > lo + 1) {
		BUN mid = (lo + hi) / 2;
		if (cand[mid] == o)
			return mid;
		if (cand[mid] < o)
			lo = mid;
		else
			hi = mid;
	}
	return hi;
}

/* initialize a candidate iterator, return number of iterations */
BUN
canditer_init(struct canditer *ci, BAT *b, BAT *s)
{
	assert(ci != NULL);

	if (s == NULL) {
		if (b == NULL) {
			/* trivially empty candidate list */
			*ci = (struct canditer) {
				.tpe = cand_dense,
			};
			return 0;
		}
		/* every row is a candidate */
		*ci = (struct canditer) {
			.tpe = cand_dense,
			.seq = b->hseqbase,
			.ncand = BATcount(b),
		};
		return ci->ncand;
	}

	assert(ATOMtype(s->ttype) == TYPE_oid);
	assert(s->tsorted);
	assert(s->tkey);
	assert(s->tnonil);
	assert(s->ttype == TYPE_void || s->tvheap == NULL);

	BUN cnt = BATcount(s);

	if (cnt == 0 || (b != NULL && BATcount(b) == 0)) {
		/* candidate list for empty BAT or empty candidate list */
		*ci = (struct canditer) {
			.tpe = cand_dense,
			.s = s,
		};
		return 0;
	}

	*ci = (struct canditer) {
		.seq = s->tseqbase,
		.s = s,
	};

	if (s->ttype == TYPE_void) {
		assert(!is_oid_nil(ci->seq));
		if (s->tvheap) {
			assert(s->tvheap->free % SIZEOF_OID == 0);
			ci->noids = s->tvheap->free / SIZEOF_OID;
			if (ci->noids > 0) {
				ci->tpe = cand_except;
				ci->oids = (const oid *) s->tvheap->base;
			} else {
				/* why the vheap? */
				ci->tpe = cand_dense;
				ci->oids = NULL;
			}
		} else {
			ci->tpe = cand_dense;
		}
	} else if (is_oid_nil(ci->seq)) {
		ci->tpe = cand_materialized;
		ci->oids = (const oid *) s->theap.base;
		ci->seq = ci->oids[0];
		ci->noids = cnt;
		if (ci->oids[ci->noids - 1] - ci->oids[0] == ci->noids - 1) {
			/* actually dense */
			ci->tpe = cand_dense;
			ci->oids = NULL;
		}
	} else {
		/* materialized dense: no exceptions */
		ci->tpe = cand_dense;
	}
	switch (ci->tpe) {
	case cand_dense:
	case_cand_dense:
		if (b != NULL) {
			if (ci->seq + cnt <= b->hseqbase ||
			    ci->seq >= b->hseqbase + BATcount(b)) {
				ci->ncand = 0;
				return 0;
			}
			if (b->hseqbase > ci->seq) {
				cnt -= b->hseqbase - ci->seq;
				ci->offset += b->hseqbase - ci->seq;
				ci->seq = b->hseqbase;
			}
			if (ci->seq + cnt > b->hseqbase + BATcount(b))
				cnt = b->hseqbase + BATcount(b) - ci->seq;
		}
		break;
	case cand_materialized:
		if (b != NULL) {
			if (ci->oids[ci->noids - 1] < b->hseqbase) {
				*ci = (struct canditer) {
					.tpe = cand_dense,
					.s = s,
				};
				return 0;
			}
			if (ci->oids[0] < b->hseqbase) {
				BUN lo = 0;
				BUN hi = cnt - 1;
				const oid o = b->hseqbase;
				/* loop invariant:
				 * ci->oids[lo] < o <= ci->oids[hi] */
				while (lo + 1 < hi) {
					BUN mid = (lo + hi) / 2;
					if (ci->oids[mid] > o)
						hi = mid;
					else
						lo = mid;
				}
				ci->offset = hi;
				cnt -= hi;
				ci->oids += hi;
				ci->seq = ci->oids[0];
			}
			if (ci->oids[cnt - 1] >= b->hseqbase + BATcount(b)) {
				BUN lo = 0;
				BUN hi = cnt - 1;
				const oid o = b->hseqbase + BATcount(b);
				/* loop invariant:
				 * ci->oids[lo] < o <= ci->oids[hi] */
				while (lo + 1 < hi) {
					BUN mid = (lo + hi) / 2;
					if (ci->oids[mid] > o)
						hi = mid;
					else
						lo = mid;
				}
				cnt = hi;
			}
			ci->noids = cnt;
		}
		break;
	case cand_except:
		/* exceptions must all be within range of s */
		assert(ci->oids[0] >= ci->seq);
		assert(ci->oids[ci->noids - 1] < ci->seq + cnt + ci->noids);
		if (b != NULL) {
			if (ci->seq + cnt + ci->noids <= b->hseqbase ||
			    ci->seq >= b->hseqbase + BATcount(b)) {
				*ci = (struct canditer) {
					.tpe = cand_dense,
				};
				return 0;
			}
		}
		/* prune exceptions at either end of range of s */
		while (ci->noids > 0 && ci->oids[0] == ci->seq) {
			ci->noids--;
			ci->oids++;
			ci->seq++;
		}
		while (ci->noids > 0 &&
		       ci->oids[ci->noids - 1] == ci->seq + cnt + ci->noids - 1)
			ci->noids--;
		/* WARNING: don't reset ci->oids to NULL when setting
		 * ci->tpe to cand_dense below: BATprojectchain will
		 * fail */
		if (ci->noids == 0) {
			ci->tpe = cand_dense;
			goto case_cand_dense;
		}
		if (b != NULL) {
			BUN p;
			p = binsearchcand(ci->oids, ci->noids - 1, b->hseqbase);
			if (p == ci->noids) {
				/* all exceptions before start of b */
				ci->offset = b->hseqbase - ci->seq - ci->noids;
				cnt = ci->seq + cnt + ci->noids - b->hseqbase;
				ci->seq = b->hseqbase;
				ci->noids = 0;
				ci->tpe = cand_dense;
				break;
			}
			assert(b->hseqbase > ci->seq || p == 0);
			if (b->hseqbase > ci->seq) {
				/* skip candidates, possibly including
				 * exceptions */
				ci->oids += p;
				ci->noids -= p;
				p = b->hseqbase - ci->seq - p;
				cnt -= p;
				ci->offset += p;
				ci->seq = b->hseqbase;
			}
			if (ci->seq + cnt + ci->noids > b->hseqbase + BATcount(b)) {
				p = binsearchcand(ci->oids, ci->noids - 1,
						  b->hseqbase + BATcount(b));
				ci->noids = p;
				cnt = b->hseqbase + BATcount(b) - ci->seq - ci->noids;
			}
			while (ci->noids > 0 && ci->oids[0] == ci->seq) {
				ci->noids--;
				ci->oids++;
				ci->seq++;
			}
			while (ci->noids > 0 &&
			       ci->oids[ci->noids - 1] == ci->seq + cnt + ci->noids - 1)
				ci->noids--;
			if (ci->noids == 0) {
				ci->tpe = cand_dense;
				goto case_cand_dense;
			}
		}
		break;
	}
	ci->ncand = cnt;
	return cnt;
}

oid
canditer_peek(struct canditer *ci)
{
	if (ci->next == ci->ncand)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + ci->next;
	case cand_materialized:
		assert(ci->next < ci->noids);
		return ci->oids[ci->next];
	case cand_except:
		/* work around compiler error: control reaches end of
		 * non-void function */
		break;
	}
	oid o = ci->seq + ci->add + ci->next;
	while (ci->add < ci->noids && o == ci->oids[ci->add]) {
		ci->add++;
		o++;
	}
	return o;
}

oid
canditer_prev(struct canditer *ci)
{
	if (ci->next == 0)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + --ci->next;
	case cand_materialized:
		return ci->oids[--ci->next];
	case cand_except:
		break;
	}
	oid o = ci->seq + ci->add + --ci->next;
	while (ci->add > 0 && o == ci->oids[ci->add - 1]) {
		ci->add--;
		o--;
	}
	return o;
}

oid
canditer_peekprev(struct canditer *ci)
{
	if (ci->next == 0)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + ci->next - 1;
	case cand_materialized:
		return ci->oids[ci->next - 1];
	case cand_except:
		break;
	}
	oid o = ci->seq + ci->add + ci->next - 1;
	while (ci->add > 0 && o == ci->oids[ci->add - 1]) {
		ci->add--;
		o--;
	}
	return o;
}

oid
canditer_last(struct canditer *ci)
{
	if (ci->ncand == 0)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + ci->ncand - 1;
	case cand_materialized:
		return ci->oids[ci->ncand - 1];
	case cand_except:
		/* work around compiler error: control reaches end of
		 * non-void function */
		break;
	}
	return ci->seq + ci->ncand + ci->noids - 1;
}

oid
canditer_idx(struct canditer *ci, BUN p)
{
	if (p >= ci->ncand)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return ci->seq + p;
	case cand_materialized:
		return ci->oids[p];
	case cand_except:
		/* work around compiler error: control reaches end of
		 * non-void function */
		break;
	}
	oid o = ci->seq + p;
	if (o < ci->oids[0])
		return o;
	if (o + ci->noids > ci->oids[ci->noids - 1])
		return o + ci->noids;
	BUN i = 0;
	if (ci->noids > 1024)
		i = binsearchcand(ci->oids, ci->noids, o);
	for (; i < ci->noids; i++)
		if (o + i < ci->oids[i])
			return o + i;
	return o + ci->noids;
}

void
canditer_setidx(struct canditer *ci, BUN p)
{
	if (p != ci->next) {
		if (p >= ci->ncand) {
			ci->next = ci->ncand;
			if (ci->tpe == cand_except)
				ci->add = ci->noids;
		} else {
			ci->next = p;
			if (ci->tpe == cand_except)
				ci->add = canditer_idx(ci, p) - ci->seq - p;
		}
	}
}

void
canditer_reset(struct canditer *ci)
{
	ci->next = 0;
	ci->add = 0;
}

BUN
canditer_search(struct canditer *ci, oid o, bool next)
{
	BUN p;

	switch (ci->tpe) {
	case cand_dense:
		if (o < ci->seq)
			return next ? 0 : BUN_NONE;
		if (o >= ci->seq + ci->ncand)
			return next ? ci->ncand : BUN_NONE;
		return o - ci->seq;
	case cand_materialized:
		if (ci->noids == 0)
			return 0;
		p = binsearchcand(ci->oids, ci->noids - 1, o);
		if (!next && (p == ci->noids || ci->oids[p] != o))
			return BUN_NONE;
		return p;
	case cand_except:
		break;
	}
	if (o < ci->seq)
		return next ? 0 : BUN_NONE;
	if (o >= ci->seq + ci->ncand + ci->noids)
		return next ? ci->ncand : BUN_NONE;
	p = binsearchcand(ci->oids, ci->noids - 1, o);
	if (next || p == ci->noids || ci->oids[p] != o)
		return o - ci->seq - p;
	return BUN_NONE;
}

/* return either an actual BATslice or a new BAT that contains the
 * "virtual" slice of the input candidate list BAT; except, unlike
 * BATslice, the hseqbase of the returned BAT is 0 */
BAT *
canditer_slice(struct canditer *ci, BUN lo, BUN hi)
{
	BAT *bn;
	oid o;
	BUN add;

	assert(ci != NULL);

	if (lo >= ci->ncand || lo >= hi)
		return BATdense(0, 0, 0);
	if (hi > ci->ncand)
		hi = ci->ncand;
	switch (ci->tpe) {
	case cand_materialized:
		if (ci->s) {
			bn = BATslice(ci->s, lo + ci->offset, hi + ci->offset);
			BAThseqbase(bn, 0);
			return bn;
		}
		bn = COLnew(0, TYPE_oid, hi - lo, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, hi - lo);
		memcpy(Tloc(bn, 0), ci->oids + lo, (hi - lo) * sizeof(oid));
		break;
	default: /* really: case cand_dense: */
		return BATdense(0, ci->seq + lo, hi - lo);
	case cand_except:
		o = canditer_idx(ci, lo);
		add = o - ci->seq - lo;
		assert(add <= ci->noids);
		if (add == ci->noids || o + hi - lo < ci->oids[add]) {
			/* after last exception or before next
			 * exception: return dense sequence */
			return BATdense(0, o, hi - lo);
		}
		bn = COLnew(0, TYPE_oid, hi - lo, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, hi - lo);
		for (oid *dst = Tloc(bn, 0); lo < hi; lo++) {
			while (add < ci->noids && o == ci->oids[add]) {
				o++;
				add++;
			}
			*dst++ = o++;
		}
		break;
	}
	bn->tsorted = true;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = true;
	bn->tseqbase = oid_nil;
	bn->tnil = false;
	bn->tnonil = true;
	return virtualize(bn);
}

BAT *
canditer_slice2(struct canditer *ci, BUN lo1, BUN hi1, BUN lo2, BUN hi2)
{
	BAT *bn;
	oid o;
	BUN add;

	assert(lo1 <= hi1);
	assert(lo2 <= hi2);
	assert(hi1 <= lo2 || (lo2 == 0 && hi2 == 0));

	if (hi1 == lo2)		/* consecutive slices: combine into one */
		return canditer_slice(ci, lo1, hi2);
	if (lo2 == hi2 || hi1 >= ci->ncand || lo2 >= ci->ncand) {
		/* empty second slice */
		return canditer_slice(ci, lo1, hi1);
	}
	if (lo1 == hi1)		/* empty first slice */
		return canditer_slice(ci, lo2, hi2);
	if (lo1 >= ci->ncand)	/* out of range */
		return BATdense(0, 0, 0);

	if (hi2 >= ci->ncand)
		hi2 = ci->ncand;

	bn = COLnew(0, TYPE_oid, hi1 - lo1 + hi2 - lo2, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, hi1 - lo1 + hi2 - lo2);
	bn->tsorted = true;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = true;
	bn->tseqbase = oid_nil;
	bn->tnil = false;
	bn->tnonil = true;

	oid *dst = Tloc(bn, 0);

	switch (ci->tpe) {
	case cand_materialized:
		memcpy(dst, ci->oids + lo1, (hi1 - lo1) * sizeof(oid));
		memcpy(dst + hi1 - lo1, ci->oids + lo2, (hi2 - lo2) * sizeof(oid));
		break;
	case cand_dense:
		while (lo1 < hi1)
			*dst++ = ci->seq + lo1++;
		while (lo2 < hi2)
			*dst++ = ci->seq + lo2++;
		break;
	case cand_except:
		o = canditer_idx(ci, lo1);
		add = o - ci->seq - lo1;
		assert(add <= ci->noids);
		if (add == ci->noids) {
			/* after last exception: return dense sequence */
			while (lo1 < hi1)
				*dst++ = ci->seq + add + lo1++;
		} else {
			while (lo1 < hi1) {
				while (add < ci->noids && o == ci->oids[add]) {
					o++;
					add++;
				}
				*dst++ = o++;
				lo1++;
			}
		}
		o = canditer_idx(ci, lo2);
		add = o - ci->seq - lo2;
		assert(add <= ci->noids);
		if (add == ci->noids) {
			/* after last exception: return dense sequence */
			while (lo2 < hi2)
				*dst++ = ci->seq + add + lo2++;
		} else {
			while (lo2 < hi2) {
				while (add < ci->noids && o == ci->oids[add]) {
					o++;
					add++;
				}
				*dst++ = o++;
				lo2++;
			}
		}
	}
	return virtualize(bn);
}

gdk_return
BATnegcands(BAT *dense_cands, BAT *odels)
{
	const char *nme;
	Heap *dels;
	BUN lo, hi;

	assert(BATtdense(dense_cands));
	assert(dense_cands->ttype == TYPE_void);
	assert(dense_cands->batRole == TRANSIENT);

	if (BATcount(odels) == 0)
		return GDK_SUCCEED;

	lo = SORTfndfirst(odels, &dense_cands->tseqbase);
	hi = SORTfndfirst(odels, &(oid) {dense_cands->tseqbase + BATcount(dense_cands)});
	if (lo == hi)
		return GDK_SUCCEED;

	nme = BBP_physical(dense_cands->batCacheid);
	if ((dels = (Heap*)GDKzalloc(sizeof(Heap))) == NULL ||
	    (dels->farmid = BBPselectfarm(dense_cands->batRole, dense_cands->ttype, varheap)) < 0){
		GDKfree(dels);
		return GDK_FAIL;
	}
	strconcat_len(dels->filename, sizeof(dels->filename),
		      nme, ".theap", NULL);

    	if (HEAPalloc(dels, hi - lo, sizeof(oid)) != GDK_SUCCEED) {
		GDKfree(dels);
        	return GDK_FAIL;
	}
    	dels->parentid = dense_cands->batCacheid;
	memcpy(dels->base, Tloc(odels, lo), sizeof(oid) * (hi - lo));
	dels->free += sizeof(oid) * (hi - lo);
	dense_cands->batDirtydesc = true;
	dense_cands->tvheap = dels;
	BATsetcount(dense_cands, dense_cands->batCount - (hi - lo));
	ALGODEBUG fprintf(stderr, "#BATnegcands(cands=" ALGOBATFMT ","
			  "dels=" ALGOBATFMT ")\n",
			  ALGOBATPAR(dense_cands),
			  ALGOBATPAR(odels));
    	return GDK_SUCCEED;
}
