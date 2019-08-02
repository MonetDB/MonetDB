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
static BAT *
newdensecand(oid first, oid last)
{
	if (last < first)
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
	const oid *restrict ap, *restrict bp, *ape, *bpe;
	oid *restrict p, i;
	oid af, al, bf, bl;
	bit ad, bd;

	BATcheck(a, "BATmergecand", NULL);
	BATcheck(b, "BATmergecand", NULL);
	assert(ATOMtype(a->ttype) == TYPE_oid);
	assert(ATOMtype(b->ttype) == TYPE_oid);
	assert(BATcount(a) <= 1 || a->tsorted);
	assert(BATcount(b) <= 1 || b->tsorted);
	assert(BATcount(a) <= 1 || a->tkey);
	assert(BATcount(b) <= 1 || b->tkey);
	assert(a->tnonil);
	assert(b->tnonil);

	/* we can return a if b is empty (and v.v.) */
	if (BATcount(a) == 0) {
		return COLcopy(b, b->ttype, false, TRANSIENT);
	}
	if (BATcount(b) == 0) {
		return COLcopy(a, a->ttype, false, TRANSIENT);
	}
	/* we can return a if a fully covers b (and v.v) */
	af = BUNtoid(a, 0);
	bf = BUNtoid(b, 0);
	al = BUNtoid(a, BUNlast(a) - 1);
	bl = BUNtoid(b, BUNlast(b) - 1);
	ad = (af + BATcount(a) - 1 == al); /* i.e., dense */
	bd = (bf + BATcount(b) - 1 == bl); /* i.e., dense */
	if (ad && bd) {
		/* both are dense */
		if (af <= bf && bf <= al + 1) {
			/* partial overlap starting with a, or b is
			 * smack bang after a */
			return newdensecand(af, al < bl ? bl + 1 : al + 1);
		}
		if (bf <= af && af <= bl + 1) {
			/* partial overlap starting with b, or a is
			 * smack bang after b */
			return newdensecand(bf, al < bl ? bl + 1 : al + 1);
		}
	}
	if (ad && af <= bf && al >= bl) {
		return newdensecand(af, al + 1);
	}
	if (bd && bf <= af && bl >= al) {
		return newdensecand(bf, bl + 1);
	}

	bn = COLnew(0, TYPE_oid, BATcount(a) + BATcount(b), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (BATtdense(a) && BATtdense(b)) {
		/* both lists are dense */
		if (a->tseqbase > b->tseqbase) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* a->tseqbase <= b->tseqbase */
		for (i = a->tseqbase; i < a->tseqbase + BATcount(a); i++)
			*p++ = i;
		for (i = MAX(b->tseqbase, i);
		     i < b->tseqbase + BATcount(b);
		     i++)
			*p++ = i;
	} else if (BATtdense(a) || BATtdense(b)) {
		if (BATtdense(b)) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* only a is dense */
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (bp < bpe && *bp < a->tseqbase)
			*p++ = *bp++;
		for (i = a->tseqbase; i < a->tseqbase + BATcount(a); i++)
			*p++ = i;
		while (bp < bpe && *bp < i)
			bp++;
		while (bp < bpe)
			*p++ = *bp++;
	} else {
		/* a and b are both not dense */
		ap = (const oid *) Tloc(a, 0);
		ape = ap + BATcount(a);
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (ap < ape && bp < bpe) {
			if (*ap < *bp)
				*p++ = *ap++;
			else if (*ap > *bp)
				*p++ = *bp++;
			else {
				*p++ = *ap++;
				bp++;
			}
		}
		while (ap < ape)
			*p++ = *ap++;
		while (bp < bpe)
			*p++ = *bp++;
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
	const oid *restrict ap, *restrict bp, *ape, *bpe;
	oid *restrict p;
	oid af, al, bf, bl;

	BATcheck(a, "BATintersectcand", NULL);
	BATcheck(b, "BATintersectcand", NULL);
	assert(ATOMtype(a->ttype) == TYPE_oid);
	assert(ATOMtype(b->ttype) == TYPE_oid);
	assert(a->tsorted);
	assert(b->tsorted);
	assert(a->tkey);
	assert(b->tkey);
	assert(a->tnonil);
	assert(b->tnonil);

	if (BATcount(a) == 0 || BATcount(b) == 0) {
		return newdensecand(0, 0);
	}

	af = BUNtoid(a, 0);
	bf = BUNtoid(b, 0);
	al = BUNtoid(a, BUNlast(a) - 1);
	bl = BUNtoid(b, BUNlast(b) - 1);

	if ((af + BATcount(a) - 1 == al) && (bf + BATcount(b) - 1 == bl)) {
		/* both lists are dense */
		return newdensecand(MAX(af, bf), MIN(al, bl) + 1);
	}

	bn = COLnew(0, TYPE_oid, MIN(BATcount(a), BATcount(b)), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (BATtdense(a) || BATtdense(b)) {
		if (BATtdense(b)) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* only a is dense */
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (bp < bpe && *bp < a->tseqbase)
			bp++;
		while (bp < bpe && *bp < a->tseqbase + BATcount(a))
			*p++ = *bp++;
	} else {
		/* a and b are both not dense */
		ap = (const oid *) Tloc(a, 0);
		ape = ap + BATcount(a);
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (ap < ape && bp < bpe) {
			if (*ap < *bp)
				ap++;
			else if (*ap > *bp)
				bp++;
			else {
				*p++ = *ap++;
				bp++;
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
	const oid *restrict ap, *restrict bp, *ape, *bpe;
	oid *restrict p;
	oid af, al, bf, bl;

	BATcheck(a, "BATdiffcand", NULL);
	BATcheck(b, "BATdiffcand", NULL);
	assert(ATOMtype(a->ttype) == TYPE_oid);
	assert(ATOMtype(b->ttype) == TYPE_oid);
	assert(a->tsorted);
	assert(b->tsorted);
	assert(a->tkey);
	assert(b->tkey);
	assert(a->tnonil);
	assert(b->tnonil);

	if (BATcount(a) == 0)
		return newdensecand(0, 0);
	if (BATcount(b) == 0)
		return COLcopy(a, a->ttype, false, TRANSIENT);

	af = BUNtoid(a, 0);
	bf = BUNtoid(b, 0);
	al = BUNtoid(a, BUNlast(a) - 1) + 1;
	bl = BUNtoid(b, BUNlast(b) - 1) + 1;

	if (bf >= al || bl <= af) {
		/* no overlap, return a */
		return COLcopy(a, a->ttype, false, TRANSIENT);
	}

	if (BATtdense(a) && BATtdense(b)) {
		/* both a and b are dense */
		if (af < bf) {
			if (al <= bl) {
				/* b overlaps with end of a */
				return newdensecand(af, bf);
			}
			/* b is subset of a */
			return doublerange(af, bf, bl, al);
		} else {
			/* af >= bf */
			if (al > bl) {
				/* b overlaps with beginning of a */
				return newdensecand(bl, al);
			}
			/* a is subset f b */
			return newdensecand(0, 0);
		}
	}
	bn = COLnew(0, TYPE_oid, BATcount(a), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = Tloc(bn, 0);
	if (BATtdense(b)) {
		BUN n;
		/* b is dense and a is not: we can copy the part of a
		 * that is before the start of b and the part of a
		 * that is after the end of b */
		ap = Tloc(a, 0);
		/* find where b starts in a */
		n = binsearch(NULL, 0, TYPE_oid, ap, NULL, SIZEOF_OID, 0,
			      BATcount(a), &bf, 1, 0);
		if (n > 0)
			memcpy(p, ap, n * SIZEOF_OID);
		p += n;
		n = binsearch(NULL, 0, TYPE_oid, ap, NULL, SIZEOF_OID, 0,
			      BATcount(a), &bl, 1, 0);
		if (n < BATcount(a))
			memcpy(p, ap + n, (BATcount(a) - n) * SIZEOF_OID);
		p += n;
	} else {
		/* b is not dense; find first position in b that is in
		 * range of a */
		bp = Tloc(b, 0);
		bpe = bp + BATcount(b);
		bp += binsearch(NULL, 0, TYPE_oid, bp, NULL, SIZEOF_OID, 0,
				BATcount(b), &af, 1, 0);
		if (BATtdense(a)) {
			/* only a is dense */
			while (af < al) {
				if (bp == bpe)
					*p++ = af;
				else if (af < *bp)
					*p++ = af;
				else
					bp++;
				af++;
			}
		} else {
			/* a and b are both not dense */
			ap = Tloc(a, 0);
			ape = ap + BATcount(a);
			while (ap < ape) {
				if (bp == bpe)
					*p++ = *ap;
				else if (*ap < *bp)
					*p++ = *ap;
				else
					bp++;
				ap++;
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

bool
BATcandcontains(BAT *s, oid o)
{
	BUN p;

	if (s == NULL)
		return true;

	assert(ATOMtype(s->ttype) == TYPE_oid);
	assert(s->tsorted);
	assert(s->tkey);
	assert(s->tnonil);

	if (BATcount(s) == 0)
		return false;
	if (s->ttype == TYPE_void && s->tvheap) {
		assert(s->tvheap->free % SIZEOF_OID == 0);
		BUN nexc = (BUN) (s->tvheap->free / SIZEOF_OID);
		if (o < s->tseqbase ||
		    o >= s->tseqbase + BATcount(s) + nexc)
			return false;
		const oid *exc = (const oid *) s->tvheap->base;
		if (nexc > 0) {
			p = binsearchcand(exc, nexc - 1, o);
			return p == nexc || exc[p] != o;
		}
	}
	if (BATtdense(s))
		return s->tseqbase <= o && o < s->tseqbase + BATcount(s);
	const oid *oids = Tloc(s, 0);
	p = binsearchcand(oids, BATcount(s) - 1, o);
	return p != BATcount(s) && oids[p] == o;
}

/* initialize a candidate iterator, return number of iterations */
BUN
canditer_init(struct canditer *ci, BAT *b, BAT *s)
{
	assert(ci != NULL);
	BUN cnt;

	if (b == NULL && s == NULL) {
		*ci = (struct canditer) {
			.tpe = cand_dense,
		};
		return 0;
	}

	if (s == NULL || BATcount(b) == 0) {
		/* every row is a candidate */
		*ci = (struct canditer) {
			.tpe = cand_dense,
			.s = s,
			.seq = b->hseqbase,
			.ncand = BATcount(b),
		};
		return ci->ncand;
	}

	*ci = (struct canditer) {
		.seq = s->tseqbase,
		.s = s,
	};

	assert(ATOMtype(s->ttype) == TYPE_oid);
	cnt = BATcount(s);
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
			}
		} else {
			ci->tpe = cand_dense;
		}
	} else if (is_oid_nil(ci->seq) && BATcount(s) > 0) {
		ci->tpe = cand_materialized;
		ci->oids = (const oid *) s->theap.base;
	} else {
		ci->tpe = cand_dense;
	}
	if (b != NULL) {
		switch (ci->tpe) {
		case cand_dense:
			if (ci->seq + cnt <= b->hseqbase)
				return 0;
			if (ci->seq >= b->hseqbase + BATcount(b))
				return 0;
			if (b->hseqbase > ci->seq) {
				ci->offset = b->hseqbase - ci->seq;
				cnt -= ci->offset;
				ci->seq = b->hseqbase;
			}
			if (ci->seq + cnt > b->hseqbase + BATcount(b))
				cnt = b->hseqbase + BATcount(b) - ci->seq;
			break;
		case cand_materialized:
			if (ci->oids[ci->noids - 1] < b->hseqbase)
				return 0;
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
			ci->seq = ci->oids[0];
			ci->noids = cnt;
			break;
		case cand_except:
			/* exceptions must all be within range of s */
			assert(ci->oids[0] >= ci->seq);
			assert(ci->oids[ci->noids - 1] < ci->seq + cnt + ci->noids);
			if (ci->seq + cnt + ci->noids <= b->hseqbase ||
			    ci->seq >= b->hseqbase + BATcount(b)) {
				*ci = (struct canditer) {
					.tpe = cand_dense,
				};
				return 0;
			}
			/* prune exceptions at either end of range of s */
			while (ci->noids > 0 && ci->oids[0] == ci->seq) {
				ci->noids--;
				ci->oids++;
				ci->seq++;
				ci->offset++;
			}
			while (ci->noids > 0 &&
			       ci->oids[ci->noids - 1] == ci->seq + cnt + ci->noids - 1)
				ci->noids--;
			if (ci->noids == 0 ||
			    ci->oids[0] >= b->hseqbase + BATcount(b)) {
				/* no exceptions left after pruning, or first
				 * exception beyond range of b */
				ci->tpe = cand_dense;
				ci->oids = NULL;
				if (b->hseqbase > ci->seq) {
					ci->offset += b->hseqbase - ci->seq;
					cnt -= b->hseqbase - ci->seq;
					ci->seq = b->hseqbase;
				}
				if (ci->seq + cnt > b->hseqbase + BATcount(b))
					cnt = b->hseqbase + BATcount(b) - ci->seq;
				ci->noids = 0;
				break;
			}
			if (ci->oids[ci->noids - 1] < b->hseqbase) {
				/* last exception before start of b */
				ci->tpe = cand_dense;
				ci->oids = NULL;
				ci->offset += b->hseqbase - ci->seq - ci->noids;
				cnt -= b->hseqbase - ci->seq - ci->noids;
				ci->seq = b->hseqbase;
				ci->noids = cnt;
				if (ci->seq + cnt > b->hseqbase + BATcount(b))
					cnt = b->hseqbase + BATcount(b) - ci->seq;
				ci->noids = 0;
				break;
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
				ci->offset += hi;
				ci->oids += hi;
				ci->noids -= hi;
				cnt += hi;
			}
			if (ci->seq < b->hseqbase) {
				ci->offset += b->hseqbase - ci->seq;
				cnt -= b->hseqbase - ci->seq;
				ci->seq = b->hseqbase;
			}
			if (ci->oids[ci->noids - 1] >= b->hseqbase + BATcount(b)) {
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
				ci->noids = hi;
			}
			if (ci->seq + cnt + ci->noids > b->hseqbase + BATcount(b))
				cnt = b->hseqbase + BATcount(b) - ci->seq - ci->noids;
			break;
		}
	}
	ci->ncand = cnt;
	return cnt;
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
	/* perform binary search on exception list
	 * loop invariant:
	 * o + lo <= ci->oids[lo] && o + hi > ci->oids[hi] */
	BUN lo = 0, hi = ci->noids - 1;
	while (lo + 1 < hi) {
		BUN mid = (lo + hi) / 2;
		if (o + mid <= ci->oids[mid])
			lo = mid;
		else
			hi = mid;
	}
	return o + hi;
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
	case cand_dense:
		return BATdense(0, ci->seq + lo, hi - lo);
	case cand_except:
		o = canditer_idx(ci, lo);
		add = o - ci->seq - lo;
		assert(add <= ci->noids);
		if (add == ci->noids) {
			/* after last exception: return dense sequence */
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
			*dst++ = o;
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
				*dst++ = o;
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
				*dst++ = o;
				lo2++;
			}
		}
	}
	return virtualize(bn);
}
