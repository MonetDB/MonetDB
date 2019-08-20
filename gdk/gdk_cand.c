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

bool
BATcandcontains(BAT *s, oid o)
{
	if (s == NULL)
		return true;

	assert(ATOMtype(s->ttype) == TYPE_oid);
	assert(s->tsorted);
	assert(s->tkey);
	assert(s->tnonil);

	if (BATcount(s) == 0)
		return false;
	if (BATtdense(s))
		return s->tseqbase <= o && o < s->tseqbase + BATcount(s);
	const oid *cand = Tloc(s, 0);
	BUN lo = 0, hi = BATcount(s) - 1;
	if (o < cand[lo] || o > cand[hi])
		return false;
	while (hi > lo) {
		BUN mid = (lo + hi) / 2;
		if (cand[mid] == o)
			return true;
		if (cand[mid] < o)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return cand[lo] == o;
}
