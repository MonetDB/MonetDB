/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_cand.h"

/* This file contains function to create and manipulate candidate
 * lists. The functions are shared across GDK and are inlined if
 * needed.
 */

/* create a new dense candiate list */
BAT *
CANDnewdense(oid first, oid last)
{
	BAT *s;
	if ((s = COLnew(0, TYPE_void, 0, TRANSIENT)) == NULL)
		return NULL;
	if (last < first)
		first = last = 0; /* empty range */
	BATsetcount(s, last - first + 1);
	BATtseqbase(s, first);
	return s;
}

/* binary search in a candidate list, return 1 if found, 0 if not */
inline int
CANDbinsearch(const oid *cand, BUN lo, BUN hi, oid v)
{
	BUN mid;

	--hi;			/* now hi is inclusive */
	if (v < cand[lo] || v > cand[hi])
		return 0;
	while (hi > lo) {
		mid = (lo + hi) / 2;
		if (cand[mid] == v)
			return 1;
		if (cand[mid] < v)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return cand[lo] == v;
}

/* makes sure that a candidate list is virtualized to dense when possible */
BAT *
CANDvirtualize(BAT *bn)
{
	/* input must be a valid candidate list or NULL */
	assert(bn == NULL ||
	       (((bn->ttype == TYPE_void && bn->tseqbase != oid_nil) ||
		 bn->ttype == TYPE_oid) &&
		bn->tkey && bn->tsorted));
	/* since bn has unique and strictly ascending tail values, we
	 * can easily check whether the tail is dense */
	if (bn && bn->ttype == TYPE_oid &&
	    (BATcount(bn) <= 1 ||
	     * (const oid *) Tloc(bn, 0) + BATcount(bn) - 1 ==
	     * (const oid *) Tloc(bn, BUNlast(bn) - 1))) {
		/* tail is dense, replace by virtual oid */
		ALGODEBUG fprintf(stderr, "#CANDvirtualize(bn=%s#"BUNFMT",seq="OIDFMT")\n",
				  BATgetId(bn), BATcount(bn),
				  BATcount(bn) > 0 ? * (const oid *) Tloc(bn, 0) : 0);
		if (BATcount(bn) == 0)
			bn->tseqbase = 0;
		else
			bn->tseqbase = * (const oid *) Tloc(bn, 0);
		bn->tdense = 1;
		HEAPfree(&bn->theap, 1);
		bn->theap.storage = bn->theap.newstorage = STORE_MEM;
		bn->theap.size = 0;
		bn->ttype = TYPE_void;
		bn->tvarsized = 1;
		bn->twidth = 0;
		bn->tshift = 0;
	}

	return bn;
}

BAT *
CANDdoublerange(oid l1, oid h1, oid l2, oid h2)
{
	BAT *bn;
	oid *restrict p;

	assert(l1 <= h1);
	assert(l2 <= h2);
	assert(h1 <= l2);
	if (l1 == h1 || l2 == h2) {
		bn = COLnew(0, TYPE_void, h1 - l1 + h2 - l2, TRANSIENT);
		if (bn == NULL)
			return NULL;
		BATsetcount(bn, h1 - l1 + h2 - l2);
		BATtseqbase(bn, l1 == h1 ? l2 : l1);
		return bn;
	}
	bn = COLnew(0, TYPE_oid, h1 - l1 + h2 - l2, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, h1 - l1 + h2 - l2);
	p = (oid *) Tloc(bn, 0);
	while (l1 < h1)
		*p++ = l1++;
	while (l2 < h2)
		*p++ = l2++;
	bn->tkey = 1;
	bn->tsorted = 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return bn;
}

BAT *
CANDdoubleslice(BAT *s, BUN l1, BUN h1, BUN l2, BUN h2)
{
	BAT *bn;
	oid *restrict p;
	const oid *restrict o;

	assert(l1 <= h1);
	assert(l2 <= h2);
	assert(h1 <= l2);
	assert(s->tsorted);
	assert(s->tkey);
	if (s->ttype == TYPE_void)
		return CANDdoublerange(l1 + s->tseqbase, h1 + s->tseqbase,
				   l2 + s->tseqbase, h2 + s->tseqbase);
	bn = COLnew(0, TYPE_oid, h1 - l1 + h2 - l2, TRANSIENT);
	if (bn == NULL)
		return NULL;
	BATsetcount(bn, h1 - l1 + h2 - l2);
	if (l1 == 0 && h1 == 0) {
		/* explicitly requesting one slice thus using memcpy */
		memcpy(Tloc(bn, 0), Tloc(s, l2),
		       (h2 - l2) * Tsize(bn));
	} else {
		p = (oid *) Tloc(bn, 0);
		o = (const oid *) Tloc(s, l1);
		while (l1++ < h1)
			*p++ = *o++;
		o = (const oid *) Tloc(s, l2);
		while (l2++ < h2)
			*p++ = *o++;
	}
	bn->tkey = 1;
	bn->tsorted = 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return CANDvirtualize(bn);
}

BAT *
CANDslice(BAT *s, BUN l, BUN h)
{
	return CANDdoubleslice(s, 0, 0, l, h);
}
