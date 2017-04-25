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
	BAT *bn;
	if ((bn = COLnew(0, TYPE_void, 0, TRANSIENT)) == NULL)
		return NULL;
	if (last < first)
		first = last = 0; /* empty range */
	BATsetcount(bn, last - first + 1);
	BATtseqbase(bn, first);
	return bn;
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

