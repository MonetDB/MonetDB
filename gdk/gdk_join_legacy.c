/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* This file contains the legacy interface to the join functions */

#undef BATsemijoin
#undef BATjoin
#undef BATleftjoin
#undef BATleftfetchjoin

/* Return a subset of l where head elements occur as head element in r. */
BAT *
BATsemijoin(BAT *l, BAT *r)
{
	BAT *lmap;
	BAT *res1, *res2;
	BAT *bn;

	if (BATcount(l) == 0)
		return BATcopy(l, l->htype, l->ttype, 0, TRANSIENT);
	if (BATcount(r) == 0) {
		bn = BATnew(l->htype, l->ttype, 0, TRANSIENT);
		if (bn) {
			if (BAThdense(l))
				BATseqbase(bn, l->hseqbase);
			if (BATtdense(l))
				BATseqbase(BATmirror(bn), l->tseqbase);
		}
		return bn;
	}

	if (BAThdense(l) && BAThdense(r)) {
		oid lo = l->hseqbase, hi = lo + BATcount(l);

		if (lo < r->hseqbase)
			lo = r->hseqbase;
		if (hi > r->hseqbase + BATcount(r))
			hi = r->hseqbase + BATcount(r);
		if (hi < lo)
			hi = lo;
		return BATslice(l, lo - l->hseqbase, hi - l->hseqbase);
	}

	/* l is [any_1,any_2]; r is [any_1,any_3] */
	l = BATmirror(l);
	r = BATmirror(r);
	/* now: l is [any_2,any_1], r is [any_3,any_1] */
	if (!BAThdense(l) || !BAThdense(r)) {
		/* l is [any_2,any_1] */
		lmap = BATmirror(BATmark(l, 0));
		/* lmap is [dense1,any_2] */
		l = BATmirror(BATmark(BATmirror(l), 0));
		/* l is [dense1,any_1] */
		/* r is [any_3,any_1] */
		r = BATmirror(BATmark(BATmirror(r), 0));
		/* r is [dense2,any_1] */
	} else {
		/* l is [dense1,any_1] (i.e. any_2==dense1) */
		lmap = NULL;
		BBPfix(l->batCacheid);
		/* r is [dense2,any_1] */
		BBPfix(r->batCacheid);
	}
	if (BATsubsemijoin(&res1, NULL, l, r, NULL, NULL, 0, BATcount(l)) != GDK_SUCCEED) {
		if (lmap)
			BBPunfix(lmap->batCacheid);
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		return NULL;
	}
	BBPunfix(r->batCacheid);
	if (lmap) {
		/* res1 is [dense,sub(dense1)] */
		bn = BATproject(res1, lmap);
		BBPunfix(lmap->batCacheid);
		lmap = NULL;
		/* bn is [dense,any_2] */
		res2 = BATproject(res1, l);
		/* res2 is [dense,any_1] */
		BBPunfix(res1->batCacheid);
		res1 = bn;
		/* res1 is [dense,any_2] */
	} else {
		/* res1 is [dense,sub(dense1)] */
		res2 = BATproject(res1, l);
		/* res2 is [dense,any_1] */
	}
	BBPunfix(l->batCacheid);
	res2 = BATmirror(res2);
	/* res2 is [any_1,dense] */
	bn = VIEWcreate(res2, res1);
	/* bn is [any_1,any_2] */
	BBPunfix(res1->batCacheid);
	BBPunfix(res2->batCacheid);
	return bn;
}

static BAT *
do_batjoin(BAT *l, BAT *r, BUN estimate,
	   gdk_return (*joinfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
				  int, BUN),

	   const char *name)
{
	BAT *lmap, *rmap;
	BAT *res1, *res2;
	BAT *bn;
	gdk_return ret;

	ALGODEBUG fprintf(stderr, "#Legacy %s(l=%s#"BUNFMT"[%s,%s]%s%s%s,"
			  "r=%s#" BUNFMT "[%s,%s]%s%s%s)\n", name,
			  BATgetId(l), BATcount(l),
			  ATOMname(l->htype), ATOMname(l->ttype),
			  BAThdense(l) ? "-hdense" : "",
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r),
			  ATOMname(r->htype), ATOMname(r->ttype),
			  BAThdense(r) ? "-hdense" : "",
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "");
	r = BATmirror(r);
	/* r is [any_3,any_2] */
	if (!BAThdense(l) || !BAThdense(r)) {
		/* l is [any_1,any_2] */
		lmap = BATmirror(BATmark(l, 0));
		/* lmap is [dense1,any_1] */
		l = BATmirror(BATmark(BATmirror(l), 0));
		/* l is [dense1,any_2] */
		/* r is [any_3,any_2] */
		rmap = BATmirror(BATmark(r, 0));
		/* rmap is [dense2,any_3] */
		r = BATmirror(BATmark(BATmirror(r), 0));
		/* r is [dense2,any_2] */
	} else {
		/* l is [dense1,any_2] */
		lmap = NULL;
		BBPfix(l->batCacheid);
		/* r is [dense2,any_2] */
		rmap = NULL;
		BBPfix(r->batCacheid);
	}
	ret = (*joinfunc)(&res1, &res2, l, r, NULL, NULL, 0, estimate);
	if (ret != GDK_SUCCEED) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		if (lmap)
			BBPunfix(lmap->batCacheid);
		if (rmap)
			BBPunfix(rmap->batCacheid);
		return NULL;
	}
	if (lmap) {
		bn = BATproject(res1, lmap);
		BBPunfix(res1->batCacheid);
		BBPunfix(lmap->batCacheid);
		res1 = bn;
		/* res1 is [dense,any_1] */
		lmap = NULL;
		bn = BATproject(res2, rmap);
		BBPunfix(res2->batCacheid);
		BBPunfix(rmap->batCacheid);
		res2 = bn;
		/* res2 is [dense,any_3] */
		rmap = NULL;
	}
	bn = VIEWcreate(BATmirror(res1), res2);
	/* bn is [any_1,any_3] */
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(res1->batCacheid);
	BBPunfix(res2->batCacheid);
	return bn;
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3] */
BAT *
BATjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, estimate,
			  BATsubjoin, "BATjoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3];
 * return value is in order of left input */
BAT *
BATleftjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, estimate,
			  BATsubleftjoin, "BATleftjoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3];
 * if there is no match for a tuple in l, return nil in tail */
BAT *
BATleftfetchjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, estimate,
			  BATsubleftfetchjoin, "BATleftfetchjoin");
}
