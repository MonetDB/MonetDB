/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/* This file contains the legacy interface to the join functions */

#undef BATsemijoin
#undef BATjoin
#undef BATleftjoin
#undef BATthetajoin
#undef BATouterjoin
#undef BATleftfetchjoin
#undef BATantijoin
#undef BATbandjoin
#undef BATrangejoin

/* Return a subset of l where head elements occur as head element in r. */
BAT *
BATsemijoin(BAT *l, BAT *r)
{
	BAT *lmap;
	BAT *res1, *res2;
	BAT *bn;

	if (BATcount(l) == 0)
		return BATcopy(l, l->htype, l->ttype, 0);
	if (BATcount(r) == 0) {
		bn = BATnew(l->htype, l->ttype, 0);
		if (BAThdense(l))
			BATseqbase(bn, l->hseqbase);
		if (BATtdense(l))
			BATseqbase(BATmirror(bn), l->tseqbase);
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
	if (BATsubsemijoin(&res1, &res2, l, r, NULL, NULL, 0, BATcount(l)) == GDK_FAIL) {
		if (lmap)
			BBPunfix(lmap->batCacheid);
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		return NULL;
	}
	BBPunfix(res2->batCacheid);
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
do_batjoin(BAT *l, BAT *r, BAT *r2, int op,
	   const void *c1, const void *c2, int li, int hi, BUN estimate,
	   gdk_return (*joinfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
				  int, BUN),
	   gdk_return (*thetajoin)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
				   int, int, BUN),
	   gdk_return (*bandjoin)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
				  const void *, const void *, int, int, BUN),
	   gdk_return (*rangejoin)(BAT **, BAT **, BAT *, BAT *, BAT *,
				   BAT *, BAT *, int, int, BUN),

	   const char *name)
{
	BAT *lmap, *rmap;
	BAT *res1, *res2;
	BAT *bn;
	gdk_return ret;

	ALGODEBUG {
		if (r2)
			fprintf(stderr, "#Legacy %s(l=%s#"BUNFMT"[%s,%s]%s%s%s,"
				"rl=%s#" BUNFMT "[%s,%s]%s%s%s,"
				"rh=%s#" BUNFMT "[%s,%s]%s%s%s)\n", name,
				BATgetId(l), BATcount(l),
				ATOMname(l->htype), ATOMname(l->ttype),
				BAThdense(l) ? "-hdense" : "",
				l->tsorted ? "-sorted" : "",
				l->trevsorted ? "-revsorted" : "",
				BATgetId(r), BATcount(r),
				ATOMname(r->htype), ATOMname(r->ttype),
				BAThdense(r) ? "-hdense" : "",
				r->tsorted ? "-sorted" : "",
				r->trevsorted ? "-revsorted" : "",
				BATgetId(r2), BATcount(r2),
				ATOMname(r2->htype), ATOMname(r2->ttype),
				BAThdense(r2) ? "-hdense" : "",
				r2->tsorted ? "-sorted" : "",
				r2->trevsorted ? "-revsorted" : "");
		else
			fprintf(stderr, "#Legacy %s(l=%s#"BUNFMT"[%s,%s]%s%s%s,"
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
	}
	/* note that in BATrangejoin, we join on the *tail* of r and r2 */
	if (r2 == NULL)
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
	if (joinfunc) {
		assert(thetajoin == NULL);
		assert(bandjoin == NULL);
		assert(rangejoin == NULL);
		assert(r2 == NULL);
		assert(c1 == NULL);
		assert(c2 == NULL);
		ret = (*joinfunc)(&res1, &res2, l, r, NULL, NULL, 0, estimate);
	} else if (thetajoin) {
		assert(bandjoin == NULL);
		assert(rangejoin == NULL);
		assert(r2 == NULL);
		assert(c1 == NULL);
		assert(c2 == NULL);
		ret = (*thetajoin)(&res1, &res2, l, r, NULL, NULL, op, 0, estimate);
	} else if (bandjoin) {
		assert(rangejoin == NULL);
		assert(r2 == NULL);
		ret = (*bandjoin)(&res1, &res2, l, r, NULL, NULL, c1, c2, li, hi, estimate);
	} else {
		assert(rangejoin != NULL);
		assert(r2 != NULL);
		assert(c1 == NULL);
		assert(c2 == NULL);
		ret = (*rangejoin)(&res1, &res2, l, r, r2, NULL, NULL, li, hi, estimate);
	}
	if (ret == GDK_FAIL) {
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
	return do_batjoin(l, r, NULL, 0, NULL, NULL, 0, 0, estimate,
			  BATsubjoin, NULL, NULL, NULL, "BATjoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3];
 * return value is in order of left input */
BAT *
BATleftjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, NULL, 0, NULL, NULL, 0, 0, estimate,
			  BATsubleftjoin, NULL, NULL, NULL, "BATleftjoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3] */
BAT *
BATthetajoin(BAT *l, BAT *r, int op, BUN estimate)
{
	if (op == JOIN_EQ)
		return do_batjoin(l, r, NULL, 0, NULL, NULL, 0, 0, estimate,
				  BATsubjoin, NULL, NULL, NULL, "BATthetajoin");
	return do_batjoin(l, r, NULL, op, NULL, NULL, 0, 0, estimate, NULL,
			  BATsubthetajoin, NULL, NULL, "BATthetajoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3];
 * if there is no match for a tuple in l, return nil in tail */
BAT *
BATouterjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, NULL, 0, NULL, NULL, 0, 0, estimate,
			  BATsubouterjoin, NULL, NULL, NULL, "BATouterjoin");
}

/* join [any_1,any_2] with [any_2,any_3], return [any_1,any_3];
 * if there is no match for a tuple in l, return nil in tail */
BAT *
BATleftfetchjoin(BAT *l, BAT *r, BUN estimate)
{
	return do_batjoin(l, r, NULL, 0, NULL, NULL, 0, 0, estimate,
			  BATsubleftfetchjoin, NULL, NULL, NULL,
			  "BATleftfetchjoin");
}

BAT *
BATantijoin(BAT *l, BAT *r)
{
	return do_batjoin(l, r, NULL, JOIN_NE, NULL, NULL, 0, 0,
			  (BUN) MIN((lng) BATcount(l) * BATcount(r), BUN_MAX),
			  NULL, BATsubthetajoin, NULL, NULL, "BATantijoin");
}

BAT *
BATbandjoin(BAT *l, BAT *r, const void *c1, const void *c2, bit li, bit hi)
{
	return do_batjoin(l, r, NULL, 0, c1, c2, li, hi, BUN_NONE,
			  NULL, NULL, BATsubbandjoin, NULL, "BATbandjoin");
}

BAT *
BATrangejoin(BAT *l, BAT *rl, BAT *rh, bit li, bit hi)
{
	return do_batjoin(l, rl, rh, 0, NULL, NULL, li, hi, BUN_NONE,
			  NULL, NULL, NULL, BATsubrangejoin, "BATrangejoin");
}
