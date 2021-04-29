/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) Peter Boncz, Martin Kersten, Niels Nes, Sjoerd Mullender
 * BAT Algebra
 * This modules contains the most common algebraic BAT manipulation
 * commands. We call them algebra, because all operations take
 * values as parameters, and produce new result values, but
 * do not modify their parameters.
 *
 * Unlike the previous Monet versions, we reduce the number
 * of functions returning a BAT reference. This was previously needed
 * to simplify recursive bat-expression and manage reference counts.
 * In the current version we return only a BAT identifier when a new
 * bat is being created.
 *
 * All parameters to the modules are passed by reference.
 * In particular, this means that
 * string values are passed to the module layer as (str *)
 * and we have to de-reference them before entering the gdk library.
 * This calls for knowlegde on the underlying BAT typs`s
 */
#define derefStr(b, v)							\
	do {										\
		int _tpe= ATOMstorage((b)->ttype);		\
		if (_tpe >= TYPE_str) {					\
			if ((v) == 0 || *(str*) (v) == 0)	\
				(v) = (str) str_nil;			\
			else								\
				(v) = *(str *) (v);				\
		}										\
	} while (0)

#include "monetdb_config.h"
#include "algebra.h"

/*
 * Command Implementations in C
 * This module contains just a wrapper implementations; since all described
 * operations are part of the GDK kernel.
 *
 * BAT sum operation
 * The sum aggregate only works for int and float fields.
 * The routines below assumes that the caller knows what type
 * is large enough to prevent overflow.
 */

static gdk_return
CMDgen_group(BAT **result, BAT *gids, BAT *cnts )
{
	lng j, gcnt = BATcount(gids);
	BAT *r = COLnew(0, TYPE_oid, BATcount(gids)*2, TRANSIENT);

	if (r == NULL)
		return GDK_FAIL;
	if (gids->ttype == TYPE_void) {
		oid id = gids->tseqbase;
		lng *cnt = (lng*)Tloc(cnts, 0);
		for(j = 0; j < gcnt; j++) {
			lng i, sz = cnt[j];
			for(i = 0; i < sz; i++) {
				if (BUNappend(r, &id, false) != GDK_SUCCEED) {
					BBPreclaim(r);
					return GDK_FAIL;
				}
			}
			id++;
		}
	} else {
		oid *id = (oid*)Tloc(gids, 0);
		lng *cnt = (lng*)Tloc(cnts, 0);
		for(j = 0; j < gcnt; j++) {
			lng i, sz = cnt[j];
			for(i = 0; i < sz; i++) {
				if (BUNappend(r, id, false) != GDK_SUCCEED) {
					BBPreclaim(r);
					return GDK_FAIL;
				}
			}
			id++;
		}
	}
	r -> tkey = false;
	r -> tseqbase = oid_nil;
	r -> tsorted = BATtordered(gids);
	r -> trevsorted = BATtrevordered(gids);
	r -> tnonil = gids->tnonil;
	*result = r;
	return GDK_SUCCEED;
}


static gdk_return
slice(BAT **retval, BAT *b, lng start, lng end)
{
	/* the internal BATslice requires exclusive end */
	if (start < 0) {
		GDKerror("start position of slice should >= 0\n");
		return GDK_FAIL;
	}
	if (is_lng_nil(end))
		end = BATcount(b);
	if (start > (lng) BUN_MAX || end >= (lng) BUN_MAX) {
		GDKerror("argument out of range\n");
		return GDK_FAIL;
	}

	return (*retval = BATslice(b, (BUN) start, (BUN) end + 1)) ? GDK_SUCCEED : GDK_FAIL;
}
/*
 *
 * The remainder of this file contains the wrapper around the V4 code base
 * The BAT identifiers passed through this module may indicate
 * that the 'reverse' view applies. This should be taken into
 * account while resolving them.
 *
 * The sum aggregate only works for int and float fields.
 * The routines below assumes that the caller knows what type
 * is large enough to prevent overflow.
 */

static str
ALGminany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.min",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			* (ptr *) result = p = BATmin_skipnil(b, NULL, *skipnil);
		} else {
			p = BATmin_skipnil(b, result, *skipnil);
			if ( p != result )
				msg = createException(MAL, "algebra.min", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if (msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "algebra.min", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGminany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGminany_skipnil(result, bid, &skipnil);
}

static str
ALGmaxany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.max",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			* (ptr *) result = p = BATmax_skipnil(b, NULL, *skipnil);
		} else {
			p = BATmax_skipnil(b, result, *skipnil);
			if ( p != result )
				msg = createException(MAL, "algebra.max", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if ( msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "algebra.max", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGmaxany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGmaxany_skipnil(result, bid, &skipnil);
}

static str
ALGgroupby(bat *res, const bat *gids, const bat *cnts)
{
	BAT *bn, *g, *c;

	g = BATdescriptor(*gids);
	if (g == NULL) {
		throw(MAL, "algebra.groupby", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	c = BATdescriptor(*cnts);
	if (c == NULL) {
		BBPunfix(g->batCacheid);
		throw(MAL, "algebra.groupby", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if( CMDgen_group(&bn, g, c) != GDK_SUCCEED){
		BBPunfix(g->batCacheid);
		BBPunfix(c->batCacheid);
		throw(MAL, "algebra.groupby",GDK_EXCEPTION);
	}
	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(c->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGcard(lng *result, const bat *bid)
{
	BAT *b, *en;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.card", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	en = BATunique(b, NULL);
	BBPunfix(b->batCacheid);
	if (en == NULL) {
		throw(MAL, "algebra.card", GDK_EXCEPTION);
	}
	struct canditer ci;
	*result = canditer_init(&ci, NULL, en);
	BBPunfix(en->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGselect2(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	BAT *b, *s = NULL, *bn;
	const void *nilptr;

	if ((*li != 0 && *li != 1) ||
		(*hi != 0 && *hi != 1) ||
		(*anti != 0 && *anti != 1)) {
		throw(MAL, "algebra.select", ILLEGAL_ARGUMENT);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.select", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, low);
	derefStr(b, high);
	nilptr = ATOMnilptr(b->ttype);
	if (*li == 1 && *hi == 1 &&
		ATOMcmp(b->ttype, low, nilptr) == 0 &&
		ATOMcmp(b->ttype, high, nilptr) == 0) {
		/* special case: equi-select for NIL */
		high = NULL;
	}
	bn = BATselect(b, s, low, high, *li, *hi, *anti);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.select", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGselect2nil(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown)
{
	BAT *b, *s = NULL, *bn;
	bit nanti = *anti, nli = *li, nhi = *hi;

	if (!*unknown)
		return ALGselect2(result, bid, sid, low, high, li, hi, anti);

	if ((nli != 0 && nli != 1) ||
		(nhi != 0 && nhi != 1) ||
		(nanti != 0 && nanti != 1)) {
		throw(MAL, "algebra.select", ILLEGAL_ARGUMENT);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.select", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, low);
	derefStr(b, high);
	/* here we don't need open ended parts with nil */
	if (!nanti) {
		const void *nilptr = ATOMnilptr(b->ttype);
		if (nli == 1 && ATOMcmp(b->ttype, low, nilptr) == 0) {
			low = high;
			nli = 0;
		}
		if (nhi == 1 && ATOMcmp(b->ttype, high, nilptr) == 0) {
			high = low;
			nhi = 0;
		}
		if (ATOMcmp(b->ttype, low, high) == 0 && ATOMcmp(b->ttype, high, nilptr) == 0) /* ugh sql nil != nil */
			nanti = 1;
	}

	bn = BATselect(b, s, low, high, nli, nhi, nanti);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.select", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	return ALGselect2(result, bid, NULL, low, high, li, hi, anti);
}

static str
ALGselect1nil(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown)
{
	return ALGselect2nil(result, bid, NULL, low, high, li, hi, anti, unknown);
}

static str
ALGthetaselect2(bat *result, const bat *bid, const bat *sid, const void *val, const char **op)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.thetaselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.thetaselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, val);
	bn = BATthetaselect(b, s, val, *op);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.select", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGselectNotNil(bat *result, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.selectNotNil", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!b->tnonil) {
		BAT *s;
		s = BATselect(b, NULL, ATOMnilptr(b->ttype), NULL, true, true, true);
		if (s) {
			BAT *bn = BATproject(s, b);
			BBPunfix(s->batCacheid);
			if (bn) {
				BBPunfix(b->batCacheid);
				*result = bn->batCacheid;
				BBPkeepref(*result);
				return MAL_SUCCEED;
			}
		}
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.selectNotNil", GDK_EXCEPTION);
	}
	/* just pass on the result */
	*result = b->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static str
do_join(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *r2id,
		const bat *slid, const bat *srid,
		int op, const void *c1, const void *c2, bool li, bool hi,
		bool anti, bool symmetric, /* these two only for rangejoin */
		const bit *nil_matches, const bit *not_in, const bit *max_one,
		const lng *estimate,
		gdk_return (*joinfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
							   bool, BUN),
		gdk_return (*semifunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
							   bool, bool, BUN),
		gdk_return (*thetafunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
								int, bool, BUN),
		gdk_return (*bandfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
							   const void *, const void *, bool, bool, BUN),
		gdk_return (*rangefunc)(BAT **, BAT **, BAT *, BAT *, BAT *,
								BAT *, BAT *, bool, bool, bool, bool, BUN),
		BAT *(*difffunc)(BAT *, BAT *, BAT *, BAT *, bool, bool, BUN),
		BAT *(*interfunc)(BAT *, BAT *, BAT *, BAT *, bool, bool, BUN),
		const char *funcname)
{
	BAT *left = NULL, *right = NULL, *right2 = NULL;
	BAT *candleft = NULL, *candright = NULL;
	BAT *result1, *result2;
	BUN est;
	const char *err = RUNTIME_OBJECT_MISSING;

	assert(r2id == NULL || rangefunc != NULL);

	if ((left = BATdescriptor(*lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(*rid)) == NULL)
		goto fail;
	if (slid && !is_bat_nil(*slid) && (candleft = BATdescriptor(*slid)) == NULL)
		goto fail;
	if (srid && !is_bat_nil(*srid) && (candright = BATdescriptor(*srid)) == NULL)
		goto fail;
	if (estimate == NULL || *estimate < 0 || is_lng_nil(*estimate) || *estimate > (lng) BUN_MAX)
		est = BUN_NONE;
	else
		est = (BUN) *estimate;

	err = NULL;		/* most likely error now is GDK_EXCEPTION */

	if (thetafunc) {
		assert(joinfunc == NULL);
		assert(semifunc == NULL);
		assert(bandfunc == NULL);
		assert(rangefunc == NULL);
		assert(difffunc == NULL);
		assert(interfunc == NULL);
		if ((*thetafunc)(&result1, r2 ? &result2 : NULL, left, right, candleft, candright, op, *nil_matches, est) != GDK_SUCCEED)
			goto fail;
	} else if (joinfunc) {
		assert(semifunc == NULL);
		assert(bandfunc == NULL);
		assert(rangefunc == NULL);
		assert(difffunc == NULL);
		assert(interfunc == NULL);
		result2 = NULL;
		if ((*joinfunc)(&result1, r2 ? &result2 : NULL, left, right, candleft, candright, *nil_matches, est) != GDK_SUCCEED)
			goto fail;
	} else if (semifunc) {
		assert(bandfunc == NULL);
		assert(rangefunc == NULL);
		assert(difffunc == NULL);
		assert(interfunc == NULL);
		result2 = NULL;
		if ((*semifunc)(&result1, r2 ? &result2 : NULL, left, right, candleft, candright, *nil_matches, *max_one, est) != GDK_SUCCEED)
			goto fail;
	} else if (bandfunc) {
		assert(rangefunc == NULL);
		assert(difffunc == NULL);
		assert(interfunc == NULL);
		if ((*bandfunc)(&result1, r2 ? &result2 : NULL, left, right, candleft, candright, c1, c2, li, hi, est) != GDK_SUCCEED)
			goto fail;
	} else if (rangefunc) {
		assert(difffunc == NULL);
		assert(interfunc == NULL);
		if ((right2 = BATdescriptor(*r2id)) == NULL) {
			err = SQLSTATE(HY002) RUNTIME_OBJECT_MISSING;
			goto fail;
		}
		if ((*rangefunc)(&result1, r2 ? &result2 : NULL, left, right, right2, candleft, candright, li, hi, anti, symmetric, est) != GDK_SUCCEED)
			goto fail;
		BBPunfix(right2->batCacheid);
	} else if (difffunc) {
		assert(r2 == NULL);
		assert(interfunc == NULL);
		if ((result1 = (*difffunc)(left, right, candleft, candright, *nil_matches, *not_in, est)) == NULL)
			goto fail;
		result2 = NULL;
	} else {
		assert(r2 == NULL);
		if ((result1 = (*interfunc)(left, right, candleft, candright, *nil_matches, *max_one, est)) == NULL)
			goto fail;
		result2 = NULL;
	}
	*r1 = result1->batCacheid;
	BBPkeepref(*r1);
	if (r2) {
		*r2 = result2->batCacheid;
		BBPkeepref(*r2);
	}
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	if (candleft)
		BBPunfix(candleft->batCacheid);
	if (candright)
		BBPunfix(candright->batCacheid);
	return MAL_SUCCEED;

  fail:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (right2)
		BBPunfix(right2->batCacheid);
	if (candleft)
		BBPunfix(candleft->batCacheid);
	if (candright)
		BBPunfix(candright->batCacheid);
	if (err == NULL)
		throw(MAL, funcname, GDK_EXCEPTION);
	throw(MAL, funcname, "%s", err);
}

static str
ALGjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
		   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.join");
}

static str
ALGjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
		   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.join");
}

static str
ALGleftjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATleftjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.leftjoin");
}

static str
ALGleftjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATleftjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.leftjoin");
}

static str
ALGouterjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			 const bit *nil_matches, const bit *match_one, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, match_one, estimate,
				   NULL, BATouterjoin, NULL, NULL, NULL, NULL, NULL, "algebra.outerjoin");
}

static str
ALGouterjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			  const bit *nil_matches, const bit *match_one, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, match_one, estimate,
				   NULL, BATouterjoin, NULL, NULL, NULL, NULL, NULL, "algebra.outerjoin");
}

static str
ALGsemijoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			const bit *nil_matches, const bit *max_one, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, max_one, estimate,
				   NULL, BATsemijoin, NULL, NULL, NULL, NULL, NULL, "algebra.semijoin");
}

static str
ALGthetajoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const int *op, const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, *op, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   NULL, NULL, BATthetajoin, NULL, NULL, NULL, NULL, "algebra.thetajoin");
}

static str
ALGthetajoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const int *op, const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, *op, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   NULL, NULL, BATthetajoin, NULL, NULL, NULL, NULL, "algebra.thetajoin");
}

static str
ALGbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const void *c1, const void *c2, const bit *li, const bit *hi,
			   const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, c1, c2,
				   *li, *hi, false, false, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, BATbandjoin, NULL, NULL, NULL, "algebra.bandjoin");
}

static str
ALGbandjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const void *c1, const void *c2, const bit *li, const bit *hi,
			   const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, c1, c2,
				   *li, *hi, false, false, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, BATbandjoin, NULL, NULL, NULL, "algebra.bandjoin");
}

static str
ALGrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid,
			 const bit *li, const bit *hi, const bit *anti, const bit *symmetric, const lng *estimate)
{
	return do_join(r1, r2, lid, rlid, rhid, slid, srid, 0, NULL, NULL,
				   *li, *hi, *anti, *symmetric, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, NULL, BATrangejoin, NULL, NULL, "algebra.rangejoin");
}

static str
ALGrangejoin1(bat *r1, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid,
			  const bit *li, const bit *hi, const bit *anti, const bit *symmetric, const lng *estimate)
{
	return do_join(r1, NULL, lid, rlid, rhid, slid, srid, 0, NULL, NULL,
				   *li, *hi, *anti, *symmetric, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, NULL, BATrangejoin, NULL, NULL, "algebra.rangejoin");
}

static str
ALGdifference(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			  const bit *nil_matches, const bit *not_in, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, not_in, NULL, estimate,
				   NULL, NULL, NULL, NULL, NULL, BATdiff, NULL, "algebra.difference");
}

static str
ALGintersect(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			 const bit *nil_matches, const bit *max_one, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, max_one, estimate,
				   NULL, NULL, NULL, NULL, NULL, NULL, BATintersect, "algebra.intersect");
}

/* algebra.firstn(b:bat[:any],
 *                [ s:bat[:oid],
 *                [ g:bat[:oid], ] ]
 *                n:lng,
 *                asc:bit,
 *                nilslast:bit,
 *                distinct:bit)
 * returns :bat[:oid] [ , :bat[:oid] ]
 */
static str
ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret1, *ret2 = NULL;
	bat bid, sid, gid;
	BAT *b, *s = NULL, *g = NULL;
	BAT *bn, *gn;
	lng n;
	bit asc, nilslast, distinct;
	gdk_return rc;

	(void) cntxt;
	(void) mb;

	assert(pci->retc == 1 || pci->retc == 2);
	assert(pci->argc - pci->retc >= 5 && pci->argc - pci->retc <= 7);

	n = *getArgReference_lng(stk, pci, pci->argc - 4);
	if (n < 0 || (lng) n >= (lng) BUN_MAX)
		throw(MAL, "algebra.firstn", ILLEGAL_ARGUMENT);
	ret1 = getArgReference_bat(stk, pci, 0);
	if (pci->retc == 2)
		ret2 = getArgReference_bat(stk, pci, 1);
	bid = *getArgReference_bat(stk, pci, pci->retc);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.firstn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc - pci->retc > 5) {
		sid = *getArgReference_bat(stk, pci, pci->retc + 1);
		if (!is_bat_nil(sid) && (s = BATdescriptor(sid)) == NULL) {
			BBPunfix(bid);
			throw(MAL, "algebra.firstn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (pci->argc - pci->retc > 6) {
			gid = *getArgReference_bat(stk, pci, pci->retc + 2);
			if (!is_bat_nil(gid) && (g = BATdescriptor(gid)) == NULL) {
				BBPunfix(bid);
				BBPunfix(sid);
				throw(MAL, "algebra.firstn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}
	}
	asc = *getArgReference_bit(stk, pci, pci->argc - 3);
	nilslast = *getArgReference_bit(stk, pci, pci->argc - 2);
	distinct = *getArgReference_bit(stk, pci, pci->argc - 1);
	rc = BATfirstn(&bn, ret2 ? &gn : NULL, b, s, g, (BUN) n, asc, nilslast, distinct);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (rc != GDK_SUCCEED)
		throw(MAL, "algebra.firstn", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	BBPkeepref(*ret1 = bn->batCacheid);
	if (ret2)
		BBPkeepref(*ret2 = gn->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGunary(bat *result, const bat *bid, BAT *(*func)(BAT *), const char *name)
{
	BAT *b,*bn;

	if ((b= BATdescriptor(*bid)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = (*func)(b);
	BBPunfix(b->batCacheid);
	if (bn == NULL)
		throw(MAL, name, GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static BAT *
BATwcopy(BAT *b)
{
	return COLcopy(b, b->ttype, true, TRANSIENT);
}

static str
ALGcopy(bat *result, const bat *bid)
{
	return ALGunary(result, bid, BATwcopy, "algebra.copy");
}

static str
ALGunique(bat *result, const bat *bid, const bat *sid)
{
	BAT *b, *s = NULL, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATunique(b, s);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.unique", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static str
ALGcrossproduct(bat *l, bat *r, const bat *left, const bat *right, const bat *slid, const bat *srid, const bit *max_one)
{
	BAT *L, *R, *bn1, *bn2 = NULL;
	BAT *sl = NULL, *sr = NULL;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (slid && !is_bat_nil(*slid) && (sl = BATdescriptor(*slid)) == NULL) {
		BBPunfix(L->batCacheid);
		BBPunfix(R->batCacheid);
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (srid && !is_bat_nil(*srid) && (sr = BATdescriptor(*srid)) == NULL) {
		BBPunfix(L->batCacheid);
		BBPunfix(R->batCacheid);
		if (sl)
			BBPunfix(sl->batCacheid);
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	ret = BATsubcross(&bn1, r ? &bn2 : NULL, L, R, sl, sr,
					  max_one && !is_bit_nil(*max_one) && *max_one);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (sl)
		BBPunfix(sl->batCacheid);
	if (sr)
		BBPunfix(sr->batCacheid);
	if (ret != GDK_SUCCEED)
		throw(MAL, "algebra.crossproduct", GDK_EXCEPTION);
	BBPkeepref(*l = bn1->batCacheid);
	if (r)
		BBPkeepref(*r = bn2->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGcrossproduct1(bat *l, const bat *left, const bat *right, const bit *max_one)
{
	return ALGcrossproduct(l, NULL, left, right, NULL, NULL, max_one);
}

static str
ALGcrossproduct2(bat *l, bat *r, const bat *left, const bat *right, const bit *max_one)
{
	return ALGcrossproduct(l, r, left, right, NULL, NULL, max_one);
}

static str
ALGcrossproduct3(bat *l, bat *r, const bat *left, const bat *right, const bat *sl, const bat *sr, const bit *max_one)
{
	return ALGcrossproduct(l, r, left, right, sl, sr, max_one);
}

static str
ALGcrossproduct4(bat *l, const bat *left, const bat *right, const bat *sl, const bat *sr, const bit *max_one)
{
	return ALGcrossproduct(l, NULL, left, right, sl, sr, max_one);
}

static str
ALGprojection2(bat *result, const bat *lid, const bat *r1id, const bat *r2id)
{
	BAT *l, *r1, *r2 = NULL, *bn;

	if ((l = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((r1 = BATdescriptor(*r1id)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(MAL, "algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (r2id && !is_bat_nil(*r2id) && (r2 = BATdescriptor(*r2id)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r1->batCacheid);
		throw(MAL, "algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATproject2(l, r1, r2);
	BBPunfix(l->batCacheid);
	BBPunfix(r1->batCacheid);
	if (r2)
		BBPunfix(r2->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.projection", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
ALGprojection(bat *result, const bat *lid, const bat *rid)
{
	return ALGprojection2(result, lid, rid, NULL);
}

static str
ALGsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *nilslast, const bit *stable)
{
	BAT *bn = NULL, *on = NULL, *gn = NULL;
	BAT *b = NULL, *o = NULL, *g = NULL;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.sort", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (order && !is_bat_nil(*order) && (o = BATdescriptor(*order)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.sort", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (group && !is_bat_nil(*group) && (g = BATdescriptor(*group)) == NULL) {
		if (o)
			BBPunfix(o->batCacheid);
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.sort", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATsort(result ? &bn : NULL,
				   norder ? &on : NULL,
				   ngroup ? &gn : NULL,
				b, o, g, *reverse, *nilslast, *stable) != GDK_SUCCEED) {
		if (o)
			BBPunfix(o->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.sort", OPERATION_FAILED);
	}
	BBPunfix(b->batCacheid);
	if (o)
		BBPunfix(o->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (result)
		BBPkeepref(*result = bn->batCacheid);
	if (norder)
		BBPkeepref(*norder = on->batCacheid);
	if (ngroup)
		BBPkeepref(*ngroup = gn->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, order, group, reverse, nilslast, stable);
}

static str
ALGsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, order, group, reverse, nilslast, stable);
}

static str
ALGsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, ngroup, bid, order, NULL, reverse, nilslast, stable);
}

static str
ALGsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, order, NULL, reverse, nilslast, stable);
}

static str
ALGsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, order, NULL, reverse, nilslast, stable);
}

static str
ALGsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, ngroup, bid, NULL, NULL, reverse, nilslast, stable);
}

static str
ALGsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, NULL, NULL, reverse, nilslast, stable);
}

static str
ALGsort11(bat *result, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, NULL, NULL, reverse, nilslast, stable);
}

static str
ALGcountCND_nil(lng *result, const bat *bid, const bat *cnd, const bit *ignore_nils)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *s = NULL;
	bool heap_loaded = false;

	if (!(b = BBPquickdesc(*bid, false))) {
		msg = createException(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (cnd && !is_bat_nil(*cnd) && !(s = BATdescriptor(*cnd))) {
		msg = createException(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (b->ttype == TYPE_void || b->ttype == TYPE_msk || (*ignore_nils && !b->tnonil)) {
		if (!(b = BATdescriptor(*bid))) { /* has to load the heap */
			msg = createException(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		heap_loaded = true;
	}

	if (b->ttype == TYPE_msk || mask_cand(b)) {
		assert(heap_loaded);
		if (BATsum(result, TYPE_lng, b, s, *ignore_nils, false, false) != GDK_SUCCEED) {
			msg = createException(MAL, "aggr.count", GDK_EXCEPTION);
			goto bailout;
		}
	} else if (*ignore_nils && !b->tnonil) {
		assert(heap_loaded);
		*result = (lng) BATcount_no_nil(b, s);
	} else {
		struct canditer ci;
		*result = (lng) canditer_init(&ci, b, s);
	}

bailout:
	if (b && heap_loaded)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	return msg;
}

static str
ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils)
{
	return ALGcountCND_nil(result, bid, NULL, ignore_nils);
}

static str
ALGcountCND_bat(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){0});
}

static str
ALGcount_bat(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){0});
}

static str
ALGcountCND_no_nil(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){1});
}

static str
ALGcount_no_nil(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){1});
}

static str
ALGslice(bat *ret, const bat *bid, const lng *start, const lng *end)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.slice", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (slice(&bn, b, *start, *end) == GDK_SUCCEED) {
		*ret = bn->batCacheid;
		BBPkeepref(*ret);
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPunfix(b->batCacheid);
	throw(MAL, "algebra.slice", GDK_EXCEPTION);
}

static str
ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end)
{
	lng s = *start;
	lng e = (is_int_nil(*end) ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

static str
ALGslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end)
{
	lng s = *start;
	lng e = *end;

	return ALGslice(ret, bid, &s, &e);
}

/* carve out a slice based on the OIDs */
/* beware that BATs may have different OID bases */
static str
ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end)
{
	lng s = (lng) (is_oid_nil(*start) ? 0 : (lng) *start);
	lng e = (is_oid_nil(*end) ? lng_nil : (lng) *end);

	return ALGslice(ret, bid, &s, &e) ;
}

static str
ALGsubslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end)
{
	BAT *b, *bn;
	BUN s, e;

	if (*start < 0 || *start > (lng) BUN_MAX ||
		(*end < 0 && !is_lng_nil(*end)) || *end >= (lng) BUN_MAX)
		throw(MAL, "algebra.subslice", ILLEGAL_ARGUMENT);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.subslice", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	s = (BUN) *start;
	if (s > BATcount(b))
		s = BATcount(b);
	e = is_lng_nil(*end) ? BATcount(b) : (BUN) *end + 1;
	if (e > BATcount(b))
		e = BATcount(b);
	if (e < s)
		e = s;
	bn = BATdense(0, b->hseqbase + s, e - s);
	BBPunfix(*bid);
	if (bn == NULL)
		throw(MAL, "algebra.subslice", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

/*
 * BUN Get/Fetch
 */

static str
doALGfetch(ptr ret, BAT *b, BUN pos)
{
	BATiter bi = bat_iterator(b);

	assert(pos <= BUN_MAX);
	if (ATOMextern(b->ttype)) {
		ptr _src = BUNtail(bi,pos);
		size_t _len = ATOMlen(b->ttype, _src);
		ptr _dst = GDKmalloc(_len);
		if( _dst == NULL)
			throw(MAL,"doAlgFetch", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		memcpy(_dst, _src, _len);
		*(ptr*) ret = _dst;
	} else {
		size_t _s = ATOMsize(ATOMtype(b->ttype));
		if (b->ttype == TYPE_void) {
			*(oid*) ret = b->tseqbase;
			if (!is_oid_nil(b->tseqbase))
				*(oid*)ret += pos;
		} else if (_s == 4) {
			*(int*) ret = *(int*) Tloc(b, pos);
		} else if (_s == 1) {
			*(bte*) ret = *(bte*) Tloc(b, pos);
		} else if (_s == 2) {
			*(sht*) ret = *(sht*) Tloc(b, pos);
		} else if (_s == 8) {
			*(lng*) ret = *(lng*) Tloc(b, pos);
#ifdef HAVE_HGE
		} else if (_s == 16) {
			*(hge*) ret = *(hge*) Tloc(b, pos);
#endif
		} else {
			memcpy(ret, Tloc(b, pos), _s);
		}
	}
	return MAL_SUCCEED;
}

static str
ALGfetch(ptr ret, const bat *bid, const lng *pos)
{
	BAT *b;
	str msg;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.fetch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (*pos < (lng) 0) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.fetch", ILLEGAL_ARGUMENT ": row index to fetch must be non negative\n");
	}
	if (BATcount(b) == 0) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.fetch", ILLEGAL_ARGUMENT ": cannot fetch a single row from an empty input\n");
	}
	if (*pos >= (lng) BUNlast(b)) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.fetch", ILLEGAL_ARGUMENT ": row index to fetch is out of range\n");
	}
	msg = doALGfetch(ret, b, (BUN) *pos);
	BBPunfix(b->batCacheid);
	return msg;
}

str
ALGfetchoid(ptr ret, const bat *bid, const oid *pos)
{
	lng o = *pos;

	return ALGfetch(ret, bid, &o);
}

static str
ALGexist(bit *ret, const bat *bid, const void *val)
{
	BAT *b;
	BUN q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.exist", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, val);
	q = BUNfnd(b, val);
	*ret = (q != BUN_NONE);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGfind(oid *ret, const bat *bid, ptr val)
{
	BAT *b;
	BUN q;
	str msg= MAL_SUCCEED;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.find", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, val);
	q = BUNfnd(b, val);

	if (q == BUN_NONE){
		*ret = oid_nil;
	} else
		*ret = (oid) q;
	BBPunfix(b->batCacheid);
	return msg;
}


static str
ALGprojecttail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat bid = * getArgReference_bat(stk, pci, 1);
	const ValRecord *v = &stk->stk[getArg(pci, 2)];
	BAT *b, *bn;

	(void) cntxt;
	(void) mb;
	if( isaBatType(getArgType(mb,pci,2)) )
		throw(MAL,"algebra.project","Scalar value expected");
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = BATconstant(b->hseqbase, v->vtype, VALptr(v), BATcount(b), TRANSIENT);
	BBPunfix(b->batCacheid);
	if (bn == NULL) {
		*ret = bat_nil;
		throw(MAL, "algebra.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}


static str
ALGreuse(bat *ret, const bat *bid)
{
	BAT *b,*bn;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.reuse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if( !b->batTransient || b->batRestricted != BAT_WRITE){
		if( ATOMvarsized(b->ttype) ){
			bn= BATwcopy(b);
			if (bn == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, "algebra.reuse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		} else {
			bn = COLnew(b->hseqbase, b->ttype, BATcount(b), TRANSIENT);
			if (bn == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, "algebra.reuse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			BATsetcount(bn,BATcount(b));
			bn->tsorted = false;
			bn->trevsorted = false;
			BATkey(bn, false);
		}
		BBPkeepref(*ret= bn->batCacheid);
		BBPunfix(b->batCacheid);
	} else
		BBPkeepref(*ret = *bid);
	return MAL_SUCCEED;
}

/*
 * BAT standard deviation
 */
static str
ALGstdev(dbl *res, const bat *bid)
{
	BAT *b;
	dbl stdev;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.stdev", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_sample(NULL, b);
	BBPunfix(b->batCacheid);
	if (is_dbl_nil(stdev) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.stdev", GDK_EXCEPTION);
	*res = stdev;
	return MAL_SUCCEED;
}

static str
ALGstdevp(dbl *res, const bat *bid)
{
	BAT *b;
	dbl stdev;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.stdevp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_population(NULL, b);
	BBPunfix(b->batCacheid);
	if (is_dbl_nil(stdev) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.stdevp", GDK_EXCEPTION);
	*res = stdev;
	return MAL_SUCCEED;
}

/*
 * BAT variance
 */
static str
ALGvariance(dbl *res, const bat *bid)
{
	BAT *b;
	dbl variance;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.variance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_sample(NULL, b);
	BBPunfix(b->batCacheid);
	if (is_dbl_nil(variance) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.variance", GDK_EXCEPTION);
	*res = variance;
	return MAL_SUCCEED;
}

static str
ALGvariancep(dbl *res, const bat *bid)
{
	BAT *b;
	dbl variance;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.variancep", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_population(NULL, b);
	BBPunfix(b->batCacheid);
	if (is_dbl_nil(variance) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.variancep", GDK_EXCEPTION);
	*res = variance;
	return MAL_SUCCEED;
}

/*
 * BAT covariance
 */
static str
ALGcovariance(dbl *res, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2;
	dbl covariance;

	if ((b1 = BATdescriptor(*bid1)) == NULL)
		throw(MAL, "aggr.covariance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(b1->batCacheid);
		throw(MAL, "aggr.covariance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	covariance = BATcalccovariance_sample(b1, b2);
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	if (is_dbl_nil(covariance) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.covariance", GDK_EXCEPTION);
	*res = covariance;
	return MAL_SUCCEED;
}

static str
ALGcovariancep(dbl *res, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2;
	dbl covariance;

	if ((b1 = BATdescriptor(*bid1)) == NULL)
		throw(MAL, "aggr.covariancep", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(b1->batCacheid);
		throw(MAL, "aggr.covariancep", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	covariance = BATcalccovariance_population(b1, b2);
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	if (is_dbl_nil(covariance) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.covariancep", GDK_EXCEPTION);
	*res = covariance;
	return MAL_SUCCEED;
}

/*
 * BAT correlation
 */
static str
ALGcorr(dbl *res, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2;
	dbl covariance;

	if ((b1 = BATdescriptor(*bid1)) == NULL)
		throw(MAL, "aggr.corr", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(b1->batCacheid);
		throw(MAL, "aggr.corr", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	covariance = BATcalccorrelation(b1, b2);
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	if (is_dbl_nil(covariance) && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.corr", GDK_EXCEPTION);
	*res = covariance;
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func algebra_init_funcs[] = {
 command("algebra", "groupby", ALGgroupby, false, "Produces a new BAT with groups identified by the head column. The result contains tail times the head value, ie the tail contains the result group sizes.", args(1,3, batarg("",oid),batarg("gids",oid),batarg("cnts",lng))),
 command("algebra", "find", ALGfind, false, "Returns the index position of a value.  If no such BUN exists return OID-nil.", args(1,3, arg("",oid),batargany("b",1),argany("t",1))),
 command("algebra", "fetch", ALGfetchoid, false, "Returns the value of the BUN at x-th position with 0 <= x < b.count", args(1,3, argany("",1),batargany("b",1),arg("x",oid))),
 pattern("algebra", "project", ALGprojecttail, false, "Fill the tail with a constant", args(1,3, batargany("",3),batargany("b",1),argany("v",3))),
 command("algebra", "projection", ALGprojection, false, "Project left input onto right input.", args(1,3, batargany("",3),batarg("left",oid),batargany("right",3))),
 command("algebra", "projection", ALGprojection2, false, "Project left input onto right inputs which should be consecutive.", args(1,4, batargany("",3),batarg("left",oid),batargany("right1",3),batargany("right2",3))),
 command("algebra", "copy", ALGcopy, false, "Returns physical copy of a BAT.", args(1,2, batargany("",1),batargany("b",1))),
 command("algebra", "exist", ALGexist, false, "Returns whether 'val' occurs in b.", args(1,3, arg("",bit),batargany("b",1),argany("val",1))),
 command("algebra", "select", ALGselect1, false, "Select all head values for which the tail value is in range.\nInput is a dense-headed BAT, output is a dense-headed BAT with in\nthe tail the head value of the input BAT for which the tail value\nis between the values low and high (inclusive if li respectively\nhi is set).  The output BAT is sorted on the tail value.  If low\nor high is nil, the boundary is not considered (effectively - and\n+ infinity).  If anti is set, the result is the complement.  Nil\nvalues in the tail are never matched, unless low=nil, high=nil,\nli=1, hi=1, anti=0.  All non-nil values are returned if low=nil,\nhigh=nil, and li, hi are not both 1, or anti=1.\nNote that the output is suitable as second input for the other\nversion of this function.", args(1,7, batarg("",oid),batargany("b",1),argany("low",1),argany("high",1),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 command("algebra", "select", ALGselect2, false, "Select all head values of the first input BAT for which the tail value\nis in range and for which the head value occurs in the tail of the\nsecond input BAT.\nThe first input is a dense-headed BAT, the second input is a\ndense-headed BAT with sorted tail, output is a dense-headed BAT\nwith in the tail the head value of the input BAT for which the\ntail value is between the values low and high (inclusive if li\nrespectively hi is set).  The output BAT is sorted on the tail\nvalue.  If low or high is nil, the boundary is not considered\n(effectively - and + infinity).  If anti is set, the result is the\ncomplement.  Nil values in the tail are never matched, unless\nlow=nil, high=nil, li=1, hi=1, anti=0.  All non-nil values are\nreturned if low=nil, high=nil, and li, hi are not both 1, or anti=1.\nNote that the output is suitable as second input for this\nfunction.", args(1,8, batarg("",oid),batargany("b",1),batarg("s",oid),argany("low",1),argany("high",1),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 command("algebra", "select", ALGselect1nil, false, "With unknown set, each nil != nil", args(1,8, batarg("",oid),batargany("b",1),argany("low",1),argany("high",1),arg("li",bit),arg("hi",bit),arg("anti",bit),arg("unknown",bit))),
 command("algebra", "select", ALGselect2nil, false, "With unknown set, each nil != nil", args(1,9, batarg("",oid),batargany("b",1),batarg("s",oid),argany("low",1),argany("high",1),arg("li",bit),arg("hi",bit),arg("anti",bit),arg("unknown",bit))),
 command("algebra", "thetaselect", ALGthetaselect2, false, "Select all head values of the first input BAT for which the tail value\nobeys the relation value OP VAL and for which the head value occurs in\nthe tail of the second input BAT.\nInput is a dense-headed BAT, output is a dense-headed BAT with in\nthe tail the head value of the input BAT for which the\nrelationship holds.  The output BAT is sorted on the tail value.", args(1,5, batarg("",oid),batargany("b",1),batarg("s",oid),argany("val",1),arg("op",str))),
 command("algebra", "selectNotNil", ALGselectNotNil, false, "Select all not-nil values", args(1,2, batargany("",2),batargany("b",2))),
 command("algebra", "sort", ALGsort11, false, "Returns a copy of the BAT sorted on tail values.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(1,5, batargany("",1),batargany("b",1),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort12, false, "Returns a copy of the BAT sorted on tail values and a BAT that\nspecifies how the input was reordered.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(2,6, batargany("",1),batarg("",oid),batargany("b",1),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort13, false, "Returns a copy of the BAT sorted on tail values, a BAT that specifies\nhow the input was reordered, and a BAT with group information.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(3,7, batargany("",1),batarg("",oid),batarg("",oid),batargany("b",1),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort21, false, "Returns a copy of the BAT sorted on tail values.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(1,6, batargany("",1),batargany("b",1),batarg("o",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort22, false, "Returns a copy of the BAT sorted on tail values and a BAT that\nspecifies how the input was reordered.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(2,7, batargany("",1),batarg("",oid),batargany("b",1),batarg("o",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort23, false, "Returns a copy of the BAT sorted on tail values, a BAT that specifies\nhow the input was reordered, and a BAT with group information.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(3,8, batargany("",1),batarg("",oid),batarg("",oid),batargany("b",1),batarg("o",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort31, false, "Returns a copy of the BAT sorted on tail values.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(1,7, batargany("",1),batargany("b",1),batarg("o",oid),batarg("g",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort32, false, "Returns a copy of the BAT sorted on tail values and a BAT that\nspecifies how the input was reordered.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(2,8, batargany("",1),batarg("",oid),batargany("b",1),batarg("o",oid),batarg("g",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "sort", ALGsort33, false, "Returns a copy of the BAT sorted on tail values, a BAT that specifies\nhow the input was reordered, and a BAT with group information.\nThe order is descending if the reverse bit is set.\nThis is a stable sort if the stable bit is set.", args(3,9, batargany("",1),batarg("",oid),batarg("",oid),batargany("b",1),batarg("o",oid),batarg("g",oid),arg("reverse",bit),arg("nilslast",bit),arg("stable",bit))),
 command("algebra", "unique", ALGunique, false, "Select all unique values from the tail of the first input.\nInput is a dense-headed BAT, the second input is a\ndense-headed BAT with sorted tail, output is a dense-headed\nBAT with in the tail the head value of the input BAT that was\nselected.  The output BAT is sorted on the tail value.  The\nsecond input BAT is a list of candidates.", args(1,3, batarg("",oid),batargany("b",1),batarg("s",oid))),
 command("algebra", "crossproduct", ALGcrossproduct2, false, "Returns 2 columns with all BUNs, consisting of the head-oids\nfrom 'left' and 'right' for which there are BUNs in 'left'\nand 'right' with equal tails", args(2,5, batarg("l",oid),batarg("r",oid),batargany("left",1),batargany("right",2),arg("max_one",bit))),
 command("algebra", "crossproduct", ALGcrossproduct1, false, "Compute the cross product of both input bats; but only produce left output", args(1,4, batarg("",oid),batargany("left",1),batargany("right",2),arg("max_one",bit))),
 command("algebra", "crossproduct", ALGcrossproduct3, false, "Compute the cross product of both input bats", args(2,7, batarg("l",oid),batarg("r",oid),batargany("left",1),batargany("right",2),batarg("sl",oid),batarg("sr",oid),arg("max_one",bit))),
 command("algebra", "crossproduct", ALGcrossproduct4, false, "Compute the cross product of both input bats; but only produce left output", args(1,6, batarg("",oid),batargany("left",1),batargany("right",2),batarg("sl",oid),batarg("sr",oid),arg("max_one",bit))),
 command("algebra", "join", ALGjoin, false, "Join", args(2,8, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "join", ALGjoin1, false, "Join; only produce left output", args(1,7, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "leftjoin", ALGleftjoin, false, "Left join with candidate lists", args(2,8, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "leftjoin", ALGleftjoin1, false, "Left join with candidate lists; only produce left output", args(1,7, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "outerjoin", ALGouterjoin, false, "Left outer join with candidate lists", args(2,9, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("match_one",bit),arg("estimate",lng))),
 command("algebra", "outerjoin", ALGouterjoin1, false, "Left outer join with candidate lists; only produce left output", args(1,8,batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("match_one",bit),arg("estimate",lng))),
 command("algebra", "semijoin", ALGsemijoin, false, "Semi join with candidate lists", args(2,9, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("max_one",bit),arg("estimate",lng))),
 command("algebra", "thetajoin", ALGthetajoin, false, "Theta join with candidate lists", args(2,9, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("op",int),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "thetajoin", ALGthetajoin1, false, "Theta join with candidate lists; only produce left output", args(1,8, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("op",int),arg("nil_matches",bit),arg("estimate",lng))),
 command("algebra", "bandjoin", ALGbandjoin, false, "Band join: values in l and r match if r - c1 <[=] l <[=] r + c2", args(2,11, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),argany("c1",1),argany("c2",1),arg("li",bit),arg("hi",bit),arg("estimate",lng))),
 command("algebra", "bandjoin", ALGbandjoin1, false, "Band join: values in l and r match if r - c1 <[=] l <[=] r + c2; only produce left output", args(1,10, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),argany("c1",1),argany("c2",1),arg("li",bit),arg("hi",bit),arg("estimate",lng))),
 command("algebra", "rangejoin", ALGrangejoin, false, "Range join: values in l and r1/r2 match if r1 <[=] l <[=] r2", args(2,12, batarg("",oid),batarg("",oid),batargany("l",1),batargany("r1",1),batargany("r2",1),batarg("sl",oid),batarg("sr",oid),arg("li",bit),arg("hi",bit),arg("anti",bit),arg("symmetric",bit),arg("estimate",lng))),
 command("algebra", "rangejoin", ALGrangejoin1, false, "Range join: values in l and r1/r2 match if r1 <[=] l <[=] r2; only produce left output", args(1,11,batarg("",oid),batargany("l",1),batargany("r1",1),batargany("r2",1),batarg("sl",oid),batarg("sr",oid),arg("li",bit),arg("hi",bit),arg("anti",bit),arg("symmetric",bit),arg("estimate",lng))),
 command("algebra", "difference", ALGdifference, false, "Difference of l and r with candidate lists", args(1,8, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("nil_clears",bit),arg("estimate",lng))),
 command("algebra", "intersect", ALGintersect, false, "Intersection of l and r with candidate lists (i.e. half of semi-join)", args(1,8, batarg("",oid),batargany("l",1),batargany("r",1),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("max_one",bit),arg("estimate",lng))),
 pattern("algebra", "firstn", ALGfirstn, false, "Calculate first N values of B with candidate list S", args(1,8, batarg("",oid),batargany("b",0),batarg("s",oid),batarg("g",oid),arg("n",lng),arg("asc",bit),arg("nilslast",bit),arg("distinct",bit))),
 pattern("algebra", "firstn", ALGfirstn, false, "Calculate first N values of B with candidate list S", args(2,9, batarg("",oid),batarg("",oid),batargany("b",0),batarg("s",oid),batarg("g",oid),arg("n",lng),arg("asc",bit),arg("nilslast",bit),arg("distinct",bit))),
 command("algebra", "reuse", ALGreuse, false, "Reuse a temporary BAT if you can. Otherwise,\nallocate enough storage to accept result of an\noperation (not involving the heap)", args(1,2, batargany("",1),batargany("b",1))),
 command("algebra", "slice", ALGslice_oid, false, "Return the slice based on head oid x till y (exclusive).", args(1,4, batargany("",1),batargany("b",1),arg("x",oid),arg("y",oid))),
 command("algebra", "slice", ALGslice, false, "Return the slice with the BUNs at position x till y.", args(1,4, batargany("",1),batargany("b",1),arg("x",lng),arg("y",lng))),
 command("algebra", "slice", ALGslice_int, false, "Return the slice with the BUNs at position x till y.", args(1,4, batargany("",1),batargany("b",1),arg("x",int),arg("y",int))),
 command("algebra", "slice", ALGslice_lng, false, "Return the slice with the BUNs at position x till y.", args(1,4, batargany("",1),batargany("b",1),arg("x",lng),arg("y",lng))),
 command("algebra", "subslice", ALGsubslice_lng, false, "Return the oids of the slice with the BUNs at position x till y.", args(1,4, batarg("",oid),batargany("b",1),arg("x",lng),arg("y",lng))),
 command("aggr", "count", ALGcount_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,2, arg("",lng),batargany("b",0))),
 command("aggr", "count", ALGcount_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,3, arg("",lng),batargany("b",0),arg("ignore_nils",bit))),
 command("aggr", "count_no_nil", ALGcount_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,2, arg("",lng),batargany("b",2))),
 command("aggr", "count", ALGcountCND_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,3, arg("",lng),batargany("b",0),batarg("cnd",oid))),
 command("aggr", "count", ALGcountCND_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,4, arg("",lng),batargany("b",0),batarg("cnd",oid),arg("ignore_nils",bit))),
 command("aggr", "count_no_nil", ALGcountCND_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,3, arg("",lng),batargany("b",2),batarg("cnd",oid))),
 command("aggr", "cardinality", ALGcard, false, "Return the cardinality of the BAT tail values.", args(1,2, arg("",lng),batargany("b",2))),
 command("aggr", "min", ALGminany, false, "Return the lowest tail value or nil.", args(1,2, argany("",2),batargany("b",2))),
 command("aggr", "min", ALGminany_skipnil, false, "Return the lowest tail value or nil.", args(1,3, argany("",2),batargany("b",2),arg("skipnil",bit))),
 command("aggr", "max", ALGmaxany, false, "Return the highest tail value or nil.", args(1,2, argany("",2),batargany("b",2))),
 command("aggr", "max", ALGmaxany_skipnil, false, "Return the highest tail value or nil.", args(1,3, argany("",2),batargany("b",2),arg("skipnil",bit))),
 command("aggr", "stdev", ALGstdev, false, "Gives the standard deviation of all tail values", args(1,2, arg("",dbl),batargany("b",2))),
 command("aggr", "stdevp", ALGstdevp, false, "Gives the standard deviation of all tail values", args(1,2, arg("",dbl),batargany("b",2))),
 command("aggr", "variance", ALGvariance, false, "Gives the variance of all tail values", args(1,2, arg("",dbl),batargany("b",2))),
 command("aggr", "variancep", ALGvariancep, false, "Gives the variance of all tail values", args(1,2, arg("",dbl),batargany("b",2))),
 command("aggr", "covariance", ALGcovariance, false, "Gives the covariance of all tail values", args(1,3, arg("",dbl),batargany("b1",2),batargany("b2",2))),
 command("aggr", "covariancep", ALGcovariancep, false, "Gives the covariance of all tail values", args(1,3, arg("",dbl),batargany("b1",2),batargany("b2",2))),
 command("aggr", "corr", ALGcorr, false, "Gives the correlation of all tail values", args(1,3, arg("",dbl),batargany("b1",2),batargany("b2",2))),
 // sql
 command("aggr", "exist", ALGexist, false, "", args(1,3, arg("",bit),batargany("b",2),argany("h",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_algebra_mal)
{ mal_module("algebra", NULL, algebra_init_funcs); }
