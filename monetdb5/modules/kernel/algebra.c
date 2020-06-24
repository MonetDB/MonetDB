/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include <math.h>

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

str
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

str
ALGminany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGminany_skipnil(result, bid, &skipnil);
}

str
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

str
ALGmaxany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGmaxany_skipnil(result, bid, &skipnil);
}

str
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

str
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
	*result = BATcount(en);
	BBPunfix(en->batCacheid);
	return MAL_SUCCEED;
}

str
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

str
ALGselect2nil(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown)
{
	BAT *b, *s = NULL, *bn;
	const void *nilptr;
	bit nanti = *anti;

	if (!*unknown)
		return ALGselect2(result, bid, sid, low, high, li, hi, anti);

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
	/* here we don't need open ended parts with nil */
	nilptr = ATOMnilptr(b->ttype);
	if (*li == 1 && ATOMcmp(b->ttype, low, nilptr) == 0)
		low = high;
	else if (*hi == 1 && ATOMcmp(b->ttype, high, nilptr) == 0)
		high = low;
	if (ATOMcmp(b->ttype, low, high) == 0 && ATOMcmp(b->ttype, high, nilptr) == 0) /* ugh sql nil != nil */
		nanti = !nanti;
	bn = BATselect(b, s, low, high, *li, *hi, nanti);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.select", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
ALGselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	return ALGselect2(result, bid, NULL, low, high, li, hi, anti);
}

str
ALGselect1nil(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown)
{
	return ALGselect2nil(result, bid, NULL, low, high, li, hi, anti, unknown);
}

str
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

str
ALGthetaselect1(bat *result, const bat *bid, const void *val, const char **op)
{
	return ALGthetaselect2(result, bid, NULL, val, op);
}

str
ALGselectNotNil(bat *result, const bat *bid)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.selectNotNil", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if( BATcount_no_nil(b) != BATcount(b) ){
		BAT *s = NULL;

		s = BATselect(b, s, ATOMnilptr(b->ttype), NULL, true, true, true);
		if (s) {
			bn = BATproject(s, b);
			BBPunfix(s->batCacheid);
		}
		BBPunfix(b->batCacheid);
		if (bn) {
			*result = bn->batCacheid;
			BBPkeepref(*result);
			return MAL_SUCCEED;
		}
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

str
ALGjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
		   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.join");
}

str
ALGjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
		   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.join");
}

str
ALGleftjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATleftjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.leftjoin");
}

str
ALGleftjoin1(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATleftjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.leftjoin");
}

str
ALGouterjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   BATouterjoin, NULL, NULL, NULL, NULL, NULL, NULL, "algebra.outerjoin");
}

str
ALGsemijoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			const bit *nil_matches, const bit *max_one, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, max_one, estimate,
				   NULL, BATsemijoin, NULL, NULL, NULL, NULL, NULL, "algebra.semijoin");
}

str
ALGthetajoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const int *op, const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, *op, NULL, NULL,
				   false, false, false, false, nil_matches, NULL, NULL, estimate,
				   NULL, NULL, BATthetajoin, NULL, NULL, NULL, NULL, "algebra.thetajoin");
}

str
ALGbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const void *c1, const void *c2, const bit *li, const bit *hi,
			   const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, c1, c2,
				   *li, *hi, false, false, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, BATbandjoin, NULL, NULL, NULL, "algebra.bandjoin");
}

str
ALGrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const bit *anti, const bit *symmetric, const lng *estimate)
{
	return do_join(r1, r2, lid, rlid, rhid, slid, srid, 0, NULL, NULL,
				   *li, *hi, *anti, *symmetric, NULL, NULL, NULL, estimate,
				   NULL, NULL, NULL, NULL, BATrangejoin, NULL, NULL, "algebra.rangejoin");
}

str
ALGdifference(bat *r1, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			  const bit *nil_matches, const bit *not_in, const lng *estimate)
{
	return do_join(r1, NULL, lid, rid, NULL, slid, srid, 0, NULL, NULL,
				   false, false, false, false, nil_matches, not_in, NULL, estimate,
				   NULL, NULL, NULL, NULL, NULL, BATdiff, NULL, "algebra.difference");
}

str
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
str
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

	n = * getArgReference_lng(stk, pci, pci->argc - 4);
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
		if ((s = BATdescriptor(sid)) == NULL) {
			BBPunfix(bid);
			throw(MAL, "algebra.firstn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (pci->argc - pci->retc > 6) {
			gid = *getArgReference_bat(stk, pci, pci->retc + 2);
			if ((g = BATdescriptor(gid)) == NULL) {
				BBPunfix(bid);
				BBPunfix(sid);
				throw(MAL, "algebra.firstn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
		}
	}
	asc = * getArgReference_bit(stk, pci, pci->argc - 3);
	nilslast = * getArgReference_bit(stk, pci, pci->argc - 2);
	distinct = * getArgReference_bit(stk, pci, pci->argc - 1);
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

str
ALGcopy(bat *result, const bat *bid)
{
	return ALGunary(result, bid, BATwcopy, "algebra.copy");
}

str
ALGunique2(bat *result, const bat *bid, const bat *sid)
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

str
ALGunique1(bat *result, const bat *bid)
{
	return ALGunique2(result, bid, NULL);
}

str
ALGcrossproduct2(bat *l, bat *r, const bat *left, const bat *right, const bit *max_one)
{
	BAT *L, *R, *bn1, *bn2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.crossproduct", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	ret = BATsubcross(&bn1, &bn2, L, R, NULL, NULL,
					  max_one && !is_bit_nil(*max_one) && *max_one);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret != GDK_SUCCEED)
		throw(MAL, "algebra.crossproduct", GDK_EXCEPTION);
	BBPkeepref(*l = bn1->batCacheid);
	BBPkeepref(*r = bn2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGprojection(bat *result, const bat *lid, const bat *rid)
{
	BAT *left, *right,*bn= NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPunfix(left->batCacheid);
		throw(MAL, "algebra.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATproject(left, right);
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.projection", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
ALGprojection2(bat *result, const bat *lid, const bat *r1id, const bat *r2id)
{
	BAT *l, *r1, *r2 = NULL, *bn;

	if ((l = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.projection2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((r1 = BATdescriptor(*r1id)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(MAL, "algebra.projection2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (r2id && !is_bat_nil(*r2id) && (r2 = BATdescriptor(*r2id)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r1->batCacheid);
		throw(MAL, "algebra.projection2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATproject2(l, r1, r2);
	BBPunfix(l->batCacheid);
	BBPunfix(r1->batCacheid);
	if (r2)
		BBPunfix(r2->batCacheid);
	if (bn == NULL)
		throw(MAL, "algegra.projection2", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
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

str
ALGsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, order, group, reverse, nilslast, stable);
}

str
ALGsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, order, group, reverse, nilslast, stable);
}

str
ALGsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, ngroup, bid, order, NULL, reverse, nilslast, stable);
}

str
ALGsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, order, NULL, reverse, nilslast, stable);
}

str
ALGsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, order, NULL, reverse, nilslast, stable);
}

str
ALGsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, ngroup, bid, NULL, NULL, reverse, nilslast, stable);
}

str
ALGsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, norder, NULL, bid, NULL, NULL, reverse, nilslast, stable);
}

str
ALGsort11(bat *result, const bat *bid, const bit *reverse, const bit *nilslast, const bit *stable)
{
	return ALGsort33(result, NULL, NULL, bid, NULL, NULL, reverse, nilslast, stable);
}

str
ALGcount_bat(lng *result, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*result = (lng) BATcount(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils)
{
	BAT *b;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (*ignore_nils)
		cnt = BATcount_no_nil(b);
	else
		cnt = BATcount(b);
	*result = (lng) cnt;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_no_nil(lng *result, const bat *bid)
{
	bit ignore_nils = 1;

	return ALGcount_nil(result, bid, &ignore_nils);
}

str
ALGcountCND_bat(lng *result, const bat *bid, const bat *cnd)
{
	BAT *b;

	if ( *cnd) {
		if ((b = BATdescriptor(*cnd)) == NULL) {
			throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		*result = (lng) BATcount(b);
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*result = (lng) BATcount(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcountCND_nil(lng *result, const bat *bid, const bat *cnd, const bit *ignore_nils)
{
	BAT *b;
	BUN cnt;

	if (*ignore_nils){
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		cnt = BATcount_no_nil(b);
	} else{
		if ( *cnd) {
			if ((b = BATdescriptor(*cnd)) == NULL) {
				throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			*result = (lng) BATcount(b);
			BBPunfix(b->batCacheid);
			return MAL_SUCCEED;
		}
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		cnt = BATcount(b);
	}
	*result = (lng) cnt;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcountCND_no_nil(lng *result, const bat *bid, const bat *cnd)
{
	bit ignore_nils = 1;

	return ALGcountCND_nil(result, bid, cnd, &ignore_nils);
}

str
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

str
ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end)
{
	lng s = *start;
	lng e = (is_int_nil(*end) ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

str
ALGslice_lng(bat *ret, const bat *bid, const lng *start, const lng *end)
{
	lng s = *start;
	lng e = *end;

	return ALGslice(ret, bid, &s, &e);
}

/* carve out a slice based on the OIDs */
/* beware that BATs may have different OID bases */
str
ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end)
{
	lng s = (lng) (is_oid_nil(*start) ? 0 : (lng) *start);
	lng e = (is_oid_nil(*end) ? lng_nil : (lng) *end);

	return ALGslice(ret, bid, &s, &e) ;
}

str
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
	if ((*pos < (lng) 0) || (*pos >= (lng) BUNlast(b))) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.fetch", ILLEGAL_ARGUMENT " Idx out of range\n");
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

str
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

str
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


str
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


str ALGreuse(bat *ret, const bat *bid)
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
str
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

str
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
str
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

str
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
str
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

str
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
str
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
