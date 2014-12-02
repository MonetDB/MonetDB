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
#define derefStr(b, s, v)							\
		do {										\
			int _tpe= ATOMstorage((b)->s##type);	\
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

static int
CMDselect_(BAT **result, BAT *b, ptr low, ptr high, const bit *l_in, const bit *h_in)
{
	int tt = b->ttype;
	ptr nil = ATOMnilptr(tt);

	if (*l_in == bit_nil && ATOMcmp(tt, low, nil)) {
		GDKerror("CMDselect: flag 'l_in' must not be NIL, unless boundary 'low' is NIL\n");
		return GDK_FAIL;
	}
	if (*h_in == bit_nil && ATOMcmp(tt, high, nil)) {
		GDKerror("CMDselect: flag 'h_in' must not be NIL, unless boundary 'high' is NIL\n");
		return GDK_FAIL;
	}
	return (*result = BATselect_(b, low, high, *l_in, *h_in)) ? GDK_SUCCEED : GDK_FAIL;
}

static int
CMDuselect_(BAT **result, BAT *b, ptr low, ptr high, const bit *l_in, const bit *h_in)
{
	int tt = b->ttype;
	ptr nil = ATOMnilptr(tt);

	if (*l_in == bit_nil && ATOMcmp(tt, low, nil)) {
		GDKerror("CMDuselect: flag 'l_in' must not be NIL, unless boundary 'low' is NIL\n");
		return GDK_FAIL;
	}
	if (*h_in == bit_nil && ATOMcmp(tt, high, nil)) {
		GDKerror("CMDuselect: flag 'h_in' must not be NIL, unless boundary 'high' is NIL\n");
		return GDK_FAIL;
	}
	return (*result = BATuselect_(b, low, high, *l_in, *h_in)) ? GDK_SUCCEED : GDK_FAIL;
}

static int
CMDgen_group(BAT **result, BAT *gids, BAT *cnts )
{
	wrd j, gcnt = BATcount(gids);
	BAT *r = BATnew(TYPE_void, TYPE_oid, BATcount(gids)*2, TRANSIENT);

	if (r == NULL)
		return GDK_FAIL;
	BATseqbase(r, 0);
	if (gids->ttype == TYPE_void) {
		oid id = gids->hseqbase;
		wrd *cnt = (wrd*)Tloc(cnts, 0);
		for(j = 0; j < gcnt; j++) {
			wrd i, sz = cnt[j];
			for(i = 0; i < sz; i++) {
				if (BUNappend(r, &id, FALSE) == NULL) {
					BBPreclaim(r);
					return GDK_FAIL;
				}
			}
			id++;
		}
	} else {
		oid *id = (oid*)Tloc(gids, 0);
		wrd *cnt = (wrd*)Tloc(cnts, 0);
		for(j = 0; j < gcnt; j++) {
			wrd i, sz = cnt[j];
			for(i = 0; i < sz; i++) {
				if (BUNappend(r, id, FALSE) == NULL) {
					BBPreclaim(r);
					return GDK_FAIL;
				}
			}
			id++;
		}
	}
	r -> hdense = TRUE;
	r -> hsorted = TRUE;
	r -> hrevsorted = FALSE;
	r -> tsorted = BATtordered(gids);
	r -> trevsorted = BATtrevordered(gids);
	r -> T ->nonil = gids->T->nonil;
	*result = r;
	return GDK_SUCCEED;
}


/*
 * The string pattern matching routine has been added. It should be
 * dynamically linked.
 * A simple string matcher is included. It should be refined later on
 */
static inline int
like(const char *x, const char *y, BUN ylen)
{
	const char *r;

	if (x == (char *) NULL) {
		return 0;
	}
	for (r = x + strlen(x) - ylen; x <= r; x++) {
		int ok = 1;
		const char *s = x;
		const char *q;

		for (q = y; *q; q++, s++)
			if (*q != tolower(*s)) {
				ok = 0;
				break;
			}
		if (ok)
			return 1;
	}
	return 0;
}

static int
CMDlike(BAT **ret, BAT *b, const char *s)
{
	BATiter bi = bat_iterator(b);
	BAT *c = BATnew(BAThtype(b), TYPE_str, BATcount(b) / 10, TRANSIENT);
	str t, p;
	BUN u, v;
	BUN yy = 0;

	if (c == NULL)
		return GDK_FAIL;
	t = GDKstrdup(s);
	for (p = t; *p; p++, yy++)
		*p = tolower(*p);

	if (b->hvarsized) {
		BATloop(b, u, v)
			if (like(BUNtvar(bi, u), t, yy) &&
				BUNfastins(c, BUNhvar(bi, u), BUNtvar(bi, u)) == NULL) {
				BBPreclaim(c);
				GDKfree(t);
				return GDK_FAIL;
			}
	} else {
		BATloop(b, u, v)
			if (like(BUNtvar(bi, u), t, yy) &&
				BUNfastins(c, BUNhloc(bi, u), BUNtvar(bi, u)) == NULL) {
				BBPreclaim(c);
				GDKfree(t);
				return GDK_FAIL;
			}
	}
	c->hsorted = BAThordered(b);
	c->hrevsorted = BAThrevordered(b);
	c->tsorted = BATtordered(b);
	c->trevsorted = BATtrevordered(b);
	c->H->nonil = b->H->nonil;
	c->T->nonil = b->T->nonil;
	*ret = c;
	GDKfree(t);
	return GDK_SUCCEED;
}

static int
slice(BAT **retval, BAT *b, lng start, lng end)
{
	/* the internal BATslice requires exclusive end */
	if (start < 0) {
		GDKerror("CMDslice: start position of slice should >= 0\n");
		return GDK_FAIL;
	}
	if (end == lng_nil)
		end = BATcount(b);
	if (start > (lng) BUN_MAX || end >= (lng) BUN_MAX) {
		GDKerror("CMDslice: argument out of range\n");
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
ALGminany(ptr result, const bat *bid)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.min", RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.min",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMvarsized(b->ttype)) {
			* (ptr *) result = p = BATmin(b, NULL);
		} else {
			p = BATmin(b, result);
			assert(p == result);
		}
		if (p == NULL)
			msg = createException(MAL, "algebra.min", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
ALGmaxany(ptr result, const bat *bid)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.max", RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.max",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMvarsized(b->ttype)) {
			* (ptr *) result = p = BATmax(b, NULL);
		} else {
			p = BATmax(b, result);
			assert(p == result);
		}
		if (p == NULL)
			msg = createException(MAL, "algebra.max", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
ALGgroupby(bat *res, const bat *gids, const bat *cnts)
{
	BAT *bn, *g, *c;

	g = BATdescriptor(*gids);
	if (g == NULL) {
		throw(MAL, "algebra.groupby", RUNTIME_OBJECT_MISSING);
	}
	c = BATdescriptor(*cnts);
	if (c == NULL) {
		BBPreleaseref(g->batCacheid);
		throw(MAL, "algebra.groupby", RUNTIME_OBJECT_MISSING);
	}
	if( CMDgen_group(&bn, g, c) == GDK_FAIL){
		BBPreleaseref(g->batCacheid);
		BBPreleaseref(c->batCacheid);
		throw(MAL, "algebra.groupby",GDK_EXCEPTION);
	}
	if( bn){
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*res = bn->batCacheid;
		BBPkeepref(bn->batCacheid);
	}
	BBPreleaseref(g->batCacheid);
	BBPreleaseref(c->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcard(lng *result, const bat *bid)
{
	BAT *b, *en;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.card", RUNTIME_OBJECT_MISSING);
	}
	if ((en = BATsubunique(b, NULL)) == NULL) {
		throw(MAL, "algebra.card", GDK_EXCEPTION);
	}
	*result = BATcount(en);
	BBPunfix(en->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGsubselect2(bat *result, const bat *bid, const bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	BAT *b, *s = NULL, *bn;
	const void *nilptr;

	if ((*li != 0 && *li != 1) ||
		(*hi != 0 && *hi != 1) ||
		(*anti != 0 && *anti != 1)) {
		throw(MAL, "algebra.subselect", ILLEGAL_ARGUMENT);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.subselect", RUNTIME_OBJECT_MISSING);
	}
	if (sid && *sid != bat_nil && (s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	nilptr = ATOMnilptr(b->ttype);
	if (*li == 1 && *hi == 1 &&
		ATOMcmp(b->ttype, low, nilptr) == 0 &&
		ATOMcmp(b->ttype, high, nilptr) == 0) {
		/* special case: equi-select for NIL */
		high = NULL;
	}
	bn = BATsubselect(b, s, low, high, *li, *hi, *anti);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.subselect", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
ALGsubselect1(bat *result, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	return ALGsubselect2(result, bid, NULL, low, high, li, hi, anti);
}

str
ALGthetasubselect2(bat *result, const bat *bid, const bat *sid, const void *val, const char **op)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.thetasubselect", RUNTIME_OBJECT_MISSING);
	}
	if (sid && *sid != bat_nil && (s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.thetasubselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, val);
	bn = BATthetasubselect(b, s, val, *op);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.subselect", GDK_EXCEPTION);
	*result = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
ALGthetasubselect1(bat *result, const bat *bid, const void *val, const char **op)
{
	return ALGthetasubselect2(result, bid, NULL, val, op);
}

str
ALGselect1(bat *result, const bat *bid, ptr value)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, value);
	bn = BATselect(b, value, 0);
	BBPreleaseref(b->batCacheid);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.select", GDK_EXCEPTION);
}

str
ALGuselect1(bat *result, const bat *bid, ptr value)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.uselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, value);
	bn = BATuselect(b, value, NULL);
	BBPreleaseref(b->batCacheid);
	if (bn) {
		if (!(bn->batDirty&2))
			bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.uselect", GDK_EXCEPTION);
}

str
ALGselect(bat *result, const bat *bid, ptr low, ptr high)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	bn = BATselect(b, low, high);
	BBPreleaseref(b->batCacheid);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.select", GDK_EXCEPTION);
}

str
ALGselectNotNil(bat *result, const bat *bid)
{
	BAT *b, *bn = NULL;
	ptr low,high;
	bit bound=FALSE;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.selectNotNil", RUNTIME_OBJECT_MISSING);

	if( BATcount_no_nil(b) != BATcount(b) ){
		low=high= ATOMnilptr(b->ttype);
		CMDselect_(&bn, b, low, high, &bound, &bound);
		if (bn) {
			if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
			*result = bn->batCacheid;
			BBPkeepref(*result);
			BBPreleaseref(b->batCacheid);
			return MAL_SUCCEED;
		}
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.selectNotNil", GDK_EXCEPTION);
	}
	/* just pass on the result */
	*result = b->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
ALGuselect(bat *result, const bat *bid, ptr low, ptr high)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.uselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	bn = BATuselect(b, low, high);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.uselect", GDK_EXCEPTION);
}

str
ALGselectInclusive(bat *result, const bat *bid, ptr low, ptr high, const bit *lin, const bit *rin)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	CMDselect_(&bn, b, low, high, lin, rin);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.select", GDK_EXCEPTION);
}

str
ALGuselectInclusive(bat *result, const bat *bid, ptr low, ptr high, const bit *lin, const bit *rin)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.uselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	CMDuselect_(&bn, b, low, high, lin, rin);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.uselect", GDK_EXCEPTION);
}

str
ALGthetajoinEstimate(bat *result, const bat *lid, const bat *rid, const int *opc, const lng *estimate)
{
	BAT *left, *right, *bn = NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.thetajoin", RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, "algebra.thetajoin", RUNTIME_OBJECT_MISSING);
	}
	if( *opc == -3 ){
		/* The NE case is not supported in the kernel */
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(right->batCacheid);
		throw(MAL, "algebra.thetajoin", ILLEGAL_ARGUMENT " Theta comparison <> not yet supported");
	}
	bn = BATthetajoin(left, right, *opc, *estimate == lng_nil || *estimate < 0 ? BUN_NONE : (*estimate >= (lng) BUN_MAX ? BUN_MAX : (BUN) *estimate));
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(right->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	throw(MAL, "algebra.thetajoin", GDK_EXCEPTION);
}

str
ALGthetajoin(bat *result, const bat *lid, const bat *rid, const int *opc)
{
	return ALGthetajoinEstimate(result, lid, rid, opc, (ptr)&lng_nil);
}

str
ALGbandjoin(bat *result, const bat *lid, const bat *rid, const void *minus, const void *plus, const bit *li, const bit *hi)
{
	BAT *left, *right, *bn = NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}
	bn = BATbandjoin(left, right, minus, plus, *li, *hi);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(right->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	throw(MAL, "algebra.bandjoin", GDK_EXCEPTION);
}

str
ALGbandjoin_default(bat *result, const bat *lid, const bat *rid, const void *minus, const void *plus)
{
	bit li = TRUE;
	bit hi = TRUE;
	return ALGbandjoin(result, lid, rid, minus, plus, &li, &hi);
}

str
ALGrangejoin(bat *result, const bat *lid, const bat *rlid, const bat *rhid, const bit *li, const bit *hi)
{
	BAT *left, *rightl, *righth, *bn = NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.rangejoin", RUNTIME_OBJECT_MISSING);
	}
	if ((rightl = BATdescriptor(*rlid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, "algebra.rangejoin", RUNTIME_OBJECT_MISSING);
	}
	if ((righth = BATdescriptor(*rhid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(rightl->batCacheid);
		throw(MAL, "algebra.rangejoin", RUNTIME_OBJECT_MISSING);
	}
	bn = BATrangejoin(left, rightl, righth, *li, *hi);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(rightl->batCacheid);
		BBPreleaseref(righth->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(rightl->batCacheid);
	BBPreleaseref(righth->batCacheid);
	throw(MAL, "algebra.rangejoin", GDK_EXCEPTION);
}

static str
do_join(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *r2id, const bat *slid, const bat *srid,
		int op, const void *c1, const void *c2, int li, int hi,
		const bit *nil_matches, const lng *estimate,
		gdk_return (*joinfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
							   int, BUN),
		gdk_return (*thetafunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
								int, int, BUN),
		gdk_return (*bandfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
							   const void *, const void *, int, int, BUN),
		gdk_return (*rangefunc)(BAT **, BAT **, BAT *, BAT *, BAT *,
								BAT *, BAT *, int, int, BUN),
		const char *funcname)
{
	BAT *left = NULL, *right = NULL, *right2 = NULL;
	BAT *candleft = NULL, *candright = NULL;
	BAT *result1, *result2;
	BUN est;

	assert(r2id == NULL || rangefunc != NULL);

	if ((left = BATdescriptor(*lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(*rid)) == NULL)
		goto fail;
	if (slid && *slid != bat_nil && (candleft = BATdescriptor(*slid)) == NULL)
		goto fail;
	if (srid && *srid != bat_nil && (candright = BATdescriptor(*srid)) == NULL)
		goto fail;
	if (estimate == NULL || *estimate < 0 || *estimate == lng_nil || *estimate > (lng) BUN_MAX)
		est = BUN_NONE;
	else
		est = (BUN) *estimate;

	if (thetafunc) {
		assert(joinfunc == NULL);
		assert(bandfunc == NULL);
		assert(rangefunc == NULL);
		if ((*thetafunc)(&result1, &result2, left, right, candleft, candright, op, *nil_matches, est) == GDK_FAIL)
			goto fail;
	} else if (joinfunc) {
		assert(bandfunc == NULL);
		assert(rangefunc == NULL);
		if ((*joinfunc)(&result1, &result2, left, right, candleft, candright, *nil_matches, est) == GDK_FAIL)
			goto fail;
	} else if (bandfunc) {
		assert(rangefunc == NULL);
		if ((*bandfunc)(&result1, &result2, left, right, candleft, candright, c1, c2, li, hi, est) == GDK_FAIL)
			goto fail;
	} else {
		if ((right2 = BATdescriptor(*r2id)) == NULL)
			goto fail;
		if ((*rangefunc)(&result1, &result2, left, right, right2, candleft, candright, li, hi, est) == GDK_FAIL)
			goto fail;
		BBPreleaseref(right2->batCacheid);
	}
	*r1 = result1->batCacheid;
	*r2 = result2->batCacheid;
	BBPkeepref(*r1);
	BBPkeepref(*r2);
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	if (candleft)
		BBPreleaseref(candleft->batCacheid);
	if (candright)
		BBPreleaseref(candright->batCacheid);
	return MAL_SUCCEED;

  fail:
	if (left)
		BBPreclaim(left);
	if (right)
		BBPreclaim(right);
	if (right2)
		BBPreclaim(right2);
	if (candleft)
		BBPreclaim(candleft);
	if (candright)
		BBPreclaim(candright);
	throw(MAL, funcname, RUNTIME_OBJECT_MISSING);
}

str
ALGsubjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
		   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL, 0, 0,
				   nil_matches, estimate,
				   BATsubjoin, NULL, NULL, NULL, "algebra.subjoin");
}

str
ALGsubleftjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL, 0, 0,
				   nil_matches, estimate,
				   BATsubleftjoin, NULL, NULL, NULL, "algebra.subleftjoin");
}

str
ALGsubouterjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, NULL, NULL, 0, 0,
				   nil_matches, estimate,
				   BATsubouterjoin, NULL, NULL, NULL, "algebra.subouterjoin");
}

str
ALGsubthetajoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
				const int *op, const bit *nil_matches, const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, *op, NULL, NULL, 0, 0,
				   nil_matches, estimate,
				   NULL, BATsubthetajoin, NULL, NULL, "algebra.subthetajoin");
}

str
ALGsubbandjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid,
			   const void *c1, const void *c2, const bit *li, const bit *hi,
			   const lng *estimate)
{
	return do_join(r1, r2, lid, rid, NULL, slid, srid, 0, c1, c2, *li, *hi,
				   NULL, estimate,
				   NULL, NULL, BATsubbandjoin, NULL, "algebra.subbandjoin");
}

str
ALGsubrangejoin(bat *r1, bat *r2, const bat *lid, const bat *rlid, const bat *rhid, const bat *slid, const bat *srid, const bit *li, const bit *hi, const lng *estimate)
{
	return do_join(r1, r2, lid, rlid, rhid, slid, srid, 0, NULL, NULL, *li, *hi,
				   NULL, estimate,
				   NULL, NULL, NULL, BATsubrangejoin, "algebra.subrangejoin");
}

/* algebra.firstn(b:bat[:oid,:any],
 *                [ s:bat[:oid,:oid],
 *                [ g:bat[:oid,:oid], ] ]
 *                n:wrd,
 *                asc:bit,
 *                distinct:bit)
 * returns :bat[:oid,:oid] [ , :bat[:oid,:oid] ]
 */
str
ALGfirstn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret1, *ret2 = NULL;
	bat bid, sid, gid;
	BAT *b, *s = NULL, *g = NULL;
	BAT *bn, *gn;
	wrd n;
	bit asc, distinct;
	gdk_return rc;

	(void) cntxt;
	(void) mb;

	assert(pci->retc == 1 || pci->retc == 2);
	assert(pci->argc - pci->retc >= 4 && pci->argc - pci->retc <= 6);

	n = * getArgReference_wrd(stk, pci, pci->argc - 3);
	if (n < 0 || (lng) n >= (lng) BUN_MAX)
		throw(MAL, "algebra.firstn", ILLEGAL_ARGUMENT);
	ret1 = getArgReference_bat(stk, pci, 0);
	if (pci->retc == 2)
		ret2 = getArgReference_bat(stk, pci, 1);
	bid = *getArgReference_bat(stk, pci, pci->retc);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.firstn", RUNTIME_OBJECT_MISSING);
	if (pci->argc - pci->retc > 4) {
		sid = *getArgReference_bat(stk, pci, pci->retc + 1);
		if ((s = BATdescriptor(sid)) == NULL) {
			BBPreleaseref(bid);
			throw(MAL, "algebra.firstn", RUNTIME_OBJECT_MISSING);
		}
		if (pci->argc - pci->retc > 5) {
			gid = *getArgReference_bat(stk, pci, pci->retc + 2);
			if ((g = BATdescriptor(gid)) == NULL) {
				BBPreleaseref(bid);
				BBPreleaseref(sid);
				throw(MAL, "algebra.firstn", RUNTIME_OBJECT_MISSING);
			}
		}
	}
	asc = * getArgReference_bit(stk, pci, pci->argc - 2);
	distinct = * getArgReference_bit(stk, pci, pci->argc - 1);
	rc = BATfirstn(&bn, ret2 ? &gn : NULL, b, s, g, (BUN) n, asc, distinct);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (g)
		BBPreleaseref(g->batCacheid);
	if (rc == GDK_FAIL)
		throw(MAL, "algebra.firstn", MAL_MALLOC_FAIL);
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
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	bn = (*func)(b);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, name, GDK_EXCEPTION);
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static str
ALGbinary(bat *result, const bat *lid, const bat *rid, BAT *(*func)(BAT *, BAT *), const char *name)
{
	BAT *left, *right,*bn= NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	bn = (*func)(left, right);
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	if (bn == NULL)
		throw(MAL, name, GDK_EXCEPTION);
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static str
ALGbinaryint(bat *result, const bat *bid, const int *param, BAT *(*func)(BAT *, BUN), const char *name)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	bn = (*func)(b, *param);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, name, GDK_EXCEPTION);
	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static str
ALGbinaryestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate,
				  BAT *(*func)(BAT *, BAT *, BUN), const char *name)
{
	BAT *left, *right, *bn = NULL;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, name, RUNTIME_OBJECT_MISSING);
	}
	bn = (*func)(left, right, estimate ? (BUN) *estimate : BUN_NONE);
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	if (bn == NULL)
		throw(MAL, name, GDK_EXCEPTION);
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

static BAT *
BATwcopy(BAT *b)
{
	return BATcopy(b, b->htype, b->ttype, 1, TRANSIENT);
}

str
ALGcopy(bat *result, const bat *bid)
{
	return ALGunary(result, bid, BATwcopy, "algebra.copy");
}

str
ALGsubunique2(bat *result, const bat *bid, const bat *sid)
{
	BAT *b, *s = NULL, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.subunique", RUNTIME_OBJECT_MISSING);
	}
	if (sid && *sid != bat_nil && (s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subunique", RUNTIME_OBJECT_MISSING);
	}
	bn = BATsubunique(b, s);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "algebra.subunique", GDK_EXCEPTION);
	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);
	*result = bn->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
ALGsubunique1(bat *result, const bat *bid)
{
	return ALGsubunique2(result, bid, NULL);
}

str
ALGantijoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinary(result, lid, rid, BATantijoin, "algebra.antijoin");
}

str
ALGantijoin2( bat *l, bat *r, const bat *left, const bat *right)
{
	BAT *L, *R, *j1, *j2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.antijoin", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.antijoin", RUNTIME_OBJECT_MISSING);
	}

	ret = BATsubthetajoin(&j1, &j2, L, R, NULL, NULL, JOIN_NE, 0, BUN_NONE);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "algebra.antijoin", GDK_EXCEPTION);
	BBPkeepref(*l = j1->batCacheid);
	BBPkeepref(*r = j2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGjoin2( bat *l, bat *r, const bat *left, const bat *right)
{
	BAT *L, *R, *j1, *j2, *lmap, *rmap;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.join", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.join", RUNTIME_OBJECT_MISSING);
	}

	if (!BAThdense(L) || !BAThdense(R)) {
		lmap = BATmirror(BATmark(L, 0));
		j1 = BATmirror(BATmark(BATmirror(L), 0));
		BBPunfix(L->batCacheid);
		L = j1;
		rmap = BATmirror(BATmark(R, 0));
		j2 = BATmirror(BATmark(BATmirror(R), 0));
		BBPunfix(R->batCacheid);
		R = j2;
	} else {
		lmap = NULL;
		rmap = NULL;
	}
	ret = BATsubjoin(&j1, &j2, L, R, NULL, NULL, 0, BUN_NONE);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret == GDK_FAIL) {
		if (lmap)
			BBPunfix(lmap->batCacheid);
		if (rmap)
			BBPunfix(rmap->batCacheid);
		throw(MAL, "algebra.join", GDK_EXCEPTION);
	}
	if (lmap) {
		L = BATproject(j1, lmap);
		BBPunfix(j1->batCacheid);
		BBPunfix(lmap->batCacheid);
		j1 = L;
		lmap = NULL;
		R = BATproject(j2, rmap);
		BBPunfix(j2->batCacheid);
		BBPunfix(rmap->batCacheid);
		j2 = R;
		rmap = NULL;
	}
	BBPkeepref(*l = j1->batCacheid);
	BBPkeepref(*r = j2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGthetajoin2( bat *l, bat *r, const bat *left, const bat *right, const int *opc)
{
	BAT *L, *R, *j1, *j2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.thetajoin", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.thetajoin", RUNTIME_OBJECT_MISSING);
	}

	ret = BATsubthetajoin(&j1, &j2, L, R, NULL, NULL, *opc, 0, BUN_NONE);

	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "algebra.thetajoin", GDK_EXCEPTION);
	BBPkeepref(*l = j1->batCacheid);
	BBPkeepref(*r = j2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcrossproduct2( bat *l, bat *r, const bat *left, const bat *right)
{
	BAT *L, *R, *bn1, *bn2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.crossproduct", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.crossproduct", RUNTIME_OBJECT_MISSING);
	}
	ret = BATcross1(&bn1, &bn2, L, R);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "algebra.crossproduct", GDK_EXCEPTION);
	BBPkeepref(*l = bn1->batCacheid);
	BBPkeepref(*r = bn2->batCacheid);
	return MAL_SUCCEED;
}
str
ALGbandjoin2(bat *l, bat *r, const bat *left, const bat *right, const void *minus, const void *plus, const bit *li, const bit *hi)
{
	BAT *L, *R, *bn1, *bn2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}

	ret = BATsubbandjoin(&bn1, &bn2, L, R, NULL, NULL, minus, plus, *li, *hi, BUN_NONE);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "algebra.bandjoin", GDK_EXCEPTION);
	BBPkeepref(*l = bn1->batCacheid);
	BBPkeepref(*r = bn2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGrangejoin2(bat *l, bat *r, const bat *left, const bat *rightl, const bat *righth, const bit *li, const bit *hi)
{
	BAT *L, *RL, *RH, *bn1, *bn2;
	gdk_return ret;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.join", RUNTIME_OBJECT_MISSING);
	}
	if ((RL = BATdescriptor(*rightl)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.join", RUNTIME_OBJECT_MISSING);
	}
	if ((RH = BATdescriptor(*righth)) == NULL) {
		BBPunfix(L->batCacheid);
		BBPunfix(RL->batCacheid);
		throw(MAL, "algebra.join", RUNTIME_OBJECT_MISSING);
	}

	ret = BATsubrangejoin(&bn1, &bn2, L, RL, RH, NULL, NULL, *li, *hi, BUN_NONE);
	BBPunfix(L->batCacheid);
	BBPunfix(RL->batCacheid);
	BBPunfix(RH->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "algebra.rangejoin", GDK_EXCEPTION);
	BBPkeepref(*l = bn1->batCacheid);
	BBPkeepref(*r = bn2->batCacheid);
	return MAL_SUCCEED;
}

str
ALGjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATjoin, "algebra.join");
}

str
ALGjoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATjoin, "algebra.join");
}

str
ALGleftjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATleftjoin, "algebra.leftjoin");
}

str
ALGleftjoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATleftjoin, "algebra.leftjoin");
}

str
ALGleftfetchjoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinary(result, lid, rid, BATproject, "algebra.leftfetchjoin");
}

str
ALGouterjoinestimate(bat *result, const bat *lid, const bat *rid, const lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATouterjoin, "algebra.outerjoin");
}

str
ALGouterjoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATouterjoin, "algebra.outerjoin");
}

str
ALGsemijoin(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinary(result, lid, rid, BATsemijoin, "algebra.semijoin");
}

str
ALGkunion(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinary(result, lid, rid, BATkunion, "algebra.kunion");
}

str
ALGkdiff(bat *result, const bat *lid, const bat *rid)
{
	return ALGbinary(result, lid, rid, BATkdiff, "algebra.kdiff");
}

str
ALGsample(bat *result, const bat *bid, const int *param)
{
	return ALGbinaryint(result, bid, param, BATsample, "algebra.sample");
}

/* add items missing in the kernel */
str
ALGtunion(bat *result, const bat *bid, const bat *bid2)
{
	BAT *b, *b2, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.tunion", RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL){
		BBPreleaseref(*bid2);
		throw(MAL, "algebra.tunion", RUNTIME_OBJECT_MISSING);
	}

	bn = BATkunion(BATmirror(b),BATmirror(b2));
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(b2->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(b2->batCacheid);
	throw(MAL, "algebra.tunion", GDK_EXCEPTION);
}

str
ALGtdifference(bat *result, const bat *bid, const bat *bid2)
{
	BAT *b, *b2, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.tdifference", RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL){
		BBPreleaseref(*bid2);
		throw(MAL, "algebra.tdifference", RUNTIME_OBJECT_MISSING);
	}

	bn = BATkdiff(BATmirror(b),BATmirror(b2));
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(b2->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(b2->batCacheid);
	throw(MAL, "algebra.tdifference", GDK_EXCEPTION);
}

str
ALGtdiff(bat *result, const bat *bid, const bat *bid2)
{
	BAT *b, *b2, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.tdiff", RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL){
		BBPreleaseref(*bid2);
		throw(MAL, "algebra.tdiff", RUNTIME_OBJECT_MISSING);
	}

	bn = BATkdiff(BATmirror(b),BATmirror(b2));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn) {
		BAT *r = BATmirror(BATmark(bn,0));

		BBPreleaseref(bn->batCacheid);
		bn = r;
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.tdiff", GDK_EXCEPTION);
}

str
ALGtintersect(bat *result, const bat *bid, const bat *bid2)
{
	BAT *b, *b2, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.tintersect", RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL){
		BBPreleaseref(*bid2);
		throw(MAL, "algebra.tintersect", RUNTIME_OBJECT_MISSING);
	}

	bn = BATsemijoin(BATmirror(b),BATmirror(b2));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.tintersect", GDK_EXCEPTION);
}

str
ALGtinter(bat *result, const bat *bid, const bat *bid2)
{
	BAT *b, *b2, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.tinter", RUNTIME_OBJECT_MISSING);
	if ((b2 = BATdescriptor(*bid2)) == NULL){
		BBPreleaseref(*bid2);
		throw(MAL, "algebra.tinter", RUNTIME_OBJECT_MISSING);
	}

	bn = BATsemijoin(BATmirror(b),BATmirror(b2));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn) {
		BAT *r = BATmirror(BATmark(bn,0));

		BBPreleaseref(bn->batCacheid);
		bn = r;
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.tinter", GDK_EXCEPTION);
}

str
ALGtsort(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.tsort", RUNTIME_OBJECT_MISSING);
	}
	bn = BATsort(BATmirror(b));
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.tsort", GDK_EXCEPTION);
}

str
ALGtsort_rev(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.tsort", RUNTIME_OBJECT_MISSING);
	}
	bn = BATsort_rev(BATmirror(b));
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.tsort", GDK_EXCEPTION);
}

str
ALGhsort(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.hsort", RUNTIME_OBJECT_MISSING);
	}
	bn = BATsort(b);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.hsort", GDK_EXCEPTION);
}

str
ALGhsort_rev(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.tsort", RUNTIME_OBJECT_MISSING);
	}
	bn = BATsort_rev(b);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.tsort", GDK_EXCEPTION);
}
str
ALGhtsort(bat *result, const bat *lid)
{
	BAT *b, *bm = NULL, *bn = NULL;

	if ((b = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.htsort", RUNTIME_OBJECT_MISSING);
	}
	bm = BATmirror(BATsort(BATmirror(b)));
	if (bm) {
		bn = BATssort(bm);
		if (bn) {
			if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
			*result = bn->batCacheid;
			BBPkeepref(*result);
			BBPunfix(bm->batCacheid);
			BBPreleaseref(b->batCacheid);
			return MAL_SUCCEED;
		}
		BBPunfix(bm->batCacheid);
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.htsort", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.htsort", GDK_EXCEPTION);
}

str
ALGthsort(bat *result, const bat *lid)
{
	BAT *b, *bm = NULL, *bn = NULL;

	if ((b = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.thsort", RUNTIME_OBJECT_MISSING);
	}
	bm = BATmirror(BATsort(b));
	if (bm) {
		bn = BATssort(bm);
		if (bn) {
			bn = BATmirror(bn);
			if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
			*result = bn->batCacheid;
			BBPkeepref(*result);
			BBPunfix(bm->batCacheid);
			BBPreleaseref(b->batCacheid);
			return MAL_SUCCEED;
		}
		BBPunfix(bm->batCacheid);
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.thsort", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.thsort", GDK_EXCEPTION);
}

str
ALGssort(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.ssort", RUNTIME_OBJECT_MISSING);
	}
	bn = BATssort(b);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.ssort", GDK_EXCEPTION);
}

str
ALGssort_rev(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.ssort_rev", RUNTIME_OBJECT_MISSING);
	}
	bn = BATssort_rev(b);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.ssort_rev", GDK_EXCEPTION);
}

str
ALGsubsort33(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable)
{
	BAT *bn = NULL, *on = NULL, *gn = NULL;
	BAT *b = NULL, *o = NULL, *g = NULL;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.subsort", RUNTIME_OBJECT_MISSING);
	if (order && *order != bat_nil && (o = BATdescriptor(*order)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subsort", RUNTIME_OBJECT_MISSING);
	}
	if (group && *group != bat_nil && (g = BATdescriptor(*group)) == NULL) {
		if (o)
			BBPreleaseref(o->batCacheid);
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subsort", RUNTIME_OBJECT_MISSING);
	}
	if (BATsubsort(result ? &bn : NULL,
				   norder ? &on : NULL,
				   ngroup ? &gn : NULL,
				   b, o, g, *reverse, *stable) == GDK_FAIL) {
		if (o)
			BBPreleaseref(o->batCacheid);
		if (g)
			BBPreleaseref(g->batCacheid);
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subsort", OPERATION_FAILED);
	}
	BBPreleaseref(b->batCacheid);
	if (o)
		BBPreleaseref(o->batCacheid);
	if (g)
		BBPreleaseref(g->batCacheid);
	if (result)
		BBPkeepref(*result = bn->batCacheid);
	if (norder)
		BBPkeepref(*norder = on->batCacheid);
	if (ngroup)
		BBPkeepref(*ngroup = gn->batCacheid);
	return MAL_SUCCEED;
}

str
ALGsubsort32(bat *result, bat *norder, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, order, group, reverse, stable);
}

str
ALGsubsort31(bat *result, const bat *bid, const bat *order, const bat *group, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, order, group, reverse, stable);
}

str
ALGsubsort23(bat *result, bat *norder, bat *ngroup, const bat *bid, const bat *order, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, norder, ngroup, bid, order, NULL, reverse, stable);
}

str
ALGsubsort22(bat *result, bat *norder, const bat *bid, const bat *order, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, order, NULL, reverse, stable);
}

str
ALGsubsort21(bat *result, const bat *bid, const bat *order, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, order, NULL, reverse, stable);
}

str
ALGsubsort13(bat *result, bat *norder, bat *ngroup, const bat *bid, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, norder, ngroup, bid, NULL, NULL, reverse, stable);
}

str
ALGsubsort12(bat *result, bat *norder, const bat *bid, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, NULL, NULL, reverse, stable);
}

str
ALGsubsort11(bat *result, const bat *bid, const bit *reverse, const bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, NULL, NULL, reverse, stable);
}

str
ALGrevert(bat *result, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.revert", RUNTIME_OBJECT_MISSING);
	}
	bn = BATcopy(b, b->htype, b->ttype, TRUE, TRANSIENT);
	BATrevert(bn);
	*result= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_bat(wrd *result, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", RUNTIME_OBJECT_MISSING);
	}
	*result = (wrd) BATcount(b);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_nil(wrd *result, const bat *bid, const bit *ignore_nils)
{
	BAT *b;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", RUNTIME_OBJECT_MISSING);
	}
	if (*ignore_nils)
		cnt = BATcount_no_nil(b);
	else
		cnt = BATcount(b);
	*result = (wrd) cnt;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_no_nil(wrd *result, const bat *bid)
{
	bit ignore_nils = 1;

	return ALGcount_nil(result, bid, &ignore_nils);
}

str
ALGtmark(bat *result, const bat *bid, const oid *base)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.mark", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmark(b, *base);
	if (bn != NULL) {
		BBPreleaseref(b->batCacheid);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.mark", GDK_EXCEPTION);
}

str
ALGtmark_default(bat *result, const bat *bid)
{
	oid o = 0;

	return ALGtmark(result, bid, &o);
}

str
ALGtmarkp(bat *result, const bat *bid, const int *nr_parts, const int *part_nr)
{
#if SIZEOF_OID == 4
	int bits = 31;
#else
	int bits = 63;
#endif
	oid base = 0;

	assert(*part_nr < *nr_parts);
	base = ((oid)1)<<bits;
	base /= *nr_parts;
	base *= *part_nr;
	return ALGtmark(result, bid, &base);
}

str
ALGmark_grp_1(bat *result, const bat *bid, const bat *gid)
{
	BAT *g, *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.mark_grp", RUNTIME_OBJECT_MISSING);
	}
	if ((g = BATdescriptor(*gid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.mark_grp", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmark_grp(b, g, NULL);
	if (bn != NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(g->batCacheid);
	throw(MAL, "algebra.mark_grp", GDK_EXCEPTION);
}

str
ALGmark_grp_2(bat *result, const bat *bid, const bat *gid, const oid *base)
{
	BAT *g, *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.mark_grp", RUNTIME_OBJECT_MISSING);
	}
	if ((g = BATdescriptor(*gid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.mark_grp", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmark_grp(b, g, base);
	if (bn != NULL) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(g->batCacheid);
	throw(MAL, "algebra.mark_grp", GDK_EXCEPTION);
}

str
ALGlike(bat *ret, const bat *bid, const str *k)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.like", RUNTIME_OBJECT_MISSING);
	}
	CMDlike(&bn, b, *k);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*ret = bn->batCacheid;
		BBPkeepref(*ret);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.like", GDK_EXCEPTION);
}

str
ALGslice(bat *ret, const bat *bid, const lng *start, const lng *end)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.slice", RUNTIME_OBJECT_MISSING);
	}
	slice(&bn, b, *start, *end);
	if (bn != NULL) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*ret = bn->batCacheid;
		BBPkeepref(*ret);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.slice", GDK_EXCEPTION);
}

str
ALGslice_int(bat *ret, const bat *bid, const int *start, const int *end)
{
	lng s = *start;
	lng e = (*end == int_nil ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

str
ALGslice_wrd(bat *ret, const bat *bid, const wrd *start, const wrd *end)
{
	lng s = *start;
	lng e = (*end == wrd_nil ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

/* carve out a slice based on the OIDs */
/* beware that BATs may have different OID bases */
str
ALGslice_oid(bat *ret, const bat *bid, const oid *start, const oid *end)
{
	BAT *b, *bv;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.slice", RUNTIME_OBJECT_MISSING);

	bv  = BATmirror( b);
	if ( bv == NULL)
		throw(MAL, "algebra.slice", MAL_MALLOC_FAIL);
	bv  = BATselect_( bv, (ptr) start, (ptr) end, TRUE, FALSE);
	if ( bv == NULL)
		throw(MAL, "algebra.slice", MAL_MALLOC_FAIL);
	bv  = BATmirror( bv);
	if ( bv == NULL)
		throw(MAL, "algebra.slice", MAL_MALLOC_FAIL);

	*ret = bv->batCacheid;
	BBPkeepref(*ret);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGsubslice_wrd(bat *ret, const bat *bid, const wrd *start, const wrd *end)
{
	lng s = *start;
	lng e = (*end == wrd_nil ? lng_nil : *end);
	bat slc;
	str msg;

	if ((msg = ALGslice(&slc, bid, &s, &e)) == MAL_SUCCEED) {
		if ((msg = ALGtmark_default(ret, &slc)) == MAL_SUCCEED) {
			BBPdecref(slc, TRUE);
			*ret = -*ret; /* ugly reverse */
			return MAL_SUCCEED;
		}
	}
	return msg;
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
		int _len = ATOMlen(b->ttype, _src);
		ptr _dst = GDKmalloc(_len);
		memcpy(_dst, _src, _len);
		*(ptr*) ret = _dst;
	} else {
		int _s = ATOMsize(ATOMtype(b->ttype));
		if (ATOMvarsized(b->ttype)) {
			memcpy(*(ptr*) ret=GDKmalloc(_s), BUNtvar(bi, pos), _s);
		} else if (b->ttype == TYPE_void) {
			*(oid*) ret = b->tseqbase;
			if (b->tseqbase != oid_nil)
				*(oid*)ret += pos - BUNfirst(b);
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
		throw(MAL, "algebra.fetch", RUNTIME_OBJECT_MISSING);
	}
	if ((*pos < (lng) BUNfirst(b)) || (*pos >= (lng) BUNlast(b)))
		throw(MAL, "algebra.fetch", ILLEGAL_ARGUMENT " Idx out of range\n");
	msg = doALGfetch(ret, b, (BUN) *pos);
	BBPreleaseref(b->batCacheid);
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
		throw(MAL, "algebra.exist", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	q = BUNfnd(b, val);
	*ret = (q != BUN_NONE);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGfind(ptr ret, const bat *bid, ptr val)
{
	BAT *b;
	BUN q;
	str msg;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.find", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	q = BUNfnd(BATmirror(b), val);

	if (q == BUN_NONE){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.find", GDK_EXCEPTION "can not find element");
	}
	msg = doALGfetch(ret, b, q);
	BBPreleaseref(b->batCacheid);
	return msg;
}


str
ALGindexjoin(bat *result, const bat *lid, const bat *rid)
{
	BAT *left, *right, *bn;

	if ((left = BATdescriptor(*lid)) == NULL) {
		throw(MAL, "algebra.indexjoin", RUNTIME_OBJECT_MISSING);
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, "algebra.indexjoin", RUNTIME_OBJECT_MISSING);
	}

	bn = BATthetajoin(left, right, JOIN_EQ, BUN_NONE);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(right->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	throw(MAL, "algebra.indexjoin", MAL_MALLOC_FAIL);
}

str
ALGprojectNIL(bat *ret, const bat *bid)
{
    BAT *b, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
    }

    bn = BATconst(b, TYPE_void, (ptr) &oid_nil, TRANSIENT);
    if (bn) {
        *ret = bn->batCacheid;
        BBPkeepref(bn->batCacheid);
		BBPunfix(b->batCacheid);
        return MAL_SUCCEED;
    }
    BBPunfix(b->batCacheid);
    throw(MAL, "algebra.project", MAL_MALLOC_FAIL);
}

/*
 * The constant versions are typed by the parser
 */
str
ALGprojecthead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	const ValRecord *v = &stk->stk[getArg(pci, 1)];
	bat bid = * getArgReference_bat(stk, pci, 2);
	BAT *b, *bn;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
	b = BATmirror(b);
	bn = BATconst(b, v->vtype, VALptr(v), TRANSIENT);
	if (bn == NULL) {
		*ret = bat_nil;
		throw(MAL, "algebra.project", MAL_MALLOC_FAIL);
	}
	bn = BATmirror(bn);
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
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
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
	bn = BATconst(b, v->vtype, VALptr(v), TRANSIENT);
	if (bn == NULL) {
		*ret = bat_nil;
		throw(MAL, "algebra.project", MAL_MALLOC_FAIL);
	}
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}


str ALGreuse(bat *ret, const bat *bid)
{
	BAT *b,*bn;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.reuse", RUNTIME_OBJECT_MISSING);

	if( b->batPersistence != TRANSIENT || b->batRestricted != BAT_WRITE){
		if( ATOMvarsized(b->ttype) || b->htype != TYPE_void){
			bn= BATwcopy(b);
			if (bn == NULL) {
				BBPreleaseref(b->batCacheid);
				throw(MAL, "algebra.reuse", MAL_MALLOC_FAIL);
			}
		} else {
			bn = BATnew(b->htype,b->ttype,BATcount(b), TRANSIENT);
			if (bn == NULL) {
				BBPreleaseref(b->batCacheid);
				throw(MAL, "algebra.reuse", MAL_MALLOC_FAIL);
			}
			BATsetcount(bn,BATcount(b));
			bn->tsorted = FALSE;
			bn->trevsorted = FALSE;
			BATkey(bn,FALSE);
			/* head is void */
			BATseqbase(bn, b->hseqbase);
		}
		BBPkeepref(*ret= bn->batCacheid);
		BBPreleaseref(b->batCacheid);
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
		throw(MAL, "aggr.stdev", RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_sample(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (stdev == dbl_nil && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.stdev", SEMANTIC_TYPE_MISMATCH);
	*res = stdev;
	return MAL_SUCCEED;
}

str
ALGstdevp(dbl *res, const bat *bid)
{
	BAT *b;
	dbl stdev;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.stdevp", RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_population(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (stdev == dbl_nil && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.stdevp", SEMANTIC_TYPE_MISMATCH);
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
		throw(MAL, "aggr.variance", RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_sample(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (variance == dbl_nil && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.variance", SEMANTIC_TYPE_MISMATCH);
	*res = variance;
	return MAL_SUCCEED;
}

str
ALGvariancep(dbl *res, const bat *bid)
{
	BAT *b;
	dbl variance;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.variancep", RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_population(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (variance == dbl_nil && GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "aggr.variancep", SEMANTIC_TYPE_MISMATCH);
	*res = variance;
	return MAL_SUCCEED;
}
