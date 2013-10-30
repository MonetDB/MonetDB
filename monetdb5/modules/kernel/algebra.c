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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a Peter Boncz, Martin Kersten, Niels Nes, Sjoerd Mullender
 * @v 2.0
 * @+ BAT Algebra
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
#include "gdk_rangejoin.h"
#include <math.h>

/*
 * @* Command Implementations in C
 * This module contains just a wrapper implementations; since all described
 * operations are part of the GDK kernel.
 *
 * @+ BAT sum operation
 * The sum aggregate only works for int and float fields.
 * The routines below assumes that the caller knows what type
 * is large enough to prevent overflow.
 */

static int
CMDselect_(BAT **result, BAT *b, ptr low, ptr high, bit *l_in, bit *h_in)
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
CMDuselect_(BAT **result, BAT *b, ptr low, ptr high, bit *l_in, bit *h_in)
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
CMDantiuselect_(BAT **result, BAT *b, ptr low, ptr high, bit *l_in, bit *h_in)
{
	int tt = b->ttype;
	ptr nil = ATOMnilptr(tt);

	if (*l_in == bit_nil && ATOMcmp(tt, low, nil)) {
		GDKerror("CMDantiuselect: flag 'l_in' must not be NIL, unless boundary 'low' is NIL\n");
		return GDK_FAIL;
	}
	if (*h_in == bit_nil && ATOMcmp(tt, high, nil)) {
		GDKerror("CMDantiuselect: flag 'h_in' must not be NIL, unless boundary 'high' is NIL\n");
		return GDK_FAIL;
	}
	return (*result = BATantiuselect_(b, low, high, *l_in, *h_in)) ? GDK_SUCCEED : GDK_FAIL;
}

static int
CMDgen_group(BAT **result, BAT *gids, BAT *cnts )
{
	wrd j, gcnt = BATcount(gids);
	BAT *r = BATnew(TYPE_void, TYPE_oid, BATcount(gids)*2);

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
 * @- Substring Select
 * The string pattern matching routine has been added. It should be
 * dynamically linked.
 * A simple string matcher is included. It should be refined later on
 */
static inline int
like(char *x, char *y, BUN ylen)
{
	char *r;

	if (x == (char *) NULL) {
		return 0;
	}
	for (r = x + strlen(x) - ylen; x <= r; x++) {
		int ok = 1;
		char *s = x;
		char *q;

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
CMDlike(BAT **ret, BAT *b, str s)
{
	BATiter bi = bat_iterator(b);
	BAT *c = BATnew(BAThtype(b), TYPE_str, BATcount(b) / 10);
	str t;
	BUN u, v;
	BUN yy = 0;

	if (c == NULL)
		return GDK_FAIL;
	t = GDKstrdup(s);
	for (s = t; *s; s++, yy++)
		*s = tolower(*s);

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

/*
 * @- BAT slice
 */
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
 * @- BUN Get/Fetch
 */
static int
CMDposition(wrd *retval, BAT *b, ptr val)
{
	BUN v = BUNfnd(b, val);

	if (v == BUN_NONE)
		return GDK_FAIL;
	*retval = (wrd) (v - BUNfirst(b));
	return GDK_SUCCEED;
}

static int
CMDpositionBUN(wrd *retval, BAT *b, ptr val, ptr tval)
{
	BUN v = BUNlocate(b, val, tval);

	if (v == BUN_NONE)
		return GDK_FAIL;
	*retval = (wrd) (v - BUNfirst(b));
	return GDK_SUCCEED;
}

static int
CMDexist(bit *ret, BAT *b, ptr val)
{
	BUN q = BUNfnd(b, val);

	*ret = (q != BUN_NONE) ? 1 : 0;
	return GDK_SUCCEED;
}
static int
CMDexistBUN(bit *ret, BAT *b, ptr val, ptr tval)
{
	BUN q = BUNlocate(b, val, tval);

	*ret = (q != BUN_NONE) ? 1 : 0;
	return GDK_SUCCEED;
}

static BAT *
BATmerge(BAT *b)
{
	BUN n = BATcount(b);
	BAT *bn = BATnew(TYPE_lng, TYPE_void, n);
	BATiter bni = bat_iterator(bn), bi = bat_iterator(b);
	BUN p, q;
	lng *r = (lng *) BUNhead(bni, BUNfirst(bn));

	BATloop(b, p, q) {
		oid hp = *(oid *) BUNhead(bi, p);
		oid tp = *(oid *) BUNtail(bi, p);

		*r++ = (((lng) hp) << 32) + tp;
	}
	BATsetcount(bn, p);
	if (!bn->batDirty)
		bn->batDirty = TRUE;

	bn->hsorted = BAThordered(b) && (BATtordered(b) || BAThkey(b));
	bn->hrevsorted = BAThrevordered(b) && (BATtrevordered(b) || BAThkey(b));
	bn->tsorted = FALSE ;
	bn->trevsorted = FALSE ;
	bn->tdense = FALSE ;
	BATkey(bn, BAThkey(b) || BATtkey(b)) ;
	BATkey(BATmirror(bn), FALSE) ;

	return bn;
}

static BAT *
BATsplit(BAT *b)
{
	BATiter bi = bat_iterator(b);
	BUN n = BATcount(b);
	BAT *bn = BATnew(TYPE_oid, TYPE_oid, n);
	BUN i;
	lng *r = (lng *) BUNhead(bi, BUNfirst(b));

	for (i = 0; i < n; i++, r++) {
		oid hp = (int) (*r >> 32);
		oid tp = (int) *r;

		bunfastins(bn, &hp, &tp);
	}

	bn->hsorted = BAThordered(b) ;
	bn->hrevsorted = BAThrevordered(b) ;
	bn->tsorted = FALSE ;
	bn->trevsorted = FALSE ;
	bn->hdense = FALSE ;
	bn->tdense = FALSE ;
	bn->H->nonil = FALSE ;
	bn->T->nonil = FALSE ;
	BATkey(bn, FALSE) ;
	BATkey(BATmirror(bn), FALSE) ;

	return bn;
bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/*
 * @- Wrapper
 * The remainder of this file contains the wrapper around the V4 code base
 * The BAT identifiers passed through this module may indicate
 * that the 'reverse' view applies. This should be taken into
 * account while resolving them.
 * @+ BAT sum and product aggregation
 * The sum aggregate only works for int and float fields.
 * The routines below assumes that the caller knows what type
 * is large enough to prevent overflow.
 */

str
ALGminany(ptr result, int *bid)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if ((b = BATdescriptor(*bid)) == NULL)
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
		}
		if (p == NULL)
			msg = createException(MAL, "algebra.min", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
ALGmaxany(ptr result, int *bid)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if ((b = BATdescriptor(*bid)) == NULL)
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
		}
		if (p == NULL)
			msg = createException(MAL, "algebra.max", GDK_EXCEPTION);
	}
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
ALGtopN(int *res, int *bid, lng *top)
{
	BAT *b;

	b = BATdescriptor(*bid);
	if (b == NULL) {
		throw(MAL, "algebra.top", RUNTIME_OBJECT_MISSING);
	}
	/* TOP N works inplace, ie deletes ... */
	(void) BATtopN(b, (BUN) *top);
	*res = b->batCacheid;
	BBPkeepref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGgroupby(int *res, int *gids, int *cnts)
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
ALGcard(lng *result, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.card", RUNTIME_OBJECT_MISSING);
	}
	bn = (BAT *) BATkunique(BATmirror(b));
	if (bn == NULL) {
		throw(MAL, "algebra.card", GDK_EXCEPTION);
	}
	*result = BATcount(bn);
	BBPunfix(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGsubselect2(bat *result, bat *bid, bat *sid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	BAT *b, *s = NULL, *bn;
	const void *nilptr;

	if ((*li != 0 && *li != 1) ||
		(*hi != 0 && *hi != 1) ||
		(*anti != 0 && *anti != 1)) {
		throw(MAL, "algebra.subselect", ILLEGAL_ARGUMENT);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	if (sid && *sid && (s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
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
ALGsubselect1(bat *result, bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti)
{
	return ALGsubselect2(result, bid, NULL, low, high, li, hi, anti);
}

str
ALGthetasubselect2(bat *result, bat *bid, bat *sid, const void *val, const char **op)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	if (sid && *sid && (s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
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
ALGthetasubselect1(bat *result, bat *bid, const void *val, const char **op)
{
	return ALGthetasubselect2(result, bid, NULL, val, op);
}

str
ALGselect1(int *result, int *bid, ptr value)
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
ALGselect1Head(int *result, int *bid, ptr value)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	b = BATmirror(b);
	derefStr(b, t, value);
	bn = BATselect(b, value, 0);
	bn = BATmirror(bn);
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
ALGuselect1(int *result, int *bid, ptr value)
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
ALGantiuselect1(int *result, int *bid, ptr value)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.antiuselect", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, value);
	bn = BATantiuselect_(b, value, NULL, TRUE, TRUE);
	BBPreleaseref(b->batCacheid);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		return MAL_SUCCEED;
	}
	throw(MAL, "algebra.antiuselect", GDK_EXCEPTION);
}

str
ALGselect(int *result, int *bid, ptr low, ptr high)
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
ALGthetaselect(int *result, int *bid, ptr val, str *OP)
{
	ptr nilptr;
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.thetaselect", RUNTIME_OBJECT_MISSING);
	}
	nilptr = ATOMnilptr(b->ttype);
	derefStr(b, t, val);
	if (ATOMcmp(b->ttype, val, nilptr) == 0) {
		bn = BATnew(b->htype,b->ttype, 0);
	} else {
		char *op = *OP;
		bit lin = TRUE, rin = TRUE;
		ptr low = nilptr, high = nilptr;

		if (op[0] == '=') {
			low = val;
			high = val;
		} else if (op[0] == '<') {
			high = val;
			rin = (op[1] == '=');
		} else if (op[0] == '>') {
			low = val;
			lin = (op[1] == '=');
		} else {
			BBPreleaseref(b->batCacheid);
			throw(MAL, "algebra.thetaselect", ILLEGAL_ARGUMENT " Unknown operator");
		}
		CMDselect_(&bn, b, low, high, &lin, &rin);
	}
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.thetaselect", GDK_EXCEPTION);
}

str
ALGselectNotNil(int *result, int *bid)
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
		throw(MAL, "algebra.select", GDK_EXCEPTION);
	}
	/* just pass on the result */
	*result = b->batCacheid;
	BBPkeepref(*result);
	return MAL_SUCCEED;
}

str
ALGselectHead(int *result, int *bid, ptr low, ptr high)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	b = BATmirror(b);
	derefStr(b, t, low);
	derefStr(b, t, high);
	bn = BATselect(b, low, high);
	bn = BATmirror(bn);
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
ALGuselect(int *result, int *bid, ptr low, ptr high)
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
ALGthetauselect(int *result, int *bid, ptr val, str *OP)
{
	ptr nilptr;
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.thetauselect", RUNTIME_OBJECT_MISSING);
	}
	nilptr = ATOMnilptr(b->ttype);
	derefStr(b, t, val);
	if (ATOMcmp(b->ttype, val, nilptr) == 0) {
		bn = BATnew(b->htype,TYPE_void, 0);
	} else {
		char *op = *OP;
		bit lin = TRUE, rin = TRUE;
		ptr low = nilptr, high = nilptr;

		if (op[0] == '=') {
			low = val;
			high = val;
		} else if (op[0] == '<') {
			high = val;
			rin = (op[1] == '=');
		} else if (op[0] == '>') {
			low = val;
			lin = (op[1] == '=');
		} else {
			BBPreleaseref(b->batCacheid);
			throw(MAL, "algebra.thetauselect", ILLEGAL_ARGUMENT " Unknown operator");
		}
		CMDuselect_(&bn, b, low, high, &lin, &rin);
	}
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.thetauselect", GDK_EXCEPTION);
}

str
ALGselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin)
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
ALGselectInclusiveHead(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	b = BATmirror(b);
	derefStr(b, t, low);
	derefStr(b, t, high);
	CMDselect_(&bn, b, low, high, lin, rin);
	bn = BATmirror(bn);
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
ALGuselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
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
ALGantiuselectInclusive(int *result, int *bid, ptr low, ptr high, bit *lin, bit *rin)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.select", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, t, low);
	derefStr(b, t, high);
	CMDantiuselect_(&bn, b, low, high, lin, rin);
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
ALGfragment(int *result, int *bid, ptr hlow, ptr hhigh, ptr tlow, ptr thigh)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.fragment", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, hlow);
	derefStr(b, h, hhigh);
	derefStr(b, t, tlow);
	derefStr(b, t, thigh);
	bn = BATrestrict(b, hlow, hhigh, tlow, thigh);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.fragment", GDK_EXCEPTION);
}

str
ALGthetajoinEstimate(int *result, int *lid, int *rid, int *opc, lng *estimate)
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
ALGthetajoin(int *result, int *lid, int *rid, int *opc)
{
	return ALGthetajoinEstimate(result, lid, rid, opc, (ptr)&lng_nil);
}

str
ALGbandjoin(int *result, int *lid, int *rid, ptr *minus, ptr *plus, bit *li, bit *hi)
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
ALGbandjoin_default(int *result, int *lid, int *rid, ptr *minus, ptr *plus)
{
	bit li = TRUE;
	bit hi = TRUE;
	return ALGbandjoin(result, lid, rid, minus, plus, &li, &hi);
}

str
ALGrangejoin(int *result, int *lid, int *rlid, int *rhid, bit *li, bit *hi)
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
do_join(bat *r1, bat *r2, bat *lid, bat *rid, bat *slid, bat *srid,
		int op, bit *nil_matches, lng *estimate,
		gdk_return (*joinfunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, BUN),
		gdk_return (*thetafunc)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, int, BUN),
		const char *funcname)
{
	BAT *left = NULL, *right = NULL, *candleft = NULL, *candright = NULL;
	BAT *result1, *result2;
	BUN est;

	if ((left = BATdescriptor(*lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(*rid)) == NULL)
		goto fail;
	if (slid && *slid && (candleft = BATdescriptor(*slid)) == NULL)
		goto fail;
	if (srid && *srid && (candright = BATdescriptor(*srid)) == NULL)
		goto fail;
	if (estimate == NULL || *estimate < 0 || *estimate == lng_nil || *estimate > (lng) BUN_MAX)
		est = BUN_NONE;
	else
		est = (BUN) *estimate;

	if (thetafunc) {
		assert(joinfunc == NULL);
		if ((*thetafunc)(&result1, &result2, left, right, candleft, candright, op, *nil_matches, est) == GDK_FAIL)
			goto fail;
	} else {
		if ((*joinfunc)(&result1, &result2, left, right, candleft, candright, *nil_matches, est) == GDK_FAIL)
			goto fail;
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
	if (candleft)
		BBPreclaim(candleft);
	if (candright)
		BBPreclaim(candright);
	throw(MAL, funcname, RUNTIME_OBJECT_MISSING);
}

str
ALGsubjoin(bat *r1, bat *r2, bat *lid, bat *rid, bat *slid, bat *srid, bit *nil_matches, lng *estimate)
{
	return do_join(r1, r2, lid, rid, slid, srid, 0, nil_matches, estimate,
				   BATsubjoin, NULL, "algebra.subjoin");
}

str
ALGsubleftjoin(bat *r1, bat *r2, bat *lid, bat *rid, bat *slid, bat *srid, bit *nil_matches, lng *estimate)
{
	return do_join(r1, r2, lid, rid, slid, srid, 0, nil_matches, estimate,
				   BATsubleftjoin, NULL, "algebra.subleftjoin");
}

str
ALGsubouterjoin(bat *r1, bat *r2, bat *lid, bat *rid, bat *slid, bat *srid, bit *nil_matches, lng *estimate)
{
	return do_join(r1, r2, lid, rid, slid, srid, 0, nil_matches, estimate,
				   BATsubouterjoin, NULL, "algebra.subouterjoin");
}

str
ALGsubthetajoin(bat *r1, bat *r2, bat *lid, bat *rid, bat *slid, bat *srid, int *op, bit *nil_matches, lng *estimate)
{
	return do_join(r1, r2, lid, rid, slid, srid, *op, nil_matches, estimate,
				   NULL, BATsubthetajoin, "algebra.subthetajoin");
}

static str
ALGunary(int *result, int *bid, BAT *(*func)(BAT *), const char *name)
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
ALGbinary(int *result, int *lid, int *rid, BAT *(*func)(BAT *, BAT *), const char *name)
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
ALGbinaryint(bat *result, bat *bid, int *param, BAT *(*func)(BAT *, BUN), const char *name)
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
ALGbinaryestimate(int *result, int *lid, int *rid, lng *estimate,
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

str
ALGhistogram(bat *result, bat *bid)
{
	return ALGunary(result, bid, BAThistogram, "algebra.histogram");
}

str
ALGmerge(bat *result, bat *bid)
{
	return ALGunary(result, bid, BATmerge, "algebra.merge");
}

str
ALGsplit(bat *result, bat *bid)
{
	return ALGunary(result, bid, BATsplit, "algebra.split");
}

static BAT *
BATwcopy(BAT *b)
{
	return BATcopy(b, b->htype, b->ttype, 1);
}

str
ALGcopy(bat *result, bat *bid)
{
	return ALGunary(result, bid, BATwcopy, "algebra.copy");
}

str
ALGkunique(bat *result, bat *bid)
{
	return ALGunary(result, bid, BATkunique, "algebra.kunique");
}

str
ALGsunique(bat *result, bat *bid)
{
	return ALGunary(result, bid, BATsunique, "algebra.sunique");
}

str
ALGcross(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATcross, "algebra.cross");
}

str
ALGantijoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATantijoin, "algebra.antijoin");
}

str
ALGantijoin2( bat *l, bat *r, bat *left, bat *right)
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
ALGjoin2( bat *l, bat *r, bat *left, bat *right)
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
ALGthetajoin2( bat *l, bat *r, bat *left, bat *right, int *opc)
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
ALGcrossproduct2( bat *l, bat *r, bat *left, bat *right)
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
ALGbandjoin2(bat *l, bat *r, bat *left, bat *right, ptr *minus, ptr *plus, bit *li, bit *hi)
{
	BAT *L, *R, *j;

	if ((L = BATdescriptor(*left)) == NULL) {
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}
	if ((R = BATdescriptor(*right)) == NULL) {
		BBPunfix(L->batCacheid);
		throw(MAL, "algebra.bandjoin", RUNTIME_OBJECT_MISSING);
	}

	/* j = bandjoin(left,reverse(right), minus, plus, li, hi)
	   l = reverse(mark(j))
	   r = reverse(mark(reverse(j)))
	*/
	j = BATbandjoin(L, BATmirror(R), minus, plus, *li, *hi);
	if (!j)
		throw(MAL, "algebra.bandjoin", GDK_EXCEPTION);
	BBPunfix(L->batCacheid);
	BBPunfix(R->batCacheid);
	L = BATmirror(BATmark(j,0));
	R = BATmirror(BATmark(BATmirror(j),0));
	BBPunfix(j->batCacheid);
	BBPkeepref(*l = L->batCacheid);
	BBPkeepref(*r = R->batCacheid);
	return MAL_SUCCEED;
}

str
ALGrangejoin2(int *l, int *r, int *left, int *rightl, int *righth, bit *li, bit *hi)
{
	BAT *L, *R, *RL, *RH, *j;

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

	/* j = join(left,rightl,righth, li, hi)
	   l = reverse(mark(j))
	   r = reverse(mark(reverse(j)))
	*/
	j = BATrangejoin(L, RL, RH, *li, *hi);
	if (!j)
		throw(MAL, "algebra.join", GDK_EXCEPTION);
	BBPunfix(L->batCacheid);
	BBPunfix(RL->batCacheid);
	BBPunfix(RH->batCacheid);
	L = BATmirror(BATmark(j,0));
	R = BATmirror(BATmark(BATmirror(j),0));
	BBPunfix(j->batCacheid);
	BBPkeepref(*l = L->batCacheid);
	BBPkeepref(*r = R->batCacheid);
	return MAL_SUCCEED;
}

str
ALGjoinestimate(bat *result, bat *lid, bat *rid, lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATjoin, "algebra.join");
}

str
ALGjoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATjoin, "algebra.join");
}

str
ALGleftjoinestimate(bat *result, bat *lid, bat *rid, lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATleftjoin, "algebra.leftjoin");
}

str
ALGleftjoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATleftjoin, "algebra.leftjoin");
}

str
ALGleftfetchjoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATproject, "algebra.leftfetchjoin");
}

str
ALGouterjoinestimate(bat *result, bat *lid, bat *rid, lng *estimate)
{
	return ALGbinaryestimate(result, lid, rid, estimate, BATouterjoin, "algebra.outerjoin");
}

str
ALGouterjoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinaryestimate(result, lid, rid, NULL, BATouterjoin, "algebra.outerjoin");
}

str
ALGsemijoin(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATsemijoin, "algebra.semijoin");
}

str
ALGsunion(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATsunion, "algebra.sunion");
}

str
ALGkunion(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATkunion, "algebra.kunion");
}

str
ALGsintersect(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATsintersect, "algebra.sintersect");
}

str
ALGsdiff(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATsdiff, "algebra.sdiff");
}

str
ALGkdiff(bat *result, bat *lid, bat *rid)
{
	return ALGbinary(result, lid, rid, BATkdiff, "algebra.kdiff");
}

str
ALGsample(bat *result, bat *bid, int *param)
{
	return ALGbinaryint(result, bid, param, BATsample, "algebra.sample");
}

str
ALGsubsample(bat *result, bat *bid, int *param)
{
	return ALGbinaryint(result, bid, param, BATsample_, "algebra.subsample");
}

/* add items missing in the kernel */
str
ALGtunique(int *result, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.tunique", RUNTIME_OBJECT_MISSING);
	}
	bn = BATkunique(BATmirror(b));
	if (bn) {
		bn = BATmirror(bn);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.tunique", GDK_EXCEPTION);
}

str
ALGtunion(int *result, int *bid, int *bid2)
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
ALGtdifference(int *result, int *bid, int *bid2)
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
ALGtdiff(int *result, int *bid, int *bid2)
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
ALGtintersect(int *result, int *bid, int *bid2)
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
ALGtinter(int *result, int *bid, int *bid2)
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
ALGtsort(int *result, int *bid)
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
ALGtsort_rev(int *result, int *bid)
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
ALGhsort(int *result, int *bid)
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
ALGhsort_rev(int *result, int *bid)
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
ALGhtsort(int *result, int *lid)
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
ALGthsort(int *result, int *lid)
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
ALGssort(int *result, int *bid)
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
ALGssort_rev(int *result, int *bid)
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
ALGsubsort33(bat *result, bat *norder, bat *ngroup, bat *bid, bat *order, bat *group, bit *reverse, bit *stable)
{
	BAT *bn = NULL, *on = NULL, *gn = NULL;
	BAT *b = NULL, *o = NULL, *g = NULL;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.subsort", RUNTIME_OBJECT_MISSING);
	if (order && *order && (o = BATdescriptor(*order)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.subsort", RUNTIME_OBJECT_MISSING);
	}
	if (group && *group && (g = BATdescriptor(*group)) == NULL) {
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
ALGsubsort32(bat *result, bat *norder, bat *bid, bat *order, bat *group, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, order, group, reverse, stable);
}

str
ALGsubsort31(bat *result, bat *bid, bat *order, bat *group, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, order, group, reverse, stable);
}

str
ALGsubsort23(bat *result, bat *norder, bat *ngroup, bat *bid, bat *order, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, norder, ngroup, bid, order, NULL, reverse, stable);
}

str
ALGsubsort22(bat *result, bat *norder, bat *bid, bat *order, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, order, NULL, reverse, stable);
}

str
ALGsubsort21(bat *result, bat *bid, bat *order, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, order, NULL, reverse, stable);
}

str
ALGsubsort13(bat *result, bat *norder, bat *ngroup, bat *bid, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, norder, ngroup, bid, NULL, NULL, reverse, stable);
}

str
ALGsubsort12(bat *result, bat *norder, bat *bid, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, norder, NULL, bid, NULL, NULL, reverse, stable);
}

str
ALGsubsort11(bat *result, bat *bid, bit *reverse, bit *stable)
{
	return ALGsubsort33(result, NULL, NULL, bid, NULL, NULL, reverse, stable);
}

str
ALGrevert(int *result, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.revert", RUNTIME_OBJECT_MISSING);
	}
	bn = BATcopy(b, b->htype, b->ttype, TRUE);
	BATrevert(bn);
	*result= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGcount_bat(wrd *result, int *bid)
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
ALGcount_nil(wrd *result, int *bid, bit *ignore_nils)
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
ALGcount_no_nil(wrd *result, int *bid)
{
	bit ignore_nils = 1;

	return ALGcount_nil(result, bid, &ignore_nils);
}

str
ALGtmark(int *result, int *bid, oid *base)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.markT", RUNTIME_OBJECT_MISSING);
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
	throw(MAL, "algebra.markT", GDK_EXCEPTION);
}

str
ALGtmark_default(int *result, int *bid)
{
	oid o = 0;

	return ALGtmark(result, bid, &o);
}

str
ALGtmarkp(int *result, int *bid, int *nr_parts, int *part_nr)
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
ALGmarkHead(int *result, int *bid, oid *base)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.markH", RUNTIME_OBJECT_MISSING);
	}
	/* M5's markH is semantically identical with M4/GDK's tmark */
	/* (Don't ask me why; wasn't my decision. Stefan.) */
	bn = BATmirror(BATmark(BATmirror(b), *base));
	if (bn != NULL) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.markH", GDK_EXCEPTION);
}

str
ALGmarkHead_default(int *result, int *bid)
{
	oid o = 0;

	return ALGmarkHead(result, bid, &o);
}

str
ALGhmarkp(int *result, int *bid, int *nr_parts, int *part_nr)
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
	return ALGmarkHead(result, bid, &base);
}

str
ALGmark_grp_1(int *result, int *bid, int *gid)
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
ALGmark_grp_2(int *result, int *bid, int *gid, oid *base)
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
ALGhistogram_rev(int *result, int *bid)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.histogram", RUNTIME_OBJECT_MISSING);
	}
	bn = BAThistogram(b);
	if (bn) {
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		*result = bn->batCacheid;
		BBPkeepref(*result);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(b->batCacheid);
	throw(MAL, "algebra.histogram", GDK_EXCEPTION);
}

str
ALGlike(int *ret, int *bid, str *k)
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
ALGslice(int *ret, bat *bid, lng *start, lng *end)
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
ALGslice_int(int *ret, bat *bid, int *start, int *end)
{
	lng s = *start;
	lng e = (*end == int_nil ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

str
ALGslice_wrd(int *ret, bat *bid, wrd *start, wrd *end)
{
	lng s = *start;
	lng e = (*end == wrd_nil ? lng_nil : *end);

	return ALGslice(ret, bid, &s, &e);
}

/* carve out a slice based on the OIDs */
/* beware that BATs may have different OID bases */
str
ALGslice_oid(int *ret, bat *bid, oid *start, oid *end)
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
ALGsubslice_wrd(int *ret, bat *bid, wrd *start, wrd *end)
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
 * @- BUN Get/Fetch
 */
str
ALGposition(wrd *retval, int *bid, ptr val)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.position", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	if (CMDposition(retval, b, val) == GDK_FAIL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.position", GDK_EXCEPTION "Item not found");
	}
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGpositionBUN(wrd *retval, int *bid, ptr val, ptr tval)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.position", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	derefStr(b, t, tval);
	if( (CMDpositionBUN(retval, b, val, tval) == GDK_FAIL) ){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.position", GDK_EXCEPTION "Item not found");
	}
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

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
		} else {
			memcpy(ret, Tloc(b, pos), _s);
		}
	}
	return MAL_SUCCEED;
}

str
ALGfetch(ptr ret, int *bid, lng *pos)
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
ALGfetchoid(int *ret, int *bid, oid *pos)
{
	lng o = *pos;

	return ALGfetch(ret, bid, &o);
}

str
ALGfetchint(int *ret, int *bid, int *pos)
{
	lng o = *pos;

	return ALGfetch(ret, bid, &o);
}

str
ALGexist(bit *ret, int *bid, ptr val)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.exist", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	CMDexist(ret, b, val);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGexistBUN(bit *ret, int *bid, ptr val, ptr tval)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.exist", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	derefStr(b, t, tval);
	CMDexistBUN(ret, b, val, tval);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
ALGfind(ptr ret, int *bid, ptr val)
{
	BAT *b;
	BUN q;
	str msg;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.find", RUNTIME_OBJECT_MISSING);
	}
	derefStr(b, h, val);
	q = BUNfnd(b, val);

	if (q == BUN_NONE){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "algebra.find", GDK_EXCEPTION "can not find element");
	}
	msg = doALGfetch(ret, b, q);
	BBPreleaseref(b->batCacheid);
	return msg;
}


str
ALGindexjoin(int *result, int *lid, int *rid)
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
ALGprojectNIL(int *ret, int *bid)
{
    BAT *b, *bn;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
    }

    bn = BATconst(b, TYPE_void, (ptr) &oid_nil);
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
	int *ret = getArgReference(stk, pci, 0);
	const ValRecord *v = &stk->stk[getArg(pci, 1)];
	bat bid = * (bat *) getArgReference(stk, pci, 2);
	BAT *b, *bn;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
	b = BATmirror(b);
	bn = BATconst(b, v->vtype, VALptr(v));
	if (bn == NULL) {
		*ret = 0;
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
	int *ret = getArgReference(stk, pci, 0);
	bat bid = * (bat *) getArgReference(stk, pci, 1);
	const ValRecord *v = &stk->stk[getArg(pci, 2)];
	BAT *b, *bn;

	(void) cntxt;
	(void) mb;
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "algebra.project", RUNTIME_OBJECT_MISSING);
	bn = BATconst(b, v->vtype, VALptr(v));
	if (bn == NULL) {
		*ret = 0;
		throw(MAL, "algebra.project", MAL_MALLOC_FAIL);
	}
	if (!(bn->batDirty&2))
		bn = BATsetaccess(bn, BAT_READ);
	*ret= bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}


/* You don;t have to materialize the oids.
This is taken care upon access */
str
ALGidentity(int *ret, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.identity", RUNTIME_OBJECT_MISSING);
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ); \
	BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}
str
ALGmaterialize(int *ret, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.materialize", RUNTIME_OBJECT_MISSING);
	if( b->htype == TYPE_void){
		bn= BATmaterialize(b);
		if( bn == NULL)
			throw(MAL, "batcalc.materialize", MAL_MALLOC_FAIL);
		if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
		BBPkeepref(*ret= bn->batCacheid);
	} else
		BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}

str ALGreuse(int *ret, int *bid)
{
	BAT *b,*bn;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.reuse", RUNTIME_OBJECT_MISSING);

	if( b->batPersistence != TRANSIENT || b->batRestricted != BAT_WRITE){
		if( ATOMvarsized(b->ttype) || b->htype != TYPE_void){
			bn= BATwcopy(b);
		} else {
			bn = BATnew(b->htype,b->ttype,BATcount(b));
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
 * @+ BAT avg operation
 * The avg aggregate only works for int and float fields.
 */
str
ALGavg(dbl *res, int *bid)
{
	BAT *b;
	int ret;
	BUN cnt;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
	ret = BATcalcavg(b, NULL, res, &cnt);
	BBPreleaseref(b->batCacheid);
	if (ret == GDK_FAIL)
		throw(MAL, "aggr.avg", SEMANTIC_TYPE_MISMATCH);
	/* backward compatibility: return nil if there are nils in the input */
	if (cnt < BATcount(b))
		*res = dbl_nil;
	return MAL_SUCCEED;
}

/*
 * @+ BAT standard deviation
 */
str
ALGstdev(dbl *res, int *bid)
{
	BAT *b;
	dbl stdev;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.stdev", RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_sample(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (stdev == dbl_nil)
		throw(MAL, "aggr.stdev", SEMANTIC_TYPE_MISMATCH);
	*res = stdev;
	return MAL_SUCCEED;
}

str
ALGstdevp(dbl *res, int *bid)
{
	BAT *b;
	dbl stdev;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.stdevp", RUNTIME_OBJECT_MISSING);
	stdev = BATcalcstdev_population(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (stdev == dbl_nil)
		throw(MAL, "aggr.stdevp", SEMANTIC_TYPE_MISMATCH);
	*res = stdev;
	return MAL_SUCCEED;
}

/*
 * @+ BAT variance
 */
str
ALGvariance(dbl *res, int *bid)
{
	BAT *b;
	dbl variance;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.variance", RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_sample(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (variance == dbl_nil)
		throw(MAL, "aggr.variance", SEMANTIC_TYPE_MISMATCH);
	*res = variance;
	return MAL_SUCCEED;
}

str
ALGvariancep(dbl *res, int *bid)
{
	BAT *b;
	dbl variance;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.variancep", RUNTIME_OBJECT_MISSING);
	variance = BATcalcvariance_population(NULL, b);
	BBPreleaseref(b->batCacheid);
	if (variance == dbl_nil)
		throw(MAL, "aggr.variancep", SEMANTIC_TYPE_MISMATCH);
	*res = variance;
	return MAL_SUCCEED;
}
