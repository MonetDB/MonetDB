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
 *  M.L. Kersten
 * String multiplexes
 * [TODO: property propagations]
 * The collection of routines provided here are map operations
 * for the atom string primitives.
 *
 * In line with the batcalc module, we assume that
 * if two bat operands are provided that they are already
 * aligned on the head. Moreover, the head of the BATs
 * are limited to :void, which can be cheaply realized using
 * the GRPsplit operation.
 */
#include "monetdb_config.h"
#include <gdk.h>
#include "ctype.h"
#include <string.h>
#include "mal_exception.h"
#include "str.h"

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif
#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batstr_export extern __declspec(dllimport)
#else
#define batstr_export extern __declspec(dllexport)
#endif
#else
#define batstr_export extern
#endif

batstr_export str STRbatPrefix(bat *ret, bat *l, bat *r);
batstr_export str STRbatPrefixcst(bat *ret, bat *l, str *cst);
batstr_export str STRcstPrefixbat(bat *ret, str *cst, bat *r);
batstr_export str STRbatSuffix(bat *ret, bat *l, bat *r);
batstr_export str STRbatSuffixcst(bat *ret, bat *l, str *cst);
batstr_export str STRcstSuffixbat(bat *ret, str *cst, bat *r);
batstr_export str STRbatstrSearch(bat *ret, bat *l, bat *r);
batstr_export str STRbatstrSearchcst(bat *ret, bat *l, str *cst);
batstr_export str STRcststrSearchbat(bat *ret, str *cst, bat *r);
batstr_export str STRbatRstrSearch(bat *ret, bat *l, bat *r);
batstr_export str STRbatRstrSearchcst(bat *ret, bat *l, str *cst);
batstr_export str STRcstRstrSearchbat(bat *ret, str *cst, bat *r);
batstr_export str STRbatTail(bat *ret, bat *l, bat *r);
batstr_export str STRbatTailcst(bat *ret, bat *l, bat *cst);
batstr_export str STRbatWChrAt(bat *ret, bat *l, bat *r);
batstr_export str STRbatWChrAtcst(bat *ret, bat *l, bat *cst);
batstr_export str STRbatSubstitutecst(bat *ret, bat *l, str *arg2, str *arg3, bit *rep);

batstr_export str STRbatLower(bat *ret, bat *l);
batstr_export str STRbatUpper(bat *ret, bat *l);
batstr_export str STRbatStrip(bat *ret, bat *l);
batstr_export str STRbatLtrim(bat *ret, bat *l);
batstr_export str STRbatRtrim(bat *ret, bat *l);

batstr_export str STRbatLength(bat *ret, bat *l);
batstr_export str STRbatstringLength(bat *ret, bat *l);
batstr_export str STRbatBytes(bat *ret, bat *l);

batstr_export str STRbatsubstringcst(bat *ret, bat *bid, int *start, int *length);
batstr_export str STRbatsubstring(bat *ret, bat *l, bat *r, bat *t);
batstr_export str STRbatreplace(bat *ret, bat *l, str *pat, str *s2);


#define prepareOperand(X,Y,Z)					\
	if( (X= BATdescriptor(*Y)) == NULL )		\
		throw(MAL, Z, RUNTIME_OBJECT_MISSING);
#define prepareOperand2(X,Y,A,B,Z)				\
	if( (X= BATdescriptor(*Y)) == NULL )		\
		throw(MAL, Z, RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){		\
		BBPreleaseref(X->batCacheid);			\
		throw(MAL, Z, RUNTIME_OBJECT_MISSING);	\
	}
#define prepareResult(X,Y,T,Z)					\
	X= BATnew(Y->htype,T,BATcount(Y));			\
	if( X == NULL){								\
		BBPreleaseref(Y->batCacheid);			\
		throw(MAL, Z, MAL_MALLOC_FAIL);			\
	}											\
	if( Y->htype== TYPE_void)					\
		BATseqbase(X, Y->hseqbase);				\
	X->hsorted=Y->hsorted;						\
	X->hrevsorted=Y->hrevsorted;				\
	X->tsorted=0;								\
	X->trevsorted=0;
#define prepareResult2(X,Y,A,T,Z)				\
	X= BATnew(Y->htype,T,BATcount(Y));			\
	if( Y->htype== TYPE_void)					\
		BATseqbase(X, Y->hseqbase);				\
	if( X == NULL){								\
		BBPreleaseref(Y->batCacheid);			\
		BBPreleaseref(A->batCacheid);			\
		throw(MAL, Z, MAL_MALLOC_FAIL);			\
	}											\
	X->hsorted=Y->hsorted;						\
	X->hrevsorted=Y->hrevsorted;				\
	X->tsorted=0;								\
	X->trevsorted=0;
#define finalizeResult(X,Y,Z)									\
	if (!((Y)->batDirty&2)) (Y) = BATsetaccess((Y), BAT_READ);	\
	*X = (Y)->batCacheid;										\
	BBPkeepref(*(X));											\
	BBPreleaseref(Z->batCacheid);

static str
do_batstr_int(bat *ret, bat *l, const char *name, int (*func)(int *, str))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x;
	int y;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_int, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		x = (str) BUNtail(bi, p);
		if (x == 0 || strcmp(x, str_nil) == 0) {
			y = int_nil;
			bn->T->nonil = 0;
			bn->T->nil = 1;
		} else
			(*func)(&y, x);
		bunfastins(bn, h, &y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

str
STRbatLength(bat *ret, bat *l)
{
	return do_batstr_int(ret, l, "batstr.Length", strLength);
}

str
STRbatstringLength(bat *ret, bat *l)
{
	return do_batstr_int(ret, l, "batstr.stringLength", strSQLLength);
}

str
STRbatBytes(bat *ret, bat *l)
{
	return do_batstr_int(ret, l, "batstr.Bytes", strBytes);
}

static str
do_batstr_str(bat *ret, bat *l, const char *name, int (*func)(str *, str))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str y = NULL;

		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0)
			(*func)(&y, x);
		if (y == NULL)
			y = (str) str_nil;
		bunfastins(bn, h, y);
		if (y == str_nil) {
			bn->T->nonil = 0;
			bn->T->nil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

str
STRbatLower(bat *ret, bat *l)
{
	return do_batstr_str(ret, l, "batstr.Lower", strLower);
}

str
STRbatUpper(bat *ret, bat *l)
{
	return do_batstr_str(ret, l, "batstr.Upper", strUpper);
}

str
STRbatStrip(bat *ret, bat *l)
{
	return do_batstr_str(ret, l, "batstr.Strip", strStrip);
}

str
STRbatLtrim(bat *ret, bat *l)
{
	return do_batstr_str(ret, l, "batstr.Ltrim", strLtrim);
}

str
STRbatRtrim(bat *ret, bat *l)
{
	return do_batstr_str(ret, l, "batstr.Rtrim", strRtrim);
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

str STRbatPrefix(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.prefix");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.startsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"batstr.prefix");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRPrefix(vp, &tl, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr." "prefix", OPERATION_FAILED " During bulk operation");
}

str STRbatPrefixcst(bat *ret, bat *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"batstr.prefix");
	prepareResult(bn,left,TYPE_bit,"batstr.prefix");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRPrefix(vp, &tl, cst);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""prefix", OPERATION_FAILED " During bulk operation");
}

str STRcstPrefixbat(bat *ret, str *cst, bat *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(right,r,"batstr.prefix");
	prepareResult(bn,right,TYPE_bit,"batstr.prefix");

	righti = bat_iterator(right);

	BATloop(right, p, q) {
		ptr h = BUNhead(righti,p);
		str tr = (str) BUNtail(righti,p);
		STRPrefix(vp, cst, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,right);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""prefix", OPERATION_FAILED " During bulk operation");
}

str STRbatSuffix(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.suffix");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.endsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"batstr.suffix");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRSuffix(vp, &tl, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr." "suffix", OPERATION_FAILED " During bulk operation");
}

str STRbatSuffixcst(bat *ret, bat *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"batstr.suffix");
	prepareResult(bn,left,TYPE_bit,"batstr.suffix");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRSuffix(vp, &tl, cst);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""suffix", OPERATION_FAILED " During bulk operation");
}

str STRcstSuffixbat(bat *ret, str *cst, bat *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(right,r,"batstr.suffix");
	prepareResult(bn,right,TYPE_bit,"batstr.suffix");

	righti = bat_iterator(right);

	BATloop(right, p, q) {
		ptr h = BUNhead(righti,p);
		str tr = (str) BUNtail(righti,p);
		STRSuffix(vp, cst, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,right);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""suffix", OPERATION_FAILED " During bulk operation");
}

str STRbatstrSearch(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.search");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_int,"batstr.search");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRstrSearch(vp, &tl, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr." "search", OPERATION_FAILED " During bulk operation");
}

str STRbatstrSearchcst(bat *ret, bat *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.search");
	prepareResult(bn,left,TYPE_int,"batstr.search");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRstrSearch(vp, &tl, cst);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""search", OPERATION_FAILED " During bulk operation");
}

str STRcststrSearchbat(bat *ret, str *cst, bat *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(right,r,"batstr.search");
	prepareResult(bn,right,TYPE_bit,"batstr.search");

	righti = bat_iterator(right);

	BATloop(right, p, q) {
		ptr h = BUNhead(righti,p);
		str tr = (str) BUNtail(righti,p);
		STRstrSearch(vp, cst, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,right);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""search", OPERATION_FAILED " During bulk operation");
}

str STRbatRstrSearch(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.r_search");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.r_search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"batstr.r_search");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRReverseStrSearch(vp, &tl, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr." "r_search", OPERATION_FAILED " During bulk operation");
}

str STRbatRstrSearchcst(bat *ret, bat *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.r_search");
	prepareResult(bn,left,TYPE_bit,"batstr.r_search");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRReverseStrSearch(vp, &tl, cst);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""r_search", OPERATION_FAILED " During bulk operation");
}

str STRcstRstrSearchbat(bat *ret, str *cst, bat *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(right,r,"batstr.r_search");
	prepareResult(bn,right,TYPE_bit,"batstr.r_search");

	righti = bat_iterator(right);

	BATloop(right, p, q) {
		ptr h = BUNhead(righti,p);
		str tr = (str) BUNtail(righti,p);
		STRReverseStrSearch(vp, cst, &tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,right);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""r_search", OPERATION_FAILED " During bulk operation");
}

str STRbatTail(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	str v;

	prepareOperand2(left,l,right,r,"batstr.string");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.string", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_str,"batstr.string");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		ptr tl = BUNtail(lefti,p);
		ptr tr = BUNtail(righti,p);
		strTail(&v, tl, tr);
		bunfastins(bn, h, v);
		GDKfree(v);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.string" , OPERATION_FAILED " During bulk operation");
}

str STRbatTailcst(bat *ret, bat *l, bat *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	str v;

	prepareOperand(left,l,"batstr.string");
	prepareResult(bn,left,TYPE_str,"batstr.string");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		ptr tl = BUNtail(lefti,p);
		strTail(&v, tl, cst);
		bunfastins(bn, h, v);
		GDKfree(v);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(*ret);
	throw(MAL, "batstr.string", OPERATION_FAILED " During bulk operation");
}

str STRbatWChrAt(bat *ret, bat *l, bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.+");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"batstr.+");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		ptr tl = BUNtail(lefti,p);
		ptr tr = BUNtail(righti,p);
		strWChrAt(vp, tl, tr);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	BBPreleaseref(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr." "+", OPERATION_FAILED " During bulk operation");
}

str STRbatWChrAtcst(bat *ret, bat *l, bat *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.+");
	prepareResult(bn,left,TYPE_bit,"batstr.+");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		ptr tl = BUNtail(lefti,p);
		strWChrAt(vp, tl, cst);
		bunfastins(bn, h, vp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPreleaseref(*ret);
	throw(MAL, "batstr""+", OPERATION_FAILED " During bulk operation");
}

str
STRbatSubstitutecst(bat *ret, bat *l, str *arg2, str *arg3, bit *rep)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, *xp = &x;

	prepareOperand(b, l, "subString");
	prepareResult(bn, b, TYPE_int, "subString");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0)
			STRSubstitute(yp, xp, arg2, arg3, rep);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(yp);
	}
	bn->T->nonil = 0;
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(bn->batCacheid);
	throw(MAL, "batstr.subString", OPERATION_FAILED " During bulk operation");
}

/*
 * The substring functions require slightly different arguments
 */
str
STRbatsubstringcst(bat *ret, bat *bid, int *start, int *length)
{
	BATiter bi;
	BAT *b,*bn;
	BUN p, q;
	str res;
	char *msg = MAL_SUCCEED;

	if( (b= BATdescriptor(*bid)) == NULL)
		throw(MAL, "batstr.substring",RUNTIME_OBJECT_MISSING);
	bn= BATnew(TYPE_void, TYPE_str, BATcount(b)/10+5);
	BATseqbase(bn, b->hseqbase);
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		str t =  (str) BUNtail(bi, p);

		if ((msg=STRsubstring(&res, &t, start, length)))
			goto bunins_failed;
		BUNappend(bn, (ptr)res, FALSE);
		GDKfree(res);
	}

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b,bn);

		BBPreleaseref(bn->batCacheid);
		bn = r;
	}

	bn->T->nonil = 0;
  bunins_failed:
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str STRbatsubstring(bat *ret, bat *l, bat *r, bat *t)
{
	BATiter lefti, starti, lengthi;
	BAT *bn, *left, *start, *length;
	BUN p,q;
	str v, *vp= &v;

	if( (left= BATdescriptor(*l)) == NULL )
		throw(MAL, "batstr.substring" , RUNTIME_OBJECT_MISSING);
	if( (start= BATdescriptor(*r)) == NULL ){
		BBPreleaseref(left->batCacheid);
		throw(MAL, "batstr.substring", RUNTIME_OBJECT_MISSING);
	}
	if( (length= BATdescriptor(*t)) == NULL ){
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(start->batCacheid);
		throw(MAL, "batstr.substring", RUNTIME_OBJECT_MISSING);
	}
	if( BATcount(left) != BATcount(start) )
		throw(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
	if( BATcount(left) != BATcount(length) )
		throw(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");

	bn= BATnew(TYPE_void, TYPE_str,BATcount(left));
	BATseqbase(bn, left->hseqbase);
	if( bn == NULL){
		BBPreleaseref(left->batCacheid);
		BBPreleaseref(start->batCacheid);
		BBPreleaseref(length->batCacheid);
		throw(MAL, "batstr.substring", MAL_MALLOC_FAIL);
	}

	bn->hsorted= left->hsorted;
	bn->hrevsorted= left->hrevsorted;
	bn->tsorted=0;
	bn->trevsorted=0;

	lefti = bat_iterator(left);
	starti = bat_iterator(start);
	lengthi = bat_iterator(length);
	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		int *t1 = (int *) BUNtail(starti,p);
		int *t2 = (int *) BUNtail(lengthi,p);
		STRsubstring(vp, &tl, t1, t2);
		BUNappend(bn, *vp, FALSE);
		GDKfree(*vp);
	}
	if (left->htype != bn->htype) {
		BAT *r = VIEWcreate(left,bn);

		BBPreleaseref(bn->batCacheid);
		bn = r;
	}
	bn->T->nonil = 0;
	BBPreleaseref(start->batCacheid);
	BBPreleaseref(length->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;
}
