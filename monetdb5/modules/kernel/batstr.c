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
#include "batstr.h"

#define prepareOperand(X,Y,Z)								\
	if( (X= BATdescriptor(*Y)) == NULL )					\
		throw(MAL, "batstr." Z, RUNTIME_OBJECT_MISSING);
#define prepareOperand2(X,Y,A,B,Z)							\
	if( (X= BATdescriptor(*Y)) == NULL )					\
		throw(MAL, "batstr." Z, RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){					\
		BBPreleaseref(X->batCacheid);						\
		throw(MAL, "batstr."Z, RUNTIME_OBJECT_MISSING);		\
	}
#define prepareResult(X,Y,T,Z)						\
	X= BATnew(Y->htype,T,BATcount(Y));				\
	if( X == NULL){									\
		BBPreleaseref(Y->batCacheid);				\
		throw(MAL, "batstr." Z, MAL_MALLOC_FAIL);	\
	}												\
	if( Y->htype== TYPE_void)						\
		BATseqbase(X, Y->hseqbase);					\
	X->hsorted=Y->hsorted;							\
	X->hrevsorted=Y->hrevsorted;					\
	X->tsorted=0;									\
	X->trevsorted=0;
#define prepareResult2(X,Y,A,T,Z)					\
	X= BATnew(Y->htype,T,BATcount(Y));				\
	if( Y->htype== TYPE_void)						\
		BATseqbase(X, Y->hseqbase);					\
	if( X == NULL){									\
		BBPreleaseref(Y->batCacheid);				\
		BBPreleaseref(A->batCacheid);				\
		throw(MAL, "batstr." Z, MAL_MALLOC_FAIL);	\
	}												\
	X->hsorted=Y->hsorted;							\
	X->hrevsorted=Y->hrevsorted;					\
	X->tsorted=0;									\
	X->trevsorted=0;
#define finalizeResult(X,Y,Z)									\
	if (!((Y)->batDirty&2)) (Y) = BATsetaccess((Y), BAT_READ);	\
	*X = (Y)->batCacheid;										\
	BBPkeepref(*(X));											\
	BBPreleaseref(Z->batCacheid);

str STRbatLength(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;
	int y, *yp = &y;

	prepareOperand(b,l,"Length");
	prepareResult(bn,b,TYPE_int,"Length");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		x = (str) BUNtail(bi,p);
		if (x== 0 || strcmp(x,str_nil)== 0) {
			y= int_nil;
			bn->T->nonil = 0;
		} else
			strLength(yp,x);
		bunfastins(bn, h, yp);
	}
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Length", OPERATION_FAILED " During bulk operation");
}

str STRbatstringLength(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;
	int y, *yp = &y;

	prepareOperand(b,l,"stringLength");
	prepareResult(bn,b,TYPE_int,"stringLength");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		x = (str) BUNtail(bi,p);
		if (x== 0 || strcmp(x,str_nil)== 0) {
			y= int_nil;
			bn->T->nonil = 0;
		} else
			strSQLLength(yp,x);
		bunfastins(bn, h, yp);
	}
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.stringLength", OPERATION_FAILED " During bulk operation");
}

str STRbatBytes(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;
	int y, *yp = &y;

	prepareOperand(b,l,"Bytes");
	prepareResult(bn,b,TYPE_int,"Bytes");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		x = (str) BUNtail(bi,p);
		if (x== 0 || strcmp(x,str_nil)== 0) {
			y= int_nil;
			bn->T->nonil = 0;
		} else
			strBytes(yp,x);
		bunfastins(bn, h, yp);
	}
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Bytes", OPERATION_FAILED " During bulk operation");
}

str STRbatLower(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;

	prepareOperand(b,l,"Lower");
	prepareResult(bn,b,TYPE_str,"Lower");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi,p);
		if (x != 0 && strcmp(x,str_nil) != 0)
			strLower(yp,x);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Lower", OPERATION_FAILED " During bulk operation");
}

str STRbatUpper(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;

	prepareOperand(b,l,"Upper");
	prepareResult(bn,b,TYPE_str,"Upper");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi,p);
		if (x != 0 && strcmp(x,str_nil) != 0)
			strUpper(yp,x);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Upper", OPERATION_FAILED " During bulk operation");
}

str STRbatStrip(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;

	prepareOperand(b,l,"Strip");
	prepareResult(bn,b,TYPE_str,"Strip");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi,p);
		if (x != 0 && strcmp(x,str_nil) != 0)
			strStrip(yp,x);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Strip", OPERATION_FAILED " During bulk operation");
}

str STRbatLtrim(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;

	prepareOperand(b,l,"Ltrim");
	prepareResult(bn,b,TYPE_str,"Ltrim");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi,p);
		if (x != 0 && strcmp(x,str_nil) != 0)
			strLtrim(yp,x);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Ltrim", OPERATION_FAILED " During bulk operation");
}

str STRbatRtrim(int *ret, int *l)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p,q;
	str x;

	prepareOperand(b,l,"Rtrim");
	prepareResult(bn,b,TYPE_str,"Rtrim");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi,p);
		str y = (str)str_nil, *yp = &y;

		x = (str) BUNtail(bi,p);
		if (x != 0 && strcmp(x,str_nil) != 0)
			strRtrim(yp,x);
		bunfastins(bn, h, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "batstr.Rtrim", OPERATION_FAILED " During bulk operation");
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

str STRbatPrefix(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"prefix");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.startsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"prefix");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRPrefix(vp, &tl, &tr);
		bunfastins(bn, h, vp);
		;
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

str STRbatPrefixcst(int *ret, int *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"prefix");
	prepareResult(bn,left,TYPE_bit,"prefix");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRPrefix(vp, &tl, cst);
		bunfastins(bn, h, vp);
		;
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""prefix", OPERATION_FAILED " During bulk operation");
}

str STRcstPrefixbat(int *ret, str *cst, int *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(right,r,"prefix");
	prepareResult(bn,right,TYPE_bit,"prefix");

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

str STRbatSuffix(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"suffix");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.endsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"suffix");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRSuffix(vp, &tl, &tr);
		bunfastins(bn, h, vp);
		;
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

str STRbatSuffixcst(int *ret, int *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"suffix");
	prepareResult(bn,left,TYPE_bit,"suffix");

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

str STRcstSuffixbat(int *ret, str *cst, int *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(right,r,"suffix");
	prepareResult(bn,right,TYPE_bit,"suffix");

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

str STRbatstrSearch(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"search");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_int,"search");

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

str STRbatstrSearchcst(int *ret, int *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"search");
	prepareResult(bn,left,TYPE_int,"search");

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

str STRcststrSearchbat(int *ret, str *cst, int *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(right,r,"search");
	prepareResult(bn,right,TYPE_bit,"search");

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

str STRbatRstrSearch(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"r_search");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.r_search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"r_search");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRReverseStrSearch(vp, &tl, &tr);
		bunfastins(bn, h, vp);
		;
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

str STRbatRstrSearchcst(int *ret, int *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"r_search");
	prepareResult(bn,left,TYPE_bit,"r_search");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRReverseStrSearch(vp, &tl, cst);
		bunfastins(bn, h, vp);
		;
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""r_search", OPERATION_FAILED " During bulk operation");
}

str STRcstRstrSearchbat(int *ret, str *cst, int *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(right,r,"r_search");
	prepareResult(bn,right,TYPE_bit,"r_search");

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

str STRbatConcat(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	str v, *vp= &v;

	prepareOperand2(left,l,right,r,"+");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.+", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_str,"+");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRConcat(vp, &tl, &tr);
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
	throw(MAL, "batstr." "+", OPERATION_FAILED " During bulk operation");
}

str STRbatConcatcst(int *ret, int *l, str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	str v, *vp= &v;

	prepareOperand(left,l,"+");
	prepareResult(bn,left,TYPE_str,"+");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		str tl = (str) BUNtail(lefti,p);
		STRConcat(vp, &tl, cst);
		bunfastins(bn, h, v);
		GDKfree(v);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""+", OPERATION_FAILED " During bulk operation");
}

str STRcstConcatbat(int *ret, str *cst, int *r)
{
	BATiter righti;
	BAT *bn, *right;
	BUN p,q;
	str v, *vp= &v;

	prepareOperand(right,r,"+");
	prepareResult(bn,right,TYPE_str,"+");

	righti = bat_iterator(right);

	BATloop(right, p, q) {
		ptr h = BUNhead(righti,p);
		str tr = (str) BUNtail(righti,p);
		STRConcat(vp, cst, &tr);
		bunfastins(bn, h, v);
		GDKfree(v);
	}
	bn->T->nonil = 0;
	finalizeResult(ret,bn,right);
	return MAL_SUCCEED;

bunins_failed:
	BBPreleaseref(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr""+", OPERATION_FAILED " During bulk operation");
}

str STRbatTail(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	str v;

	prepareOperand2(left,l,right,r,);
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.string", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_str,);

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
	throw(MAL, "batstr." , OPERATION_FAILED " During bulk operation");
}

str STRbatTailcst(int *ret, int *l, int *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	str v;

	prepareOperand(left,l,);
	prepareResult(bn,left,TYPE_str,);

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
	throw(MAL, "batstr", OPERATION_FAILED " During bulk operation");
}

str STRbatWChrAt(int *ret, int *l, int *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"+");
	if( BATcount(left) != BATcount(right) )
		throw(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
	prepareResult2(bn,left,right,TYPE_bit,"+");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		ptr h = BUNhead(lefti,p);
		ptr tl = BUNtail(lefti,p);
		ptr tr = BUNtail(righti,p);
		strWChrAt(vp, tl, tr);
		bunfastins(bn, h, vp);
		;
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

str STRbatWChrAtcst(int *ret, int *l, int *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"+");
	prepareResult(bn,left,TYPE_bit,"+");

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
STRbatSubstitutecst(int *ret, int *l, str *arg2, str *arg3, bit *rep)
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
 * The pattern matching routine is optimized for SQL pattern structures.
 */
#define percent "\001"
#define underscore "\002"
batstr_export str STRbatlike_uselect(int *ret, int *bid, str *pat, str *esc);
str
STRbatlike_uselect(int *ret, int *bid, str *pat, str *esc)
{
	BATiter bi;
	BAT *b,*bn;
	BUN p, q;
	oid o = oid_nil;

	if( (b= BATdescriptor(*bid)) == NULL)
		throw(MAL, "batstr.like", RUNTIME_OBJECT_MISSING);
	bn= BATnew(BAThtype(b),TYPE_void, BATcount(b)/10+5);
	BATseqbase(BATmirror(b),o);
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	bn->tsorted = 1;
	bn->trevsorted = 1;

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		ptr t = BUNtail(bi, p);

		if (STRlike((str) t, *pat, *esc))
			bunfastins(bn, h, &o);
	}
	bn->T->nonil = 0;
bunins_failed:
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}
batstr_export str STRbatlike_uselect2(int *ret, int *bid, str *pat);
str
STRbatlike_uselect2(int *ret, int *bid, str *pat)
{
	str esc="";
	return STRbatlike_uselect(ret,bid,pat,&esc);
}
/*
 * The substring functions require slightly different arguments
 */
batstr_export str STRbatsubstringcst(int *ret, int *bid, int *start, int *length);
str
STRbatsubstringcst(int *ret, int *bid, int *start, int *length)
{
	BATiter bi;
	BAT *b,*bn;
	BUN p, q;
	str res;
	char *msg = MAL_SUCCEED;

	if( (b= BATdescriptor(*bid)) == NULL)
		throw(MAL, "batstr.substring",RUNTIME_OBJECT_MISSING);
	bn= BATnew(TYPE_void, TYPE_str, BATcount(b)/10+5);
	BATseqbase(bn, 0);
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

batstr_export str STRbatsubstring(int *ret, int *l, int *r, int *t);
str STRbatsubstring(int *ret, int *l, int *r, int *t)
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
	BATseqbase(bn, 0);
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

batstr_export str STRbatreplace(int *ret, int *l, str *pat, str *s2);
str STRbatreplace(int *ret, int *l, str *pat, str *s2)
{
	BATiter li;
	BAT *bn, *left;
	BUN p,q;
	str v, *vp= &v;

	if( (left= BATdescriptor(*l)) == NULL )
		throw(MAL, "batstr.replace" , RUNTIME_OBJECT_MISSING);
	bn= BATnew(TYPE_void, TYPE_str,BATcount(left));
	BATseqbase(bn, 0);
	if (bn == NULL){
		BBPreleaseref(left->batCacheid);
		throw(MAL, "batstr.replace", MAL_MALLOC_FAIL);
	}
	bn->hsorted= left->hsorted;
	bn->hrevsorted= left->hrevsorted;
	bn->tsorted=0;
	bn->trevsorted=0;

	li = bat_iterator(left);
	BATloop(left, p, q) {
		str tl = (str) BUNtail(li,p);
		STRreplace(vp, &tl, pat, s2);
		BUNappend(bn, *vp, FALSE);
		GDKfree(*vp);
	}
        if (left->htype != bn->htype) {
                BAT *r = VIEWcreate(left,bn);

                BBPreleaseref(bn->batCacheid);
                bn = r;
        }
	bn->T->nonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;
}


