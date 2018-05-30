/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *  M.L. Kersten
 * String multiplexes
 * [TODO: property propagations]
 * The collection of routines provided here are map operations
 * for the atom string primitives.
 *
 * In line with the batcalc module, we assume that if two bat operands
 * are provided that they are aligned.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <ctype.h>
#include <string.h>
#include "mal_exception.h"
#include "str.h"

mal_export str STRbatPrefix(bat *ret, const bat *l, const bat *r);
mal_export str STRbatPrefixcst(bat *ret, const bat *l, const str *cst);
mal_export str STRbatSuffix(bat *ret, const bat *l, const bat *r);
mal_export str STRbatSuffixcst(bat *ret, const bat *l, const str *cst);
mal_export str STRbatstrSearch(bat *ret, const bat *l, const bat *r);
mal_export str STRbatstrSearchcst(bat *ret, const bat *l, const str *cst);
mal_export str STRbatRstrSearch(bat *ret, const bat *l, const bat *r);
mal_export str STRbatRstrSearchcst(bat *ret, const bat *l, const str *cst);
mal_export str STRbatTail(bat *ret, const bat *l, const bat *r);
mal_export str STRbatTailcst(bat *ret, const bat *l, const int *cst);
mal_export str STRbatWChrAt(bat *ret, const bat *l, const bat *r);
mal_export str STRbatWChrAtcst(bat *ret, const bat *l, const int *cst);
mal_export str STRbatSubstitutecst(bat *ret, const bat *l, const str *arg2, const str *arg3, const bit *rep);

mal_export str STRbatLower(bat *ret, const bat *l);
mal_export str STRbatUpper(bat *ret, const bat *l);
mal_export str STRbatStrip(bat *ret, const bat *l);
mal_export str STRbatLtrim(bat *ret, const bat *l);
mal_export str STRbatRtrim(bat *ret, const bat *l);
mal_export str STRbatStrip2_const(bat *ret, const bat *l, const str *s2);
mal_export str STRbatLtrim2_const(bat *ret, const bat *l, const str *s2);
mal_export str STRbatRtrim2_const(bat *ret, const bat *l, const str *s2);
mal_export str STRbatStrip2_bat(bat *ret, const bat *l, const bat *l2);
mal_export str STRbatLtrim2_bat(bat *ret, const bat *l, const bat *l2);
mal_export str STRbatRtrim2_bat(bat *ret, const bat *l, const bat *l2);

mal_export str STRbatLpad_const(bat *ret, const bat *l, const int *n);
mal_export str STRbatRpad_const(bat *ret, const bat *l, const int *n);
mal_export str STRbatLpad_bat(bat *ret, const bat *l, const bat *n);
mal_export str STRbatRpad_bat(bat *ret, const bat *l, const bat *n);
mal_export str STRbatLpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2);
mal_export str STRbatRpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2);
mal_export str STRbatLpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2);
mal_export str STRbatRpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2);
mal_export str STRbatLpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2);
mal_export str STRbatRpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2);
mal_export str STRbatLpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2);
mal_export str STRbatRpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2);

mal_export str STRbatLength(bat *ret, const bat *l);
mal_export str STRbatBytes(bat *ret, const bat *l);

mal_export str STRbatsubstringcst(bat *ret, const bat *bid, const int *start, const int *length);
mal_export str STRbatsubstring(bat *ret, const bat *l, const bat *r, const bat *t);


#define prepareOperand(X,Y,Z)									\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
#define prepareOperand2(X,Y,A,B,Z)								\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}
#define prepareOperand3(X,Y,A,B,I,J,Z)							\
	if( (X= BATdescriptor(*Y)) == NULL )						\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	if( (A= BATdescriptor(*B)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}															\
	if( (I= BATdescriptor(*J)) == NULL ){						\
		BBPunfix(X->batCacheid);								\
		BBPunfix(A->batCacheid);								\
		throw(MAL, Z, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}
#define prepareResult(X,Y,T,Z)							\
	X= COLnew((Y)->hseqbase,T,BATcount(Y), TRANSIENT);	\
	if( X == NULL){										\
		BBPunfix(Y->batCacheid);						\
		throw(MAL, Z, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}													\
	X->tsorted=0;										\
	X->trevsorted=0;
#define prepareResult2(X,Y,A,T,Z)						\
	X= COLnew((Y)->hseqbase,T,BATcount(Y), TRANSIENT);	\
	if( X == NULL){										\
		BBPunfix(Y->batCacheid);						\
		BBPunfix(A->batCacheid);						\
		throw(MAL, Z, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}													\
	X->tsorted=0;										\
	X->trevsorted=0;
#define finalizeResult(X,Y,Z)								\
	*X = (Y)->batCacheid;									\
	BBPkeepref(*(X));										\
	BBPunfix(Z->batCacheid);

static str
do_batstr_int(bat *ret, const bat *l, const char *name, str (*func)(int *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x;
	int y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_int, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		x = (str) BUNtail(bi, p);
		if (x == 0 || strcmp(x, str_nil) == 0) {
			y = int_nil;
			bn->tnonil = 0;
			bn->tnil = 1;
		} else if ((msg = (*func)(&y, &x)) != MAL_SUCCEED) {
			goto bunins_failed;
		}
		bunfastapp(bn, &y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

str
STRbatLength(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.Length", STRLength);
}

str
STRbatBytes(bat *ret, const bat *l)
{
	return do_batstr_int(ret, l, "batstr.Bytes", STRBytes);
}

static str
do_batstr_str(bat *ret, const bat *l, const char *name, str (*func)(str *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l' and a constant string 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_conststr_str(bat *ret, const bat *l, const str *s2, const char *name, str (*func)(str *, const str *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x, s2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: two BATs of strings 'l' and 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batstr_str(bat *ret, const bat *l, const bat *l2, const char *name, str (*func)(str *, const str *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	str x, x2, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, l2, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn, b, b2, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		x2 = (str) BUNtail(bi2, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			x2 != 0 && strcmp(x2, str_nil) != 0 &&
			(msg = (*func)(&y, &x, &x2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l' and a constant int 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_str(bat *ret, const bat *l, const int *n, const char *name, str (*func)(str *, const str *, const int *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x, n)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l' and a BAT of integers 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_str(bat *ret, const bat *l, const bat *n, const char *name, str (*func)(str *, const str *, const int *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	int nn;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, n, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn, b, b2, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		nn = *(int *)BUNtail(bi2, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x, &nn)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a constant int 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_conststr_str(bat *ret, const bat *l, const int *n, const str *s2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand(b, l, name);
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x, n, s2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a BAT of integers 'n' and a constant str 's2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_conststr_str(bat *ret, const bat *l, const bat *n, const str *s2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	int nn;
	str x, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, n, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		nn = *(int *)BUNtail(bi2, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(msg = (*func)(&y, &x, &nn, s2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a constant int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_batstr_str(bat *ret, const bat *l, const int *n, const bat *l2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2;
	BAT *bn, *b, *b2;
	BUN p, q;
	str x, x2, y;
	str msg = MAL_SUCCEED;

	prepareOperand2(b, l, b2, l2, name);
	if(BATcount(b) != BATcount(b2)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult(bn, b, TYPE_str, name);

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		x2 = (str) BUNtail(bi2, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			x2 != 0 && strcmp(x2, str_nil) != 0 &&
			(msg = (*func)(&y, &x, n, &x2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

/* Input: a BAT of strings 'l', a BAT of int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_batstr_str(bat *ret, const bat *l, const bat *n, const bat *l2, const char *name, str (*func)(str *, const str *, const int *, const str *))
{
	BATiter bi, bi2, bi3;
	BAT *bn, *b, *b2, *b3;
	BUN p, q;
	int nn;
	str x, x2, y;
	str msg = MAL_SUCCEED;


	prepareOperand3(b, l, b2, n, b3, l2, name);
	if (BATcount(b) != BATcount(b2) || BATcount(b) != BATcount(b3)) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		BBPunfix(b3->batCacheid);
		throw(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(b2->batCacheid);
		BBPunfix(b3->batCacheid);
		throw(MAL, name, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tsorted=0;
	bn->trevsorted=0;

	bi = bat_iterator(b);
	bi2 = bat_iterator(b2);
	bi3 = bat_iterator(b3);

	BATloop(b, p, q) {
		y = NULL;
		x = (str) BUNtail(bi, p);
		nn = *(int *)BUNtail(bi2, p);
		x2 = (str) BUNtail(bi3, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			x2 != 0 && strcmp(x2, str_nil) != 0 &&
			(msg = (*func)(&y, &x, &nn, &x2)) != MAL_SUCCEED)
			goto bunins_failed1;
		if (y == NULL)
			y = (str) str_nil;
		bunfastapp(bn, y);
		if (y == str_nil) {
			bn->tnonil = 0;
			bn->tnil = 1;
		} else
			GDKfree(y);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (y != str_nil)
		GDKfree(y);
bunins_failed1:
	BBPunfix(b->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPunfix(b3->batCacheid);
	BBPunfix(bn->batCacheid);
	if (msg != MAL_SUCCEED)
		return msg;
	throw(MAL, name, OPERATION_FAILED " During bulk operation");
}

str
STRbatLower(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.Lower", STRLower);
}

str
STRbatUpper(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.Upper", STRUpper);
}

str
STRbatStrip(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.Strip", STRStrip);
}

str
STRbatLtrim(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.Ltrim", STRLtrim);
}

str
STRbatRtrim(bat *ret, const bat *l)
{
	return do_batstr_str(ret, l, "batstr.Rtrim", STRRtrim);
}

str
STRbatStrip2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.Strip", STRStrip2);
}

str
STRbatLtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.Ltrim", STRLtrim2);
}

str
STRbatRtrim2_const(bat *ret, const bat *l, const str *s2)
{
	return do_batstr_conststr_str(ret, l, s2, "batstr.Rtrim", STRRtrim2);
}

str
STRbatStrip2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Strip", STRStrip2);
}

str
STRbatLtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Ltrim", STRLtrim2);
}

str
STRbatRtrim2_bat(bat *ret, const bat *l, const bat *l2)
{
	return do_batstr_batstr_str(ret, l, l2, "batstr.Rtrim", STRRtrim2);
}

str
STRbatLpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.Lpad", STRLpad);
}

str
STRbatRpad_const(bat *ret, const bat *l, const int *n)
{
	return do_batstr_constint_str(ret, l, n, "batstr.Rpad", STRRpad);
}

str
STRbatLpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.Lpad", STRLpad);
}

str
STRbatRpad_bat(bat *ret, const bat *l, const bat *n)
{
	return do_batstr_batint_str(ret, l, n, "batstr.Rpad", STRRpad);
}

str
STRbatLpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.Lpad", STRLpad2);
}

str
STRbatRpad2_const_const(bat *ret, const bat *l, const int *n, const str *s2)
{
	return do_batstr_constint_conststr_str(ret, l, n, s2, "batstr.Rpad", STRRpad2);
}

str
STRbatLpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.Lpad", STRLpad2);
}

str
STRbatRpad2_bat_const(bat *ret, const bat *l, const bat *n, const str *s2)
{
	return do_batstr_batint_conststr_str(ret, l, n, s2, "batstr.Rpad", STRRpad2);
}

str
STRbatLpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.Lpad", STRLpad2);
}

str
STRbatRpad2_const_bat(bat *ret, const bat *l, const int *n, const bat *l2)
{
	return do_batstr_constint_batstr_str(ret, l, n, l2, "batstr.Rpad", STRRpad2);
}

str
STRbatLpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.Lpad", STRLpad2);
}

str
STRbatRpad2_bat_bat(bat *ret, const bat *l, const bat *n, const bat *l2)
{
	return do_batstr_batint_batstr_str(ret, l, n, l2, "batstr.Rpad", STRRpad2);
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

str STRbatPrefix(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.startsWith");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.startsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_bit,"batstr.startsWith");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRPrefix(vp, &tl, &tr);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.startsWith", OPERATION_FAILED " During bulk operation");
}

str STRbatPrefixcst(bat *ret, const bat *l, const str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"batstr.startsWith");
	prepareResult(bn,left,TYPE_bit,"batstr.startsWith");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		STRPrefix(vp, &tl, cst);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.startsWith", OPERATION_FAILED " During bulk operation");
}

str STRbatSuffix(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.endsWith");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.endsWith", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_bit,"batstr.endsWith");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRSuffix(vp, &tl, &tr);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.endsWith", OPERATION_FAILED " During bulk operation");
}

str STRbatSuffixcst(bat *ret, const bat *l, const str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	bit v, *vp= &v;

	prepareOperand(left,l,"batstr.endsWith");
	prepareResult(bn,left,TYPE_bit,"batstr.endsWith");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		STRSuffix(vp, &tl, cst);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.endsWith", OPERATION_FAILED " During bulk operation");
}

str STRbatstrSearch(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.search");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_int,"batstr.search");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRstrSearch(vp, &tl, &tr);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.search", OPERATION_FAILED " During bulk operation");
}

str STRbatstrSearchcst(bat *ret, const bat *l, const str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.search");
	prepareResult(bn,left,TYPE_int,"batstr.search");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		STRstrSearch(vp, &tl, cst);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.search", OPERATION_FAILED " During bulk operation");
}

str STRbatRstrSearch(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.r_search");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.r_search", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_int,"batstr.r_search");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		str tr = (str) BUNtail(righti,p);
		STRReverseStrSearch(vp, &tl, &tr);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.r_search", OPERATION_FAILED " During bulk operation");
}

str STRbatRstrSearchcst(bat *ret, const bat *l, const str *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.r_search");
	prepareResult(bn,left,TYPE_int,"batstr.r_search");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		STRReverseStrSearch(vp, &tl, cst);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.r_search", OPERATION_FAILED " During bulk operation");
}

str STRbatTail(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	str v;
	str msg = MAL_SUCCEED;

	prepareOperand2(left,l,right,r,"batstr.string");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.string", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_str,"batstr.string");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		int *tr = (int *) BUNtail(righti,p);
		if ((msg = STRTail(&v, &tl, tr)) != MAL_SUCCEED)
			goto bunins_failed;
		bunfastapp(bn, v);
		GDKfree(v);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	if (msg)
		return msg;
	GDKfree(v);
	throw(MAL, "batstr.string" , OPERATION_FAILED " During bulk operation");
}

str STRbatTailcst(bat *ret, const bat *l, const int *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	str v;
	str msg = MAL_SUCCEED;

	prepareOperand(left,l,"batstr.string");
	prepareResult(bn,left,TYPE_str,"batstr.string");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		if ((msg = STRTail(&v, &tl, cst)) != MAL_SUCCEED)
			goto bunins_failed;
		bunfastapp(bn, v);
		GDKfree(v);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	if (msg)
		return msg;
	GDKfree(v);
	throw(MAL, "batstr.string", OPERATION_FAILED " During bulk operation");
}

str STRbatWChrAt(bat *ret, const bat *l, const bat *r)
{
	BATiter lefti, righti;
	BAT *bn, *left, *right;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand2(left,l,right,r,"batstr.unicodeAt");
	if(BATcount(left) != BATcount(right)) {
		BBPunfix(left->batCacheid);
		BBPunfix(right->batCacheid);
		throw(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}
	prepareResult2(bn,left,right,TYPE_int,"batstr.unicodeAt");

	lefti = bat_iterator(left);
	righti = bat_iterator(right);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		ptr tr = BUNtail(righti,p);
		STRWChrAt(vp, &tl, tr);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	BBPunfix(right->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.unicodeAt", OPERATION_FAILED " During bulk operation");
}

str STRbatWChrAtcst(bat *ret, const bat *l, const int *cst)
{
	BATiter lefti;
	BAT *bn, *left;
	BUN p,q;
	int v, *vp= &v;

	prepareOperand(left,l,"batstr.unicodeAt");
	prepareResult(bn,left,TYPE_int,"batstr.unicodeAt");

	lefti = bat_iterator(left);

	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		STRWChrAt(vp, &tl, cst);
		bunfastapp(bn, vp);
	}
	bn->tnonil = 0;
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;

bunins_failed:
	BBPunfix(left->batCacheid);
	BBPunfix(*ret);
	throw(MAL, "batstr.unicodeAt", OPERATION_FAILED " During bulk operation");
}

str
STRbatSubstitutecst(bat *ret, const bat *l, const str *arg2, const str *arg3, const bit *rep)
{
	BATiter bi;
	BAT *bn, *b;
	BUN p, q;
	str x;
	str y;
	str err = MAL_SUCCEED;

	prepareOperand(b, l, "subString");
	prepareResult(bn, b, TYPE_int, "subString");

	bi = bat_iterator(b);

	BATloop(b, p, q) {
		y = (str) str_nil;
		x = (str) BUNtail(bi, p);
		if (x != 0 && strcmp(x, str_nil) != 0 &&
			(err = STRSubstitute(&y, &x, arg2, arg3, rep)) != MAL_SUCCEED)
			goto bunins_failed;
		bunfastapp(bn, y);
		if (y != str_nil)
			GDKfree(y);
	}
	bn->tnonil = 0;
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	if (err == MAL_SUCCEED && y != str_nil)
		GDKfree(y);
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (err)
		return err;
	throw(MAL, "batstr.subString", OPERATION_FAILED " During bulk operation");
}

/*
 * The substring functions require slightly different arguments
 */
str
STRbatsubstringcst(bat *ret, const bat *bid, const int *start, const int *length)
{
	BATiter bi;
	BAT *b,*bn;
	BUN p, q;
	str res;
	char *msg = MAL_SUCCEED;

	if( (b= BATdescriptor(*bid)) == NULL)
		throw(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn= COLnew(b->hseqbase, TYPE_str, BATcount(b)/10+5, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batstr.substring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;

	bi = bat_iterator(b);
	BATloop(b, p, q) {
		str t =  (str) BUNtail(bi, p);

		if ((msg = STRsubstring(&res, &t, start, length)) != MAL_SUCCEED ||
			BUNappend(bn, (ptr)res, FALSE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			if (msg != MAL_SUCCEED)
				return msg;
			GDKfree(res);
			throw(MAL, "batstr.substring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(res);
	}

	bn->tnonil = 0;
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str STRbatsubstring(bat *ret, const bat *l, const bat *r, const bat *t)
{
	BATiter lefti, starti, lengthi;
	BAT *bn, *left, *start, *length;
	BUN p,q;
	str v;

	if( (left= BATdescriptor(*l)) == NULL )
		throw(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if( (start= BATdescriptor(*r)) == NULL ){
		BBPunfix(left->batCacheid);
		throw(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if( (length= BATdescriptor(*t)) == NULL ){
		BBPunfix(left->batCacheid);
		BBPunfix(start->batCacheid);
		throw(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATcount(left) != BATcount(start) ||
		BATcount(left) != BATcount(length)) {
		BBPunfix(left->batCacheid);
		BBPunfix(start->batCacheid);
		BBPunfix(length->batCacheid);
		throw(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
	}

	bn= COLnew(left->hseqbase, TYPE_str,BATcount(left), TRANSIENT);
	if( bn == NULL){
		BBPunfix(left->batCacheid);
		BBPunfix(start->batCacheid);
		BBPunfix(length->batCacheid);
		throw(MAL, "batstr.substring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	bn->tsorted=0;
	bn->trevsorted=0;

	lefti = bat_iterator(left);
	starti = bat_iterator(start);
	lengthi = bat_iterator(length);
	BATloop(left, p, q) {
		str tl = (str) BUNtail(lefti,p);
		int *t1 = (int *) BUNtail(starti,p);
		int *t2 = (int *) BUNtail(lengthi,p);
		str msg;
		if ((msg = STRsubstring(&v, &tl, t1, t2)) != MAL_SUCCEED ||
			BUNappend(bn, v, FALSE) != GDK_SUCCEED) {
			BBPunfix(left->batCacheid);
			BBPunfix(start->batCacheid);
			BBPreclaim(bn);
			if (msg)
				return msg;
			GDKfree(v);
			throw(MAL, "batstr.substring", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(v);
	}
	bn->tnonil = 0;
	BBPunfix(start->batCacheid);
	BBPunfix(length->batCacheid);
	finalizeResult(ret,bn,left);
	return MAL_SUCCEED;
}
