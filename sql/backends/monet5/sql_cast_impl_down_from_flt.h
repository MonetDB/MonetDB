/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* This file is included multiple times (from sql_cast.c).
 * We expect the tokens TP1 & TP2 to be defined by the including file.
 */

/* ! ENSURE THAT THESE LOCAL MACROS ARE UNDEFINED AT THE END OF THIS FILE ! */

/* stringify token */
#define _STRNG_(s) #s
#define STRNG(t) _STRNG_(t)

/* concatenate two, three or four tokens */
#define CONCAT_2(a,b)     a##b
#define CONCAT_3(a,b,c)   a##b##c
#define CONCAT_4(a,b,c,d) a##b##c##d

#define NIL(t)       CONCAT_2(t,_nil)
#define ISNIL(t)     CONCAT_3(is_,t,_nil)
#define TPE(t)       CONCAT_2(TYPE_,t)
#define GDKmin(t)    CONCAT_3(GDK_,t,_min)
#define GDKmax(t)    CONCAT_3(GDK_,t,_max)
#define FUN(a,b,c,d) CONCAT_4(a,b,c,d)


/* when casting a floating point to a decimal we like to preserve the
 * precision.  This means we first scale the float before converting.
*/
str
FUN(,TP1,_num2dec_,TP2)(TP2 *res, const TP1 *v, const int *d2, const int *s2)
{
	TP1 val = *v;
	int scale = *s2;
	int precision = *d2;
	int inlen;

	if (ISNIL(TP1)(val)) {
		*res = NIL(TP2);
		return MAL_SUCCEED;
	}

	if (val <= -1) {
		/* (-Inf, -1] */
		inlen = (int) floor(log10(-val)) + 1;
	} else if (val < 1) {
		/* (-1, 1) */
		inlen = 1;
	} else {
		/* [1, Inf) */
		inlen = (int) floor(log10(val)) + 1;
	}
	if (inlen + scale > precision)
		throw(SQL, "convert", SQLSTATE(22003) "too many digits (%d > %d)",
		      inlen + scale, precision);

#ifndef TRUNCATE_NUMBERS
	*res = (TP2) round_float(val * scales[scale]);
#endif


	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const int *d2, const int *s2)
{
	BAT *b, *dst;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	const TP1 *v = (const TP1 *) Tloc(b, 0);
	BATloop(b, p, q) {
		TP2 r;
		msg = FUN(,TP1,_num2dec_,TP2) (&r, v, d2, s2);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		v++;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
FUN(bat,TP1,_ce_num2dec_,TP2) (bat *res, const bat *bid, const int *d2, const int *s2, const bat *r)
{
	BAT *b, *c, *dst;
	BUN p, q;
	char *msg = NULL;
	bit *ce;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((c = BATdescriptor(*r)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	ce = Tloc(c, 0);
	dst = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(c->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	const TP1 *v = (const TP1 *) Tloc(b, 0);
	BATloop(b, p, q) {
		TP2 r;
		if (ce[p])
			msg = FUN(,TP1,_num2dec_,TP2) (&r, v, d2, s2);
		else {
			r = NIL(TP2);
			dst->tnonil = false;
		}
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		v++;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(c->batCacheid);
	return msg;
}


/* undo local defines */
#undef FUN
#undef NIL
#undef ISNIL
#undef TPE
#undef GDKmin
#undef GDKmax
#undef CONCAT_2
#undef CONCAT_3
#undef CONCAT_4
#undef STRNG
#undef _STRNG_

