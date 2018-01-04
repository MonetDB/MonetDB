/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* This file is included multiple times (from sql_cast.c).
 * We expect the tokens TP1 & TP2 to be defined by the including file.
 */

/* ! ENSURE THAT THESE LOCAL MACROS ARE UNDEFINED AT THE END OF THIS FILE ! */

/* stringify token */
#define _STRNG_(s) #s
#define STRNG(t) _STRNG_(t)

/* concatenate two or four tokens */
#define CONCAT_2(a,b)     a##b
#define CONCAT_3(a,b,c)   a##b##c
#define CONCAT_4(a,b,c,d) a##b##c##d

#define NIL(t)       CONCAT_2(t,_nil)
#define ISNIL(t)     CONCAT_3(is_,t,_nil)
#define TPE(t)       CONCAT_2(TYPE_,t)
#define FUN(a,b,c,d) CONCAT_4(a,b,c,d)


str
FUN(,TP1,_dec2_,TP2) (TP2 *res, const int *s1, const TP1 *v)
{
	int scale = *s1;
	TP2 r;

	/* shortcut nil */
	if (ISNIL(TP1)(*v)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	/* since the TP2 type is bigger than or equal to the TP1 type, it will
	   always fit */
	r = (TP2) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
FUN(,TP1,_dec2dec_,TP2) (TP2 *res, const int *S1, const TP1 *v, const int *d2, const int *S2)
{
	int p = *d2, inlen = 1;
	TP1 cpyval = *v;
	int s1 = *S1, s2 = *S2;
	TP2 r;

	/* shortcut nil */
	if (ISNIL(TP1)(*v)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", SQLSTATE(22003) "too many digits (%d > %d)", inlen, p);
	}

	/* since the TP2 type is bigger than or equal to the TP1 type, it will
	   always fit */
	r = (TP2) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
FUN(,TP1,_num2dec_,TP2) (TP2 *res, const TP1 *v, const int *d2, const int *s2)
{
	int zero = 0;
	return FUN(,TP1,_dec2dec_,TP2)(res, &zero, v, d2, s2);
}

str
FUN(bat,TP1,_dec2_,TP2) (bat *res, const int *s1, const bat *bid)
{
	BAT *b, *bn;
	TP1 *p, *q;
	char *msg = NULL;
	int scale = *s1;
	TP2 *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	o = (TP2 *) Tloc(bn, 0);
	p = (TP1 *) Tloc(b, 0);
	q = (TP1 *) Tloc(b, BUNlast(b));
	bn->tnonil = 1;
	if (b->tnonil) {
		for (; p < q; p++, o++)
			*o = (((TP2) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (ISNIL(TP1)(*p)) {
				*o = NIL(TP2);
				bn->tnonil = FALSE;
			} else
				*o = (((TP2) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(bn, FALSE);

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
FUN(bat,TP1,_dec2dec_,TP2) (bat *res, const int *S1, const bat *bid, const int *d2, const int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		TP1 *v = (TP1 *) BUNtail(bi, p);
		TP2 r;
		msg = FUN(,TP1,_dec2dec_,TP2)(&r, S1, v, d2, S2);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const int *d2, const int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		TP1 *v = (TP1 *) BUNtail(bi, p);
		TP2 r;
		msg = FUN(,TP1,_num2dec_,TP2)(&r, v, d2, s2);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}


/* undo local defines */
#undef FUN
#undef ISNIL
#undef NIL
#undef TPE
#undef CONCAT_2
#undef CONCAT_3
#undef CONCAT_4
#undef STRNG
#undef _STRNG_

