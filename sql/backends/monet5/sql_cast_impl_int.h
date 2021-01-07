/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* ! ENSURE THAT THESE LOCAL MACROS ARE UNDEFINED AT THE END OF THIS FILE ! */

/* stringify token */
#define _STRNG_(s) #s
#define STRNG(t) _STRNG_(t)

/* concatenate two, three or four tokens */
#define CONCAT_2(a,b)		a##b
#define CONCAT_3(a,b,c)		a##b##c
#define CONCAT_4(a,b,c,d)	a##b##c##d

#define NIL(t)				CONCAT_2(t,_nil)
#define ISNIL(t)			CONCAT_3(is_,t,_nil)
#define TPE(t)				CONCAT_2(TYPE_,t)
#define GDKmin(t)			CONCAT_3(GDK_,t,_min)
#define GDKmax(t)			CONCAT_3(GDK_,t,_max)
#define FUN(a,b,c,d)		CONCAT_4(a,b,c,d)
#define IS_NUMERIC(t)		CONCAT_2(t,_is_numeric)

static inline str
FUN(do_,TP1,_dec2dec_,TP2) (TP2 *restrict res, int s1, TP1 val, int p, int s2)
{
	ValRecord v1, v2;

	VALset(&v1, TPE(TP1), &val);
	v2.vtype = TPE(TP2);
	if (VARconvert(&v2, &v1, true, s1, s2, p) != GDK_SUCCEED)
		throw(SQL, STRNG(FUN(,TP1,_2_,TP2)), GDK_EXCEPTION);
	*res = *(TP2 *) VALptr(&v2);
	return MAL_SUCCEED;
}

#if IS_NUMERIC(TP1)
str
FUN(,TP1,_dec2_,TP2) (TP2 *res, const int *s1, const TP1 *v)
{
	return FUN(do_,TP1,_dec2dec_,TP2) (res, *s1, *v, 0, 0);
}

str
FUN(,TP1,_dec2dec_,TP2) (TP2 *res, const int *S1, const TP1 *v, const int *d2, const int *S2)
{
	return FUN(do_,TP1,_dec2dec_,TP2) (res, *S1, *v, *d2, *S2);
}
#endif

str
FUN(,TP1,_num2dec_,TP2) (TP2 *res, const TP1 *v, const int *d2, const int *s2)
{
	return FUN(do_,TP1,_dec2dec_,TP2)(res, 0, *v, *d2, *s2);
}

#if IS_NUMERIC(TP1)
str
FUN(bat,TP1,_dec2_,TP2) (bat *res, const int *s1, const bat *bid, const bat *sid)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), true, *s1, 0, 0);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(dec,TP1,_2_,TP2)), GDK_EXCEPTION);
	BBPkeepref(*res = bn->batCacheid);
	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_dec2dec_,TP2) (bat *res, const int *S1, const bat *bid, const bat *sid, const int *d2, const int *S2)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), true, *S1, *S2, *d2);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), GDK_EXCEPTION);

	BBPkeepref(*res = bn->batCacheid);
	return MAL_SUCCEED;
}
#endif

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const bat *sid, const int *d2, const int *s2)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), true, 0, *s2, *d2);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), GDK_EXCEPTION);
	BBPkeepref(*res = bn->batCacheid);
	return MAL_SUCCEED;
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
