/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

static inline str
FUN(do_,TP1,_dec2dec_,TP2) (TP2 *restrict res, int s1, TP1 val, int p, int s2)
{
	ValRecord v1, v2;

	VALset(&v1, TPE(TP1), &val);
	v2.vtype = TPE(TP2);
	if (VARconvert(&v2, &v1, s1, s2, p) != GDK_SUCCEED)
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
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), *s1, 0, 0);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(dec,TP1,_2_,TP2)), GDK_EXCEPTION);
	*res = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_dec2dec_,TP2) (bat *res, const int *S1, const bat *bid, const bat *sid, const int *d2, const int *S2)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), *S1, *S2, *d2);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), GDK_EXCEPTION);

	*res = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}
#endif

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const bat *sid, const int *d2, const int *s2)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TPE(TP2), 0, *s2, *d2);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), GDK_EXCEPTION);
	*res = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}
