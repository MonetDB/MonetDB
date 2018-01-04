/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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


/* There are two compile-time options in this code: DOWNCAST and
 * TRUNCATE_NUMBERS.  If DOWNCAST is defined, additional checks are
 * done to see whether the result fits since the result type is
 * smaller than the source type.  If TRUNCATE_NUMBERS is defined,
 * values are truncated towards zero instead of rounded.  DOWNCAST
 * should be defined by the file that includes this file. */
static inline str
FUN(do_,TP1,_dec2dec_,TP2) (TP2 *restrict res, int s1, TP1 val, int p, int s2)
{
#ifndef DOWNCAST
	TP2 r = (TP2) val;
#endif

#ifdef DOWNCAST
	if (s2 > s1) {
		if (val < GDKmin(TP2) / scales[s2 - s1] ||
		    val > GDKmax(TP2) / scales[s2 - s1]) {
			char *buf = NULL, *msg;
			size_t len = 0;
			if (BATatoms[TPE(TP1)].atomToStr(&buf, &len, &val) < 0)
				msg = createException(SQL, "convert", SQLSTATE(22003) "value exceeds limits of type "STRNG(TP2));
			else
				msg = createException(SQL, "convert", SQLSTATE(22003) "value (%s%0*d) exceeds limits of type "STRNG(TP2), buf, s2 - s1, 0);
			GDKfree(buf);
			return msg;
		}
		val *= (TP1) scales[s2 - s1];
	} else if (s2 < s1) {
		if (val / scales[s1 - s2] < GDKmin(TP2) ||
		    val / scales[s1 - s2] > GDKmax(TP2)) {
			char *buf = NULL, *msg;
			size_t len = 0;
			if (BATatoms[TPE(TP1)].atomToStr(&buf, &len, &val) < 0)
				msg = createException(SQL, "convert", SQLSTATE(22003) "Value exceeds limits of type "STRNG(TP2));
			else
				msg = createException(SQL, "convert", SQLSTATE(22003) "Value (%.*s) exceeds limits of type "STRNG(TP2), s1 - s2, buf);
			GDKfree(buf);
			return msg;
		}
		val = (TP1) ((val
#ifndef TRUNCATE_NUMBERS
			      + (val < 0 ? -5 : 5) * scales[s1 - s2 - 1]
#endif
				     ) / scales[s1 - s2]);
	} else if (val < GDKmin(TP2) || val > GDKmax(TP2)) {
		char *buf = NULL, *msg;
		size_t len = 0;
		if (BATatoms[TPE(TP1)].atomToStr(&buf, &len, &val) < 0)
			msg = createException(SQL, "convert", SQLSTATE(22003) "Value exceeds limits of type "STRNG(TP2));
		else
			msg = createException(SQL, "convert", SQLSTATE(22003) "Value (%s) exceeds limits of type "STRNG(TP2), buf);
		GDKfree(buf);
		return msg;
	}
	*res = (TP2) val;
#else
	if (s2 > s1) {
		r *= (TP2) scales[s2 - s1];
	} else if (s2 < s1) {
		r = (TP2) ((r
#ifndef TRUNCATE_NUMBERS
			      + (val < 0 ? -5 : 5) * scales[s1 - s2 - 1]
#endif
				     ) / scales[s1 - s2]);
	}
	*res = r;
#endif
	if (p) {
		TP2 cpyval = *res;
		int inlen = 1;

		/* count the number of digits in the input */
		while (cpyval /= 10)
			inlen++;
		/* rounding is allowed */
		if (inlen > p) {
			throw(SQL, STRNG(FUN(,TP1,_2_,TP2)), SQLSTATE(22003) "Too many digits (%d > %d)", inlen, p);
		}
	}

	return MAL_SUCCEED;
}

str
FUN(,TP1,_dec2_,TP2) (TP2 *res, const int *s1, const TP1 *v)
{
	/* shortcut nil */
	if (ISNIL(TP1)(*v)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	return FUN(do_,TP1,_dec2dec_,TP2) (res, *s1, *v, 0, 0);
}

str
FUN(,TP1,_dec2dec_,TP2) (TP2 *res, const int *S1, const TP1 *v, const int *d2, const int *S2)
{
	/* shortcut nil */
	if (ISNIL(TP1)(*v)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	return FUN(do_,TP1,_dec2dec_,TP2) (res, *S1, *v, *d2, *S2);
}

str
FUN(,TP1,_num2dec_,TP2) (TP2 *res, const TP1 *v, const int *d2, const int *s2)
{
	/* shortcut nil */
	if (ISNIL(TP1)(*v)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	return FUN(do_,TP1,_dec2dec_,TP2)(res, 0, *v, *d2, *s2);
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
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(dec,TP1,_2_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	o = (TP2 *) Tloc(bn, 0);
	p = (TP1 *) Tloc(b, 0);
	q = (TP1 *) Tloc(b, BUNlast(b));
	bn->tnonil = 1;
	if (b->tnonil) {
		for (; p < q; p++, o++) {
			msg = FUN(do_,TP1,_dec2dec_,TP2)(o, scale, *p, 0, 0);
			if (msg) {
				BBPreclaim(bn);
				BBPunfix(b->batCacheid);
				return msg;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			/* shortcut nil */
			if (ISNIL(TP1)(*p)) {
				*o = NIL(TP2);
				bn->tnonil = 0;
				bn->tnil = 1;
			} else {
				msg = FUN(do_,TP1,_dec2dec_,TP2)(o, scale, *p, 0, 0);
				if (msg) {
					BBPreclaim(bn);
					BBPunfix(b->batCacheid);
					return msg;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(bn, FALSE);

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_dec2dec_,TP2) (bat *res, const int *S1, const bat *bid, const int *d2, const int *S2)
{
	BAT *b, *bn;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		TP1 val = * (TP1 *) BUNtail(bi, p);
		TP2 r;

		/* shortcut nil */
		if (ISNIL(TP1)(val)) {
			r = NIL(TP2);
			bn->tnonil = 0;
			bn->tnil = 1;
		} else {
			msg = FUN(do_,TP1,_dec2dec_,TP2)(&r, *S1, val, *d2, *S2);
			if (msg) {
				BBPunfix(bn->batCacheid);
				BBPunfix(b->batCacheid);
				return msg;
			}
		}
		if (BUNappend(bn, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const int *d2, const int *s2)
{
	BAT *b, *bn;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		TP1 val = * (TP1 *) BUNtail(bi, p);
		TP2 r;
		/* shortcut nil */
		if (ISNIL(TP1)(val)) {
			r = NIL(TP2);
			bn->tnonil = 0;
			bn->tnil = 1;
		} else {
			msg = FUN(do_,TP1,_dec2dec_,TP2)(&r, 0, val, *d2, *s2);
			if (msg) {
				BBPunfix(bn->batCacheid);
				BBPunfix(b->batCacheid);
				return msg;
			}
		}
		if (BUNappend(bn, &r, FALSE) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			BBPunfix(b->batCacheid);
			throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
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

