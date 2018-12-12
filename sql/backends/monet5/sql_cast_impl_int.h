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
#define SIZEOF(t)    CONCAT_2(SIZEOF_,t)

#define SIZEOF_bte   1
#define SIZEOF_sht   2
#define SIZEOF_int   4
#define SIZEOF_lng   8
#ifdef HAVE_HGE
#define SIZEOF_hge   16
#endif

/* sizeof(scales)/sizeof(scales[0]), see sql_atom.c */
#ifdef HAVE_HGE
#define MAXDIG 39
#else
#define MAXDIG 19
#endif

static inline str
FUN(do_,TP1,_dec2dec_,TP2) (TP2 *restrict res, int s1, TP1 val, int p, int s2)
{
	int scale = s2 - s1;
	if (ISNIL(TP1)(val)) {
		*res = NIL(TP2);
		return MAL_SUCCEED;
	}
	/* if p == 0, the destination is a "normal" integral type (not
	 * a decimal), so we need to check whether the converted
	 * source value fits in the domain of the type, else we need
	 * to check whether the number of decimal digits does or does
	 * not exceed the precision given in p */
	assert(p >= 0 && p < MAXDIG);
	assert(s1 >= 0 && s1 < MAXDIG);
	assert(s2 >= 0 && s2 <= p);
	if (p != 0 &&
	    p - scale < MAXDIG && /* note, p-scale >= 0 since p >= s2 and s1 >= 0 */
	    (val <= -scales[p - scale] || val >= scales[p - scale])) {
		int inlen;
		if (val < 0)
			val = -val;
		for (inlen = 1; inlen < MAXDIG; inlen++)
			if (scales[inlen] > val)
				break;
		throw(SQL, STRNG(FUN(,TP1,_2_,TP2)), SQLSTATE(22003) "Too many digits (%d > %d)", inlen + scale, p);
	}
	if (scale >= 0) {
#if SIZEOF(TP1) > SIZEOF(TP2)
		if (p == 0 &&	/* this implies scale<=0, thus scale==0 */
		    (val < GDKmin(TP2) || val > GDKmax(TP2)))
			throw(SQL, STRNG(FUN(,TP1,_2_,TP2)),
			      SQLSTATE(22003) "value exceeds limits of type " STRNG(TP2));
#endif
		*res = (TP2) (val * scales[scale]);
	} else {
		scale = -scale;	/* i.e. scale > 0 now */
		if (p == 0 &&
		    (val / scales[scale] < GDKmin(TP2) ||
		     val / scales[scale] > GDKmax(TP2)))
			throw(SQL, STRNG(FUN(,TP1,_2_,TP2)),
			      SQLSTATE(22003) "value exceeds limits of type " STRNG(TP2));
		*res = (TP2) ((val
#ifndef TRUNCATE_NUMBERS
			      + (val < 0 ? -5 : 5) * scales[scale - 1]
#endif
				      )
			      / scales[scale]);
	}
	return MAL_SUCCEED;
}

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

str
FUN(,TP1,_num2dec_,TP2) (TP2 *res, const TP1 *v, const int *d2, const int *s2)
{
	return FUN(do_,TP1,_dec2dec_,TP2)(res, 0, *v, *d2, *s2);
}

str
FUN(bat,TP1,_dec2_,TP2) (bat *res, const int *s1, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	char *msg = NULL;
	int scale = *s1;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(dec,TP1,_2_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	const TP1 *vals = (const TP1 *) Tloc(b, 0);
	TP2 *o = (TP2 *) Tloc(bn, 0);
	BATloop(b, p, q) {
		msg = FUN(do_,TP1,_dec2dec_,TP2)(o, scale, vals[p], 0, 0);
		if (msg) {
			BBPreclaim(bn);
			BBPunfix(b->batCacheid);
			return msg;
		}
		o++;
	}
	BATsetcount(bn, BATcount(b));
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;
	bn->tsorted = false;
	bn->trevsorted = false;
	BATkey(bn, false);

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_dec2dec_,TP2) (bat *res, const int *S1, const bat *bid, const int *d2, const int *S2)
{
	BAT *b, *bn;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_dec2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	const TP1 *vals = (const TP1 *) Tloc(b, 0);
	TP2 *o = (TP2 *) Tloc(bn, 0);
	BATloop(b, p, q) {
		msg = FUN(do_,TP1,_dec2dec_,TP2)(o, *S1, vals[p], *d2, *S2);
		if (msg) {
			BBPunfix(bn->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		o++;
	}
	BATsetcount(bn, BATcount(b));
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;
	bn->tsorted = false;
	bn->trevsorted = false;
	BATkey(bn, false);

	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
FUN(bat,TP1,_num2dec_,TP2) (bat *res, const bat *bid, const int *d2, const int *s2)
{
	BAT *b, *bn;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY005) "Cannot access descriptor");
	}
	bn = COLnew(b->hseqbase, TPE(TP2), BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	const TP1 *vals = (const TP1 *) Tloc(b, 0);
	TP2 *o = (TP2 *) Tloc(bn, 0);
	BATloop(b, p, q) {
		msg = FUN(do_,TP1,_dec2dec_,TP2)(o, 0, vals[p], *d2, *s2);
		if (msg) {
			BBPunfix(bn->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		o++;
	}
	BATsetcount(bn, BATcount(b));
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;
	bn->tsorted = false;
	bn->trevsorted = false;
	BATkey(bn, false);

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

