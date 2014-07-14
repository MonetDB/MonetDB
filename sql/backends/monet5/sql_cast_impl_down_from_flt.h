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
#define TPE(t)       CONCAT_2(TYPE_,t)
#define GDKmin(t)    CONCAT_3(GDK_,t,_min)
#define GDKmax(t)    CONCAT_3(GDK_,t,_max)
#define FUN(a,b,c,d) CONCAT_4(a,b,c,d)


str
FUN(,TP1,_2_,TP2) (TP2 *res, TP1 *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == NIL(TP1)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (TP2) val > (dbl) GDKmin(TP2) && val > (dbl) GDKmin(TP2) && val <= (dbl) GDKmax(TP2)) {
		*res = (TP2) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type "STRNG(TP2), val);
	}
}

str
FUN(bat,TP1,_2_,TP2) (int *res, int *bid)
{
	BAT *b, *bn;
	TP1 *p, *q;
	char *msg = NULL;
	TP2 *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_2_,TP2)), "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TPE(TP2), BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_2_,TP2)), MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (TP2 *) Tloc(bn, BUNfirst(bn));
	p = (TP1 *) Tloc(b, BUNfirst(b));
	q = (TP1 *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (TP2) val > (dbl) GDKmin(TP2) && val > (dbl) GDKmin(TP2) && val <= (dbl) GDKmax(TP2)) {
				*o = (TP2) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type "STRNG(TP2), val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == NIL(TP1)) {
				*o = NIL(TP2);
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (TP2) val > (dbl) GDKmin(TP2) && val > (dbl) GDKmin(TP2) && val <= (dbl) GDKmax(TP2)) {
					*o = (TP2) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type "STRNG(TP2), val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
FUN(,TP1,_num2dec_,TP2) (TP2 *res, TP1 *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	TP1 r;
	lng cpyval;

	/* shortcut nil */
	if (*v == NIL(TP1)) {
		*res = NIL(TP2);
		return (MAL_SUCCEED);
	}

	/* since the TP2 type is bigger than or equal to the TP1 type, it will
	   always fit */
	r = (TP1) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (TP2) r;
	return MAL_SUCCEED;
}

str
FUN(bat,TP1,_num2dec_,TP2) (int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc."STRNG(FUN(,TP1,_num2dec_,TP2)), "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TPE(TP2), BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql."STRNG(FUN(,TP1,_num2dec_,TP2)), MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		TP1 *v = (TP1 *) BUNtail(bi, p);
		TP2 r;
		msg = FUN(,TP1,_num2dec_,TP2) (&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}


/* undo local defines */
#undef FUN
#undef NIL
#undef TPE
#undef GDKmin
#undef GDKmax
#undef CONCAT_2
#undef CONCAT_3
#undef CONCAT_4
#undef STRNG
#undef _STRNG_

