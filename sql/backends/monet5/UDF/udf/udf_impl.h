/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* This file is included multiple times.  We expect the tokens UI, UU, UO, US
 * to be defined by the including file, and we expect that the
 * combination (UI,UU,UO,US) is unique to each inclusion. */


/* ! ENSURE THAT THESE LOCAL MACROS ARE UNDEFINED AT THE END OF THIS FILE ! */

/* concatenate two or five tokens */
#define U_CONCAT_2(a,b)       a##b
#define U_CONCAT_5(a,b,c,d,e) a##b##c##d##e

/* function names, *_nil & TYPE_* macros */
#define UF(p,i,o,s) U_CONCAT_5(p,i,_,o,s)
#define UN(t)       U_CONCAT_2(t,_nil)
#define UT(t)       U_CONCAT_2(TYPE_,t)


/* scalar fuse */

/* fuse two (shift-byte) in values into one (2*shift-byte) out value */
/* actual implementation */
static char *
UF(UDFfuse_,UI,UO,_) ( UO *ret , UI one , UI two )
{
	int shift = sizeof(UI) * 8;

	/* assert calling sanity */
	assert(ret != NULL);

	if (one == UN(UI) || two == UN(UI))
		/* NULL/nil in => NULL/nil out */
		*ret = UN(UO);
	else
		/* do the work; watch out for sign bits */
		*ret = ((UO) (UU) one << shift) | (UU) two;

	return MAL_SUCCEED;
}
/* MAL wrapper */
char *
UF(UDFfuse_,UI,UO,) ( UO *ret , const UI *one , const UI *two )
{
	/* assert calling sanity */
	assert(ret != NULL && one != NULL && two != NULL);

	return UF(UDFfuse_,UI,UO,_) ( ret, *one, *two );
}

/* BAT fuse */
/*
 * TYPE-expanded optimized version,
 * accessing value arrays directly.
 */

/* type-specific core algorithm on arrays */
static char *
UF(UDFarrayfuse_,UI,UO,)  ( UO *res, const UI *one, const UI *two, BUN n )
{
	BUN i;
	int shift = sizeof(UI) * 8;

	/* assert calling sanity */
	assert(res != NULL && one != NULL && two != NULL);

	/* iterate over all values/tuples and do the work */
	for (i = 0; i < n; i++)
		if (one[i] == UN(UI) || two[i] == UN(UI))
			/* NULL/nil in => NULL/nil out */
			res[i] = UN(UO);
		else
			/* do the work; watch out for sign bits */
			res[i] = ((UO) (UU) one[i] << shift) | (UU) two[i];

	return MAL_SUCCEED;
}

/* type-specific core algorithm on BATs */
static char *
UF(UDFBATfuse_,UI,UO,)  ( const BAT *bres, const BAT *bone, const BAT *btwo, BUN n,
                          bit *two_tail_sorted_unsigned,
                          bit *two_tail_revsorted_unsigned )
{
	UI *one = NULL, *two = NULL;
	UO *res = NULL;
	str msg = NULL;

	/* assert calling sanity */
	assert(bres != NULL && bone != NULL && btwo != NULL);
	assert(BATcapacity(bres) >= n);
	assert(BATcount(bone) >= n && BATcount(btwo) >= n);
	assert(bone->ttype == UT(UI) && btwo->ttype == UT(UI));
	assert(bres->ttype == UT(UO));

	/* get direct access to the tail arrays	*/
	one = (UI*) Tloc(bone, 0);
	two = (UI*) Tloc(btwo, 0);
	res = (UO*) Tloc(bres, 0);

	/* call core function on arrays */
	msg = UF(UDFarrayfuse_,UI,UO,) ( res, one, two , n );
	if (msg != MAL_SUCCEED)
		return msg;

	*two_tail_sorted_unsigned =
		BATtordered(btwo) && (two[0] >= 0 || two[n-1] < 0);
	*two_tail_revsorted_unsigned =
		BATtrevordered(btwo) &&	(two[0] < 0 || two[n-1] >= 0);

	return MAL_SUCCEED;
}


/* undo local defines */
#undef UT
#undef UN
#undef UF
#undef U_CONCAT_5
#undef U_CONCAT_2

