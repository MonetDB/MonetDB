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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
	one = (UI*) Tloc(bone, BUNfirst(bone));
	two = (UI*) Tloc(btwo, BUNfirst(btwo));
	res = (UO*) Tloc(bres, BUNfirst(bres));

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

