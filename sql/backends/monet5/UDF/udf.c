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

/* monetdb_config.h must be the first include in each .c file */
#include "monetdb_config.h"
#include "udf.h"

/* Reverse a string */

/* actual implementation */
/* all non-exported functions must be declared static */
static char *
UDFreverse_(char **ret, const char *src)
{
	size_t len = 0;
	char *dst = NULL;

	/* assert calling sanity */
	assert(ret != NULL);

	/* handle NULL pointer and NULL value */
	if (src == NULL || strcmp(src, str_nil) == 0) {
		*ret = GDKstrdup(str_nil);
		if (*ret == NULL)
			throw(MAL, "udf.reverse",
			      "failed to create copy of str_nil");

		return MAL_SUCCEED;
	}

	/* allocate result string */
	len = strlen(src);
	*ret = dst = GDKmalloc(len + 1);
	if (dst == NULL)
		throw(MAL, "udf.reverse",
		      "failed to allocate string of length " SZFMT, len + 1);

	/* copy characters from src to dst in reverse order */
	dst[len] = 0;
	while (len > 0)
		*dst++ = src[--len];

	return MAL_SUCCEED;
}

/* MAL wrapper */
char *
UDFreverse(char **ret, const char **arg)
{
	/* assert calling sanity */
	assert(ret != NULL && arg != NULL);

	return UDFreverse_ ( ret, *arg );
}


/* Reverse a BAT of strings */
/*
 * Generic "type-oblivious" version,
 * using generic "type-oblivious" BAT access interface.
 */

/* actual implementation */
static char *
UDFBATreverse_(BAT **ret, BAT *src)
{
	BATiter li;
	BAT *bn = NULL;
	BUN p = 0, q = 0;

	/* assert calling sanity */
	assert(ret != NULL);

	/* handle NULL pointer */
	if (src == NULL)
		throw(MAL, "batudf.reverse", RUNTIME_OBJECT_MISSING);

	/* check tail type */
	if (src->ttype != TYPE_str) {
		throw(MAL, "batudf.reverse",
		      "tail-type of input BAT must be TYPE_str");
	}

	/* allocate result BAT */
	bn = BATnew(src->htype, TYPE_str, BATcount(src));
	if (bn == NULL) {
		throw(MAL, "batudf.reverse", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, src->hseqbase);

	/* create BAT iterator */
	li = bat_iterator(src);

	/* the core of the algorithm, expensive due to malloc/frees */
	BATloop(src, p, q) {
		char *tr = NULL, *err = NULL;

		/* get original head & tail value */
		ptr h = BUNhead(li, p);
		const char *t = (const char *) BUNtail(li, p);

		/* revert tail value */
		err = UDFreverse_(&tr, t);
		if (err != MAL_SUCCEED) {
			/* error -> bail out */
			BBPreleaseref(bn->batCacheid);
			return err;
		}

		/* assert logical sanity */
		assert(tr != NULL);

		/* insert original head and reversed tail in result BAT */
		/* BUNins() takes care of all necessary administration */
		BUNins(bn, h, tr, FALSE);

		/* free memory allocated in UDFreverse_() */
		GDKfree(tr);
	}

	*ret = bn;

	return MAL_SUCCEED;
}

/* MAL wrapper */
char *
UDFBATreverse(bat *ret, const bat *arg)
{
	BAT *res = NULL, *src = NULL;
	char *msg = NULL;

	/* assert calling sanity */
	assert(ret != NULL && arg != NULL);

	/* bat-id -> BAT-descriptor */
	if ((src = BATdescriptor(*arg)) == NULL)
		throw(MAL, "batudf.reverse", RUNTIME_OBJECT_MISSING);

	/* do the work */
	msg = UDFBATreverse_ ( &res, src );

	/* release input BAT-descriptor */
	BBPreleaseref(src->batCacheid);

	if (msg == MAL_SUCCEED) {
		/* register result BAT in buffer pool */
		BBPkeepref((*ret = res->batCacheid));
	}

	return msg;
}



/* fuse */

/* instantiate type-specific functions */

#define UI bte
#define UU unsigned char
#define UO sht
#include "udf_impl.h"
#undef UI
#undef UU
#undef UO

#define UI sht
#define UU unsigned short
#define UO int
#include "udf_impl.h"
#undef UI
#undef UU
#undef UO

#define UI int
#define UU unsigned int
#define UO lng
#include "udf_impl.h"
#undef UI
#undef UU
#undef UO

/* BAT fuse */

/* actual implementation */
static char *
UDFBATfuse_(BAT **ret, const BAT *bone, const BAT *btwo)
{
	BAT *bres = NULL;
	bit two_tail_sorted_unsigned = FALSE;
	bit two_tail_revsorted_unsigned = FALSE;
	BUN n;
	char *msg = NULL;

	/* assert calling sanity */
	assert(ret != NULL);

	/* handle NULL pointer */
	if (bone == NULL || btwo == NULL)
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);

	/* check for dense & aligned heads */
	if (!BAThdense(bone) ||
	    !BAThdense(btwo) ||
	    BATcount(bone) != BATcount(btwo) ||
	    bone->hseqbase != btwo->hseqbase) {
		throw(MAL, "batudf.fuse",
		      "heads of input BATs must be aligned");
	}
	n = BATcount(bone);

	/* check tail types */
	if (bone->ttype != btwo->ttype) {
		throw(MAL, "batudf.fuse",
		      "tails of input BATs must be identical");
	}

	/* allocate result BAT */
	switch (bone->ttype) {
	case TYPE_bte:
		bres = BATnew(TYPE_void, TYPE_sht, n);
		break;
	case TYPE_sht:
		bres = BATnew(TYPE_void, TYPE_int, n);
		break;
	case TYPE_int:
		bres = BATnew(TYPE_void, TYPE_lng, n);
		break;
	default:
		throw(MAL, "batudf.fuse",
		      "tails of input BATs must be one of {bte, sht, int}");
	}
	if (bres == NULL)
		throw(MAL, "batudf.fuse", MAL_MALLOC_FAIL);

	/* call type-specific core algorithm */
	switch (bone->ttype) {
	case TYPE_bte:
		msg = UDFBATfuse_bte_sht ( bres, bone, btwo, n,
			&two_tail_sorted_unsigned, &two_tail_revsorted_unsigned );
		break;
	case TYPE_sht:
		msg = UDFBATfuse_sht_int ( bres, bone, btwo, n,
			&two_tail_sorted_unsigned, &two_tail_revsorted_unsigned );
		break;
	case TYPE_int:
		msg = UDFBATfuse_int_lng ( bres, bone, btwo, n,
			&two_tail_sorted_unsigned, &two_tail_revsorted_unsigned );
		break;
	default:
		BBPreleaseref(bres->batCacheid);
		throw(MAL, "batudf.fuse",
		      "tails of input BATs must be one of {bte, sht, int}");
	}

	if (msg != MAL_SUCCEED) {
		BBPreleaseref(bres->batCacheid);
	} else {
		/* set number of tuples in result BAT */
		BATsetcount(bres, n);

		/* set result properties */
		bres->hdense = TRUE;              /* result head is dense */
		BATseqbase(bres, bone->hseqbase); /* result head has same seqbase as input */
		bres->hsorted = 1;                /* result head is sorted */
		bres->hrevsorted = (BATcount(bres) <= 1);
		BATkey(bres, TRUE);               /* result head is key (unique) */

		/* Result tail is sorted, if the left/first input tail is
		 * sorted and key (unique), or if the left/first input tail is
		 * sorted and the second/right input tail is sorted and the
		 * second/right tail values are either all >= 0 or all < 0;
		 * otherwise, we cannot tell.
		 */
		if (BATtordered(bone)
		    && (BATtkey(bone) || two_tail_sorted_unsigned))
			bres->tsorted = 1;
		else
			bres->tsorted = (BATcount(bres) <= 1);
		if (BATtrevordered(bone)
		    && (BATtkey(bone) || two_tail_revsorted_unsigned))
			bres->trevsorted = 1;
		else
			bres->trevsorted = (BATcount(bres) <= 1);
		/* result tail is key (unique), iff both input tails are */
		BATkey(BATmirror(bres), BATtkey(bone) || BATtkey(btwo));

		*ret = bres;
	}

	return msg;
}

/* MAL wrapper */
char *
UDFBATfuse(bat *ires, const bat *ione, const bat *itwo)
{
	BAT *bres = NULL, *bone = NULL, *btwo = NULL;
	char *msg = NULL;

	/* assert calling sanity */
	assert(ires != NULL && ione != NULL && itwo != NULL);

	/* bat-id -> BAT-descriptor */
	if ((bone = BATdescriptor(*ione)) == NULL)
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);

	/* bat-id -> BAT-descriptor */
	if ((btwo = BATdescriptor(*itwo)) == NULL) {
		BBPreleaseref(bone->batCacheid);
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);
	}

	/* do the work */
	msg = UDFBATfuse_ ( &bres, bone, btwo );

	/* release input BAT-descriptors */
	BBPreleaseref(bone->batCacheid);
	BBPreleaseref(btwo->batCacheid);

	if (msg == MAL_SUCCEED) {
		/* register result BAT in buffer pool */
		BBPkeepref((*ires = bres->batCacheid));
	}

	return msg;
}
