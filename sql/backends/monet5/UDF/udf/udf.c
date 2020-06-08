/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
	if (strNil(src)) {
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
		      "failed to allocate string of length %zu", len + 1);

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
		throw(MAL, "batudf.reverse",  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* check tail type */
	if (src->ttype != TYPE_str) {
		throw(MAL, "batudf.reverse",
		      "tail-type of input BAT must be TYPE_str");
	}

	/* allocate void-headed result BAT */
	bn = COLnew(src->hseqbase, TYPE_str, BATcount(src), TRANSIENT);
	if (bn == NULL) {
		throw(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* create BAT iterator */
	li = bat_iterator(src);

	/* the core of the algorithm, expensive due to malloc/frees */
	BATloop(src, p, q) {
		char *tr = NULL, *err = NULL;

		const char *t = (const char *) BUNtvar(li, p);

		/* revert tail value */
		err = UDFreverse_(&tr, t);
		if (err != MAL_SUCCEED) {
			/* error -> bail out */
			BBPunfix(bn->batCacheid);
			return err;
		}

		/* assert logical sanity */
		assert(tr != NULL);

		/* append reversed tail in result BAT */
		if (BUNappend(bn, tr, false) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			throw(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

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
		throw(MAL, "batudf.reverse",  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* do the work */
	msg = UDFBATreverse_ ( &res, src );

	/* release input BAT-descriptor */
	BBPunfix(src->batCacheid);

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

#ifdef HAVE_HGE
#define UI lng
#define UU ulng
#define UO hge
#include "udf_impl.h"
#undef UI
#undef UU
#undef UO
#endif

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
		throw(MAL, "batudf.fuse",  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* check for aligned heads */
	if (BATcount(bone) != BATcount(btwo) ||
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
		bres = COLnew(bone->hseqbase, TYPE_sht, n, TRANSIENT);
		break;
	case TYPE_sht:
		bres = COLnew(bone->hseqbase, TYPE_int, n, TRANSIENT);
		break;
	case TYPE_int:
		bres = COLnew(bone->hseqbase, TYPE_lng, n, TRANSIENT);
		break;
#ifdef HAVE_HGE
	case TYPE_lng:
		bres = COLnew(bone->hseqbase, TYPE_hge, n, TRANSIENT);
		break;
#endif
	default:
		throw(MAL, "batudf.fuse",
		      "tails of input BATs must be one of {bte, sht, int"
#ifdef HAVE_HGE
		      ", lng"
#endif
		      "}");
	}
	if (bres == NULL)
		throw(MAL, "batudf.fuse", SQLSTATE(HY013) MAL_MALLOC_FAIL);

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
#ifdef HAVE_HGE
	case TYPE_lng:
		msg = UDFBATfuse_lng_hge ( bres, bone, btwo, n,
			&two_tail_sorted_unsigned, &two_tail_revsorted_unsigned );
		break;
#endif
	default:
		BBPunfix(bres->batCacheid);
		throw(MAL, "batudf.fuse",
		      "tails of input BATs must be one of {bte, sht, int"
#ifdef HAVE_HGE
		      ", lng"
#endif
		      "}");
	}

	if (msg != MAL_SUCCEED) {
		BBPunfix(bres->batCacheid);
	} else {
		/* set number of tuples in result BAT */
		BATsetcount(bres, n);

		/* Result tail is sorted, if the left/first input tail is
		 * sorted and key (unique), or if the left/first input tail is
		 * sorted and the second/right input tail is sorted and the
		 * second/right tail values are either all >= 0 or all < 0;
		 * otherwise, we cannot tell.
		 */
		if (BATtordered(bone)
		    && (BATtkey(bone) || two_tail_sorted_unsigned))
			bres->tsorted = true;
		else
			bres->tsorted = (BATcount(bres) <= 1);
		if (BATtrevordered(bone)
		    && (BATtkey(bone) || two_tail_revsorted_unsigned))
			bres->trevsorted = true;
		else
			bres->trevsorted = (BATcount(bres) <= 1);
		/* result tail is key (unique), iff both input tails are */
		BATkey(bres, BATtkey(bone) || BATtkey(btwo));

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
		throw(MAL, "batudf.fuse",  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* bat-id -> BAT-descriptor */
	if ((btwo = BATdescriptor(*itwo)) == NULL) {
		BBPunfix(bone->batCacheid);
		throw(MAL, "batudf.fuse",  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* do the work */
	msg = UDFBATfuse_ ( &bres, bone, btwo );

	/* release input BAT-descriptors */
	BBPunfix(bone->batCacheid);
	BBPunfix(btwo->batCacheid);

	if (msg == MAL_SUCCEED) {
		/* register result BAT in buffer pool */
		BBPkeepref((*ires = bres->batCacheid));
	}

	return msg;
}

#include "mel.h"
static mel_func udf_init_funcs[] = {
 command("udf", "reverse", UDFreverse, false, "Reverse a string", args(1,2, arg("",str),arg("ra1",str))),
 command("batudf", "reverse", UDFBATreverse, false, "Reverse a BAT of strings", args(1,2, batarg("",str),batarg("b",str))),
 command("udf", "fuse", UDFfuse_bte_sht, false, "fuse two (1-byte) bte values into one (2-byte) sht value", args(1,3, arg("",sht),arg("one",bte),arg("two",bte))),
 command("udf", "fuse", UDFfuse_sht_int, false, "fuse two (2-byte) sht values into one (4-byte) int value", args(1,3, arg("",int),arg("one",sht),arg("two",sht))),
 command("udf", "fuse", UDFfuse_int_lng, false, "fuse two (4-byte) int values into one (8-byte) lng value", args(1,3, arg("",lng),arg("one",int),arg("two",int))),
 command("batudf", "fuse", UDFBATfuse, false, "fuse two (1-byte) bte values into one (2-byte) sht value", args(1,3, batarg("",sht),batarg("one",bte),batarg("two",bte))),
 command("batudf", "fuse", UDFBATfuse, false, "fuse two (2-byte) sht values into one (4-byte) int value", args(1,3, batarg("",int),batarg("one",sht),batarg("two",sht))),
 command("batudf", "fuse", UDFBATfuse, false, "fuse two (4-byte) int values into one (8-byte) lng value", args(1,3, batarg("",lng),batarg("one",int),batarg("two",int))),
#ifdef HAVE_HGE
 command("udf", "fuse", UDFfuse_lng_hge, false, "fuse two (8-byte) lng values into one (16-byte) hge value", args(1,3, arg("",hge),arg("one",lng),arg("two",lng))),
 command("batudf", "fuse", UDFBATfuse, false, "fuse two (8-byte) lng values into one (16-byte) hge value", args(1,3, batarg("",hge),batarg("one",lng),batarg("two",lng))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_udf_mal)
{ mal_module("udf", NULL, udf_init_funcs); }
