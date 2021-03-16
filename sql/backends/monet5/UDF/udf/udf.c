/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* monetdb_config.h must be the first include in each .c file */
#include "monetdb_config.h"
#include "udf.h"
#include "str.h"

/* Reverse a string */

/* actual implementation */
/* all non-exported functions must be declared static */
static str
UDFreverse_(str *buf, size_t *buflen, const char *src)
{
	size_t len = strlen(src);
	char *dst = NULL;

	/* assert calling sanity */
	assert(buf);
	/* test if input buffer is large enough for result string, otherwise re-allocate it */
	CHECK_STR_BUFFER_LENGTH(buf, buflen, (len + 1), "udf.reverse");
	dst = *buf;

	dst[len] = 0;
	/* all strings in MonetDB are encoded using UTF-8; we must
	 * make sure that the reversed string is also encoded in valid
	 * UTF-8, so we treat multibyte characters as single units */
	while (*src) {
		if ((*src & 0xF8) == 0xF0) {
			/* 4 byte UTF-8 sequence */
			assert(len >= 4);
			dst[len - 4] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 4;
		} else if ((*src & 0xF0) == 0xE0) {
			/* 3 byte UTF-8 sequence */
			assert(len >= 3);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 3;
		} else if ((*src & 0xE0) == 0xC0) {
			/* 2 byte UTF-8 sequence */
			assert(len >= 2);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 2;
		} else {
			/* 1 byte UTF-8 "sequence" */
			assert(len >= 1);
			assert((*src & 0x80) == 0);
			dst[--len] = *src++;
		}
	}
	assert(len == 0);

	return MAL_SUCCEED;
}

/* MAL wrapper */
str
UDFreverse(str *res, const str *arg)
{
	str msg = MAL_SUCCEED, s;

	/* assert calling sanity */
	assert(res && arg);
	s = *arg;
	if (strNil(s)) {
		if (!(*res = GDKstrdup(str_nil)))
			throw(MAL, "udf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		size_t buflen = strlen(s) + 1;

		if (!(*res = GDKmalloc(buflen)))
			throw(MAL, "udf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = UDFreverse_(res, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(*res);
			*res = NULL;
			return msg;
		}
	}
	return msg;
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
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str msg = MAL_SUCCEED, buf;
	bool nils = false;

	/* assert calling sanity */
	assert(ret);

	/* handle NULL pointer */
	if (src == NULL)
		throw(MAL, "batudf.reverse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	/* check tail type */
	if (src->ttype != TYPE_str)
		throw(MAL, "batudf.reverse", SQLSTATE(42000) "tail-type of input BAT must be TYPE_str");

	/* to avoid many allocations, we allocate a single buffer, which will reallocate if a
	   larger string is found and freed at the end */
	if (!(buf = GDKmalloc(buflen))) {
		msg = createException(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	q = BATcount(src);
	/* allocate void-headed result BAT */
	if (!(bn = COLnew(src->hseqbase, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	/* create BAT iterator */
	li = bat_iterator(src);
	/* the core of the algorithm */
	for (p = 0; p < q ; p++) {
		str x = (str) BUNtvar(li, p);

		if (strNil(x)) {
			/* if the input string is null, then append directly */
			if (tfastins_nocheckVAR(bn, p, str_nil, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils = true;
		} else {
			/* revert tail value */
			if ((msg = UDFreverse_(&buf, &buflen, x)) != MAL_SUCCEED)
				goto bailout;
			/* assert logical sanity */
			assert(buf && x);
			/* append to the output BAT. We are using a faster route, because we know what we are doing */
			if (tfastins_nocheckVAR(bn, p, buf, Tsize(bn)) != GDK_SUCCEED) {
				msg = createException(MAL, "batudf.reverse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		}
	}

bailout:
	GDKfree(buf);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap->dirty = true;
	} else if (bn) {
		BBPreclaim(bn);
		bn = NULL;
	}
	*ret = bn;
	return msg;
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
		throw(MAL, "batudf.reverse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* do the work */
	msg = UDFBATreverse_( &res, src );

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
