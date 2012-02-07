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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/


#include "monetdb_config.h"
#include "udf.h"

/* Reverse a string */
static str
reverse(const char *src)
{
	size_t len;
	str ret, new;

	/* The scalar function returns the new space */
	len = strlen(src);
	ret = new = GDKmalloc(len + 1);
	if (new == NULL)
		return NULL;
	new[len] = 0;
	while (len > 0)
		*new++ = src[--len];
	return ret;
}

str UDFreverse(str *ret, str *src)
{
	if (*src == 0 || strcmp(*src, str_nil) == 0)
		*ret = GDKstrdup(str_nil);
	else
		*ret = reverse(*src);
	return MAL_SUCCEED;
}

/*
 * Reverse a BAT of strings
 *
 * The BAT version is much more complicated, because we need to
 * ensure that properties are maintained.
 */

#define UDFBATreverse_loop_body									\
	str t = (str) BUNtail(li, p);								\
	str tr = reverse(t);									\
												\
	if (tr == NULL) {									\
		BATaccessEnd(left, USE_HEAD | USE_TAIL, MMAP_SEQUENTIAL);			\
		BBPreleaseref(left->batCacheid);						\
		BBPreleaseref(*ret);								\
		throw(MAL, "batudf.reverse", OPERATION_FAILED " During bulk operation");	\
	}

str UDFBATreverse(int *ret, int *bid)
{
	BATiter li;
	BAT *bn, *left;
	BUN p, q;

	/* check for NULL pointers */
	if (ret == NULL || bid == NULL)
		throw(MAL, "batudf.reverse", RUNTIME_OBJECT_MISSING);

	/* locate the BAT in the buffer pool */
	if ((left = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batudf.reverse", RUNTIME_OBJECT_MISSING);

	/* create the result container */
	bn = BATnew(left->htype, TYPE_str, BATcount(left));
	if (bn == NULL) {
		BBPreleaseref(left->batCacheid);
		throw(MAL, "batudf.reverse", MAL_MALLOC_FAIL);
	}

	li = bat_iterator(left);

	/* advice on sequential scan */
	BATaccessBegin(left, USE_HEAD | USE_TAIL, MMAP_SEQUENTIAL);

	/* the core of the algorithm, expensive due to malloc/frees */
	if (BAThdense(left)) {
		/* dense [v]oid head */
		/* initialize dense head of result */
		bn->hdense = BAThdense(left);
		BATseqbase(bn, left->hseqbase);
		BATloop(left, p, q) {

			UDFBATreverse_loop_body

			BUNappend(bn, tr, FALSE);
			GDKfree(tr);
		}
	} else {
		/* arbitrary (non-dense) head */
		BATloop(left, p, q) {
			ptr h = BUNhead(li, p);

			UDFBATreverse_loop_body

			BUNins(bn, h, tr, FALSE);
			GDKfree(tr);
		}
	}

	BATaccessEnd(left, USE_HEAD | USE_TAIL, MMAP_SEQUENTIAL);

	BBPreleaseref(left->batCacheid);

	*ret = bn->batCacheid;
	BBPkeepref(*ret);

	return MAL_SUCCEED;
}


/* scalar fuse */

#define UDFfuse_scalar_impl(in,uin,out,shift)				\
/* fuse two (shift-byte) in values into one (2*shift-byte) out value */	\
str UDFfuse_##in##_##out ( out *ret , in *one , in *two )		\
{									\
	if (ret == NULL || one == NULL || two == NULL)			\
		throw(MAL, "udf.fuse", RUNTIME_OBJECT_MISSING);		\
	if (*one == in##_nil || *two == in##_nil)			\
		*ret = out##_nil;					\
	else								\
		*ret = ( ((out)((uin)*one)) << shift ) | ((uin)*two);	\
	return MAL_SUCCEED;						\
}

UDFfuse_scalar_impl(bte,unsigned char ,sht, 8)
UDFfuse_scalar_impl(sht,unsigned short,int,16)
UDFfuse_scalar_impl(int,unsigned int  ,lng,32)


/* BAT fuse */

str UDFBATfuse(int *ires, int *ione, int *itwo)
{
	BAT *bres, *bone, *btwo;

	/* check for NULL pointers */
	if (ires == NULL || ione == NULL || itwo == NULL)
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);

	/* locate the BAT in the buffer pool */
	if ((bone = BATdescriptor(*ione)) == NULL)
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);

	/* locate the BAT in the buffer pool */
	if ((btwo = BATdescriptor(*itwo)) == NULL) {
		BBPreleaseref(bone->batCacheid);
		throw(MAL, "batudf.fuse", RUNTIME_OBJECT_MISSING);
	}

	/* check for dense & aligned heads */
	if (!BAThdense(bone) || !BAThdense(btwo) || BATcount(bone) != BATcount(btwo) || bone->hseqbase != btwo->hseqbase) {
		BBPreleaseref(bone->batCacheid);
		BBPreleaseref(btwo->batCacheid);
		throw(MAL, "batudf.fuse", "heads of input BATs must be aligned");
	}

	/* check tail types */
	if (bone->ttype != btwo->ttype) {
		BBPreleaseref(bone->batCacheid);
		BBPreleaseref(btwo->batCacheid);
		throw(MAL, "batudf.fuse", "tails of input BATs must be identical");
	}

#define UDFBATfuse_TYPE(in,uin,out,shift)						\
{											\
	in *one, *two;									\
	out *res;									\
	BUN i, n = BATcount(bone);							\
											\
	/* create the result container */						\
	bres = BATnew(TYPE_void, TYPE_##out, n);					\
	if (bres == NULL) {								\
		BBPreleaseref(bone->batCacheid);					\
		BBPreleaseref(btwo->batCacheid);					\
		throw(MAL, "batudf.fuse", MAL_MALLOC_FAIL);				\
	}										\
											\
	one = (in *) Tloc(bone, BUNfirst(bone));					\
	two = (in *) Tloc(btwo, BUNfirst(btwo));					\
	res = (out*) Tloc(bres, BUNfirst(bres));					\
											\
	for (i = 0; i < n; i++)								\
		if (one[i] == in##_nil || two[i] == in##_nil)				\
			res[i] = out##_nil;						\
		else									\
			res[i] = ( ((out)((uin)one[i])) << shift ) | ((uin)two[i]);	\
											\
	BATsetcount(bres, n);								\
}

	switch (bone->ttype) {
	case TYPE_bte:
		UDFBATfuse_TYPE(bte,unsigned char ,sht, 8)
		break;
	case TYPE_sht:
		UDFBATfuse_TYPE(sht,unsigned short,int,16)
		break;
	case TYPE_int:
		UDFBATfuse_TYPE(int,unsigned int  ,lng,32)
		break;
	default:
		throw(MAL, "batudf.fuse", "tails of input BATs must be one of {bte, sht, int}");
	}

	/* manage the properties of the result */
	bres->hdense = BAThdense(bone);
	BATseqbase(bres, bone->hseqbase);
	bres->hsorted = GDK_SORTED;
	BATkey(bone, TRUE);

	if (BATtordered(bone)&1 && (BATtkey(bone) || BATtordered(btwo)&1))
		bres->tsorted = GDK_SORTED;
	else
		bres->tsorted = 0;
	BATkey(BATmirror(bres), BATtkey(bone) || BATtkey(btwo));

	BBPreleaseref(bone->batCacheid);
	BBPreleaseref(btwo->batCacheid);

	*ires = bres->batCacheid;
	BBPkeepref(*ires);

	return MAL_SUCCEED;
}
