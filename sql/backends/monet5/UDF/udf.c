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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
*/


#include "udf.h"

static str
reverse(str src)
{
	int len;
	str ret, new;

	/* The scalar function returns the new space */
	len = (int) strlen(src);
	ret = new = GDKzalloc( len + 1);
	for(len--; len >= 0; len--)
		*new++ = src[len];
	return ret;
}

str 
UDFreverse(str *ret, str *src)
{
	if( *src == 0 || strcmp(*src, str_nil) == 0 ) 
		*ret =  GDKstrdup(str_nil);
	else *ret = reverse(*src);
	return MAL_SUCCEED;
}

/* 
 * The BAT version is much more complicated, because we need to
 * ensure that properties are maintained.
*/
str UDFBATreverse(int *ret, int *bid)
{
    BATiter li;
    BAT *bn, *left;
    BUN p,q;
    str v;

	/* locate the BAT in the buffer pool */
    if( (left= BATdescriptor(*bid)) == NULL )
        throw(MAL, "mal.reverse" , RUNTIME_OBJECT_MISSING);

	/* create the result container */
    bn= BATnew(left->htype,TYPE_str,BATcount(left));
    if (left->htype== TYPE_void)
        BATseqbase(bn, left->hseqbase);
    if (bn == NULL){
        BBPreleaseref(left->batCacheid);
        throw(MAL, "mal.reverse", MAL_MALLOC_FAIL);
    }

	/* manage the properties of the result */
    bn->hsorted= left->hsorted;
    bn->tsorted=0;	/* assume not sorted afterwards */

    li = bat_iterator(left);
	/* advice on sequential scan */
    BATaccessBegin(left, USE_HEAD|USE_TAIL, MMAP_SEQUENTIAL);

	/* the core of the algorithm, expensive due to malloc/frees */
    BATloop(left, p, q) {
        ptr h = BUNhead(li,p);
        str tl = (str) BUNtail(li,p);
		v= reverse(tl);
        bunfastins(bn, h, v);
        GDKfree(v);
    }
    BATaccessEnd(left, USE_HEAD|USE_TAIL, MMAP_SEQUENTIAL);
    if (!(bn->batDirty & 2)) (bn) = BATsetaccess(bn, BAT_READ); 
    *ret = (bn)->batCacheid; 
    BBPkeepref(*ret);
    BBPreleaseref(left->batCacheid);
    return MAL_SUCCEED;

bunins_failed:
    BATaccessEnd(left, USE_HEAD|USE_TAIL, MMAP_SEQUENTIAL);
    BBPreleaseref(left->batCacheid);
    BBPreleaseref(*ret);
    throw(MAL, "mal.reverse", OPERATION_FAILED " During bulk operation");
}
