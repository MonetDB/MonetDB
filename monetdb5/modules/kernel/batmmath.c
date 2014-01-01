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

/*
 * M.L. Kersten
 * BAT math calculator
 * This module contains the multiplex versions of the linked
 * in mathematical functions.
 * Scientific routines
 * The mmath functions are also overloaded to provide for
 * the fast execution of expanded code blocks.
 * The common set of math functions is supported.
 */
#include "monetdb_config.h"
#include "batmmath.h"

#define voidresultBAT(X1,X2)\
	bn = BATnew(TYPE_void, X1, BATcount(b));\
	BATseqbase(bn, b->hseqbase);\
	if (bn == NULL) {\
		throw(MAL, X2, MAL_MALLOC_FAIL);\
	}\
	bn->hsorted = b->hsorted;\
	bn->hrevsorted = b->hrevsorted;\
	bn->tsorted = b->tsorted;\
	bn->trevsorted = b->trevsorted;\
	bn->H->nonil = 1;\
	bn->T->nonil = b->T->nonil;


#define scienceFcnImpl(X1,X2,X3)							\
str CMDscience_bat_##X2##_##X1(int *ret, int *bid)			\
{															\
	BAT *b,*bn;												\
	X2 *o, *p, *q;											\
	if ((b = BATdescriptor(*bid)) == NULL) {				\
		throw(MAL, #X2, RUNTIME_OBJECT_MISSING);			\
	}														\
	voidresultBAT(TYPE_##X2,"batcalc." #X1);				\
	o = (X2*) Tloc(bn, BUNfirst(bn));						\
	p = (X2*) Tloc(b, BUNfirst(b));							\
	q = (X2*) Tloc(b, BUNlast(b));							\
															\
	if (b->T->nonil){										\
		for(;p<q; o++, p++)									\
			*o = X1##X3(*p);								\
	} else													\
		for(;p<q; o++, p++){								\
			*o = *p == X2##_nil? X2##_nil: X1##X3(*p);		\
		}													\
	BATsetcount(bn, BATcount(b));							\
	bn->tsorted = 0;										\
	bn->trevsorted = 0;										\
	BATkey(BATmirror(bn),0);								\
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ); \
	if (b->htype != bn->htype) {							\
		BAT *r = VIEWcreate(b,bn);							\
															\
		BBPreleaseref(bn->batCacheid);						\
		bn = r;												\
	}														\
	BBPkeepref(*ret = bn->batCacheid);						\
	BBPreleaseref(b->batCacheid);							\
	return MAL_SUCCEED;										\
}

#define scienceBinaryImpl(X1,X2,X3)								\
str CMDscience_bat_cst_##X1##_##X2(int *ret, int *bid, X2 *d)	\
{																\
	BAT *b,*bn;													\
	X2 *o, *p, *q;												\
																\
	if ((b = BATdescriptor(*bid)) == NULL) {					\
		throw(MAL, #X2, RUNTIME_OBJECT_MISSING);				\
	}															\
	voidresultBAT(TYPE_##X2,"batcalc." #X1)						\
	o = (X2*) Tloc(bn, BUNfirst(bn));							\
	p = (X2*) Tloc(b, BUNfirst(b));								\
	q = (X2*) Tloc(b, BUNlast(b));								\
																\
	if (b->T->nonil){											\
		for(;p<q; o++, p++)										\
			*o = X1##X3(*p,*d);									\
	} else														\
		for(;p<q; o++, p++){									\
			*o = *p == X2##_nil? X2##_nil: X1##X3(*p,*d);		\
		}														\
																\
	BATsetcount(bn, BATcount(b));								\
	bn->tsorted = 0;											\
	bn->trevsorted = 0;											\
	BATkey(BATmirror(bn),0);									\
																\
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);		\
																\
	if (b->htype != bn->htype) {								\
		BAT *r = VIEWcreate(b,bn);								\
																\
		BBPreleaseref(bn->batCacheid);							\
		bn = r;													\
	}															\
	BBPkeepref(*ret = bn->batCacheid);							\
	BBPreleaseref(b->batCacheid);								\
	return MAL_SUCCEED;											\
}

#define scienceImpl(Operator)					\
	scienceFcnImpl(Operator,dbl,)				\
	scienceFcnImpl(Operator,flt,f)

scienceImpl(asin)
scienceImpl(acos)
scienceImpl(atan)
scienceImpl(cos)
scienceImpl(sin)
scienceImpl(tan)
scienceImpl(cosh)
scienceImpl(sinh)
scienceImpl(tanh)
scienceImpl(radians)
scienceImpl(degrees)
scienceImpl(exp)
scienceImpl(log)
scienceImpl(log10)
scienceImpl(sqrt)
scienceImpl(ceil)
scienceImpl(fabs)
scienceImpl(floor)
/*
 * 	round is not binary...
 * 	scienceBinaryImpl(round,int)
 */
scienceBinaryImpl(atan2,dbl,)
scienceBinaryImpl(atan2,flt,f)
scienceBinaryImpl(pow,dbl,)
scienceBinaryImpl(pow,flt,f)
