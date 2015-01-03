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
 * Copyright August 2008-2015 MonetDB B.V.
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
#ifdef HAVE_FENV_H
#include <fenv.h>
#else
#define feclearexcept(x)
#define fetestexcept(x)		0
#endif

#define voidresultBAT(X1,X2)								\
	do {													\
		bn = BATnew(TYPE_void, X1, BATcount(b), TRANSIENT);	\
		if (bn == NULL) {									\
			BBPreleaseref(b->batCacheid);					\
			throw(MAL, X2, MAL_MALLOC_FAIL);				\
		}													\
		BATseqbase(bn, b->hseqbase);						\
		bn->hsorted = b->hsorted;							\
		bn->hrevsorted = b->hrevsorted;						\
		bn->tsorted = b->tsorted;							\
		bn->trevsorted = b->trevsorted;						\
		bn->H->nonil = 1;									\
		bn->T->nonil = b->T->nonil;							\
	} while (0)


#define scienceFcnImpl(FUNC,TYPE,SUFF)								\
str CMDscience_bat_##TYPE##_##FUNC(bat *ret, const bat *bid)		\
{																	\
	BAT *b, *bn;													\
	TYPE *o, *p, *q;												\
	if ((b = BATdescriptor(*bid)) == NULL) {						\
		throw(MAL, #TYPE, RUNTIME_OBJECT_MISSING);					\
	}																\
	voidresultBAT(TYPE_##TYPE, "batcalc." #FUNC);					\
	o = (TYPE *) Tloc(bn, BUNfirst(bn));							\
	p = (TYPE *) Tloc(b, BUNfirst(b));								\
	q = (TYPE *) Tloc(b, BUNlast(b));								\
																	\
	errno = 0;														\
	feclearexcept(FE_ALL_EXCEPT);									\
	if (b->T->nonil) {												\
		for (; p < q; o++, p++)										\
			*o = FUNC##SUFF(*p);									\
	} else {														\
		for (; p < q; o++, p++)										\
			*o = *p == TYPE##_nil ? TYPE##_nil : FUNC##SUFF(*p);	\
	}																\
	if (errno != 0 ||												\
		fetestexcept(FE_INVALID | FE_DIVBYZERO |					\
					 FE_OVERFLOW | FE_UNDERFLOW) != 0) {			\
		int e = errno;												\
		BBPreleaseref(bn->batCacheid);								\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s",			\
			  strerror(e));											\
	}																\
	BATsetcount(bn, BATcount(b));									\
	bn->tsorted = 0;												\
	bn->trevsorted = 0;												\
	bn->T->nil = b->T->nil;											\
	bn->T->nonil = b->T->nonil;										\
	BATkey(BATmirror(bn), 0);										\
	if (!(bn->batDirty&2))											\
		bn = BATsetaccess(bn, BAT_READ);							\
	if (b->htype != bn->htype) {									\
		BAT *r = VIEWcreate(b,bn);									\
																	\
		BBPreleaseref(bn->batCacheid);								\
		bn = r;														\
	}																\
	BBPkeepref(*ret = bn->batCacheid);								\
	BBPreleaseref(b->batCacheid);									\
	return MAL_SUCCEED;												\
}

#define scienceBinaryImpl(FUNC,TYPE,SUFF)								\
str CMDscience_bat_cst_##FUNC##_##TYPE(bat *ret, const bat *bid,		\
									   const TYPE *d)					\
{																		\
	BAT *b, *bn;														\
	TYPE *o, *p, *q;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, RUNTIME_OBJECT_MISSING);						\
	}																	\
	voidresultBAT(TYPE_##TYPE, "batcalc." #FUNC);						\
	o = (TYPE *) Tloc(bn, BUNfirst(bn));								\
	p = (TYPE *) Tloc(b, BUNfirst(b));									\
	q = (TYPE *) Tloc(b, BUNlast(b));									\
																		\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->T->nonil) {													\
		for (; p < q; o++, p++)											\
			*o = FUNC##SUFF(*p, *d);									\
	} else {															\
		for (; p < q; o++, p++)											\
			*o = *p == TYPE##_nil ? TYPE##_nil : FUNC##SUFF(*p, *d);	\
	}																	\
	if (errno != 0 ||													\
		fetestexcept(FE_INVALID | FE_DIVBYZERO |						\
					 FE_OVERFLOW | FE_UNDERFLOW) != 0) {				\
		int e = errno;													\
		BBPreleaseref(bn->batCacheid);									\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s",				\
			  strerror(e));												\
	}																	\
	BATsetcount(bn, BATcount(b));										\
	bn->tsorted = 0;													\
	bn->trevsorted = 0;													\
	bn->T->nil = b->T->nil;												\
	bn->T->nonil = b->T->nonil;											\
	BATkey(BATmirror(bn),0);											\
	if (!(bn->batDirty&2))												\
		bn = BATsetaccess(bn, BAT_READ);								\
	if (b->htype != bn->htype) {										\
		BAT *r = VIEWcreate(b,bn);										\
																		\
		BBPreleaseref(bn->batCacheid);									\
		bn = r;															\
	}																	\
	BBPkeepref(*ret = bn->batCacheid);									\
	BBPreleaseref(b->batCacheid);										\
	return MAL_SUCCEED;													\
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
