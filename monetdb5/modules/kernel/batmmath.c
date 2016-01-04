/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
			BBPunfix(b->batCacheid);						\
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
		BBPunfix(bn->batCacheid);									\
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
		BATsetaccess(bn, BAT_READ);									\
	BBPkeepref(*ret = bn->batCacheid);								\
	BBPunfix(b->batCacheid);										\
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
		BBPunfix(bn->batCacheid);										\
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
		BATsetaccess(bn, BAT_READ);										\
	BBPkeepref(*ret = bn->batCacheid);									\
	BBPunfix(b->batCacheid);											\
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
