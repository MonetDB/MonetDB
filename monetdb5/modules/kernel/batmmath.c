/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include <fenv.h>
#ifndef FE_INVALID
#define FE_INVALID			0
#endif
#ifndef FE_DIVBYZERO
#define FE_DIVBYZERO		0
#endif
#ifndef FE_OVERFLOW
#define FE_OVERFLOW			0
#endif

#define voidresultBAT(X1,X2)									\
	do {														\
		bn = COLnew(b->hseqbase, X1, BATcount(b), TRANSIENT);	\
		if (bn == NULL) {										\
			BBPunfix(b->batCacheid);							\
			throw(MAL, X2, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
		}														\
		bn->tsorted = b->tsorted;								\
		bn->trevsorted = b->trevsorted;							\
		bn->tnonil = b->tnonil;									\
	} while (0)


#define scienceFcnImpl(FUNC,TYPE,SUFF)								\
str CMDscience_bat_##TYPE##_##FUNC(bat *ret, const bat *bid)		\
{																	\
	BAT *b, *bn;													\
	TYPE *o, *p, *q;												\
	int e = 0, ex = 0;												\
																	\
	if ((b = BATdescriptor(*bid)) == NULL) {						\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);	\
	}																\
	voidresultBAT(TYPE_##TYPE, "batcalc." #FUNC);					\
	o = (TYPE *) Tloc(bn, 0);										\
	p = (TYPE *) Tloc(b, 0);										\
	q = (TYPE *) Tloc(b, BUNlast(b));								\
																	\
	errno = 0;														\
	feclearexcept(FE_ALL_EXCEPT);									\
	if (b->tnonil) {												\
		for (; p < q; o++, p++)										\
			*o = FUNC##SUFF(*p);									\
	} else {														\
		for (; p < q; o++, p++)										\
			*o = is_##TYPE##_nil(*p) ? TYPE##_nil : FUNC##SUFF(*p);	\
	}																\
	if ((e = errno) != 0 ||											\
		(ex = fetestexcept(FE_INVALID | FE_DIVBYZERO |				\
						   FE_OVERFLOW)) != 0) {					\
		const char *err;											\
		BBPunfix(bn->batCacheid);									\
		BBPunfix(b->batCacheid);									\
		if (e) {													\
			err = strerror(e);										\
		} else if (ex & FE_DIVBYZERO)								\
			err = "Divide by zero";									\
		else if (ex & FE_OVERFLOW)									\
			err = "Overflow";										\
		else														\
			err = "Invalid result";									\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);	\
	}																\
	BATsetcount(bn, BATcount(b));									\
	bn->tsorted = 0;												\
	bn->trevsorted = 0;												\
	bn->tnil = b->tnil;												\
	bn->tnonil = b->tnonil;											\
	BATkey(bn, 0);													\
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
	int e = 0, ex = 0;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	voidresultBAT(TYPE_##TYPE, "batcalc." #FUNC);						\
	o = (TYPE *) Tloc(bn, 0);											\
	p = (TYPE *) Tloc(b, 0);											\
	q = (TYPE *) Tloc(b, BUNlast(b));									\
																		\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->tnonil) {													\
		for (; p < q; o++, p++)											\
			*o = FUNC##SUFF(*p, *d);									\
	} else {															\
		for (; p < q; o++, p++)											\
			*o = is_##TYPE##_nil(*p) ? TYPE##_nil : FUNC##SUFF(*p, *d);	\
	}																	\
	if ((e = errno) != 0 ||												\
		(ex = fetestexcept(FE_INVALID | FE_DIVBYZERO |					\
						   FE_OVERFLOW)) != 0) {						\
		const char *err;												\
		BBPunfix(b->batCacheid);										\
		BBPunfix(bn->batCacheid);										\
		if (e) {														\
			err = strerror(e);											\
		} else if (ex & FE_DIVBYZERO)									\
			err = "Divide by zero";										\
		else if (ex & FE_OVERFLOW)										\
			err = "Overflow";											\
		else															\
			err = "Invalid result";										\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);		\
	}																	\
	BATsetcount(bn, BATcount(b));										\
	bn->tsorted = 0;													\
	bn->trevsorted = 0;													\
	bn->tnil = b->tnil;													\
	bn->tnonil = b->tnonil;												\
	BATkey(bn,0);														\
	BBPkeepref(*ret = bn->batCacheid);									\
	BBPunfix(b->batCacheid);											\
	return MAL_SUCCEED;													\
}																		\
																		\
str CMDscience_cst_bat_##FUNC##_##TYPE(bat *ret, const TYPE *d,			\
									   const bat *bid)					\
{																		\
	BAT *b, *bn;														\
	TYPE *o, *p, *q;													\
	int e = 0, ex = 0;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	voidresultBAT(TYPE_##TYPE, "batcalc." #FUNC);						\
	o = (TYPE *) Tloc(bn, 0);											\
	p = (TYPE *) Tloc(b, 0);											\
	q = (TYPE *) Tloc(b, BUNlast(b));									\
																		\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->tnonil) {													\
		for (; p < q; o++, p++)											\
			*o = FUNC##SUFF(*d, *p);									\
	} else {															\
		for (; p < q; o++, p++)											\
			*o = is_##TYPE##_nil(*p) ? TYPE##_nil : FUNC##SUFF(*d, *p);	\
	}																	\
	if ((e = errno) != 0 ||												\
		(ex = fetestexcept(FE_INVALID | FE_DIVBYZERO |					\
						   FE_OVERFLOW)) != 0) {						\
		const char *err;												\
		BBPunfix(b->batCacheid);										\
		BBPunfix(bn->batCacheid);										\
		if (e) {														\
			err = strerror(e);											\
		} else if (ex & FE_DIVBYZERO)									\
			err = "Divide by zero";										\
		else if (ex & FE_OVERFLOW)										\
			err = "Overflow";											\
		else															\
			err = "Invalid result";										\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);		\
	}																	\
	BATsetcount(bn, BATcount(b));										\
	bn->tsorted = 0;													\
	bn->trevsorted = 0;													\
	bn->tnil = b->tnil;													\
	bn->tnonil = b->tnonil;												\
	BATkey(bn,0);														\
	BBPkeepref(*ret = bn->batCacheid);									\
	BBPunfix(b->batCacheid);											\
	return MAL_SUCCEED;													\
}

#define scienceImpl(Operator)					\
	scienceFcnImpl(Operator,dbl,)				\
	scienceFcnImpl(Operator,flt,f)

#define scienceNotImpl(FUNC)							\
str CMDscience_bat_flt_##FUNC(bat *ret, const bat *bid)	\
{														\
	throw(MAL, "batmmath." #FUNC, PROGRAM_NYI);			\
}														\
str CMDscience_bat_dbl_##FUNC(bat *ret, const bat *bid)	\
{														\
	throw(MAL, "batmmath." #FUNC, PROGRAM_NYI);			\
}

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
#ifdef HAVE_CBRT
scienceImpl(cbrt)
#else
scienceNotImpl(cbrt)
#endif
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
