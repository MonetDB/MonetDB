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
#include "gdk_cand.h"
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


/* from gdk_calc.c */
#define CANDLOOP(dst, i, NIL, low, high)		\
	do {										\
		for ((i) = (low); (i) < (high); (i)++)	\
			(dst)[i] = NIL;						\
		nils += (high) - (low);					\
	} while (0)

#define scienceFcnImpl(FUNC,TYPE,SUFF)									\
str CMDscience_bat_##TYPE##_##FUNC##_cand(bat *ret, const bat *bid, const bat *sid)	\
{																		\
	BAT *b, *s = NULL, *bn;												\
	BUN i, cnt, start, end;												\
	const oid *restrict cand = NULL, *candend = NULL;					\
	BUN nils = 0;														\
	int e = 0, ex = 0;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	if (sid != NULL && !is_bat_nil(*sid) &&								\
		(s = BATdescriptor(*sid)) == NULL) {							\
		BBPunfix(b->batCacheid);										\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	CANDINIT(b, s, start, end, cnt, cand, candend);						\
	bn = COLnew(b->hseqbase, TYPE_##TYPE, cnt, TRANSIENT);				\
	if (bn == NULL) {													\
		BBPunfix(b->batCacheid);										\
		BBPunfix(s->batCacheid);										\
		throw(MAL, "batcalc." #FUNC, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}																	\
																		\
	const TYPE *restrict src = (const TYPE *) Tloc(b, 0);				\
	TYPE *restrict dst = (TYPE *) Tloc(bn, 0);							\
	CANDLOOP(dst, i, TYPE##_nil, 0, start);								\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->tnonil && cand == NULL) {									\
		for (i = start; i < end; i++)									\
			dst[i] = FUNC##SUFF(src[i]);								\
	} else {															\
		if (cand) {														\
			for (i = start; i < end; i++) {								\
				if (i < *cand - b->hseqbase) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
					continue;											\
				}														\
				assert(i == *cand - b->hseqbase);						\
				if (++cand == candend)									\
					end = i + 1;										\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(src[i]);						\
				}														\
			}															\
		} else {														\
			for (i = start; i < end; i++) {								\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(src[i]);						\
				}														\
			}															\
		}																\
	}																	\
	e = errno;															\
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);			\
	BBPunfix(b->batCacheid);											\
	if (s)																\
		BBPunfix(s->batCacheid);										\
	if (e != 0 || ex != 0) {											\
		const char *err;												\
		BBPunfix(bn->batCacheid);										\
		if (e)															\
			err = strerror(e);											\
		else if (ex & FE_DIVBYZERO)										\
			err = "Divide by zero";										\
		else if (ex & FE_OVERFLOW)										\
			err = "Overflow";											\
		else															\
			err = "Invalid result";										\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);		\
	}																	\
	CANDLOOP(dst, i, TYPE##_nil, end, cnt);								\
	BATsetcount(bn, cnt);												\
	bn->theap.dirty = true;												\
																		\
	bn->tsorted = false;												\
	bn->trevsorted = false;												\
	bn->tnil = nils != 0;												\
	bn->tnonil = nils == 0;												\
	BATkey(bn, false);													\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}																		\
																		\
str CMDscience_bat_##TYPE##_##FUNC(bat *ret, const bat *bid)			\
{																		\
	return CMDscience_bat_##TYPE##_##FUNC##_cand(ret, bid, NULL);		\
}

#define scienceBinaryImpl(FUNC,TYPE,SUFF)								\
str CMDscience_bat_cst_##FUNC##_##TYPE##_cand(bat *ret, const bat *bid, \
											  const TYPE *d, const bat *sid) \
{																		\
	BAT *b, *s = NULL, *bn;												\
	BUN i, cnt, start, end;												\
	const oid *restrict cand = NULL, *candend = NULL;					\
	BUN nils = 0;														\
	int e = 0, ex = 0;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	if (sid != NULL && !is_bat_nil(*sid) &&								\
		(s = BATdescriptor(*sid)) == NULL) {							\
		BBPunfix(b->batCacheid);										\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	CANDINIT(b, s, start, end, cnt, cand, candend);						\
	bn = COLnew(b->hseqbase, TYPE_##TYPE, cnt, TRANSIENT);				\
	if (bn == NULL) {													\
		BBPunfix(b->batCacheid);										\
		BBPunfix(s->batCacheid);										\
		throw(MAL, "batcalc." #FUNC, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}																	\
																		\
	const TYPE *restrict src = (const TYPE *) Tloc(b, 0);				\
	TYPE *restrict dst = (TYPE *) Tloc(bn, 0);							\
	CANDLOOP(dst, i, TYPE##_nil, 0, start);								\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->tnonil && cand == NULL) {									\
		for (i = start; i < end; i++)									\
			dst[i] = FUNC##SUFF(src[i], *d);							\
	} else {															\
		if (cand) {														\
			for (i = start; i < end; i++) {								\
				if (i < *cand - b->hseqbase) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
					continue;											\
				}														\
				assert(i == *cand - b->hseqbase);						\
				if (++cand == candend)									\
					end = i + 1;										\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(src[i], *d);					\
				}														\
			}															\
		} else {														\
			for (i = start; i < end; i++) {								\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(src[i], *d);					\
				}														\
			}															\
		}																\
	}																	\
	e = errno;															\
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);			\
	BBPunfix(b->batCacheid);											\
	if (s)																\
		BBPunfix(s->batCacheid);										\
	if (e != 0 || ex != 0) {											\
		const char *err;												\
		BBPunfix(bn->batCacheid);										\
		if (e)															\
			err = strerror(e);											\
		else if (ex & FE_DIVBYZERO)										\
			err = "Divide by zero";										\
		else if (ex & FE_OVERFLOW)										\
			err = "Overflow";											\
		else															\
			err = "Invalid result";										\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);		\
	}																	\
	CANDLOOP(dst, i, TYPE##_nil, end, cnt);								\
	BATsetcount(bn, cnt);												\
	bn->theap.dirty = true;												\
																		\
	bn->tsorted = false;												\
	bn->trevsorted = false;												\
	bn->tnil = nils != 0;												\
	bn->tnonil = nils == 0;												\
	BATkey(bn, false);													\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}																		\
																		\
str CMDscience_bat_cst_##FUNC##_##TYPE(bat *ret, const bat *bid,		\
									   const TYPE *d)					\
{																		\
	return CMDscience_bat_cst_##FUNC##_##TYPE##_cand(ret, bid, d, NULL); \
}																		\
																		\
str CMDscience_cst_bat_##FUNC##_##TYPE##_cand(bat *ret, const TYPE *d,	\
											  const bat *bid, const bat *sid) \
{																		\
	BAT *b, *s = NULL, *bn;												\
	BUN i, cnt, start, end;												\
	const oid *restrict cand = NULL, *candend = NULL;					\
	BUN nils = 0;														\
	int e = 0, ex = 0;													\
																		\
	if ((b = BATdescriptor(*bid)) == NULL) {							\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	if (sid != NULL && !is_bat_nil(*sid) &&								\
		(s = BATdescriptor(*sid)) == NULL) {							\
		BBPunfix(b->batCacheid);										\
		throw(MAL, #TYPE, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);		\
	}																	\
	CANDINIT(b, s, start, end, cnt, cand, candend);						\
	bn = COLnew(b->hseqbase, TYPE_##TYPE, cnt, TRANSIENT);				\
	if (bn == NULL) {													\
		BBPunfix(b->batCacheid);										\
		BBPunfix(s->batCacheid);										\
		throw(MAL, "batcalc." #FUNC, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
	}																	\
																		\
	const TYPE *restrict src = (const TYPE *) Tloc(b, 0);				\
	TYPE *restrict dst = (TYPE *) Tloc(bn, 0);							\
	CANDLOOP(dst, i, TYPE##_nil, 0, start);								\
	errno = 0;															\
	feclearexcept(FE_ALL_EXCEPT);										\
	if (b->tnonil && cand == NULL) {									\
		for (i = start; i < end; i++)									\
			dst[i] = FUNC##SUFF(*d, src[i]);							\
	} else {															\
		if (cand) {														\
			for (i = start; i < end; i++) {								\
				if (i < *cand - b->hseqbase) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
					continue;											\
				}														\
				assert(i == *cand - b->hseqbase);						\
				if (++cand == candend)									\
					end = i + 1;										\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(*d, src[i]);					\
				}														\
			}															\
		} else {														\
			for (i = start; i < end; i++) {								\
				if (is_##TYPE##_nil(src[i])) {							\
					nils++;												\
					dst[i] = TYPE##_nil;								\
				} else {												\
					dst[i] = FUNC##SUFF(*d, src[i]);					\
				}														\
			}															\
		}																\
	}																	\
	e = errno;															\
	ex = fetestexcept(FE_INVALID | FE_DIVBYZERO | FE_OVERFLOW);			\
	BBPunfix(b->batCacheid);											\
	if (s)																\
		BBPunfix(s->batCacheid);										\
	if (e != 0 || ex != 0) {											\
		const char *err;												\
		BBPunfix(bn->batCacheid);										\
		if (e)															\
			err = strerror(e);											\
		else if (ex & FE_DIVBYZERO)										\
			err = "Divide by zero";										\
		else if (ex & FE_OVERFLOW)										\
			err = "Overflow";											\
		else															\
			err = "Invalid result";										\
		throw(MAL, "batmmath." #FUNC, "Math exception: %s", err);		\
	}																	\
	CANDLOOP(dst, i, TYPE##_nil, end, cnt);								\
	BATsetcount(bn, cnt);												\
	bn->theap.dirty = true;												\
																		\
	bn->tsorted = false;												\
	bn->trevsorted = false;												\
	bn->tnil = nils != 0;												\
	bn->tnonil = nils == 0;												\
	BATkey(bn, false);													\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}																		\
																		\
str CMDscience_cst_bat_##FUNC##_##TYPE(bat *ret, const TYPE *d,			\
									   const bat *bid)					\
{																		\
	return CMDscience_cst_bat_##FUNC##_##TYPE##_cand(ret, d, bid, NULL); \
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
