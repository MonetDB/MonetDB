/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, Aris Koning
 */
#ifndef _MOSAIC_UTILITY_
#define _MOSAIC_UTILITY_

#include "mosaic_join.h"

#define ID(a)				a
#define glue2(a, b)			a ## b
#define glue(a, b, c)		a ## b ## c
#define glue4(a, b, c, d)	a ## b ## c ## d
#define CONCAT2(a, b)		glue2(a, b)
#define CONCAT3(a, b, c)	glue(a, b, c)
#define CONCAT4(a, b, c, d)	glue4(a, b, c, d)
#define CONCAT6(a, b, c, d, e, f)	CONCAT2(glue(a, b, c), glue(d, e, f))
#define _STRINGIFY(ARG)		#ARG
#define STRINGIFY(ARG)		_STRINGIFY(ARG)

#define IS_NIL(TPE, VAL) CONCAT3(is_, TPE, _nil)(VAL)
#define ARE_EQUAL(v, w, HAS_NIL, TPE) ((v == w || (HAS_NIL && IS_NIL(TPE, v) && IS_NIL(TPE, w)) ) )

#define MOSBlockHeaderTpe(NAME, TPE) CONCAT4(MOSBlockHeader_, NAME, _, TPE)
#define ALIGNMENT_HELPER_TPE(NAME, TPE) struct CONCAT4(ALIGNMENT_HELPER_MOSBlockHeader_, NAME, _, TPE)

#define ALIGNMENT_HELPER__DEF(NAME, TPE) \
ALIGNMENT_HELPER_TPE(NAME, TPE)\
{\
	char a;\
	MOSBlockHeaderTpe(NAME, TPE) b;\
};

#define MOSadvance_SIGNATURE(NAME, TPE) void CONCAT4(MOSadvance_, NAME, _, TPE)(MOStask* task)
#define MOSprepareDictionaryContext_ID(NAME) MOSprepareDictionaryContext_##NAME
#define MOSprepareDictionaryContext_SIGNATURE(NAME) str MOSprepareDictionaryContext_ID(NAME)(MOStask* task)
#define MOSestimate_SIGNATURE(NAME, TPE) str CONCAT4(MOSestimate_, NAME, _, TPE)(MOStask* task, MosaicEstimation* current, const MosaicEstimation* previous)
#define MOSpostEstimate_SIGNATURE(NAME, TPE) void CONCAT4(MOSpostEstimate_, NAME, _, TPE)(MOStask* task)
#define MOSfinalizeDictionary_ID(NAME, TPE) MOSfinalizeDictionary_##NAME##_##TPE
#define MOSfinalizeDictionary_SIGNATURE(NAME, TPE) str MOSfinalizeDictionary_ID(NAME, TPE)(MOStask* task)
#define MOScompress_SIGNATURE(NAME, TPE) void CONCAT4(MOScompress_, NAME, _, TPE)(MOStask* task, MosaicBlkRec* estimate)
#define MOSdecompress_SIGNATURE(NAME, TPE) void CONCAT4(MOSdecompress_, NAME, _, TPE)(MOStask* task)
#define MOSBlockHeader_DEF(NAME, TPE) MosaicBlkHeader_DEF_##NAME(TPE)

#define MOSscanloop_ID(NAME, TPE, CAND_ITER, TEST) CONCAT2(CONCAT4(scan_loop_, NAME, _, TPE), CONCAT4(_, CAND_ITER, _, TEST))
#define MOSscanloop_ARGS(TPE) (const bool has_nil, const bool anti, MOStask* task, BUN first, BUN last, TPE tl, TPE th, bool li, bool hi)
#define MOSscanloop_SIGNATURE(NAME, TPE, CAND_ITER, TEST) static inline void MOSscanloop_ID(NAME, TPE, CAND_ITER, TEST) MOSscanloop_ARGS(TPE)

#define MOSselect_FUNC(NAME, TPE) CONCAT4(MOSselect_, NAME, _, TPE)
#define MOSselect_SIGNATURE(NAME, TPE) str MOSselect_FUNC(NAME, TPE)(MOStask* task, TPE tl, TPE th, bool li, bool hi, bool anti)

#define MOSprojectionloop_ID(NAME, TPE, CAND_ITER) CONCAT6(projection_loop_, NAME, _, TPE, _, CAND_ITER)
#define MOSprojectionloop_ARGS (MOStask* task, BUN first, BUN last)
#define MOSprojectionloop_SIGNATURE(NAME, TPE, CAND_ITER, TEST) static inline void MOSprojectionloop_ID(NAME, TPE, CAND_ITER) MOSprojectionloop_ARGS

#define MOSprojection_FUNC(NAME, TPE) CONCAT4(MOSprojection_, NAME, _, TPE)
#define MOSprojection_SIGNATURE(NAME, TPE)  str MOSprojection_FUNC(NAME, TPE) (MOStask* task)

#define ALGEBRA_INTERFACE(NAME, TPE) \
MOSadvance_SIGNATURE(NAME, TPE);\
MOSestimate_SIGNATURE(NAME, TPE);\
MOSpostEstimate_SIGNATURE(NAME, TPE);\
MOScompress_SIGNATURE(NAME, TPE);\
MOSdecompress_SIGNATURE(NAME, TPE);\
MOSselect_SIGNATURE(NAME, TPE);\
MOSprojection_SIGNATURE(NAME, TPE);\
MOSjoin_COUI_SIGNATURE(NAME, TPE);\
MOSBlockHeader_DEF(NAME, TPE);\
ALIGNMENT_HELPER__DEF(NAME, TPE);

#ifdef HAVE_HGE
#define ALGEBRA_INTERFACES_INTEGERS_ONLY(NAME) \
ALGEBRA_INTERFACE(NAME, bte);\
ALGEBRA_INTERFACE(NAME, sht);\
ALGEBRA_INTERFACE(NAME, int);\
ALGEBRA_INTERFACE(NAME, lng);\
ALGEBRA_INTERFACE(NAME, hge);
#else
#define ALGEBRA_INTERFACES_INTEGERS_ONLY(NAME) \
ALGEBRA_INTERFACE(NAME, bte);\
ALGEBRA_INTERFACE(NAME, sht);\
ALGEBRA_INTERFACE(NAME, int);\
ALGEBRA_INTERFACE(NAME, lng);
#endif

#define ALGEBRA_INTERFACES_ALL_TYPES(NAME) \
ALGEBRA_INTERFACES_INTEGERS_ONLY(NAME)\
ALGEBRA_INTERFACE(NAME, flt);\
ALGEBRA_INTERFACE(NAME, dbl);

#ifdef HAVE_HGE
#define ALGEBRA_INTERFACES_ALL_TYPES_WITH_DICTIONARY(NAME) \
ALGEBRA_INTERFACES_ALL_TYPES(NAME);\
MOSprepareDictionaryContext_SIGNATURE(NAME);\
MOSfinalizeDictionary_SIGNATURE(NAME, bte);\
MOSfinalizeDictionary_SIGNATURE(NAME, sht);\
MOSfinalizeDictionary_SIGNATURE(NAME, int);\
MOSfinalizeDictionary_SIGNATURE(NAME, lng);\
MOSfinalizeDictionary_SIGNATURE(NAME, hge);\
MOSfinalizeDictionary_SIGNATURE(NAME, flt);\
MOSfinalizeDictionary_SIGNATURE(NAME, dbl);
#else
#define ALGEBRA_INTERFACES_ALL_TYPES_WITH_DICTIONARY(NAME) \
ALGEBRA_INTERFACES_ALL_TYPES(NAME);\
MOSprepareDictionaryContext_SIGNATURE(NAME);\
MOSfinalizeDictionary_SIGNATURE(NAME, bte);\
MOSfinalizeDictionary_SIGNATURE(NAME, sht);\
MOSfinalizeDictionary_SIGNATURE(NAME, int);\
MOSfinalizeDictionary_SIGNATURE(NAME, lng);\
MOSfinalizeDictionary_SIGNATURE(NAME, flt);\
MOSfinalizeDictionary_SIGNATURE(NAME, dbl);
#endif

// This is just an ugly work around for Microsoft Visual Studio to get the expansion of __VA_ARGS__ right.
#define EXPAND(X) X

#define DO_OPERATION_ON_INTEGERS_ONLY_bte(OPERATION, NAME, ...) EXPAND(OPERATION(NAME, bte, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_sht(OPERATION, NAME, ...) EXPAND(OPERATION(NAME, sht, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_int(OPERATION, NAME, ...) EXPAND(OPERATION(NAME, int, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_lng(OPERATION, NAME, ...) EXPAND(OPERATION(NAME, lng, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_flt(OPERATION, NAME, ...) assert(0)
#define DO_OPERATION_ON_INTEGERS_ONLY_dbl(OPERATION, NAME, ...) assert(0)
#ifdef HAVE_HGE
#define DO_OPERATION_ON_INTEGERS_ONLY_hge(OPERATION, NAME, ...) EXPAND(OPERATION(NAME, hge, __VA_ARGS__))
#endif

#define DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, NAME, TPE, ...)    DO_OPERATION_ON_INTEGERS_ONLY_##TPE(OPERATION, NAME, __VA_ARGS__)
#define DO_OPERATION_ON_ALL_TYPES(OPERATION, NAME, TPE, ...)        EXPAND(OPERATION(NAME, TPE, __VA_ARGS__))

/*DUMMY_PARAM is just an ugly workaround for the fact that a variadic macro must have at least one variadic parameter*/
#define DO_OPERATION_IF_ALLOWED(OPERATION, NAME, TPE)               DO_OPERATION_ON_##NAME(OPERATION, TPE, 0 /*DUMMY_PARAM*/)
#define DO_OPERATION_IF_ALLOWED_VARIADIC(OPERATION, NAME, TPE, ...) DO_OPERATION_ON_##NAME(OPERATION, TPE, __VA_ARGS__)

#endif /* _MOSAIC_UTILITY_ */
