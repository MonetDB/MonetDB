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
#include "gdk_commons.h"


#define MAKE_TEMPLATES_INCLUDE_FILE(METHOD) STRINGIFY(CONCAT3(mosaic_, METHOD, _templates.h))

#define IS_NIL(TPE, VAL) CONCAT3(is_, TPE, _nil)(VAL)
#define ARE_EQUAL(v, w, HAS_NIL, TPE) ((v == w || (HAS_NIL && IS_NIL(TPE, v) && IS_NIL(TPE, w)) ) )

#define MOSBlockHeaderTpe(METHOD, TPE) CONCAT4(MOSBlockHeader_, METHOD, _, TPE)
#define ALIGNMENT_HELPER_TPE(METHOD, TPE) struct CONCAT4(ALIGNMENT_HELPER_MOSBlockHeader_, METHOD, _, TPE)

#define ALIGNMENT_HELPER__DEF(METHOD, TPE) \
ALIGNMENT_HELPER_TPE(METHOD, TPE)\
{\
	char a;\
	MOSBlockHeaderTpe(METHOD, TPE) b;\
};

#define MOSadvance_SIGNATURE(METHOD, TPE) void CONCAT4(MOSadvance_, METHOD, _, TPE)(MOStask* task)
#define MOSprepareDictionaryContext_ID(METHOD) MOSprepareDictionaryContext_##METHOD
#define MOSprepareDictionaryContext_SIGNATURE(METHOD) str MOSprepareDictionaryContext_ID(METHOD)(MOStask* task)
#define MOScleanUpInfo_ID(METHOD) CONCAT2(clean_up_info_, METHOD)
#define MOScleanUpInfo_SIGNATURE(METHOD) void MOScleanUpInfo_ID(METHOD)(MOStask* task)
#define MOSestimate_SIGNATURE(METHOD, TPE) str CONCAT4(MOSestimate_, METHOD, _, TPE)(MOStask* task, MosaicEstimation* current, const MosaicEstimation* previous)
#define MOSpostEstimate_SIGNATURE(METHOD, TPE) void CONCAT4(MOSpostEstimate_, METHOD, _, TPE)(MOStask* task)
#define MOSfinalizeDictionary_ID(METHOD, TPE) MOSfinalizeDictionary_##METHOD##_##TPE
#define MOSfinalizeDictionary_SIGNATURE(METHOD, TPE) str MOSfinalizeDictionary_ID(METHOD, TPE)(MOStask* task)

#define MOSlayoutARGUMENTS (MOStask* task, MosaicLayout* layout, lng current_bsn)
#define MOSlayout_ID(METHOD, TPE) CONCAT4(MOSlayout_, METHOD, _, TPE)
#define MOSlayout_SIGNATURE(METHOD, TPE) str MOSlayout_ID(METHOD, TPE) MOSlayoutARGUMENTS
#define MOSlayoutDictionary_ID(METHOD) CONCAT3(MOSlayout_, METHOD, _hdr)
#define MOSlayoutDictionary_SIGNATURE(METHOD) str MOSlayoutDictionary_ID(METHOD) MOSlayoutARGUMENTS

#define MOScompress_SIGNATURE(METHOD, TPE) void CONCAT4(MOScompress_, METHOD, _, TPE)(MOStask* task, MosaicBlkRec* estimate)
#define MOSdecompress_SIGNATURE(METHOD, TPE) void CONCAT4(MOSdecompress_, METHOD, _, TPE)(MOStask* task)
#define MOSBlockHeader_DEF(METHOD, TPE) MosaicBlkHeader_DEF_##METHOD(TPE)

#define MOSscanloop_ID(METHOD, TPE, CAND_ITER, TEST) CONCAT2(CONCAT4(scan_loop_, METHOD, _, TPE), CONCAT4(_, CAND_ITER, _, TEST))
#define MOSscanloop_ARGS(TPE) (const bool has_nil, const bool anti, MOStask* task, BUN first, BUN last, TPE tl, TPE th, bool li, bool hi)
#define MOSscanloop_SIGNATURE(METHOD, TPE, CAND_ITER, TEST) static inline void MOSscanloop_ID(METHOD, TPE, CAND_ITER, TEST) MOSscanloop_ARGS(TPE)

#define MOSselect_FUNC(METHOD, TPE) CONCAT4(MOSselect_, METHOD, _, TPE)
#define MOSselect_SIGNATURE(METHOD, TPE) str MOSselect_FUNC(METHOD, TPE)(MOStask* task, TPE tl, TPE th, bool li, bool hi, bool anti)

#define MOSprojectionloop_ID(METHOD, TPE, CAND_ITER) CONCAT6(projection_loop_, METHOD, _, TPE, _, CAND_ITER)
#define MOSprojectionloop_ARGS (MOStask* task, BUN first, BUN last)
#define MOSprojectionloop_SIGNATURE(METHOD, TPE, CAND_ITER) static inline void MOSprojectionloop_ID(METHOD, TPE, CAND_ITER) MOSprojectionloop_ARGS

#define MOSprojection_FUNC(METHOD, TPE) CONCAT4(MOSprojection_, METHOD, _, TPE)
#define MOSprojection_SIGNATURE(METHOD, TPE)  str MOSprojection_FUNC(METHOD, TPE) (MOStask* task)

#define MOSjoin_generic_ID(TPE) CONCAT2(MOSjoin_, TPE)
#define MOSjoin_generic_SIGNATURE static str MOSjoin_generic_ID(TPE) (MOStask* task, BAT* l, struct canditer* lci, bool nil_matches)
#define MOSjoin_inner_loop_ID(METHOD, TPE, NIL, RIGHT_CI_NEXT) CONCAT2(CONCAT4(MOSjoin_inner_loop_, METHOD, _, TPE), CONCAT4(_, NIL, _, RIGHT_CI_NEXT)) 
#define MOSjoin_inner_loop_SIGNATURE(METHOD, TPE, NIL, RIGHT_CI_NEXT) str MOSjoin_inner_loop_ID(METHOD, TPE, NIL, RIGHT_CI_NEXT) (MOStask* task, BAT* r1p, BAT* r2p, oid lo, TPE lval)

#define ALGEBRA_INTERFACE(METHOD, TPE) \
MOSadvance_SIGNATURE(METHOD, TPE);\
MOSestimate_SIGNATURE(METHOD, TPE);\
MOSpostEstimate_SIGNATURE(METHOD, TPE);\
MOScompress_SIGNATURE(METHOD, TPE);\
MOSdecompress_SIGNATURE(METHOD, TPE);\
MOSlayout_SIGNATURE(METHOD, TPE);\
MOSselect_SIGNATURE(METHOD, TPE);\
MOSprojection_SIGNATURE(METHOD, TPE);\
MOSjoin_COUI_SIGNATURE(METHOD, TPE);\
MOSBlockHeader_DEF(METHOD, TPE);\
MOSjoin_inner_loop_SIGNATURE(METHOD, TPE, _HAS_NIL		, canditer_next);\
MOSjoin_inner_loop_SIGNATURE(METHOD, TPE, _HAS_NO_NIL	, canditer_next);\
MOSjoin_inner_loop_SIGNATURE(METHOD, TPE, _HAS_NIL		, canditer_next_dense);\
MOSjoin_inner_loop_SIGNATURE(METHOD, TPE, _HAS_NO_NIL	, canditer_next_dense);\
ALIGNMENT_HELPER__DEF(METHOD, TPE);

#ifdef HAVE_HGE
#define ALGEBRA_INTERFACES_INTEGERS_ONLY(METHOD) \
ALGEBRA_INTERFACE(METHOD, bte);\
ALGEBRA_INTERFACE(METHOD, sht);\
ALGEBRA_INTERFACE(METHOD, int);\
ALGEBRA_INTERFACE(METHOD, lng);\
ALGEBRA_INTERFACE(METHOD, hge);
#else
#define ALGEBRA_INTERFACES_INTEGERS_ONLY(METHOD) \
ALGEBRA_INTERFACE(METHOD, bte);\
ALGEBRA_INTERFACE(METHOD, sht);\
ALGEBRA_INTERFACE(METHOD, int);\
ALGEBRA_INTERFACE(METHOD, lng);
#endif

#define ALGEBRA_INTERFACES_ALL_TYPES(METHOD) \
ALGEBRA_INTERFACES_INTEGERS_ONLY(METHOD)\
ALGEBRA_INTERFACE(METHOD, flt);\
ALGEBRA_INTERFACE(METHOD, dbl);

#ifdef HAVE_HGE
#define ALGEBRA_INTERFACES_ALL_TYPES_WITH_DICTIONARY(METHOD) \
ALGEBRA_INTERFACES_ALL_TYPES(METHOD);\
MOSprepareDictionaryContext_SIGNATURE(METHOD);\
MOScleanUpInfo_SIGNATURE(METHOD);\
MOSfinalizeDictionary_SIGNATURE(METHOD, bte);\
MOSfinalizeDictionary_SIGNATURE(METHOD, sht);\
MOSfinalizeDictionary_SIGNATURE(METHOD, int);\
MOSfinalizeDictionary_SIGNATURE(METHOD, lng);\
MOSfinalizeDictionary_SIGNATURE(METHOD, hge);\
MOSfinalizeDictionary_SIGNATURE(METHOD, flt);\
MOSfinalizeDictionary_SIGNATURE(METHOD, dbl);\
MOSlayoutDictionary_SIGNATURE(METHOD)
#else
#define ALGEBRA_INTERFACES_ALL_TYPES_WITH_DICTIONARY(METHOD) \
ALGEBRA_INTERFACES_ALL_TYPES(METHOD);\
MOSprepareDictionaryContext_SIGNATURE(METHOD);\
MOScleanUpInfo_SIGNATURE(METHOD);\
MOSfinalizeDictionary_SIGNATURE(METHOD, bte);\
MOSfinalizeDictionary_SIGNATURE(METHOD, sht);\
MOSfinalizeDictionary_SIGNATURE(METHOD, int);\
MOSfinalizeDictionary_SIGNATURE(METHOD, lng);\
MOSfinalizeDictionary_SIGNATURE(METHOD, flt);\
MOSfinalizeDictionary_SIGNATURE(METHOD, dbl);\
MOSlayoutDictionary_SIGNATURE(METHOD)
#endif

// This is just an ugly work around for Microsoft Visual Studio to get the expansion of __VA_ARGS__ right.
#define EXPAND(X) X

#define DO_OPERATION_ON_INTEGERS_ONLY_bte(OPERATION, METHOD, ...) EXPAND(OPERATION(METHOD, bte, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_sht(OPERATION, METHOD, ...) EXPAND(OPERATION(METHOD, sht, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_int(OPERATION, METHOD, ...) EXPAND(OPERATION(METHOD, int, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_lng(OPERATION, METHOD, ...) EXPAND(OPERATION(METHOD, lng, __VA_ARGS__))
#define DO_OPERATION_ON_INTEGERS_ONLY_flt(OPERATION, METHOD, ...) assert(0)
#define DO_OPERATION_ON_INTEGERS_ONLY_dbl(OPERATION, METHOD, ...) assert(0)
#ifdef HAVE_HGE
#define DO_OPERATION_ON_INTEGERS_ONLY_hge(OPERATION, METHOD, ...) EXPAND(OPERATION(METHOD, hge, __VA_ARGS__))
#endif

#define DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, METHOD, TPE, ...)    DO_OPERATION_ON_INTEGERS_ONLY_##TPE(OPERATION, METHOD, __VA_ARGS__)
#define DO_OPERATION_ON_ALL_TYPES(OPERATION, METHOD, TPE, ...)        EXPAND(OPERATION(METHOD, TPE, __VA_ARGS__))

/*DUMMY_PARAM is just an ugly workaround for the fact that a variadic macro must have at least one variadic parameter*/
#define DO_OPERATION_IF_ALLOWED(OPERATION, METHOD, TPE)               DO_OPERATION_ON_##METHOD(OPERATION, TPE, 0 /*DUMMY_PARAM*/)
#define DO_OPERATION_IF_ALLOWED_VARIADIC(OPERATION, METHOD, TPE, ...) DO_OPERATION_ON_##METHOD(OPERATION, TPE, __VA_ARGS__)

#define INTEGERS_ONLY_bte	!0
#define INTEGERS_ONLY_sht	!0
#define INTEGERS_ONLY_int	!0
#define INTEGERS_ONLY_lng	!0
#define INTEGERS_ONLY_flt	 0
#define INTEGERS_ONLY_dbl	 0
#ifdef HAVE_HGE
#define INTEGERS_ONLY_hge	!0
#endif
#define ALL_TYPES			!0
#define INTEGERS_ONLY(TPE)				INTEGERS_ONLY_##TPE
#define ALL_TYPES_SUPPORTED(TPE)		ALL_TYPES
#define TYPE_IS_SUPPORTED(METHOD, TPE)	TYPE_IS_SUPPORTED_##METHOD(TPE)

#define LAYOUT_BUFFER_SIZE 10000

#endif /* _MOSAIC_UTILITY_ */
