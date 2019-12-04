#ifndef _MOSAIC_UTILITY_
#define _MOSAIC_UTILITY_

#include "mosaic_select.h"
#include "mosaic_projection.h"
#include "mosaic_join.h"

#define MOSadvance_SIGNATURE(NAME, TPE) void MOSadvance_##NAME##_##TPE(MOStask task)
#define MOSestimate_SIGNATURE(NAME, TPE) str MOSestimate_##NAME##_##TPE(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
#define MOSpostEstimate_SIGNATURE(NAME, TPE) void MOSpostEstimate_##NAME##_##TPE(MOStask task)
#define MOScompress_SIGNATURE(NAME, TPE) void MOScompress_##NAME##_##TPE(MOStask task, MosaicBlkRec* estimate)
#define MOSdecompress_SIGNATURE(NAME, TPE) void MOSdecompress_##NAME##_##TPE(MOStask task)

#define ALGEBRA_INTERFACE(NAME, TPE) \
MOSadvance_SIGNATURE(NAME, TPE);\
MOSestimate_SIGNATURE(NAME, TPE);\
MOSpostEstimate_SIGNATURE(NAME, TPE);\
MOScompress_SIGNATURE(NAME, TPE);\
MOSdecompress_SIGNATURE(NAME, TPE);\
MOSselect_SIGNATURE(NAME, TPE);\
MOSprojection_SIGNATURE(NAME, TPE);\
MOSjoin_COUI_SIGNATURE(NAME, TPE);

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

#define DO_OPERATION_ON_INTEGERS_ONLY_bte(OPERATION, NAME, ...) do_##OPERATION(NAME, bte, __VA_ARGS__)
#define DO_OPERATION_ON_INTEGERS_ONLY_sht(OPERATION, NAME, ...) do_##OPERATION(NAME, sht, __VA_ARGS__)
#define DO_OPERATION_ON_INTEGERS_ONLY_int(OPERATION, NAME, ...) do_##OPERATION(NAME, int, __VA_ARGS__)
#define DO_OPERATION_ON_INTEGERS_ONLY_lng(OPERATION, NAME, ...) do_##OPERATION(NAME, lng, __VA_ARGS__)
#define DO_OPERATION_ON_INTEGERS_ONLY_flt(OPERATION, NAME, ...) assert(0)
#define DO_OPERATION_ON_INTEGERS_ONLY_dbl(OPERATION, NAME, ...) assert(0)
#ifdef HAVE_HGE
#define DO_OPERATION_ON_INTEGERS_ONLY_hge(OPERATION, NAME, ...) do_##OPERATION(NAME, hge, __VA_ARGS__)
#endif

#define DO_OPERATION_ON_INTEGERS_ONLY(OPERATION, NAME, TPE, ...)    DO_OPERATION_ON_INTEGERS_ONLY_##TPE(OPERATION, NAME, __VA_ARGS__)
#define DO_OPERATION_ON_ALL_TYPES(OPERATION, NAME, TPE, ...)        do_##OPERATION(NAME, TPE, __VA_ARGS__)

/*DUMMY_PARAM is just an ugly workaround for the fact that a variadic macro must have at least one variadic parameter*/
#define DO_OPERATION_IF_ALLOWED(OPERATION, NAME, TPE)               DO_OPERATION_ON_##NAME(OPERATION, TPE, 0 /*DUMMY_PARAM*/)
#define DO_OPERATION_IF_ALLOWED_VARIADIC(OPERATION, NAME, TPE, ...) DO_OPERATION_ON_##NAME(OPERATION, TPE, __VA_ARGS__)

#endif /* _MOSAIC_UTILITY_ */
