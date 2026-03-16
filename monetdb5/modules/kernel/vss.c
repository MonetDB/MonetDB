/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * BOND: Branch-and-bound ON Decomposed data
 *
 * MAL module providing k-NN search and L2 squared distance computation
 * on decomposed vector columns (one BAT per dimension).
 */


/* monetdb_config.h must be included as the first include file */
#include <monetdb_config.h>
#include <gdk.h>

/* mal_exception.h actually contains everything we need */
#include <mal.h>
#include <mal_exception.h>
#include <mal_client.h>
#include <mal_arguments.h>

/* system include files */
#include <string.h>
#include <stdlib.h>


#include <math.h>
#include <stddef.h>
#include <stdint.h>

#define DIMENSION_MISMATCH_ERR "Vector dimensions mismatch"

/**
 * @brief Generic Macro to generate L1(Manhattan) distance functions.
 * @param NAME The suffix for the function name (e.g., f32, f64).
 * @param T    The scalar type for input vectors (e.g., float, double).
 * @param R    The result/accumulation type.
 */
#define DEFINE_METRIC_L1(NAME, T, R)                                          \
static R \
metric_l1_##NAME(MalStkPtr stk, InstrPtr pci, size_t dim)                   \
{                                                                             \
    R dist = 0;                                                               \
    /* SIMD optimization hints */                                             \
    _Pragma("GCC ivdep")                                                      \
    for (size_t i=0; i < dim; i++) {                                          \
        T ai = *getArgReference_##T(stk, pci, 1 + i);                         \
        T bi = *getArgReference_##T(stk, pci, 1 + dim + i);                   \
        R diff = (R)ai - (R)bi;                                               \
        dist += (diff < 0) ? -diff : diff;  /*absolute value*/                \
    }                                                                         \
    return dist;                                                              \
}

// For float (32-bit)
//DEFINE_METRIC_L1(f32, flt, flt)

// For double (64-bit)
DEFINE_METRIC_L1(f64, dbl, dbl)

/**
 * @brief Generic Macro to generate Inner (Dot) Product distance functions.
 * @param NAME The suffix for the function name (e.g., f32, f64).
 * @param T    The scalar type for input vectors (e.g., float, double).
 * @param R    The result/accumulation type.
 */
#define DEFINE_METRIC_IP(NAME, T, R)                                            \
static R \
metric_ip_##NAME(MalStkPtr stk, InstrPtr pci, size_t dim)                     \
{                                                                               \
    R ab = 0;                                                                   \
    /* SIMD optimization hints */                                               \
    _Pragma("GCC ivdep")                                                        \
    for (size_t i=0; i < dim; i++) {                                            \
        T ai = *getArgReference_##T(stk, pci, 1 + i);                           \
        T bi = *getArgReference_##T(stk, pci, 1 + dim + i);                     \
        ab += (R)ai * (R)bi;                                                    \
    }                                                                           \
    return (R)1.0 - ab;                                                         \
}

// For float (32-bit)
//DEFINE_METRIC_IP(f32, flt, flt)

// For double (64-bit)
DEFINE_METRIC_IP(f64, dbl, dbl)

/**
 * @brief Generic Macro to generate Euclidean Distance functions.
 * @param NAME The suffix for the function name (e.g., f32, f64).
 * @param T    The scalar type for input vectors (e.g., float, double).
 * @param R    The result/accumulation type (to prevent overflow).
 */
#define DEFINE_METRIC_L2SQ(NAME, T, R)                                          \
static R \
metric_l2sq_##NAME(MalStkPtr stk, InstrPtr pci, size_t dim)                   \
{                                                                               \
    R dist = 0;                                                                 \
    /* SIMD optimization hints */                                               \
    _Pragma("GCC ivdep")                                                        \
    for (size_t i=0; i < dim; i++) {                                            \
        T ai = *getArgReference_##T(stk, pci, 1 + i);                           \
        T bi = *getArgReference_##T(stk, pci, 1 + dim + i);                     \
        R diff = (R)ai - (R)bi;                                                 \
        dist += diff * diff;                                                    \
    }                                                                           \
    return dist;                                                                \
}

// For float (32-bit)
//DEFINE_METRIC_L2SQ(f32, flt, flt)

// For double (64-bit)
DEFINE_METRIC_L2SQ(f64, dbl, dbl)


// Helper to handle the sqrt selection based on type size
#define SELECT_SQRT(R, val) ((sizeof(R) == 8) ? sqrt(val) : sqrtf((float)(val)))

/**
 * @brief Generic Macro to generate Cosine(Angular) Distance functions.
 * @param NAME The suffix for the function name (e.g., f32, f64).
 * @param T    The scalar type for input vectors (e.g., float, double).
 * @param R    The result/accumulation type (to prevent overflow).
 */
#define DEFINE_METRIC_COS(NAME, T, R)                                          \
static R																	\
metric_cos_##NAME(MalStkPtr stk, InstrPtr pci, size_t dim) {                 \
    R ab = 0, a2 = 0, b2 = 0;                                                  \
                                                                               \
    /* SIMD optimization hints */                                              \
    _Pragma("GCC ivdep")                                                       \
    for (size_t i = 0; i < dim; i++) {                                         \
        T ai = *getArgReference_##T(stk, pci, 1 + i);                           \
        T bi = *getArgReference_##T(stk, pci, 1 + dim + i);                     \
        ab += ai * bi;                                                         \
        a2 += ai * ai;                                                         \
        b2 += bi * bi;                                                         \
    }                                                                          \
    R result_if_zero[2][2];                                                    \
    R norm_a = SELECT_SQRT(R, a2);                                             \
    R norm_b = SELECT_SQRT(R, b2);                                             \
                                                                               \
    result_if_zero[0][0] = (R)1.0 - (ab / (norm_a * norm_b));                  \
    result_if_zero[0][1] = (R)1.0;                                             \
    result_if_zero[1][0] = (R)1.0;                                             \
    result_if_zero[1][1] = (R)0.0;                                             \
    return result_if_zero[a2 == 0][b2 == 0];                                   \
}

// For float (32-bit)
//DEFINE_METRIC_COS(f32, flt, flt)

// For double (64-bit)
DEFINE_METRIC_COS(f64, dbl, dbl)

// For int8_t (quantized), but accumulating in float to avoid overflow
//DEFINE_METRIC_COS(i8, int8_t, flt)


/**
 * @brief Generic Macro to generate scalar handlers.
 * @param METRIC The prefix for the function name (e.g. l2sq, cos, ip).
 * @param TNAME The suffix for the function name (e.g., f32, f64).
 * @param T    The scalar type for input vectors (e.g., float, double).
 * @param R    The result/accumulation type (to prevent overflow).
 */
#define DEFINE_SCALAR_HANDLER(METRIC, TNAME, T, R)                                      \
static char*                                                                            \
METRIC##_distance_##TNAME(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)        \
{                                                                                       \
    (void) ctx;                                                                         \
    (void) mb;                                                                          \
    size_t ncols = (size_t) (pci->argc - pci->retc);                                    \
    /*check for even num bats*/                                                         \
    if ((ncols & 1) != 0)                                                               \
        throw(MAL, "vss."#METRIC"_distance", SQLSTATE(HY097) DIMENSION_MISMATCH_ERR);   \
    size_t dim = ncols / 2;                                                             \
    *getArgReference_##R(stk, pci, 0) = metric_##METRIC##_##TNAME(stk, pci, dim);       \
    return MAL_SUCCEED;                                                                 \
}

// Inner (Dot) Product float (32-bit)
//DEFINE_SCALAR_HANDLER(ip, f32, flt, flt)
// Inner (Dot) Product double (64-bit)
DEFINE_SCALAR_HANDLER(ip, f64, dbl, dbl)

// l2sq float (32-bit)
//DEFINE_SCALAR_HANDLER(l2sq, f32, flt, flt)
// l2sq double (64-bit)
DEFINE_SCALAR_HANDLER(l2sq, f64, dbl, dbl)

// L1(Manhattan) float (32-bit)
//DEFINE_SCALAR_HANDLER(l1, f32, flt, flt)
// L1(Manhattan) (64-bit)
DEFINE_SCALAR_HANDLER(l1, f64, dbl, dbl)

// cos float (32-bit)
//DEFINE_SCALAR_HANDLER(cos, f32, flt, flt)
// cos double (64-bit)
DEFINE_SCALAR_HANDLER(cos, f64, dbl, dbl)


//static char*
//l2sq_distance(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
//{
//    //usearch_error_t err = NULL;
//
//    dbl *ret = getArgReference_dbl(stk,pci,0);
//    size_t ncols = (size_t) (pci->argc - pci->retc);
//    // check for even num bats
//    if ((ncols & 1) != 0)
//        throw(MAL, "vss.l2sq_distance", SQLSTATE(HY097) "Dimension mismatch");
//    size_t dim = ncols / 2;
//    dbl dist = 0;
//
//    //dbl dist = usearch_distance(a, b, usearch_scalar_f32_k, dim, usearch_metric_l2sq_k, &err);
//    for (size_t i=0; i<dim; i++) {
//        dbl a = *getArgReference_dbl(stk,pci, 1 + i);
//        dbl b = *getArgReference_dbl(stk,pci, 1 + i + dim);
//        dbl diff = a - b;
//        dist += diff * diff;
//    }
//    //if (err)
//    //    throw(MAL, "vss.l2sq_distance", SQLSTATE(HY103) "usearch_distance error %s", err);
//    *getArgReference_dbl(stk,pci,0) = dist;
//
//    return MAL_SUCCEED;
//}

#define PATIAL_L2sq(R, BL, BR, CNT, T)                  \
do {                                                    \
    const T *a = (const T*) Tloc(BL, 0);                \
    const T *b = (const T*) Tloc(BR, 0);                \
    for(BUN p = 0; p<cnt; p++) {                        \
        T d = a[p] - b[p];                              \
        R[p] += (T)d*d;                                 \
    }                                                   \
} while (0)

static char*
BATl2sq_distance(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
    //usearch_error_t err = NULL;
    size_t ncols = (size_t) (pci->argc - pci->retc);
    // check for even num bats
    if ((ncols & 1) != 0)
        throw(MAL, "batvss.l2sq_distance", SQLSTATE(HY097) DIMENSION_MISMATCH_ERR);
    bat *ret = getArgReference_bat(stk, pci, 0);
    // 1st dimmension bat
    BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
    size_t cnt = 0;
    if (b)
        cnt = BATcount(b);
    BBPreclaim(b);

    BAT *bn = COLnew(0, TYPE_dbl, cnt, TRANSIENT);
    if (bn == NULL)
        throw(MAL, "batvss.l2sq_distance", MAL_MALLOC_FAIL);

    dbl *dest = (dbl *) Tloc(bn, 0);
    memset(dest, 0, sizeof(dbl) * cnt);

    size_t dims = ncols / 2;

    for (size_t i = 0; i < dims; i++) {
        BAT *bl = BATdescriptor(*getArgReference_bat(stk, pci, (i+1)));
        BAT *br = BATdescriptor(*getArgReference_bat(stk, pci, (i+1) + dims));
        assert(bl&&br);
        int err = !(bl&&br);
        if (!err)
            PATIAL_L2sq(dest, bl, br, cnt, dbl);
        BBPreclaim(bl);
        BBPreclaim(br);
        if (err) {
            if (bn) BBPunfix(bn->batCacheid);
            throw(MAL, "batvss.l2sq_distance", RUNTIME_OBJECT_MISSING);
        }
    }
    // Finalize BAT metadata
    BATsetcount(bn, cnt);
    bn->tnonil = true;
    bn->tkey = false;
    bn->tsorted = false;
    bn->trevsorted = false;

    *ret = bn->batCacheid;
    BBPkeepref(bn);
    return MAL_SUCCEED;
}


#include "mel.h"
static mel_func vss_init_funcs[] = {
	pattern("vss", "ip_distance", ip_distance_f64, false,
		"Inner (Dot) Product distance",
		args(1, 2, arg("res", dbl), vararg("cols", dbl))),
	pattern("vss", "cos_distance", cos_distance_f64, false,
		"Cosine (Angular) distance",
		args(1, 2, arg("res", dbl), vararg("cols", dbl))),
	pattern("vss", "l1_distance", l1_distance_f64, false,
		"L1(Manhattan) distance",
		args(1, 2, arg("res", dbl), vararg("cols", dbl))),
	pattern("vss", "l2sq_distance", l2sq_distance_f64, false,
		"Euclidean distanse L2",
		args(1, 2, arg("res", dbl), vararg("cols", dbl))),
	pattern("batvss", "l2sq_distance", BATl2sq_distance, false,
		"Euclidean distanse L2",
		args(1, 3, batarg("res", dbl), batvararg("cols", dbl), optbatarg("k", int))),
	{ .imp=NULL }		/* sentinel */
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_vss_mal)
{ mal_module("vss", NULL, vss_init_funcs); }
