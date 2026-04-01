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
 * Vector columns are stored decomposed: each dimension is a separate BAT.
 * BOND exploits this layout by computing partial L2 distances dimension
 * by dimension, pruning candidates whose partial distance already exceeds
 * the current k-th best after each batch of dimensions.
 * Dimension ordering: dimensions are processed in decreasing order of
 * |query[d] - mean[d]|, so that the most discriminating dimensions come
 * first and pruning kicks in early.
 */


/* monetdb_config.h must be included as the first include file */
#include "mal_errors.h"
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

#define DIMENSION_MISMATCH_ERR "Dimensions mismatch error"

typedef struct bond_collection {
	BAT **dims;        /* array of dimension BATs (not owned, just referenced) */
	int ndims;         /* number of dimensions */
	BUN nvecs;         /* number of vectors (rows) */
	dbl *dim_means;    /* mean value per dimension (for dimension ordering) */
	dbl *dim_max;		/* max value per dimension */
	dbl *dim_min;		/* min value per dimension */
	dbl kth_upper;		/* kth upper bound */
} bond_collection;

/* Helper: comparison function for sorting dimensions by discriminating power */
typedef struct {
	int dim;
	dbl score;
} dim_score;

static int
dim_score_cmp(const void *a, const void *b)
{
	dbl sa = ((const dim_score *)a)->score;
	dbl sb = ((const dim_score *)b)->score;
	/* descending order */
	if (sa > sb)
		return -1;
	if (sa < sb)
		return 1;
	return 0;
}

/*
 * bond_dim_order: return an array of dimension indices sorted by
 * |query[d] - mean[d]| descending (most discriminating first).
 */
static int *
bond_dim_order(allocator *ma, bond_collection *bc, const dbl *query_vals)
{
	dim_score *scores = ma_alloc(ma, bc->ndims * sizeof(dim_score));
	int *order = ma_alloc(ma, bc->ndims * sizeof(int));

	if (!scores || !order) {
		TRC_ERROR(MAL_SERVER, "bond_dim_order: allocation failed\n");
		return NULL;
	}

	for (int d = 0; d < bc->ndims; d++) {
		scores[d].dim = d;
		dbl diff = query_vals[d] - bc->dim_means[d];
		scores[d].score = diff < 0 ? -diff : diff;
	}

	qsort(scores, bc->ndims, sizeof(dim_score), dim_score_cmp);

	for (int i = 0; i < bc->ndims; i++)
		order[i] = scores[i].dim;

	return order;
}

static bond_collection *
bond_create(allocator *ma, BAT **dim_bats, int ndims)
{
	if (dim_bats && ndims > 0) {
		bond_collection *bc = ma_alloc(ma, sizeof(bond_collection));
		*bc = (bond_collection) {
			.ndims = ndims,
			.kth_upper = DBL_MAX,
		};
		if (bc) {
			bc->dims = ma_alloc(ma, ndims * sizeof(BAT *));
			bc->dim_means = ma_alloc(ma, ndims * sizeof(dbl));
			bc->dim_max = ma_alloc(ma, ndims * sizeof(dbl));
			bc->dim_min = ma_alloc(ma, ndims * sizeof(dbl));
			bc->nvecs = BATcount(dim_bats[0]);
			for (int d = 0; d < ndims; d++) {
				BAT *b = dim_bats[d];
				bc->dims[d] = b;
				dbl avg;
				BUN cnt;
				if (BATcalcavg(b, NULL, &avg, &cnt, 0) != GDK_SUCCEED)
					return NULL;
				bc->dim_means[d] = avg;
				bc->dim_max[d] = *(dbl*)BATmax(b, NULL);
				bc->dim_min[d] = *(dbl*)BATmin(b, NULL);
			}
			return bc;
		}
	}
	return NULL;
}


static dbl
bond_upper_bound(bond_collection *bc, const dbl *query_vals, BUN k, dbl *acc, dbl *res)
{
	for (int i = 0; i < bc->ndims; i++) {
		BAT *b = bc->dims[i];
		size_t cnt = BATcount(b);
		assert(cnt > k);
		dbl *d = (dbl*) Tloc(b, 0);
		dbl q = query_vals[i];
		for (BUN j=0; j<k && j<cnt; j++) {
			dbl diff = q - d[j];
			dbl sq = diff * diff;
			acc[j] += sq;
			if (acc[j] > *res)
				*res = acc[j];
		}
	}
	return *res;
}


static char*
bond_search(bond_collection *bc, const dbl *query_vals,
			BUN k, int *dim_order, BAT *cands,
		   	BAT **oid_result, BAT **dist_result)
{
	BAT *partial = NULL;
	BAT *candidates = cands;
	bool own_candidates = false;

	if (!bc || !query_vals || k == 0 || !oid_result || !dist_result || !dim_order)
		throw(MAL, "vss.bond_search", "invalid arguments");

	*oid_result = NULL;
	*dist_result = NULL;

	for (int i = 0; i < bc->ndims; i++) {
		int d = dim_order[i];

		ValRecord qval = {.vtype = TYPE_dbl};
		qval.val.dval = query_vals[d];

		/* diff = dim[d] - query[d] */
		BAT *diff = BATcalcsubcst(bc->dims[d], &qval, candidates, TYPE_dbl);
		if (!diff)
			goto bailout;

		/* sq = diff * diff */
		BAT *sq = BATcalcmul(diff, diff, NULL, NULL, TYPE_dbl);
		BBPreclaim(diff);
		if (!sq)
			goto bailout;

		if (partial) {
			BAT *new_partial = BATcalcadd(partial, sq, NULL, NULL, TYPE_dbl);
			BBPreclaim(partial);
			BBPreclaim(sq);
			if (!new_partial)
				goto bailout;
			partial = new_partial;
		} else {
			partial = sq;
		}

		/* Prune after every m=2 step dimension (except the last) when
		 * there are more than 2*k candidates remaining */
		int step = 2;
		if ((i % step == 0) && i > 0 && i < (bc->ndims - 1) && BATcount(partial) > k * 2) {
			BAT *topn_oids = NULL;
			BAT *oids = NULL;

			if (BATfirstn(&topn_oids, NULL, partial, NULL,
				      NULL, k, true, false, false) != GDK_SUCCEED)
				goto bailout;

			///* Find the k-th (worst) distance among top-k */
			BATiter pi = bat_iterator(partial);
			BATiter ti = bat_iterator(topn_oids);
			dbl kth_dist = DBL_MAX;
			for (BUN j = 0; j < BATcount(topn_oids); j++) {
				oid o = BATtdense(topn_oids)? topn_oids->tseqbase + j : ((const oid *)ti.base)[j];
				dbl v = ((const dbl *)pi.base)[o - partial->hseqbase];
				if (v > kth_dist || kth_dist == DBL_MAX)
					kth_dist = v;
			}
			bat_iterator_end(&ti);
			bat_iterator_end(&pi);
			BBPreclaim(topn_oids);
			dbl theoretical_reminder = 0.0;
			for (int j=i+1; j < bc->ndims; j++) {
				// calc theoretical upper bound
				dbl min_val = bc->dim_min[j];
				dbl max_val = bc->dim_max[j];
				dbl d1 = query_vals[j] - min_val;
				dbl d2 = query_vals[j] - max_val;
				dbl sq = fmax(d1*d1, d2*d2);
				theoretical_reminder += sq;
			}

			// potentially tighten upper bound
			if (kth_dist != DBL_MAX && (bc->kth_upper > (kth_dist + theoretical_reminder)))
				bc->kth_upper = kth_dist + theoretical_reminder;

			/* Keep only candidates whose partial distance <= kth_dist */
			dbl zero = 0.0;
			BAT *new_cands = BATselect(partial, NULL,
						   &zero, &bc->kth_upper,
						   true, true, false, false);
			if (!new_cands)
				goto bailout;
			BAT *new_partial = BATproject(new_cands, partial);
			if (!new_partial) {
				BBPreclaim(new_cands);
				goto bailout;
			}
			BBPreclaim(partial);

			if (own_candidates) {
				oids = BATproject(new_cands, candidates);
				BBPreclaim(new_cands);
				BBPreclaim(candidates);
				candidates = oids;
			} else {
				candidates = new_cands;
			}
			partial = new_partial;
			own_candidates = true;
		}

		if (candidates && BATcount(candidates) < 10*k)
			break;
	}

	/* Final top-k selection */
	{
		BAT *topn_oids = NULL;
		BAT *res_oids = NULL;

		if (BATfirstn(&topn_oids, NULL, partial, NULL,
			      NULL, k, true, false, false) != GDK_SUCCEED)
			goto bailout;

		/* Project distances through top-k oids */
		BAT *result_dists = BATproject(topn_oids, partial);
		BBPreclaim(partial);
		if (!result_dists) {
			BBPreclaim(topn_oids);
			goto bailout;
		}
		if (own_candidates) {
			res_oids = BATproject(topn_oids, candidates);
			BBPreclaim(candidates);
			BBPreclaim(topn_oids);
		} else {
			res_oids = topn_oids;
		}

		*oid_result = res_oids;
		*dist_result = result_dists;
	}

	return MAL_SUCCEED;

bailout:
	BBPreclaim(partial);
	if (own_candidates)
		BBPreclaim(candidates);
	throw(MAL, "vss.bond_search", GDK_EXCEPTION);
}

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


static str
BONDknn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	lng k_lng = *getArgReference_lng(stk, pci, 2);
	if (k_lng <= 0)
		throw(MAL, "vss.knn", ILLEGAL_ARGUMENT ": k must be positive");
	int k = (int) k_lng;

	/* Calculate number of dimensions:
	 * pci->argc = 2 (returns) + 1 (k) + ndims (BATs) + ndims (query vals) */
	int nargs = pci->argc - 3;	/* args after k */
	if (nargs <= 0 || (nargs & 1) != 0)
		throw(MAL, "vss.knn", ILLEGAL_ARGUMENT ": expected equal number of dimension BATs and query values");
	int ndims = nargs / 2;

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	/* Get dimension BATs */
	BAT **dim_bats = ma_alloc(ta, ndims * sizeof(BAT *));
	dbl *query_vals = ma_alloc(ta, ndims * sizeof(dbl));
	dbl *kbuf = ma_alloc(ta, k * sizeof(dbl));
    memset(kbuf, 0, sizeof(dbl) * k);
	if (!dim_bats || !query_vals || !kbuf) {
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	for (int i = 0; i < ndims; i++) {
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 3 + i));
		if (!b) {
			for (int j = 0; j < i; j++)
				BBPreclaim(dim_bats[j]);
			ma_close(&ta_state);
			throw(MAL, "vss.knn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		dim_bats[i] = b;
		int qarg = 3 + ndims + i;
		//int qtype = getArgType(mb, pci, qarg);
		query_vals[i] = *getArgReference_dbl(stk, pci, qarg);
	}

	/* Create BOND collection and search */
	bond_collection *bc = bond_create(ta, dim_bats, ndims);
	if (!bc) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	int *dim_order = bond_dim_order(ta, bc, query_vals);
	if (!dim_order) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}
	dbl zero = 0.0;
	bc->kth_upper = bond_upper_bound(bc, query_vals, k, kbuf, &zero);
	BAT *oid_result = NULL, *dist_result = NULL;
	char *rc = bond_search(bc, query_vals, (BUN) k, dim_order, NULL, &oid_result, &dist_result);

	for (int i = 0; i < ndims; i++)
		BBPreclaim(dim_bats[i]);

	ma_close(&ta_state);
	if (rc != MAL_SUCCEED)
		return rc;

	*getArgReference_bat(stk, pci, 0) = oid_result->batCacheid;
	*getArgReference_bat(stk, pci, 1) = dist_result->batCacheid;
	BBPkeepref(oid_result);
	BBPkeepref(dist_result);
	return MAL_SUCCEED;
}


#include "mel.h"
static mel_func vss_init_funcs[] = {
	pattern("vss", "bond", BONDknn, false, "BOND k-NN search on decomposed vector columns",
		args(2, 4, batarg("", oid), batarg("", dbl),
		     arg("k", lng), varargany("args", 0))),
	pattern("vss", "bond", BONDknn, false, "BOND k-NN search on decomposed vector columns",
		args(2, 4, batarg("", oid), batarg("", dbl),
		     arg("k", lng), batvararg("args", dbl))),
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
		args(1, 2, batarg("", dbl), batvararg("cols", dbl))),
	{ .imp=NULL }		/* sentinel */
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_vss_mal)
{ mal_module("vss", NULL, vss_init_funcs); }
