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
	oid *candidates;
	oid *tcand;
	oid *tc;
	dbl *dists;
	dbl *tdist;
	dbl *td;
	dbl kth_upper;		/* kth upper bound */
} bond_collection;

/* Helper: comparison function for sorting dimensions by discriminating power */
typedef struct {
	int dim;
	dbl score;
} dim_score;

//static int dbl_cmp(const void *a, const void *b) {
//    dbl fa = *(const dbl *)a;
//    dbl fb = *(const dbl *)b;
//    return (fa > fb) - (fa < fb);
//}

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

#define VS 1024
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
			bc->candidates = ma_alloc(ma, VS * sizeof(oid));
			bc->dists = ma_alloc(ma, VS * sizeof(dbl));

			bc->tcand = ma_alloc(ma, VS * sizeof(oid));
			bc->tdist = ma_alloc(ma, VS * sizeof(dbl));
			bc->tc = ma_alloc(ma, VS * sizeof(oid));
			bc->td = ma_alloc(ma, VS * sizeof(dbl));
			bc->dims = ma_alloc(ma, ndims * sizeof(BAT *));
			bc->dim_means = ma_zalloc(ma, ndims * sizeof(dbl));
			bc->dim_max = ma_zalloc(ma, ndims * sizeof(dbl));
			bc->dim_min = ma_zalloc(ma, ndims * sizeof(dbl));
			bc->nvecs = BATcount(dim_bats[0]);
			for (int d = 0; d < ndims; d++) {
				BAT *b = dim_bats[d];
				bc->dims[d] = b;
			}
			return bc;
		}
	}
	return NULL;
}

static inline void
heap_down(oid *hcand, dbl *hd, int p, int k)
{
	int l = p*2+1, r = p*2+2, q = p;
	if (l < k && hd[q] < hd[l])
		q = l;
	if (r < k && hd[q] < hd[r])
		q = r;
	if (q != p) {
		oid s = hcand[q];
		hcand[q] = hcand[p];
		hcand[p] = s;
		dbl d = hd[q];
		hd[q] = hd[p];
		hd[p] = d;
		heap_down(hcand, hd, q, k);
	}
}

static inline void
heap_del(oid *hcand, dbl *hd, int k)
{
	hcand[0] = hcand[k-1];
	hd[0] = hd[k-1];
	heap_down(hcand, hd, 0, k-1);
}

static inline void
heap_up(oid *hcand, dbl *hd, int p)
{
	if (p == 0)
		return;
	size_t q = (p-1)/2;
	if (hd[q] < hd[p]) { /* max heap */
		oid s = hcand[q];
		hcand[q] = hcand[p];
		hcand[p] = s;
		dbl d = hd[q];
		hd[q] = hd[p];
		hd[p] = d;
		heap_up(hcand, hd, q);
	}
}

/* max heap */
static dbl
topn_merge(oid *cands, dbl *d, oid *icands, dbl *id, BUN n, BUN k)
{
	for (BUN i = 0; i < n; i++) {
		if (id[i] < d[0]) {
			heap_del(cands, d, k);
			cands[k-1] = icands[i];
			d[k-1] = id[i];
			heap_up(cands, d, k-1);
		}
	}
	/*
	for(i = 0; i < k; i++)
		printf("%d %F\n", (int)hcand[i], hd[i]);
		*/
	return d[0];
}

/* max heap */
static dbl
topn(oid *cands, dbl *d, bond_collection *bc, BUN n, BUN k)
{
	oid *hcand = bc->tcand;
	dbl *hd = bc->tdist;
	BUN i = 0;
	for (; i < k && i < n; i++) {
		hcand[i] = cands[i];
		hd[i] = d[i];
		heap_up(hcand, hd, i);
	}

	for (; i < n; i++) {
		if (d[i] < hd[0]) {
			heap_del(hcand, hd, k);
			hcand[k-1] = cands[i];
			hd[k-1] = d[i];
			heap_up(hcand, hd, k-1);
		}
	}
	/*
	for(i = 0; i < k; i++)
		printf("%d %F\n", (int)hcand[i], hd[i]);
		*/
	return hd[0];
}

static dbl
bond_upper_bound_sampled(bond_collection *bc, const dbl *query_vals, BUN k)
{
	oid *cands = bc->candidates;
	dbl *dists = bc->dists;

	for (BUN i = 0; i < VS; i++) {
		cands[i] = i;
		dists[i] = 0;
	}
	// calc avg over the sample instead
	for (int d = 0; d < bc->ndims; d++) {
		dbl *v = (dbl*)Tloc(bc->dims[d], 0);
		bc->dim_min[d] = bc->dim_max[d] = bc->dim_means[d] = v[0];
		for(BUN i = 1; i < VS; i++) {
	 		bc->dim_means[d] += v[i];
			if (bc->dim_min[d] > v[i])
				bc->dim_min[d] = v[i];
			if (bc->dim_max[d] < v[i])
				bc->dim_max[d] = v[i];
		}
		bc->dim_means[d] /= VS;
	}

	// calc full distance for the sample across all dimensions
    for (int d = 0; d < bc->ndims; d++) {
        const dbl *vals = (const dbl *) Tloc(bc->dims[d], 0);
        dbl q = query_vals[d];

        for (BUN i = 0; i < VS; i++) {
            dbl diff = vals[cands[i]] - q;
            dists[i] += diff * diff;
        }
    }
	dbl res = topn(cands, dists, bc, VS, k);
	for(BUN i = 0; i < k; i++) {
		cands [i] = bc->tcand[i];
		dists[i] = bc->tdist[i];
	}
	return res;
}


static char*
bond_search_fast(allocator *ma, bond_collection *bc, const dbl *query_vals,
			BUN k, int *dim_order, BAT *cands,
		   	BAT **oid_result, BAT **dist_result)
{
	lng T0 = GDKusec();
	if (cands || !bc || !query_vals || k == 0 || !oid_result || !dist_result || !dim_order)
		throw(MAL, "vss.bond_search", "invalid arguments");

    dbl *rem_per_dim = ma_zalloc(ma, sizeof(dbl) * bc->ndims);
	if (!rem_per_dim)
		throw(MAL, "vss.bond_search", MAL_MALLOC_FAIL);
	// pre-calculate theoretical maximums
    dbl total_rem = 0;
    for (int i = bc->ndims - 1; i >= 0; i--) {
        int d = dim_order[i];
		dbl d1 = query_vals[d] - bc->dim_min[d];
        dbl d2 = query_vals[d] - bc->dim_max[d];
		rem_per_dim[d] = total_rem;
        total_rem += fmax(d1*d1, d2*d2);
    }

	lng Tdists = 0;
	lng Tprune = 0;
	lng Ttopn = 0;
	BUN ncands = bc->nvecs;
	for (BUN z = 0; z < ncands; z+=VS) {
		BUN end = z+VS > ncands?ncands:z+VS, j, i = 0;
		// initialize all vectors are candidates
		for (j = z, i = 0; j<end; j++, i++) {
			bc->tc[i] = j;
			bc->td[i] = 0;
		}

		BUN sz = i;
		int step = 4;
		for (int d = 0; d < bc->ndims; d++) {
			/* partial dists */
			int o = dim_order[d];
			dbl qd = query_vals[o];
			BAT *b = bc->dims[o];
			const dbl *col = (const dbl*) Tloc(b, 0);

			lng T0 = GDKusec();
			for (BUN i = 0; i < sz; i++) {
				dbl diff = col[bc->tc[i]] - qd;
				bc->td[i] += diff * diff;
			}
			Tdists += GDKusec() - T0;
			//printf("partial dists chunk " BUNFMT " " BUNFMT " d=%zu t=" LLFMT "\n", j, sz, (size_t)d,  GDKusec() - T0);
			if (sz <= 2*k || ((d&(step-1)) != 0))
				continue;

			dbl kth_dist = topn(bc->tc, bc->td, bc, sz, k);
			dbl theoretical_reminder = rem_per_dim[o];
			// potentially tighten upper bound
			if (bc->kth_upper > (kth_dist + theoretical_reminder)) {
				printf("better bound\n");
				bc->kth_upper = kth_dist + theoretical_reminder;
			}

			/* prune */
			T0 = GDKusec();
			BUN write_pos = 0;
			for (BUN read_pos = 0; read_pos < sz; read_pos++) {
				if (bc->td[read_pos] <= bc->kth_upper) {
					bc->tc[write_pos] = bc->tc[read_pos];
					bc->td[write_pos] = bc->td[read_pos];
					write_pos++;
				}
			}
			sz = write_pos;
			Tprune += GDKusec() - T0;
			//printf("prune " BUNFMT " d=%zu " "t=" LLFMT "\n",  sz, (size_t)d,  GDKusec() - T0);
		}

		lng T0 = GDKusec();
		bc->kth_upper = topn_merge(bc->candidates, bc->dists, bc->tc, bc->td, sz, k);
		Ttopn += GDKusec() - T0;
		//printf("topn chnkd " BUNFMT " t=" LLFMT " %F\n", j, GDKusec() - T0, bc->kth_upper);
	}
	printf("vectors t= " LLFMT " dists=" LLFMT " prune " LLFMT " topn " LLFMT " upper %F\n", GDKusec() - T0, Tdists, Tprune, Ttopn, bc->kth_upper);

	//T0 = GDKusec();
	BAT *koids = COLnew(0, TYPE_oid, k, TRANSIENT);
    BAT *kdist = COLnew(0, TYPE_dbl, k, TRANSIENT);
	// Final top k
	{
		BUN write_pos = 0;
		for (; write_pos < k; write_pos++) {
				*(oid*) Tloc(koids, write_pos) = bc->candidates[write_pos];
				*(dbl*) Tloc(kdist, write_pos) = bc->dists[write_pos];
		}
		k = write_pos;
	}
	BATsetcount(koids, k);
    koids->tsorted = false;
    koids->trevsorted = false;
	BATsetcount(kdist, k);
	kdist->tsorted = false;
    kdist->trevsorted = false;
    //kdist->tnonil = true;
    kdist->tkey = false;
	//printf("final top k " LLFMT "\n", GDKusec() - T0);

	//BATprint(GDKstdout, koids);
	//BATprint(GDKstdout, kdist);

	*oid_result = koids;
	*dist_result = kdist;
	return MAL_SUCCEED;
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
    GCC_Pragma("GCC ivdep")                                                      \
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
    GCC_Pragma("GCC ivdep")                                                        \
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
    GCC_Pragma("GCC ivdep")                                                        \
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
    GCC_Pragma("GCC ivdep")                                                       \
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

#define PATIAL_L2sq_const(R, BL, BR, CNT, T)            \
do {                                                    \
    const T *a = (const T*) Tloc(BL, 0);                \
    const T *b = (const T*) Tloc(BR, 0);                \
    for(BUN p = 0; p<cnt; p++) {                        \
        T d = a[p] - b[0];                              \
        R[p] += (T)d*d;                                 \
    }                                                   \
} while (0)

static char*
BATl2sq_distance(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
	//lng T0 = GDKusec();
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

	BAT *br = BATdescriptor(*getArgReference_bat(stk, pci, 1 + dims));
	BUN qcnt = BATcount(br);
	BBPreclaim(br);
	if (qcnt == 1) {
		for (size_t i = 0; i < dims; i++) {
			BAT *bl = BATdescriptor(*getArgReference_bat(stk, pci, (i+1)));
			BAT *br = BATdescriptor(*getArgReference_bat(stk, pci, (i+1) + dims));
			assert(bl&&br);
			int err = !(bl&&br);
			if (!err)
				PATIAL_L2sq_const(dest, bl, br, cnt, dbl);
			BBPreclaim(bl);
			BBPreclaim(br);
			if (err) {
				if (bn) BBPunfix(bn->batCacheid);
				throw(MAL, "batvss.l2sq_distance", RUNTIME_OBJECT_MISSING);
			}
		}
	} else {
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
	}
    // Finalize BAT metadata
    BATsetcount(bn, cnt);
    bn->tnonil = true;
    bn->tkey = false;
    bn->tsorted = false;
    bn->trevsorted = false;

	//printf("#BATl2sq " LLFMT "\n", GDKusec() - T0);

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

	//lng T0 = GDKusec();
	/* Get dimension BATs */
	BAT **dim_bats = ma_alloc(ta, ndims * sizeof(BAT *));
	dbl *query_vals = ma_alloc(ta, ndims * sizeof(dbl));
	dbl *kbuf = ma_zalloc(ta, k * sizeof(dbl));
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
	//lng T1 = GDKusec();
	//printf("creation " LLFMT "\n", T1 - T0);
	//T0 = T1;

	if (!bc) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	bc->kth_upper = bond_upper_bound_sampled(bc, query_vals, k);
	//T1 = GDKusec();
	//printf("upperbound " LLFMT " %F\n", T1 - T0, bc->kth_upper);
	//T0 = T1;

	int *dim_order = bond_dim_order(ta, bc, query_vals);
	if (!dim_order) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}
	//T1 = GDKusec();
	//printf("order " LLFMT "\n", T1 - T0);
	//T0 = T1;

	BAT *oid_result = NULL, *dist_result = NULL;
	char *rc = bond_search_fast(ta, bc, query_vals, (BUN) k, dim_order, NULL, &oid_result, &dist_result);
	//T1 = GDKusec();
	//printf("search " LLFMT "\n", T1 - T0);
	//T0 = T1;

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
