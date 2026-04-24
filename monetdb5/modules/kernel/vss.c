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
	dbl **dimsp;
	int ndims;         /* number of dimensions */
	int bsz;			   /* block size */
	BUN nvecs;         /* number of vectors (rows) */
	dbl *dim_means;    /* mean value per dimension (for dimension ordering) */
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
bond_dim_order(allocator *ma, bond_collection *bc)
{
	dim_score *scores = ma_alloc(ma, bc->ndims * sizeof(dim_score));
	int *order = ma_alloc(ma, bc->ndims * sizeof(int));

	if (!scores || !order) {
		TRC_ERROR(MAL_SERVER, "bond_dim_order: allocation failed\n");
		return NULL;
	}

	for (int d = 0; d < bc->ndims; d++) {
		scores[d].dim = d;
		scores[d].score = bc->dim_means[d];
	}

	/* TODO use gdk order, with 2 arrays, ie order and bc->dim_means */
	qsort(scores, bc->ndims, sizeof(dim_score), dim_score_cmp);
	/*
	for (int d = 0; d < bc->ndims; d++) {
		printf("%d %F ", scores[d].dim, scores[d].score);
	}
	printf("\n");
	*/

	for (int i = 0; i < bc->ndims; i++)
		order[i] = scores[i].dim;

	return order;
}

#define VS (2048)
static bond_collection *
bond_create(allocator *ma, BAT **dim_bats, int ndims, int k)
{
	if (dim_bats && ndims > 0) {
		bond_collection *bc = ma_alloc(ma, sizeof(bond_collection));
		*bc = (bond_collection) {
			.ndims = ndims,
			.kth_upper = DBL_MAX,
		};
		if (bc) {
			bc->bsz = VS;
			bc->candidates = ma_alloc(ma, k * sizeof(oid));
			bc->dists = ma_alloc(ma, k * sizeof(dbl));

			bc->tcand = ma_alloc(ma, k * sizeof(oid));
			bc->tdist = ma_alloc(ma, k * sizeof(dbl));
			bc->tc = ma_alloc(ma, bc->bsz * sizeof(oid));
			bc->td = ma_alloc(ma, bc->bsz * sizeof(dbl));
			bc->dims = ma_alloc(ma, ndims * sizeof(BAT *));
			bc->dimsp = ma_alloc(ma, ndims * sizeof(dbl *));
			bc->dim_means = ma_alloc(ma, ndims * sizeof(dbl));
			bc->nvecs = BATcount(dim_bats[0]);
			for (int d = 0; d < ndims; d++) {
				BAT *b = dim_bats[d];
				bc->dims[d] = b;
				bc->dimsp[d] = (dbl*)Tloc(b,0);
			}
			return bc;
		}
	}
	return NULL;
}

static void
heap_down(oid *hcand, dbl *hd, BUN p, BUN k)
{
	BUN l = p*2+1, r = p*2+2, q = p;
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

static void
heap_up(oid *hcand, dbl *hd, BUN p)
{
	if (p == 0)
		return;
	BUN q = (p-1)/2;
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
    if (k == 0) return DBL_MAX;
    if (n == 0) return d[0];
	for (BUN i = 0; i < n; i++) {
		if (id[i] < d[0]) {
			cands[0] = icands[i];
            d[0] = id[i];
            heap_down(cands, d, 0, k);
		}
	}
	return d[0];
}

/* max heap */
static dbl
topn(oid *cands, dbl *d, oid *hcand, dbl* hd, BUN n, BUN k)
{
	BUN i = 0;
    // Fill the heap initially
	for (; i < k && i < n; i++) {
		hcand[i] = cands[i];
		hd[i] = d[i];
		heap_up(hcand, hd, i);
	}

    // If we have more candidates than k, treat them as a merge
    for (; i < n; i++) {
        if (d[i] < hd[0]) {
            hcand[0] = cands[i];
            hd[0] = d[i];
            heap_down(hcand, hd, 0, k);
        }
    }
    return hd[0];
}

static dbl
bond_upper_bound(bond_collection *bc, const dbl *query_vals, BUN k)
{
	oid *cands = bc->candidates, *tc = bc->tc;
	dbl *dists = bc->dists, *td = bc->td;

	// calc full distance for the sample across all dimensions
	dbl res = 0;
	for (int i = 0; i < bc->bsz; i++) {
		tc[i] = i;
		td[i] = 0;
	}

	for (int d = 0; d < bc->ndims; d++) {
		const dbl *vals = (const dbl *) bc->dimsp[d];
		dbl q = query_vals[d];
		dbl sum = 0;

		for (int i = 0; i < bc->bsz; i++) {
			dbl diff = vals[tc[i]] - q;
			dbl m = diff * diff;
			td[i] += m;
			sum += m;
		}
		bc->dim_means[d] += sum;
	}
	res = topn(tc, td, bc->tcand, bc->tdist, bc->bsz, k);
	for(BUN i = 0; i < k; i++) {
		cands[i] = bc->tcand[i];
		dists[i] = bc->tdist[i];
	}
	for (int d = 0; d < bc->ndims; d++)
		bc->dim_means[d] /= bc->bsz;
	return res;
}

static char*
bond_search_fast(bond_collection *bc, const dbl *query_vals,
			BUN k, int *dim_order, BAT *cands,
		   	BAT **oid_result, BAT **dist_result)
{
	if (cands || !bc || !query_vals || k == 0 || !oid_result || !dist_result || !dim_order)
		throw(MAL, "vss.bond_search", "invalid arguments");

	//lng T0 = GDKusec();
	//lng Tinit = 0, Tdists = 0, Tprune = 0, Ttopn = 0;
	BUN ncands = bc->nvecs, pruned = 0;
	oid *tc = bc->tc;
	dbl *td = bc->td;
	for (BUN z = bc->bsz; z < ncands; z+=bc->bsz) {
		BUN end = z+bc->bsz > ncands?ncands:z+bc->bsz, j, i = 0;
		// initialize, all vectors are candidates

		//lng T0 = GDKusec();
		for (j = z, i = 0; j<end; j++, i++) {
			tc[i] = i;
			td[i] = 0;
		}
		//Tinit += GDKusec() - T0;

		BUN sz = i, cur_pruned = 0;
		int step = 2, mask = step - 1, d = 0;
		for (d = 0; d < bc->ndims && (cur_pruned*4) < sz; d++) {
			/* partial dists */
			int o = dim_order[d];
			dbl qd = query_vals[o];
			const dbl *col = (const dbl *) (bc->dimsp[o]+z);

	//		lng T0 = GDKusec();
			for (i=0; i < sz; i++) {
				dbl diff = col[i] - qd;
				td[i] += diff * diff;
				cur_pruned += (td[i] > bc->kth_upper);
			}
	//		Tdists += GDKusec() - T0;
			//printf("partial dists chunk " BUNFMT " " BUNFMT " d=%zu t=" LLFMT "\n", j, sz, (size_t)d,  GDKusec() - T0);
		}
		for (; d < bc->ndims && sz; d++) {
			/* partial dists */
			int o = dim_order[d];
			dbl qd = query_vals[o];
			const dbl *col = (const dbl *) (bc->dimsp[o]+z);

	//		lng T0 = GDKusec();
			for (i=0; i < sz; i++) {
				dbl diff = col[tc[i]] - qd;
				td[i] += diff * diff;
			}
	//		Tdists += GDKusec() - T0;
			//printf("partial dists chunk " BUNFMT " " BUNFMT " d=%zu t=" LLFMT "\n", j, sz, (size_t)d,  GDKusec() - T0);
			if ((d&mask) != mask)
				continue;
			step *= 2;
			mask = step - 1;

			/* prune */
	//		T0 = GDKusec();
			BUN write_pos = 0;
			for (BUN read_pos = 0; read_pos < sz; read_pos++) {
				if (td[read_pos] <= bc->kth_upper) {
					tc[write_pos] = tc[read_pos];
					td[write_pos] = td[read_pos];
					write_pos++;
				}
			}
			pruned += sz - write_pos;
			sz = write_pos;
	//		Tprune += GDKusec() - T0;
			//printf("prune " BUNFMT " d=%zu " "t=" LLFMT "\n",  sz, (size_t)d,  GDKusec() - T0);
		}

	//	T0 = GDKusec();
		for(BUN i = 0; i<sz; i++)
			tc[i] += z;
		bc->kth_upper = topn_merge(bc->candidates, bc->dists, bc->tc, bc->td, sz, k);
	//	Ttopn += GDKusec() - T0;
		//printf("topn chnkd " BUNFMT " t=" LLFMT " %F\n", j, GDKusec() - T0, bc->kth_upper);
	}
	(void)pruned;
	//printf("vectors t= " LLFMT " init=" LLFMT " dists=" LLFMT " prune " LLFMT " topn " LLFMT " upper %F, pruned " BUNFMT " \n",
	//		GDKusec() - T0, Tinit, Tdists, Tprune, Ttopn, bc->kth_upper, pruned);
	//printf("vectors t= " LLFMT " upper %F, pruned " BUNFMT " \n", GDKusec() - T0, bc->kth_upper, pruned);

	//T0 = GDKusec();
	oid base = bc->dims[0]->hseqbase;
	BAT *koids = COLnew(0, TYPE_oid, k, TRANSIENT);
    BAT *kdist = COLnew(0, TYPE_dbl, k, TRANSIENT);
	// Final top k
	{
		BUN write_pos = 0;
		for (; write_pos < k; write_pos++) {
				*(oid*) Tloc(koids, write_pos) = bc->candidates[write_pos] + base;
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
    const T b = *(const T*) Tloc(BR, 0);                \
    for(BUN p = 0; p<cnt; p++) {                        \
        T d = a[p] - b;                                \
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

	//lng T0 = GDKusec(), T1 = 0;
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
	bond_collection *bc = bond_create(ta, dim_bats, ndims, k);
	//lng T1 = GDKusec();
	//printf("creation " LLFMT "\n", T1 - T0);
	//T0 = T1;

	if (!bc) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	bc->kth_upper = bond_upper_bound(bc, query_vals, k);
	//T1 = GDKusec();
	//printf("upperbound " LLFMT " %F\n", T1 - T0, bc->kth_upper);
	//T0 = T1;

	int *dim_order = bond_dim_order(ta, bc);
	if (!dim_order) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}
	//T1 = GDKusec();
	//printf("create upperbound order " LLFMT "\n", T1 - T0);
	//T0 = T1;

	BAT *oid_result = NULL, *dist_result = NULL;
	char *rc = bond_search_fast(bc, query_vals, (BUN) k, dim_order, NULL, &oid_result, &dist_result);
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

//#if 0
static str
_process_block(fblock *blk, dbl *query_vals,
             size_t nrows, size_t ncols, dbl threshold,
             oid *bc, dbl *bd, BUN *kcands)
{
    BUN ncand = (BUN)nrows;
	size_t TILE_SIZE = 8;
	bool prun_ok = false;

    // Outer dimension loop in tiles of TILE_SIZE
    for (size_t i = 0; i < ncols; i += TILE_SIZE) {
        size_t i_end = (i + TILE_SIZE > ncols) ? ncols : i + TILE_SIZE;

        // Inner candidate loop in tiles of TILE_SIZE
        for (BUN j = 0; j < ncand; j += TILE_SIZE) {
            BUN j_end = (j + TILE_SIZE > ncand) ? ncand : j + TILE_SIZE;

            // Process the 8x8 (or smaller) tile
            for (size_t ii = i; ii < i_end; ii++) {
                dbl qv = query_vals[ii];
                dbl *col = (dbl*)blk + (ii * nrows);

                for (BUN jj = j; jj < j_end; jj++) {
                    oid idx = bc[jj];
                    dbl dv = col[idx];
                    dbl diff = qv - dv;
                    bd[jj] += diff * diff;
                }
            }
        }

        // Prune logic: only check threshold after completing a dimension tile
        if (threshold != DBL_MAX) {
            BUN write_pos = 0;
            for (BUN read_pos = 0; read_pos < ncand; read_pos++) {
                if (bd[read_pos] <= threshold) {
					if (prun_ok) {
						bc[write_pos] = bc[read_pos];
						bd[write_pos] = bd[read_pos];
					}
					write_pos++;
                }
            }
			if (prun_ok) {
				ncand = write_pos;
			} else {
				prun_ok = write_pos > (0.75 * nrows);
			}

        }
		// EXIT EARLY: If no candidates left, stop processing this block
        if (ncand == 0) break;
    }
	//printf("prunned %zu candidates %zu nrows %zu \n", (nrows - ncand), ncand, nrows);

    *kcands = ncand;
    return MAL_SUCCEED;
}
//#endif

#if 0
static str
process_block(fblock *blk, dbl *query_vals,
	   	size_t nrows, size_t ncols, dbl threshold,
	   	oid *bc, dbl *bd, BUN *kcands)
{
	BUN ncand = nrows;

	for (size_t i = 0; i < ncols; i++) {
		dbl *col = (dbl*)blk + (i * nrows);

		for (BUN j = 0; j < ncand; j++) {
			oid idx = bc[j];
			dbl dv = col[idx];
			dbl qv = query_vals[i];
			dbl diff = qv - dv;
			bd[j] += diff*diff;
		}

		// prune at specific intervals
		if (threshold != DBL_MAX && (i % 4 == 3)) {
			BUN write_pos = 0;
			for (BUN read_pos = 0; read_pos < ncand; read_pos++) {
				if (bd[read_pos] <= threshold) {
					bc[write_pos] = bc[read_pos];
					bd[write_pos] = bd[read_pos];
					write_pos++;
				}
			}
			ncand = write_pos;
		}
	}

	*kcands = ncand;
	return MAL_SUCCEED;
}
#endif

static char*
pdx(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
	//lng T0 = GDKusec(), T1 = 0;
    bat *ret = getArgReference_bat(stk, pci, 0);
	// data
    BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	size_t k = *getArgReference_int(stk, pci, 2);
	size_t nblocks = BATcount(b);
	(void) nblocks;
    size_t ndims = (size_t) (pci->argc - pci->retc - 2);
	size_t block_capacity = sizeof(fblock) / sizeof(dbl);
	// Number of rows per block
	size_t nrows = block_capacity / ndims;
	(void) nrows;
	fblock *blocks = (fblock *) Tloc(b, 0);
	(void) blocks;

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	dbl *query_vals = ma_alloc(ta, ndims * sizeof(dbl));
	// block candidates
	oid *bc = ma_alloc(ta, nrows * sizeof(oid));
	// block distances
	dbl *bd = ma_alloc(ta, nrows * sizeof(dbl));

	// Global result buffers
    oid *cands = ma_alloc(ta, k * sizeof(oid));
    dbl *dists = ma_alloc(ta, k * sizeof(dbl));

    // Heap storage for the first block top-n
    oid *hcands = ma_alloc(ta, k * sizeof(oid));
    dbl *hdists = ma_alloc(ta, k * sizeof(dbl));

	if (!query_vals || !bc || !bd || !cands || !dists) {
		ma_close(&ta_state);
		BBPunfix(b->batCacheid);
		throw(MAL, "vss.pdx", MAL_MALLOC_FAIL);
	}

	// Initialize
	for (size_t j = 0; j < k; j++) {
		dists[j] = DBL_MAX;
		cands[j] = j;
	}

	for (size_t i = 0; i < ndims; i++) {
		BAT *b_dim = BATdescriptor(*getArgReference_bat(stk, pci, i + 3));
		if (!b_dim) {
			ma_close(&ta_state);
			BBPunfix(b->batCacheid);
			throw(MAL, "vss.knn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		query_vals[i] = *(dbl*)Tloc(b_dim, 0);
		BBPunfix(b_dim->batCacheid);
	}
	//T1 = GDKusec();
	//printf("Init Step loading query values " LLFMT "\n", T1 - T0);
	//T0 = T1;

	// Process blocks
	dbl threshold = DBL_MAX;
	for (size_t i = 0; i < nblocks; i++) {
		// curr blk
		fblock *blk = blocks + i;
		// reset bc and bc for current block
		for (size_t j = 0; j < nrows; j++) {
			bd[j] = 0;
			bc[j] = (oid)j;
		}
		str msg;
		BUN ncand = (BUN) nrows;
		if ((msg = _process_block(blk, query_vals, nrows, ndims, threshold, bc, bd, &ncand)) != MAL_SUCCEED) {
			ma_close(&ta_state);
			BBPunfix(b->batCacheid);
			return msg;
		}
	//T1 = GDKusec();
	//printf("block processed " LLFMT " block cands %d\n", T1 - T0, (int)nrows);
	//T0 = T1;

		// Convert local block indices to global OIDs
        for (BUN j = 0; j < ncand; j++) {
            bc[j] += (oid)(i * nrows);
        }

		if (i == 0 && ncand > k) {
			threshold = topn(bc, bd, hcands, hdists, ncand, k);
			for(BUN j = 0; j < k; j++) {
				cands[j] = hcands[j];
				dists[j] = hdists[j];
			}
		} else {
			threshold = topn_merge(cands, dists, bc, bd, ncand, k);
		}
		//T1 = GDKusec();
		//printf("TOP N " LLFMT "\n", T1 - T0);
		//T0 = T1;
	}

    BAT *bn = COLnew(0, TYPE_oid, k, TRANSIENT);
    if (bn == NULL) {
		ma_close(&ta_state);
		BBPunfix(b->batCacheid);
        throw(MAL, "batvss.pdx", MAL_MALLOC_FAIL);
	}


	// Copy heap results to BAT
    for (size_t i = 0; i < k; i++) {
        ((oid*)Tloc(bn, 0))[i] = cands[i];
    }

    // Finalize BAT metadata
    BATsetcount(bn, k);
    bn->tnonil = true;
    bn->tkey = false;
    bn->tsorted = false;
    bn->trevsorted = false;

	//printf("#pdx took" LLFMT "\n", GDKusec() - T0);

	ma_close(&ta_state);
	BBPunfix(b->batCacheid);
    *ret = bn->batCacheid;
    BBPkeepref(bn);
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
	//pattern("vss", "pdx", pdx, false,
	//	"Euclidean distanse L2",
	//	args(1, 3, arg("", oid), arg("", fblock), varargany("args", 0))),
	pattern("batvss", "pdx", pdx, false,
		"Euclidean distanse L2",
		args(1, 4, batarg("", oid), batarg("", fblock), arg("k", int), varargany("args", 0))),
	{ .imp=NULL }		/* sentinel */
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_vss_mal)
{ mal_module("vss", NULL, vss_init_funcs); }
