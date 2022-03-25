/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/* Author: Pedro Ferreira
 *
 * This module attempts to implement a basic histogram over a BAT.
 * 
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#include "gdk_histogram.h"

#define NBUCKETS 64
#define SAMPLE_SIZE 1024

static_assert(NBUCKETS > 2, "The number of buckets must be > 2");
static_assert(SAMPLE_SIZE > NBUCKETS, "The sample size must be > number of buckets");
static_assert(SAMPLE_SIZE < INT_MAX, "The sample size must be on the integer range");

#ifdef HAVE_HGE
#define	VAR_UPCAST hge
#define	VAR_TPE    TYPE_hge
#else
#define	VAR_UPCAST lng
#define	VAR_TPE    TYPE_lng
#endif

#define histogram_entry(TPE)	\
typedef struct HistogramBucket_##TPE {	\
	TPE min, max;	/* The minimum and maximum value in the range */	\
	int count;	/* number of values */	\
} HistogramBucket_##TPE;

histogram_entry(bte)
histogram_entry(sht)
histogram_entry(int)
histogram_entry(lng)
#ifdef HAVE_HGE
histogram_entry(hge)
#endif

static bool
can_create_histogram(int tpe)
{
	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		return true;
	default:		/* type not supported */
		return false;
	}
}

#define set_limit(LIMIT, TPE) \
	do { \
		TPE x = GDK_##TPE##_##LIMIT; \
		VALset(LIMIT##v, TYPE_##TPE, &x); \
	} while (0)

/* retrieve min/max values to determine the limits of the histogram, if the range of values is < nbuckets,
   a perfect histogram can be created */
static void
min_and_max_values(BATiter *bi, ValPtr minv, ValPtr maxv)
{
	if (bi->minpos != BUN_NONE) {
		VALset(minv, bi->type, BUNtloc(*bi, bi->minpos));
	} else {
		switch (ATOMbasetype(bi->type)) {
		case TYPE_bte:
			set_limit(min, bte);
			break;
		case TYPE_sht:
			set_limit(min, sht);
			break;
		case TYPE_int:
			set_limit(min, int);
			break;
		case TYPE_lng:
			set_limit(min, lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			set_limit(min, hge);
			break;
#endif
		default:
			assert(0);
		}
	}
	if (bi->maxpos != BUN_NONE) {
		VALset(maxv, bi->type, BUNtloc(*bi, bi->maxpos));
	} else {
		switch (ATOMbasetype(bi->type)) {
		case TYPE_bte:
			set_limit(max, bte);
			break;
		case TYPE_sht:
			set_limit(max, sht);
			break;
		case TYPE_int:
			set_limit(max, int);
			break;
		case TYPE_lng:
			set_limit(max, lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			set_limit(max, hge);
			break;
#endif
		default:
			assert(0);
		}
	}
}

#define perfect_histogram_loop(TPE)	\
	do { \
		TPE i = *(TPE*)VALget(min), ii = i, j = *(TPE*)VALget(max); \
		const TPE *restrict v = Tloc(sam, 0); \
		BUN c = BATcount(sam), m;	\
		HistogramBucket_##TPE *restrict hist; \
	\
		h->nbuckets = (int) (j - i + 1); \
		if (!(h->histogram = GDKmalloc(sizeof(HistogramBucket_##TPE) * h->nbuckets))) \
			return NULL; \
	\
		hist = (HistogramBucket_##TPE *) h->histogram; \
		m = (BUN) h->nbuckets; \
		for (BUN k = 0 ; k < m ; k++) {  /* Initialize bucket ranges */ \
			HistogramBucket_##TPE *restrict b = &(hist[k]); \
			b->min = b->max = ii++; /* TODO? maybe I don't need to materialize this for 'perfect' histograms */ \
			b->count = 0; \
		} \
	\
		for (BUN k = 0 ; k < c ; k++) { /* Now create the histogram */ \
			TPE next = v[k]; \
			if (is_##TPE##_nil(next)) { \
				nulls++; \
			} else { \
				int offset = (int) (next - i); /* get the offset from min, then increment */ \
				hist[offset].count++; \
			} \
		} \
		h->nulls = nulls; \
	} while (0)

static Histogram *
create_perfect_histogram(BAT *sam, Histogram *h, ValPtr min, ValPtr max)
{
	int nulls = 0, tpe = ATOMbasetype(sam->ttype);

	switch (tpe) {
	case TYPE_bte:
		perfect_histogram_loop(bte);
		break;
	case TYPE_sht:
		perfect_histogram_loop(sht);
		break;
	case TYPE_int:
		perfect_histogram_loop(int);
		break;
	case TYPE_lng:
		perfect_histogram_loop(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		perfect_histogram_loop(hge);
		break;
#endif
	default:
		assert(0);
	}
	return h;
}

#define generic_histogram_loop(TPE)	\
	do { \
		TPE i = *(TPE*)VALget(min), ii = i, j = *(TPE*)VALget(max), gap; \
		const TPE *restrict v = Tloc(sam, 0); \
		BUN c = BATcount(sam);	\
		HistogramBucket_##TPE *restrict hist; \
	\
		h->nbuckets = NBUCKETS; \
		if (!(h->histogram = GDKmalloc(sizeof(HistogramBucket_##TPE) * h->nbuckets))) \
			return NULL; \
		gap = (TPE) floor((double) (j - i) / (double) h->nbuckets); /* TODO this may overflow for large integer types */ \
	\
		hist = (HistogramBucket_##TPE *) h->histogram; \
		for (BUN k = 0 ; k < NBUCKETS ; k++) { \
			HistogramBucket_##TPE *restrict b = &(hist[k]); \
			b->min = ii;	\
			if (k == NBUCKETS - 1) { \
				b->max = j; /* The last bucket will contain the max value inclusive */ \
			} else { \
				ii += gap; \
				b->max = ii; \
			} \
			b->count = 0; \
		} \
	\
		for (BUN k = 0 ; k < c ; k++) { /* Now create the histogram */ \
			TPE next = v[k]; \
			if (is_##TPE##_nil(next)) { \
				nulls++; \
			} else { \
				int l = 0, r = NBUCKETS - 1; \
				while (l <= r) { /* Do binary search to find the bucket */ \
					int m = l + (r - l) / 2; \
					HistogramBucket_##TPE *restrict b = &(hist[m]); \
					/* Check if on the bucket. Don't forget last bucket case where max is inclusive */ \
					if (next >= b->min && (next < b->max || (next == j && m == (NBUCKETS - 1)))) { \
						b->count++; \
					} else if (next < b->min) { /* value is smaller, ignore right half */ \
						r = m - 1; \
					} else { /* value is greater, ignore left half */ \
						l = m + 1; \
					} \
				} \
				assert(0); /* the value must be found */ \
			} \
		} \
		h->nulls = nulls; \
	} while (0)

static Histogram *
create_generic_histogram(BAT *sam, Histogram *h, ValPtr min, ValPtr max)
{
	int nulls = 0, tpe = ATOMbasetype(sam->ttype);

	switch (tpe) {
	case TYPE_bte:
		generic_histogram_loop(bte);
		break;
	case TYPE_sht:
		generic_histogram_loop(sht);
		break;
	case TYPE_int:
		generic_histogram_loop(int);
		break;
	case TYPE_lng:
		generic_histogram_loop(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		generic_histogram_loop(hge);
		break;
#endif
	default:
		assert(0);
	}
	return h;
}

gdk_return
HISTOGRAMcreate(BAT *b)
{
	lng t0 = 0;
	BATiter bi = {0};

	MT_thread_setalgorithm("create histogram");
	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if (!can_create_histogram(b->ttype)) {
		GDKerror("Cannot create histogram for %s bats\n", ATOMname(b->ttype));
		return GDK_FAIL;
	}

	if (VIEWtparent(b)) /* don't create histograms on views */
		b = BBP_cache(VIEWtparent(b));

	if (BATcount(b) == 0) /* no histograms on empty BATs */
		return GDK_SUCCEED;

	if (b->thistogram == NULL) {
		bi = bat_iterator(b);
		MT_lock_set(&b->batIdxLock);
		if (b->thistogram == NULL) {
			BAT *sids, *sam;
			Histogram *h;
			bool perfect_histogram = false;
			ValRecord min, max, diff, conv = {.vtype = VAR_TPE}, nbuckets, truth;

			if (!(sids = BATsample_with_seed(b, SAMPLE_SIZE, (uint64_t) GDKusec() * (uint64_t) b->batCacheid)))
				goto fail;
			sam = BATproject(sids, b);
			BBPreclaim(sids);
			if (!sam)
				goto fail;

			if (bi.type == TYPE_bit) {
				VALinit(&min, TYPE_bit, &(bit){0});
				VALinit(&max, TYPE_bit, &(bit){1});
				perfect_histogram = true;
			} else {
				min_and_max_values(&bi, &min, &max);
				if (VARcalcsub(&diff, &max, &min, true) == GDK_SUCCEED) {
					if (VARconvert(&conv, &diff, true, 0, 0, 0) == GDK_SUCCEED) {
						VAR_UPCAST v = (VAR_UPCAST) NBUCKETS;
						VALinit(&nbuckets, VAR_TPE, &v);
						/* if the number of buckets is greater than max and min difference, then there is a 'perfect' histogram */
						if (VARcalcgt(&truth, &nbuckets, &conv) == GDK_SUCCEED) {
							perfect_histogram = truth.val.btval == 1;
						} else {
							GDKclrerr();
						}
					} else {
						GDKclrerr();
					}
				} else {
					GDKclrerr();
				}
			}

			if (!(b->thistogram = GDKmalloc(sizeof(struct Histogram)))) {
				BBPreclaim(sam);
				goto fail;
			}

			h = b->thistogram;
			h->nulls = 0;
			h->size = (int) BATcount(sam); /* it should fit */
			if (perfect_histogram) {
				h = create_perfect_histogram(sam, h, &min, &max);
			} else {
				h = create_generic_histogram(sam, h, &min, &max);
			}

			BBPreclaim(sam);
			if (!h) {
				GDKfree(b->thistogram);
				b->thistogram = NULL;
				goto fail;
			}
		}
		MT_lock_unset(&b->batIdxLock);
		bat_iterator_end(&bi);
	}
	TRC_DEBUG(ACCELERATOR, "histogram creation took " LLFMT " usec\n", GDKusec()-t0);
	return GDK_SUCCEED;
fail:
	MT_lock_unset(&b->batIdxLock);
	bat_iterator_end(&bi);
	return GDK_FAIL;
}

void
HISTOGRAMdestroy(BAT *b)
{
	if (b && b->thistogram) {
		MT_lock_set(&b->batIdxLock);
		GDKfree(b->thistogram);
		b->thistogram = NULL;
		MT_lock_unset(&b->batIdxLock);
	}
}
