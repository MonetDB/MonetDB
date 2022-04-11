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
#include "gdk_calc_private.h"

#define NBUCKETS 64
#define SAMPLE_SIZE 1024

static_assert(NBUCKETS > 2, "The number of buckets must be > 2");
static_assert(SAMPLE_SIZE > NBUCKETS, "The sample size must be > number of buckets");
static_assert(SAMPLE_SIZE < INT_MAX, "The sample size must be on the integer range");
static_assert((SAMPLE_SIZE % NBUCKETS) == 0, "The sample size must be a multiple of the number of buckets");

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
	case TYPE_str:
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
		/* no perfect histograms on strings yet */
		assert(0);
	}
	return h;
}

#define absbte(x)	abs(x)
#define abssht(x)	abs(x)
#define absint(x)	abs(x)
#define abslng(x)	llabs(x)
#define abshge(x)	ABSOLUTE(x)

#define generic_histogram_loop(TPE, abs)	\
	do { \
		TPE i = *(TPE*)VALget(min), ii = i, j = *(TPE*)VALget(max), gap; \
		const TPE *restrict v = Tloc(sam, 0); \
		BUN c = BATcount(sam);	\
		HistogramBucket_##TPE *restrict hist; \
	\
		h->nbuckets = NBUCKETS; \
		if (!(h->histogram = GDKmalloc(sizeof(HistogramBucket_##TPE) * h->nbuckets))) \
			return NULL; \
		gap = (j / NBUCKETS) - (i / NBUCKETS) + (TPE) ceil(abs((j % NBUCKETS) - (i % NBUCKETS)) / (dbl) NBUCKETS); \
		/* TODO for temporal types, this is giving values out of range such as 1994-02-00 */ \
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
				bool found = false; \
				int l = 0, r = NBUCKETS - 1; \
				while (l <= r) { /* Do binary search to find the bucket */ \
					int m = l + (r - l) / 2; \
					HistogramBucket_##TPE *restrict b = &(hist[m]); \
					/* Check if on the bucket. Don't forget last bucket case where max is inclusive */ \
					if (next >= b->min && (next < b->max || (next == j && m == (NBUCKETS - 1)))) { \
						b->count++; \
						found = true; \
						break; \
					} else if (next < b->min) { /* value is smaller, ignore right half */ \
						r = m - 1; \
					} else { /* value is greater, ignore left half */ \
						l = m + 1; \
					} \
				} \
				assert(found); /* the value must be found */ \
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
		generic_histogram_loop(bte, absbte);
		break;
	case TYPE_sht:
		generic_histogram_loop(sht, abssht);
		break;
	case TYPE_int:
		generic_histogram_loop(int, absint);
		break;
	case TYPE_lng:
		generic_histogram_loop(lng, abslng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		generic_histogram_loop(hge, abshge);
		break;
#endif
	case TYPE_str: { /* for strings use the hashed value (it cannot be used in ranges) */
		int i = 0;
		const int gap = SAMPLE_SIZE / NBUCKETS;
		HistogramBucket_int *restrict hist;

		h->nbuckets = NBUCKETS;
		if (!(h->histogram = GDKmalloc(sizeof(HistogramBucket_int) * h->nbuckets)))
			return NULL;

		hist = (HistogramBucket_int *) h->histogram;
		for (BUN k = 0 ; k < NBUCKETS ; k++) {
			HistogramBucket_int *restrict b = &(hist[k]);
			b->min = i;
			i += gap;
			b->max = i;
			b->count = 0;
		}

		BATiter bi = bat_iterator_nolock(sam);
		BUN c = bi.count;
		for (BUN k = 0 ; k < c ; k++) { /* Now create the histogram */
			const char *next = BUNtvar(bi, k);

			if (strNil(next)) {
				nulls++;
			} else {
				bool found = false;
				int l = 0, r = NBUCKETS - 1;
				/* pick the lowest bits according to the sample size from the hash value */
				int value = (int)(strHash(next) & (SAMPLE_SIZE - 1));

				while (l <= r) { /* Do binary search to find the bucket */
					int m = l + (r - l) / 2;
					HistogramBucket_int *restrict b = &(hist[m]);
					if (value >= b->min && value < b->max) {
						b->count++;
						found = true;
						break;
					} else if (value < b->min) { /* value is smaller, ignore right half */
						r = m - 1;
					} else { /* value is greater, ignore left half */
						l = m + 1;
					}
				}
				assert(found); /* the value must be found */
			}
		}
		h->nulls = nulls;
	} break;
	default:
		assert(0);
	}
	return h;
}

static gdk_return
HISTOGRAMcreate_nolock(BAT *b, BATiter *bi)
{
	BAT *sids, *sam;
	Histogram *h;
	bool perfect_histogram = false;
	ValRecord min = {.vtype = bi->type}, max = {.vtype = bi->type};

	if (bi->count == 0) /* no histograms on empty BATs */
		return GDK_SUCCEED;

	if (!(sids = BATsample_with_seed(b, SAMPLE_SIZE, (uint64_t) GDKusec() * (uint64_t) b->batCacheid)))
		return GDK_FAIL;
	sam = BATproject(sids, b);
	BBPreclaim(sids);
	if (!sam)
		return GDK_FAIL;

	if (ATOMbasetype(bi->type) != TYPE_str) {
		ValRecord diff = {.vtype = VAR_TPE}, conv = {.vtype = VAR_TPE}, nbuckets = {.vtype = VAR_TPE}, truth = {.vtype = TYPE_bit};

		if (bi->type == TYPE_bit) {
			VALinit(&min, TYPE_bit, &(bit){0});
			VALinit(&max, TYPE_bit, &(bit){1});
			perfect_histogram = true;
		} else {
			min_and_max_values(bi, &min, &max);
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
	}

	if (!(b->thistogram = GDKmalloc(sizeof(struct Histogram)))) {
		BBPreclaim(sam);
		return GDK_FAIL;
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
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

gdk_return
HISTOGRAMcreate(BAT *b)
{
	lng t0 = 0;
	BATiter bi = {0};
	gdk_return res = GDK_SUCCEED;

	MT_thread_setalgorithm("create histogram");
	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if (VIEWtparent(b)) /* don't create histograms on views */
		b = BBP_cache(VIEWtparent(b));

	bi = bat_iterator(b);
	if (!can_create_histogram(bi.type)) {
		bat_iterator_end(&bi);
		GDKerror("Cannot create histogram for %s bats\n", ATOMname(bi.type));
		return GDK_FAIL;
	}

	if (b->thistogram == NULL) {
		MT_lock_set(&b->batIdxLock);
		if (b->thistogram == NULL) {
			res = HISTOGRAMcreate_nolock(b, &bi);
		}
		MT_lock_unset(&b->batIdxLock);
	}
	bat_iterator_end(&bi);
	TRC_DEBUG(ACCELERATOR, "histogram creation took " LLFMT " usec\n", GDKusec()-t0);
	return res;
}

#define histogram_print_loop(TPE) \
	do { \
		ssize_t (*atomtostr)(str *, size_t *, const void *, bool) = BATatoms[TYPE_##TPE].atomToStr; \
		const HistogramBucket_##TPE *restrict hist = (HistogramBucket_##TPE *) b->thistogram->histogram; \
		for (int i = 0 ; i < nbuckets ; i++) { \
			const HistogramBucket_##TPE *restrict hb = &(hist[i]); \
			if (atomtostr(&minbuf, &minlen, &hb->min, true) < 0 || \
				atomtostr(&maxbuf, &maxlen, &hb->max, true) < 0) { \
				MT_lock_unset(&b->batIdxLock); \
				GDKfree(res); \
				GDKfree(minbuf); \
				GDKfree(maxbuf); \
				return NULL; \
			} \
			size_t next_len = strlen(minbuf) + strlen(maxbuf) + 10 + 8 + len; \
			if (next_len >= rlen) { \
				rlen = (rlen * 2) + ((next_len + 2047) & ~2047); \
				str newbuf = GDKrealloc(res, rlen); \
				if (!newbuf) { \
					MT_lock_unset(&b->batIdxLock); \
					GDKfree(res); \
					GDKfree(minbuf); \
					GDKfree(maxbuf); \
					return NULL; \
				} \
				res = newbuf; \
			} \
			len += sprintf(res + len, "[%s,%s%c -> %d\n", minbuf, maxbuf, i == (nbuckets - 1) ? ']' : '[', hb->count); \
		} \
	} while (0)

str
HISTOGRAMprint(BAT *b)
{
	size_t len = 0, rlen = 2048, minlen = 256, maxlen = 256;
	str res = NULL, minbuf = NULL, maxbuf = NULL;
	int tpe = ATOMbasetype(b->ttype), nbuckets;

	if (VIEWtparent(b)) /* don't look on views */
		b = BBP_cache(VIEWtparent(b));

	MT_lock_set(&b->batIdxLock);
	if (!b->thistogram) {
		MT_lock_unset(&b->batIdxLock);
		GDKerror("No histogram present\n");
		return NULL;
	}

	if (!(res = GDKmalloc(rlen)) || !(minbuf = GDKmalloc(minlen)) || !(maxbuf = GDKmalloc(maxlen))) {
		MT_lock_unset(&b->batIdxLock);
		GDKfree(res);
		GDKfree(minbuf);
		GDKfree(maxbuf);
		return NULL;
	}

	len = sprintf(res, "Total entries: %d, buckets: %d\n", b->thistogram->size, b->thistogram->nbuckets);
	len += sprintf(res + len, "nulls -> %d\n", b->thistogram->nulls);

	nbuckets = b->thistogram->nbuckets;
	switch (tpe) {
	case TYPE_bte:
		histogram_print_loop(bte);
		break;
	case TYPE_sht:
		histogram_print_loop(sht);
		break;
	case TYPE_str: /* strings use integer size buckets */
	case TYPE_int:
		histogram_print_loop(int);
		break;
	case TYPE_lng:
		histogram_print_loop(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		histogram_print_loop(hge);
		break;
#endif
	default:
		MT_lock_unset(&b->batIdxLock);
		GDKfree(res);
		GDKfree(minbuf);
		GDKfree(maxbuf);
		GDKerror("Histogram print function not available for %s bats\n", ATOMname(b->ttype));
		return NULL;
	}
	MT_lock_unset(&b->batIdxLock);
	GDKfree(minbuf);
	GDKfree(maxbuf);
	return res;
}

void
HISTOGRAMdestroy(BAT *b)
{
	if (b && b->thistogram) {
		MT_lock_set(&b->batIdxLock);
		if (b->thistogram) {
			GDKfree(b->thistogram->histogram);
			GDKfree(b->thistogram);
		}
		b->thistogram = NULL;
		MT_lock_unset(&b->batIdxLock);
	}
}

gdk_return
HISTOGRAMrecreate(BAT *b)
{
	lng t0 = 0;
	gdk_return res = GDK_SUCCEED;

	MT_thread_setalgorithm("re-create histogram");
	TRC_DEBUG_IF(ACCELERATOR) t0 = GDKusec();

	if (VIEWtparent(b)) /* don't create histograms on views */
		b = BBP_cache(VIEWtparent(b));

	if (b && b->thistogram) {
		BATiter	bi = bat_iterator(b);
		MT_lock_set(&b->batIdxLock);
		if (b->thistogram) {
			GDKfree(b->thistogram->histogram);
			GDKfree(b->thistogram);
		}
		res = HISTOGRAMcreate_nolock(b, &bi);
		MT_lock_unset(&b->batIdxLock);
		bat_iterator_end(&bi);
	}
	TRC_DEBUG(ACCELERATOR, "histogram re-creation took " LLFMT " usec\n", GDKusec()-t0);
	return res;
}
