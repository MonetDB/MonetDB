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

#ifdef HAVE_HGE
#define	VAR_UPCAST hge
#define	VAR_TPE    TYPE_hge
#else
#define	VAR_UPCAST lng
#define	VAR_TPE    TYPE_lng
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
		BUN k = 0, l = BATcount(sam);	\
	\
		h->nbuckets = (int) (j - i + 1); \
		if (!(h->histogram = GDKmalloc(sizeof(struct HistogramEntry) * h->nbuckets))) \
			return NULL; \
	\
		while (true) { \
			struct HistogramEntry *restrict e = &(h->histogram[k++]); \
			VALinit(&(e->min), tpe, &ii); /* TODO? maybe I don't need to materialize this for 'perfect' histograms */\
			VALinit(&(e->max), tpe, &ii);	\
			e->count = 0; \
			if (ii == j) \
				break; \
			ii++; \
		} \
	\
		for (BUN k = 0 ; k < l ; k++) { \
			TPE next = v[k]; \
			if (is_##TPE##_nil(next)) { \
				h->nulls++; \
			} else { \
				int offset = (next - i); /* get the offset from min, then increment */ \
				h->histogram[offset].count++; \
			} \
		} \
	} while (0)

static Histogram *
create_perfect_histogram(BAT *sam, Histogram *h, ValPtr min, ValPtr max)
{
	int tpe = ATOMbasetype(sam->ttype);
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
			}/* else {
				 do the generic way
			}*/

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
