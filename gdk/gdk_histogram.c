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

/* retrieve min/max values to determine the limits of the histogram, if the range of values is < nbuckets,
   a perfect histogram can be created */
static void
min_and_max_values(BATiter *bi, int tpe, ValPtr minv, ValPtr maxv)
{
	VAR_UPCAST val = 0;

	if (bi->minpos != BUN_NONE) {
		VALset(minv, tpe, BUNtloc(*bi, bi->minpos));
	} else {
		switch (ATOMbasetype(tpe)) {
		case TYPE_bte:
			val = (VAR_UPCAST) GDK_bte_min;
			break;
		case TYPE_sht:
			val = (VAR_UPCAST) GDK_sht_min;
			break;
		case TYPE_int:
			val = (VAR_UPCAST) GDK_int_min;
			break;
		case TYPE_lng:
			val = (VAR_UPCAST) GDK_lng_min;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			val = GDK_hge_min;
			break;
#endif
		default:
			assert(0);
		}
		VALset(minv, tpe, &val);
	}
	if (bi->maxpos != BUN_NONE) {
		VALset(maxv, tpe, BUNtloc(*bi, bi->maxpos));
	} else {
		switch (ATOMbasetype(tpe)) {
		case TYPE_bte:
			val = (VAR_UPCAST) GDK_bte_max;
			break;
		case TYPE_sht:
			val = (VAR_UPCAST) GDK_sht_max;
			break;
		case TYPE_int:
			val = (VAR_UPCAST) GDK_int_max;
			break;
		case TYPE_lng:
			val = (VAR_UPCAST) GDK_lng_max;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			val = GDK_hge_max;
			break;
#endif
		default:
			assert(0);
		}
		VALset(maxv, tpe, &val);
	}
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

	if (b->thistogram == NULL) {
		bi = bat_iterator(b);
		MT_lock_set(&b->batIdxLock);
		if (b->thistogram == NULL) {
			BAT *sids, *sam;
			bool perfect_histogram = false;
			ValRecord min, max, diff, conv = {.vtype = VAR_TPE}, nbuckets, truth;

			if (!(b->thistogram = GDKmalloc(sizeof(struct Histogram))) ||
				!(b->thistogram->histogram = GDKmalloc(sizeof(struct HistogramEntry) * NBUCKETS))) {
				GDKfree(b->thistogram);
				b->thistogram = NULL;
				goto fail;
			}

			sids = BATsample_with_seed(b, SAMPLE_SIZE, (uint64_t) GDKusec() * (uint64_t) b->batCacheid);
			if (!sids)
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
				min_and_max_values(&bi, bi.type, &min, &max);
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

			(void) perfect_histogram;
			// ....
			BBPreclaim(sam);
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
