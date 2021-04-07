/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Implementation for the column imprints index.
 * See paper:
 * Column Imprints: A Secondary Index Structure,
 * L.Sidirourgos and M.Kersten.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_imprints.h"

/*
 * The imprints heap consists of five parts:
 * - header
 * - bins
 * - stats
 * - imps
 * - dict
 *
 * The header consists of four size_t values `size_t hdata[4]':
 * - hdata[0] = (1 << 16) | (IMPRINTS_VERSION << 8) | (uint8_t) imprints->bits
 * - hdata[1] = imprints->impcnt
 * - hdata[2] = imprints->dictcnt
 * - hdata[3] = BATcount(b)
 * The first word of the header includes a version number in case the
 * format changes, and a bit to indicate that the data was synced to disk.
 * This bit is the last thing written, so if it is set when reading back
 * the imprints heap, the data is complete.  The fourth word is the size of
 * the BAT for which the imprints were created.  If this size differs from
 * the size of the BAT at the time of reading the imprints back from disk,
 * the imprints are not usable.
 *
 * The bins area starts immediately after the header.  It consists of 64
 * values in the domain of the BAT `TYPE bins[64]'.
 *
 * The stats area starts immediately after the bins area.  It consists of
 * three times an array of 64 64 (32) bit integers `BUN stats[3][64]'.  The
 * three arrays represent respectively min, max, and cnt for each of the 64
 * bins, so stats can be seen as `BUN min_bins[64]; BUN max_bins[64]; BUN
 * cnt_bins[64];'.  The min and max values are positions of the smallest
 * and largest non-nil value in the corresponding bin.
 *
 * The imps area starts immediately after the stats area.  It consists of
 * one mask per "page" of the input BAT indicating in which bins the values
 * in that page fall.  The size of the mask is given by imprints->bits.
 * The list of masks may be run-length compressed, see the dict area.  A
 * "page" is 64 bytes worth of values, so the number of values depends on
 * the type of the value.
 *
 * The dict area starts immediately after the imps area.  It consists of
 * one cchdc_t value per "page" of the input.  The precise use is described
 * below.
 *
 * There are up to 64 bins into which values are sorted.  The number of
 * bins depends on the number of unique values in the input BAT (actually
 * on the number of unique values in a random sample of 2048 values of the
 * input BAT) and is 8, 16, 32, or 64.  The number of bits in the mask is
 * stored in imprints->bits.  The boundaries of the bins are dynamically
 * determined when the imprints are created and stored in the bins array.
 * In fact, bins[n] contains the lower boundary of the n-th bin (0 <= n <
 * N, N the number of bins).  The value of bins[0] is not actually used:
 * all values smaller than bins[1] are sorted into this bin, including NIL.
 * The boundaries are simply computed by stepping with large steps through
 * the sorted sample and taking 63 (or 31, 15, 7) equally spaced values
 * from there.
 *
 * Once the appropriate bin n is determined for a particular value v
 * (bins[n] <= v < bins[n+1]), a bitmask can be constructed for the value
 * as ((uintN_t)1 << n) where N is the number of bits that are used for the
 * bitmasks and n is the number of the bin (0 <= n < N).
 *
 * The input BAT is divided into "pages" where a page is 64 bytes.  This
 * means the number of rows in a page depends on the size of the values: 64
 * for byte-sized values down to 4 for hugeint (128 bit) values.  For each
 * page, a bitmask is created which is the imprint for that page.  The
 * bitmask has a bit set for each bin into which a value inside the page
 * falls.  These bitmasks (imprints) are stored in the imprints->imps
 * array, but with a twist, see below.
 *
 * The imprints->dict array is an array of cchdc_t values.  A cchdc_t value
 * consists of a bit .repeat and a 24-bit value .cnt.  The sum of the .cnt
 * values is equal to the total number of pages in the input BAT.  If the
 * .repeat value is 0, there are .cnt consecutive imprint bitmasks in the
 * imprints->imps array, each for one page.  If the .repeat value is 1,
 * there is a single imprint bitmask in the imprints->imps array which is
 * valid for the next .cnt pages.  In this way a run-length encoding
 * compression scheme is implemented for imprints.
 *
 * Imprints are used for range selects, i.e. finding all rows in a BAT
 * whose value is inside some given range, or alternatively, all rows in a
 * BAT whose value is outside some given range (anti select).
 *
 * A range necessarily covers one or more consecutive bins.  A bit mask is
 * created for all bins that fall fully inside the range being selected (in
 * gdk_select.c called "innermask"), and a bit mask is created for all bins
 * that fall fully or partially inside the range (called "mask" in
 * gdk_select.c).  Note that for an "anti" select, i.e. a select which
 * matches everything except a given range, the bits in the bit masks are
 * not consecutive.
 *
 * We then go through the imps table.  All pages where the only set bits
 * are also set in "innermask" can be blindly added to the result: all
 * values fall inside the range.  All pages where none of the set bits are
 * also set in "mask" can be blindly skipped: no value falls inside the
 * range.  For the remaining pages, we scan the page and check each
 * individual value to see whether it is selected.
 *
 * Extra speed up is achieved by the run-length encoding of the imps table.
 * If a mask is in the category of fully inside the range or fully outside,
 * the complete set of pages can be added/skipped in one go.
 */

#define IMPRINTS_VERSION	2
#define IMPRINTS_HEADER_SIZE	4 /* nr of size_t fields in header */

#define BINSIZE(B, FUNC, T) \
	do {					\
		switch (B) {			\
		case 8: FUNC(T,8); break;	\
		case 16: FUNC(T,16); break;	\
		case 32: FUNC(T,32); break;	\
		case 64: FUNC(T,64); break;	\
		default: assert(0); break;	\
		}				\
	} while (0)


#define GETBIN(Z,X,B)				\
	do {					\
		int _i;				\
		Z = 0;				\
		for (_i = 1; _i < B; _i++)	\
			Z += ((X) >= bins[_i]);	\
	} while (0)


#define IMPS_CREATE(TYPE,B)						\
	do {								\
		uint##B##_t mask, prvmask;				\
		uint##B##_t *restrict im = (uint##B##_t *) imps;	\
		const TYPE *restrict col = (TYPE *) Tloc(b, 0);		\
		const TYPE *restrict bins = (TYPE *) inbins;		\
		const BUN page = IMPS_PAGE / sizeof(TYPE);		\
		prvmask = 0;						\
		for (i = 0; i < b->batCount; ) {			\
			const BUN lim = MIN(i + page, b->batCount);	\
			/* new mask */					\
			mask = 0;					\
			/* build mask for all BUNs in one PAGE */	\
			for ( ; i < lim; i++) {				\
				const TYPE val = col[i];		\
				GETBIN(bin,val,B);			\
				mask = IMPSsetBit(B,mask,bin);		\
				if (!is_##TYPE##_nil(val)) { /* do not count nils */ \
					if (!cnt_bins[bin]++) {		\
						/* first in the bin */	\
						min_bins[bin] = max_bins[bin] = i; \
					} else {			\
						if (val < col[min_bins[bin]]) \
							min_bins[bin] = i; \
						if (val > col[max_bins[bin]]) \
							max_bins[bin] = i; \
					}				\
				}					\
			}						\
			/* same mask as previous and enough count to add */ \
			if ((prvmask == mask) && (dcnt > 0) &&		\
			    (dict[dcnt-1].cnt < (IMPS_MAX_CNT-1))) {	\
				/* not a repeat header */		\
				if (!dict[dcnt-1].repeat) {		\
					/* if compressed */		\
					if (dict[dcnt-1].cnt > 1) {	\
						/* uncompress last */	\
						dict[dcnt-1].cnt--;	\
						/* new header */	\
						dict[dcnt].cnt = 1;	\
						dict[dcnt].flags = 0;	\
						dcnt++;			\
					}				\
					/* set repeat */		\
					dict[dcnt-1].repeat = 1;	\
				}					\
				/* increase cnt */			\
				dict[dcnt-1].cnt++;			\
			} else { /* new mask (or run out of header count) */ \
				prvmask=mask;				\
				im[icnt] = mask;			\
				icnt++;					\
				if ((dcnt > 0) && !(dict[dcnt-1].repeat) && \
				    (dict[dcnt-1].cnt < (IMPS_MAX_CNT-1))) { \
					dict[dcnt-1].cnt++;		\
				} else {				\
					dict[dcnt].cnt = 1;		\
					dict[dcnt].repeat = 0;		\
					dict[dcnt].flags = 0;		\
					dcnt++;				\
				}					\
			}						\
		}							\
	} while (0)

static void
imprints_create(BAT *b, void *inbins, BUN *stats, bte bits,
		void *imps, BUN *impcnt, cchdc_t *dict, BUN *dictcnt)
{
	BUN i;
	BUN dcnt, icnt;
	BUN *restrict min_bins = stats;
	BUN *restrict max_bins = min_bins + 64;
	BUN *restrict cnt_bins = max_bins + 64;
	int bin = 0;
	dcnt = icnt = 0;
#ifndef NDEBUG
	memset(min_bins, 0, 64 * SIZEOF_BUN);
	memset(max_bins, 0, 64 * SIZEOF_BUN);
#endif
	memset(cnt_bins, 0, 64 * SIZEOF_BUN);

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
		BINSIZE(bits, IMPS_CREATE, bte);
		break;
	case TYPE_sht:
		BINSIZE(bits, IMPS_CREATE, sht);
		break;
	case TYPE_int:
		BINSIZE(bits, IMPS_CREATE, int);
		break;
	case TYPE_lng:
		BINSIZE(bits, IMPS_CREATE, lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		BINSIZE(bits, IMPS_CREATE, hge);
		break;
#endif
	case TYPE_flt:
		BINSIZE(bits, IMPS_CREATE, flt);
		break;
	case TYPE_dbl:
		BINSIZE(bits, IMPS_CREATE, dbl);
		break;
	default:
		/* should never reach here */
		assert(0);
	}

	*dictcnt = dcnt;
	*impcnt = icnt;
}

#ifdef NDEBUG
#define CLRMEM()	((void) 0)
#else
#define CLRMEM()	while (k < 64) h[k++] = 0
#endif

#define FILL_HISTOGRAM(TYPE)						\
	do {								\
		BUN k;							\
		TYPE *restrict s = (TYPE *) Tloc(s4, 0);		\
		TYPE *restrict h = imprints->bins;			\
		if (cnt < 64-1) {					\
			TYPE max = GDK_##TYPE##_max;			\
			for (k = 0; k < cnt; k++)			\
				h[k] = s[k];				\
			while (k < (BUN) imprints->bits)		\
				h[k++] = max;				\
			CLRMEM();					\
		} else {						\
			double y, ystep = (double) cnt / (64 - 1);	\
			for (k = 0, y = 0; (BUN) y < cnt; y += ystep, k++) \
				h[k] = s[(BUN) y];			\
			if (k == 64 - 1) /* there is one left */	\
				h[k] = s[cnt - 1];			\
		}							\
	} while (0)

/* Check whether we have imprints on b (and return true if we do).  It
 * may be that the imprints were made persistent, but we hadn't seen
 * that yet, so check the file system.  This also returns true if b is
 * a view and there are imprints on b's parent.
 *
 * Note that the b->timprints pointer can be NULL, meaning there are
 * no imprints; (Imprints *) 1, meaning there are no imprints loaded,
 * but they may exist on disk; or a valid pointer to loaded imprints.
 * These values are maintained here, in the IMPSdestroy and IMPSfree
 * functions, and in BBPdiskscan during initialization. */
bool
BATcheckimprints(BAT *b)
{
	bool ret;

	if (/* DISABLES CODE */ (0) && VIEWtparent(b)) {
		assert(b->timprints == NULL);
		b = BBPdescriptor(VIEWtparent(b));
	}

	if (b->timprints == (Imprints *) 1) {
		MT_lock_set(&b->batIdxLock);
		if (b->timprints == (Imprints *) 1) {
			Imprints *imprints;
			const char *nme = BBP_physical(b->batCacheid);

			assert(!GDKinmemory(b->theap->farmid));
			b->timprints = NULL;
			if ((imprints = GDKzalloc(sizeof(Imprints))) != NULL &&
			    (imprints->imprints.farmid = BBPselectfarm(b->batRole, b->ttype, imprintsheap)) >= 0) {
				int fd;

				strconcat_len(imprints->imprints.filename,
					      sizeof(imprints->imprints.filename),
					      nme, ".timprints", NULL);
				/* check whether a persisted imprints index
				 * can be found */
				if ((fd = GDKfdlocate(imprints->imprints.farmid, nme, "rb", "timprints")) >= 0) {
					size_t hdata[4];
					struct stat st;
					size_t pages;

					pages = (((size_t) BATcount(b) * b->twidth) + IMPS_PAGE - 1) / IMPS_PAGE;
					if (read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
					    hdata[0] & ((size_t) 1 << 16) &&
					    ((hdata[0] & 0xFF00) >> 8) == IMPRINTS_VERSION &&
					    hdata[3] == (size_t) BATcount(b) &&
					    fstat(fd, &st) == 0 &&
					    st.st_size >= (off_t) (imprints->imprints.size =
								   imprints->imprints.free =
								   64 * b->twidth +
								   64 * 3 * SIZEOF_BUN +
								   pages * ((bte) hdata[0] / 8) +
								   hdata[2] * sizeof(cchdc_t) +
								   sizeof(uint64_t) /* padding for alignment */
								   + 4 * SIZEOF_SIZE_T) &&
					    HEAPload(&imprints->imprints, nme, "timprints", false) == GDK_SUCCEED) {
						/* usable */
						imprints->bits = (bte) (hdata[0] & 0xFF);
						imprints->impcnt = (BUN) hdata[1];
						imprints->dictcnt = (BUN) hdata[2];
						imprints->bins = imprints->imprints.base + 4 * SIZEOF_SIZE_T;
						imprints->stats = (BUN *) ((char *) imprints->bins + 64 * b->twidth);
						imprints->imps = (void *) (imprints->stats + 64 * 3);
						imprints->dict = (void *) ((uintptr_t) ((char *) imprints->imps + pages * (imprints->bits / 8) + sizeof(uint64_t)) & ~(sizeof(uint64_t) - 1));
						close(fd);
						imprints->imprints.parentid = b->batCacheid;
						b->timprints = imprints;
						TRC_DEBUG(ACCELERATOR, "BATcheckimprints(" ALGOBATFMT "): reusing persisted imprints\n", ALGOBATPAR(b));
						MT_lock_unset(&b->batIdxLock);

						return true;
					}
					close(fd);
					/* unlink unusable file */
					GDKunlink(imprints->imprints.farmid, BATDIR, nme, "timprints");
				}
			}
			GDKfree(imprints);
			GDKclrerr();	/* we're not currently interested in errors */
		}
		MT_lock_unset(&b->batIdxLock);
	}
	ret = b->timprints != NULL;
	if( ret)
		TRC_DEBUG(ACCELERATOR, "BATcheckimprints(" ALGOBATFMT "): already has imprints\n", ALGOBATPAR(b));
	return ret;
}

static void
BATimpsync(void *arg)
{
	BAT *b = arg;
	Imprints *imprints;
	int fd;
	lng t0 = GDKusec();
	const char *failed = " failed";

	MT_lock_set(&b->batIdxLock);
	if ((imprints = b->timprints) != NULL) {
		Heap *hp = &imprints->imprints;
		if (HEAPsave(hp, hp->filename, NULL, true) == GDK_SUCCEED) {
			if (hp->storage == STORE_MEM) {
				if ((fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) >= 0) {
					/* add version number */
					((size_t *) hp->base)[0] |= (size_t) IMPRINTS_VERSION << 8;
					/* sync-on-disk checked bit */
					((size_t *) hp->base)[0] |= (size_t) 1 << 16;
					if (write(fd, hp->base, SIZEOF_SIZE_T) >= 0) {
						failed = ""; /* not failed */
						if (!(GDKdebug & NOSYNCMASK)) {
#if defined(NATIVE_WIN32)
							_commit(fd);
#elif defined(HAVE_FDATASYNC)
							fdatasync(fd);
#elif defined(HAVE_FSYNC)
							fsync(fd);
#endif
						}
						hp->dirty = false;
					} else {
						failed = " write failed";
						perror("write hash");
					}
					close(fd);
				}
			} else {
				/* add version number */
				((size_t *) hp->base)[0] |= (size_t) IMPRINTS_VERSION << 8;
				/* sync-on-disk checked bit */
				((size_t *) hp->base)[0] |= (size_t) 1 << 16;
				if (!(GDKdebug & NOSYNCMASK) &&
				    MT_msync(hp->base, SIZEOF_SIZE_T) < 0) {
					failed = " sync failed";
					((size_t *) hp->base)[0] &= ~((size_t) IMPRINTS_VERSION << 8);
				} else {
					hp->dirty = false;
					failed = ""; /* not failed */
				}
			}
			TRC_DEBUG(ACCELERATOR, "BATimpsync(" ALGOBATFMT "): "
				  "imprints persisted "
				  "(" LLFMT " usec)%s\n", ALGOBATPAR(b),
				  GDKusec() - t0, failed);
		}
	}
	MT_lock_unset(&b->batIdxLock);
	BBPunfix(b->batCacheid);
}

gdk_return
BATimprints(BAT *b)
{
	BAT *s1 = NULL, *s2 = NULL, *s3 = NULL, *s4 = NULL;
	Imprints *imprints;
	lng t0 = GDKusec();

	/* we only create imprints for types that look like types we know */
	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		break;
	default:		/* type not supported */
		/* doesn't look enough like base type: do nothing */
		GDKerror("unsupported type\n");
		return GDK_FAIL;
	}

	BATcheck(b, GDK_FAIL);

	if (BATcheckimprints(b))
		return GDK_SUCCEED;

	if (/* DISABLES CODE */ (0) && VIEWtparent(b)) {
		/* views always keep null pointer and need to obtain
		 * the latest imprint from the parent at query time */
		s2 = b;		/* remember for ACCELDEBUG print */
		b = BBPdescriptor(VIEWtparent(b));
		assert(b);
		if (BATcheckimprints(b))
			return GDK_SUCCEED;
	}
	MT_lock_set(&b->batIdxLock);


	if (b->timprints == NULL) {
		BUN cnt;
		const char *nme = GDKinmemory(b->theap->farmid) ? ":memory:" : BBP_physical(b->batCacheid);
		size_t pages;

		MT_lock_unset(&b->batIdxLock);
		MT_thread_setalgorithm("create imprints");

		if (s2)
			TRC_DEBUG(ACCELERATOR, "BATimprints(b=" ALGOBATFMT
				  "): creating imprints on parent "
				  ALGOBATFMT "\n",
				  ALGOBATPAR(s2), ALGOBATPAR(b));
		else
			TRC_DEBUG(ACCELERATOR, "BATimprints(b=" ALGOBATFMT
				  "): creating imprints\n",
				  ALGOBATPAR(b));

		s2 = NULL;

		imprints = GDKzalloc(sizeof(Imprints));
		if (imprints == NULL) {
			return GDK_FAIL;
		}
		strconcat_len(imprints->imprints.filename,
			      sizeof(imprints->imprints.filename),
			      nme, ".timprints", NULL);
		pages = (((size_t) BATcount(b) * b->twidth) + IMPS_PAGE - 1) / IMPS_PAGE;
		imprints->imprints.farmid = BBPselectfarm(b->batRole, b->ttype,
							   imprintsheap);

#define SMP_SIZE 2048
		s1 = BATsample_with_seed(b, SMP_SIZE, (uint64_t) GDKusec() * (uint64_t) b->batCacheid);
		if (s1 == NULL) {
			GDKfree(imprints);
			return GDK_FAIL;
		}
		s2 = BATunique(b, s1);
		if (s2 == NULL) {
			BBPunfix(s1->batCacheid);
			GDKfree(imprints);
			return GDK_FAIL;
		}
		s3 = BATproject(s2, b);
		if (s3 == NULL) {
			BBPunfix(s1->batCacheid);
			BBPunfix(s2->batCacheid);
			GDKfree(imprints);
			return GDK_FAIL;
		}
		s3->tkey = true;	/* we know is unique on tail now */
		if (BATsort(&s4, NULL, NULL, s3, NULL, NULL, false, false, false) != GDK_SUCCEED) {
			BBPunfix(s1->batCacheid);
			BBPunfix(s2->batCacheid);
			BBPunfix(s3->batCacheid);
			GDKfree(imprints);
			return GDK_FAIL;
		}
		/* s4 now is ordered and unique on tail */
		assert(s4->tkey && s4->tsorted);
		cnt = BATcount(s4);
		imprints->bits = 64;
		if (cnt <= 32)
			imprints->bits = 32;
		if (cnt <= 16)
			imprints->bits = 16;
		if (cnt <= 8)
			imprints->bits = 8;

		/* The heap we create here consists of four parts:
		 * bins, max 64 entries with bin boundaries, domain of b;
		 * stats, min/max/count for each bin, min/max are oid, and count BUN;
		 * imps, max one entry per "page", entry is "bits" wide;
		 * dict, max two entries per three "pages".
		 * In addition, we add some housekeeping entries at
		 * the start so that we can determine whether we can
		 * trust the imprints when encountered on startup (including
		 * a version number -- CURRENT VERSION is 2). */
		MT_lock_set(&b->batIdxLock);
		if (b->timprints != NULL ||
		    HEAPalloc(&imprints->imprints,
			      IMPRINTS_HEADER_SIZE * SIZEOF_SIZE_T + /* extra info */
			      64 * b->twidth + /* bins */
			      64 * 3 * SIZEOF_BUN + /* {min,max,cnt}_bins */
			      pages * (imprints->bits / 8) + /* imps */
			      sizeof(uint64_t) + /* padding for alignment */
			      pages * sizeof(cchdc_t), /* dict */
			      1, 1) != GDK_SUCCEED) {
			MT_lock_unset(&b->batIdxLock);
			GDKfree(imprints);
			BBPunfix(s1->batCacheid);
			BBPunfix(s2->batCacheid);
			BBPunfix(s3->batCacheid);
			BBPunfix(s4->batCacheid);
			if (b->timprints != NULL)
				return GDK_SUCCEED; /* we were beaten to it */
			GDKerror("memory allocation error");
			return GDK_FAIL;
		}
		imprints->bins = imprints->imprints.base + IMPRINTS_HEADER_SIZE * SIZEOF_SIZE_T;
		imprints->stats = (BUN *) ((char *) imprints->bins + 64 * b->twidth);
		imprints->imps = (void *) (imprints->stats + 64 * 3);
		imprints->dict = (void *) ((uintptr_t) ((char *) imprints->imps + pages * (imprints->bits / 8) + sizeof(uint64_t)) & ~(sizeof(uint64_t) - 1));

		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
			FILL_HISTOGRAM(bte);
			break;
		case TYPE_sht:
			FILL_HISTOGRAM(sht);
			break;
		case TYPE_int:
			FILL_HISTOGRAM(int);
			break;
		case TYPE_lng:
			FILL_HISTOGRAM(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			FILL_HISTOGRAM(hge);
			break;
#endif
		case TYPE_flt:
			FILL_HISTOGRAM(flt);
			break;
		case TYPE_dbl:
			FILL_HISTOGRAM(dbl);
			break;
		default:
			/* should never reach here */
			assert(0);
		}

		imprints_create(b,
				imprints->bins,
				imprints->stats,
				imprints->bits,
				imprints->imps,
				&imprints->impcnt,
				imprints->dict,
				&imprints->dictcnt);
		assert(imprints->impcnt <= pages);
		assert(imprints->dictcnt <= pages);
#ifndef NDEBUG
		memset((char *) imprints->imps + imprints->impcnt * (imprints->bits / 8), 0, (char *) imprints->dict - ((char *) imprints->imps + imprints->impcnt * (imprints->bits / 8)));
#endif
		imprints->imprints.free = (size_t) ((char *) ((cchdc_t *) imprints->dict + imprints->dictcnt) - imprints->imprints.base);
		/* add info to heap for when they become persistent */
		((size_t *) imprints->imprints.base)[0] = (size_t) (imprints->bits);
		((size_t *) imprints->imprints.base)[1] = (size_t) imprints->impcnt;
		((size_t *) imprints->imprints.base)[2] = (size_t) imprints->dictcnt;
		((size_t *) imprints->imprints.base)[3] = (size_t) BATcount(b);
		imprints->imprints.parentid = b->batCacheid;
		b->timprints = imprints;
		if (BBP_status(b->batCacheid) & BBPEXISTING &&
		    !b->theap->dirty &&
		    !GDKinmemory(b->theap->farmid)) {
			MT_Id tid;
			BBPfix(b->batCacheid);
			char name[MT_NAME_LEN];
			snprintf(name, sizeof(name), "impssync%d", b->batCacheid);
			if (MT_create_thread(&tid, BATimpsync, b,
					     MT_THR_DETACHED, name) < 0)
				BBPunfix(b->batCacheid);
		}
	}

	TRC_DEBUG(ACCELERATOR, "BATimprints(%s): imprints construction " LLFMT " usec\n", BATgetId(b), GDKusec() - t0);
	MT_lock_unset(&b->batIdxLock);

	/* BBPUnfix tries to get the imprints lock which might lead to
	 * a deadlock if those were unfixed earlier */
	if (s1) {
		BBPunfix(s1->batCacheid);
		BBPunfix(s2->batCacheid);
		BBPunfix(s3->batCacheid);
		BBPunfix(s4->batCacheid);
	}
	return GDK_SUCCEED;
}

#define getbin(TYPE,B)				\
	do {					\
		const TYPE val = * (TYPE *) v;	\
		GETBIN(ret,val,B);		\
	} while (0)

int
IMPSgetbin(int tpe, bte bits, const char *restrict inbins, const void *restrict v)
{
	int ret = -1;

	switch (tpe) {
	case TYPE_bte: {
		const bte *restrict bins = (bte *) inbins;
		BINSIZE(bits, getbin, bte);
		break;
	}
	case TYPE_sht: {
		const sht *restrict bins = (sht *) inbins;
		BINSIZE(bits, getbin, sht);
		break;
	}
	case TYPE_int: {
		const int *restrict bins = (int *) inbins;
		BINSIZE(bits, getbin, int);
		break;
	}
	case TYPE_lng: {
		const lng *restrict bins = (lng *) inbins;
		BINSIZE(bits, getbin, lng);
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		const hge *restrict bins = (hge *) inbins;
		BINSIZE(bits, getbin, hge);
		break;
	}
#endif
	case TYPE_flt: {
		const flt *restrict bins = (flt *) inbins;
		BINSIZE(bits, getbin, flt);
		break;
	}
	case TYPE_dbl: {
		const dbl *restrict bins = (dbl *) inbins;
		BINSIZE(bits, getbin, dbl);
		break;
	}
	default:
		assert(0);
		(void) inbins;
		break;
	}
	return ret;
}

lng
IMPSimprintsize(BAT *b)
{
	lng sz = 0;
	if (b->timprints && b->timprints != (Imprints *) 1) {
		sz = b->timprints->impcnt * b->timprints->bits / 8;
		sz += b->timprints->dictcnt * sizeof(cchdc_t);
	}
	return sz;
}

static void
IMPSremove(BAT *b)
{
	Imprints *imprints;

	assert(b->timprints != NULL);
	assert(/* DISABLES CODE */ (1) || !VIEWtparent(b));

	if ((imprints = b->timprints) != NULL) {
		b->timprints = NULL;

		TRC_DEBUG_IF(ACCELERATOR) {
			if (* (size_t *) imprints->imprints.base & (1 << 16))
				TRC_DEBUG_ENDIF(ACCELERATOR, "Removing persisted imprints\n");
		}
		if (HEAPdelete(&imprints->imprints, BBP_physical(b->batCacheid),
			       "timprints") != GDK_SUCCEED)
			TRC_DEBUG(IO_, "IMPSremove(%s): imprints heap\n", BATgetId(b));

		GDKfree(imprints);
	}
}

void
IMPSdestroy(BAT *b)
{
	if (b && b->timprints) {
		MT_lock_set(&b->batIdxLock);
		if (b->timprints == (Imprints *) 1) {
			b->timprints = NULL;
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, imprintsheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  "timprints");
		} else if (b->timprints != NULL &&
			   (/* DISABLES CODE */ (1) || !VIEWtparent(b)))
			IMPSremove(b);
		MT_lock_unset(&b->batIdxLock);
	}
}

/* free the memory associated with the imprints, do not remove the
 * heap files; indicate that imprints are available on disk by setting
 * the imprints pointer to 1 */
void
IMPSfree(BAT *b)
{
	Imprints *imprints;

	if (b && b->timprints) {
		assert(b->batCacheid > 0);
		MT_lock_set(&b->batIdxLock);
		imprints = b->timprints;
		if (imprints != NULL && imprints != (Imprints *) 1) {
			if (GDKinmemory(b->theap->farmid)) {
				b->timprints = NULL;
				if (/* DISABLES CODE */ (1) || !VIEWtparent(b)) {
					HEAPfree(&imprints->imprints, true);
					GDKfree(imprints);
				}
			} else {
				b->timprints = (Imprints *) 1;
				if (/* DISABLES CODE */ (1) || !VIEWtparent(b)) {
					HEAPfree(&imprints->imprints, false);
					GDKfree(imprints);
				}
			}
		}
		MT_lock_unset(&b->batIdxLock);
	}
}

#ifndef NDEBUG
/* never called, useful for debugging */

#define IMPSPRNTMASK(T, B)						\
	do {								\
		uint##B##_t *restrict im = (uint##B##_t *) imprints->imps; \
		for (j = 0; j < imprints->bits; j++)			\
			s[j] = IMPSisSet(B, im[icnt], j) ? 'x' : '.';	\
		s[j] = '\0';						\
	} while (0)

/* function used for debugging */
void
IMPSprint(BAT *b)
{
	Imprints *imprints;
	cchdc_t *restrict d;
	char s[65];		/* max number of bits + 1 */
	BUN icnt, dcnt, l, pages;
	BUN *restrict min_bins, *restrict max_bins;
	BUN *restrict cnt_bins;
	bte j;
	int i;

	if (!BATcheckimprints(b)) {
		fprintf(stderr, "No imprint\n");
		return;
	}
	imprints = b->timprints;
	d = (cchdc_t *) imprints->dict;
	min_bins = imprints->stats;
	max_bins = min_bins + 64;
	cnt_bins = max_bins + 64;

	fprintf(stderr,
		"bits = %d, impcnt = " BUNFMT ", dictcnt = " BUNFMT "\n",
		imprints->bits, imprints->impcnt, imprints->dictcnt);
	fprintf(stderr, "MIN\n");
	for (i = 0; i < imprints->bits; i++) {
		fprintf(stderr, "[ " BUNFMT " ]\n", min_bins[i]);
	}

	fprintf(stderr, "MAX\n");
	for (i = 0; i < imprints->bits; i++) {
		fprintf(stderr, "[ " BUNFMT " ]\n", max_bins[i]);
	}
	fprintf(stderr, "COUNT\n");
	for (i = 0; i < imprints->bits; i++) {
		fprintf(stderr, "[ " BUNFMT " ]\n", cnt_bins[i]);
	}
	for (dcnt = 0, icnt = 0, pages = 1; dcnt < imprints->dictcnt; dcnt++) {
		if (d[dcnt].repeat) {
			BINSIZE(imprints->bits, IMPSPRNTMASK, " ");
			pages += d[dcnt].cnt;
			fprintf(stderr, "[ " BUNFMT " ]r %s\n", pages, s);
			icnt++;
		} else {
			l = icnt + d[dcnt].cnt;
			for (; icnt < l; icnt++) {
				BINSIZE(imprints->bits, IMPSPRNTMASK, " ");
				fprintf(stderr, "[ " BUNFMT " ]  %s\n", pages++, s);
			}
		}
	}
}
#endif
