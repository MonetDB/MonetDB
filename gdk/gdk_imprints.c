/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

#define IMPRINTS_VERSION	2
#define IMPRINTS_HEADER_SIZE	4 /* nr of size_t fields in header */

#define BINSIZE(B, FUNC, T) do {		\
	switch (B) {				\
		case 8: FUNC(T,8); break;	\
		case 16: FUNC(T,16); break;	\
		case 32: FUNC(T,32); break;	\
		case 64: FUNC(T,64); break;	\
		default: assert(0); break;	\
	}					\
} while (0)


#define GETBIN(Z,X,B)				\
do {						\
	int _i;					\
	Z = 0;					\
	for (_i = 1; _i < B; _i++)		\
		Z += ((X) >= bins[_i]);		\
} while (0)


#define IMPS_CREATE(TYPE,B)						\
do {									\
	uint##B##_t mask, prvmask;					\
	uint##B##_t *restrict im = (uint##B##_t *) imps;		\
	const TYPE *restrict col = (TYPE *) Tloc(b, 0);			\
	const TYPE *restrict bins = (TYPE *) inbins;			\
	const BUN page = IMPS_PAGE / sizeof(TYPE);			\
	prvmask = 0;							\
	for (i = 0; i < b->batCount; ) {				\
		const BUN lim = MIN(i + page, b->batCount);		\
		/* new mask */						\
		mask = 0;						\
		/* build mask for all BUNs in one PAGE */		\
		for ( ; i < lim; i++) {					\
			register const TYPE val = col[i];		\
			GETBIN(bin,val,B);				\
			mask = IMPSsetBit(B,mask,bin);			\
			if (!is_##TYPE##_nil(val)) { /* do not count nils */ \
				if (!cnt_bins[bin]++) {			\
					min_bins[bin] = max_bins[bin] = i; \
				} else {				\
					if (val < col[min_bins[bin]])	\
						min_bins[bin] = i;	\
					if (val > col[max_bins[bin]])	\
						max_bins[bin] = i;	\
				}					\
			}						\
		}							\
		/* same mask as previous and enough count to add */	\
		if ((prvmask == mask) && (dcnt > 0) &&			\
		    (dict[dcnt-1].cnt < (IMPS_MAX_CNT-1))) {		\
			/* not a repeat header */			\
			if (!dict[dcnt-1].repeat) {			\
				/* if compressed */			\
				if (dict[dcnt-1].cnt > 1) {		\
					/* uncompress last */		\
					dict[dcnt-1].cnt--;		\
					/* new header */		\
					dict[dcnt].cnt = 1;		\
					dict[dcnt].flags = 0;		\
					dcnt++;				\
				}					\
				/* set repeat */			\
				dict[dcnt-1].repeat = 1;		\
			}						\
			/* increase cnt */				\
			dict[dcnt-1].cnt++;				\
		} else { /* new mask (or run out of header count) */	\
			prvmask=mask;					\
			im[icnt] = mask;				\
			icnt++;						\
			if ((dcnt > 0) && !(dict[dcnt-1].repeat) &&	\
			    (dict[dcnt-1].cnt < (IMPS_MAX_CNT-1))) {	\
				dict[dcnt-1].cnt++;			\
			} else {					\
				dict[dcnt].cnt = 1;			\
				dict[dcnt].repeat = 0;			\
				dict[dcnt].flags = 0;			\
				dcnt++;					\
			}						\
		}							\
	}								\
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
do {									\
	BUN k;								\
	TYPE *restrict s = (TYPE *) Tloc(s4, 0);			\
	TYPE *restrict h = imprints->bins;				\
	if (cnt < 64-1) {						\
		TYPE max = GDK_##TYPE##_max;				\
		for (k = 0; k < cnt; k++)				\
			h[k] = s[k];					\
		while (k < (BUN) imprints->bits)			\
			h[k++] = max;					\
		CLRMEM();						\
	} else {							\
		double y, ystep = (double) cnt / (64 - 1);		\
		for (k = 0, y = 0; (BUN) y < cnt; y += ystep, k++)	\
			h[k] = s[(BUN) y];				\
		if (k == 64 - 1) /* there is one left */		\
			h[k] = s[cnt - 1];				\
	}								\
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

	if (VIEWtparent(b)) {
		assert(b->timprints == NULL);
		b = BBPdescriptor(VIEWtparent(b));
	}

	if (b->timprints == (Imprints *) 1) {
		MT_lock_set(&b->batIdxLock);
		if (b->timprints == (Imprints *) 1) {
			Imprints *imprints;
			const char *nme = BBP_physical(b->batCacheid);

			assert(!GDKinmemory());
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
								   64 * 2 * SIZEOF_OID +
								   64 * SIZEOF_BUN +
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
		GDKerror("BATimprints: unsupported type\n");
		return GDK_FAIL;
	}

	BATcheck(b, "BATimprints", GDK_FAIL);

	if (BATcheckimprints(b))
		return GDK_SUCCEED;

	if (VIEWtparent(b)) {
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
		const char *nme = GDKinmemory() ? ":inmemory" : BBP_physical(b->batCacheid);
		size_t pages;

		MT_lock_unset(&b->batIdxLock);

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
		s1 = BATsample(b, SMP_SIZE);
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
			      64 * 2 * SIZEOF_OID + /* {min,max}_bins */
			      64 * SIZEOF_BUN +	    /* cnt_bins */
			      pages * (imprints->bits / 8) + /* imps */
			      sizeof(uint64_t) + /* padding for alignment */
			      pages * sizeof(cchdc_t), /* dict */
			      1) != GDK_SUCCEED) {
			MT_lock_unset(&b->batIdxLock);
			GDKfree(imprints);
			BBPunfix(s1->batCacheid);
			BBPunfix(s2->batCacheid);
			BBPunfix(s3->batCacheid);
			BBPunfix(s4->batCacheid);
			if (b->timprints != NULL)
				return GDK_SUCCEED; /* we were beaten to it */
			GDKerror("#BATimprints: memory allocation error");
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
		    !b->theap.dirty &&
		    !GDKinmemory()) {
			MT_Id tid;
			BBPfix(b->batCacheid);
			char name[16];
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
do {						\
	register const TYPE val = * (TYPE *) v;	\
	GETBIN(ret,val,B);			\
} while (0)

int
IMPSgetbin(int tpe, bte bits, const char *restrict inbins, const void *restrict v)
{
	int ret = -1;

	switch (tpe) {
	case TYPE_bte:
	{
		const bte *restrict bins = (bte *) inbins;
		BINSIZE(bits, getbin, bte);
	}
		break;
	case TYPE_sht:
	{
		const sht *restrict bins = (sht *) inbins;
		BINSIZE(bits, getbin, sht);
	}
		break;
	case TYPE_int:
	{
		const int *restrict bins = (int *) inbins;
		BINSIZE(bits, getbin, int);
	}
		break;
	case TYPE_lng:
	{
		const lng *restrict bins = (lng *) inbins;
		BINSIZE(bits, getbin, lng);
	}
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
	{
		const hge *restrict bins = (hge *) inbins;
		BINSIZE(bits, getbin, hge);
	}
		break;
#endif
	case TYPE_flt:
	{
		const flt *restrict bins = (flt *) inbins;
		BINSIZE(bits, getbin, flt);
	}
		break;
	case TYPE_dbl:
	{
		const dbl *restrict bins = (dbl *) inbins;
		BINSIZE(bits, getbin, dbl);
	}
		break;
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
	assert(!VIEWtparent(b));

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
		} else if (b->timprints != NULL && !VIEWtparent(b))
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
			if (GDKinmemory()) {
				b->timprints = NULL;
				if (!VIEWtparent(b)) {
					HEAPfree(&imprints->imprints, true);
					GDKfree(imprints);
				}
			} else {
				b->timprints = (Imprints *) 1;
				if (!VIEWtparent(b)) {
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
		TRC_DEBUG(ACCELERATOR, "No imprint\n");
		return;
	}
	imprints = b->timprints;
	d = (cchdc_t *) imprints->dict;
	min_bins = imprints->stats;
	max_bins = min_bins + 64;
	cnt_bins = max_bins + 64;

	TRC_DEBUG(ACCELERATOR,
		  "bits = %d, impcnt = " BUNFMT ", dictcnt = " BUNFMT "\n",
		  imprints->bits, imprints->impcnt, imprints->dictcnt);
	TRC_DEBUG(ACCELERATOR, "MIN\n");
	for (i = 0; i < imprints->bits; i++) {
		TRC_DEBUG(ACCELERATOR, "[ " BUNFMT " ]\n", min_bins[i]);
	}
	
	TRC_DEBUG(ACCELERATOR, "MAX\n");
	for (i = 0; i < imprints->bits; i++) {
		TRC_DEBUG(ACCELERATOR, "[ " BUNFMT " ]\n", max_bins[i]);
	}
	TRC_DEBUG(ACCELERATOR, "COUNT\n");
	for (i = 0; i < imprints->bits; i++) {
		TRC_DEBUG(ACCELERATOR, "[ " BUNFMT " ]\n", cnt_bins[i]);
	}
	for (dcnt = 0, icnt = 0, pages = 1; dcnt < imprints->dictcnt; dcnt++) {
		if (d[dcnt].repeat) {
			BINSIZE(imprints->bits, IMPSPRNTMASK, " ");
			pages += d[dcnt].cnt;
			TRC_DEBUG(ACCELERATOR, "[ " BUNFMT " ]r %s\n", pages, s);
			icnt++;
		} else {
			l = icnt + d[dcnt].cnt;
			for (; icnt < l; icnt++) {
				BINSIZE(imprints->bits, IMPSPRNTMASK, " ");
				TRC_DEBUG(ACCELERATOR, "[ " BUNFMT " ]  %s\n", pages++, s);
			}
		}
	}
}
#endif
