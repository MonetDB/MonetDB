#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_rtree.h"

//TODO Why use BBPselectfarm?

// Persist rtree to disk if the conditions are right
static gdk_return
persistRtree (BAT *b)
{
	/* Conditions to persist the RTree:
	 * - BAT has to be persistent
	 * - No deleted rows (when does batInserted update?)
	 * - The heap is not dirty -> no new values
	 * - DB Farm is persistent i.e. not in memory
	 */
	if ((BBP_status(b->batCacheid) & BBPEXISTING)
	     && b->batInserted == b->batCount
	     && !b->theap->dirty
	     && !GDKinmemory(b->theap->farmid)) {
		//TODO Necessary?
		BBPfix(b->batCacheid);
		rtree_t *rtree = b->T.rtree;

		if (rtree) {
			const char * filename = BBP_physical(b->batCacheid);
			int farmid = b->theap->farmid;

			int fd = GDKfdlocate(farmid, filename, "w", "bsrt");
			FILE *file_stream = fdopen(fd,"w");

			if (file_stream != NULL) {
				int err;
				if ((err = rtree_bsrt_write(rtree,file_stream)) != 0) {
					GDKerror("%s", rtree_strerror(err));
					fclose(file_stream);
					BBPunfix(b->batCacheid);
					return GDK_FAIL;
				}

				if (!(GDKdebug & NOSYNCMASK)) {
	#if defined(NATIVE_WIN32)
					_commit(fd);
	#elif defined(HAVE_FDATASYNC)
					fdatasync(fd);
	#elif defined(HAVE_FSYNC)
					fsync(fd);
	#endif
				}
				fclose(file_stream);
			}
			else {
				GDKerror("%s",strerror(errno));
				close(fd);
				return GDK_FAIL;
			}
		}
		BBPunfix(b->batCacheid);
	}
	//TODO Should we just return sucess if the rtree is not persisted?
	return GDK_SUCCEED;
}

static gdk_return
BATcheckrtree(BAT *b) {
	const char * filename = BBP_physical(b->batCacheid);
	int farmid = b->theap->farmid;
	int fd = GDKfdlocate(farmid, filename, "r", "bsrt");

	//Do we have the rtree on file?
	if (fd == -1)
		return GDK_SUCCEED;

	FILE *file_stream = fdopen(fd,"r");
	if (file_stream != NULL) {
		rtree_t* rtree = rtree_bsrt_read(file_stream);
		if (!rtree) {
			GDKerror("%s", errno != 0 ? strerror(errno) : "Failed rtree_bsrt_read");
			fclose(file_stream);
			return GDK_FAIL;
		}
		b->T.rtree = rtree;
		fclose(file_stream);
	}
	else {
		GDKerror("%s",strerror(errno));
		close(fd);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

//Check if RTree exists
bool
RTREEexists(BAT *b)
{
	BAT *pb;
	bool ret;
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	MT_lock_set(&pb->batIdxLock);
	ret = pb->T.rtree != NULL;
	MT_lock_unset(&pb->batIdxLock);

	return ret;

}

gdk_return
BATrtree(BAT *wkb, BAT *mbr)
{
	BAT *pb;
	BATiter bi;
	rtree_t *rtree = NULL;
	struct canditer ci;

	//Check for a parent BAT of wkb, load if exists
	if (VIEWtparent(wkb)) {
		pb = BBP_cache(VIEWtparent(wkb));
		assert(pb);
	} else {
		pb = wkb;
	}

	//Check if rtree already exists
	if (pb->T.rtree == NULL) {
		//If it doesn't exist, take the lock to create/get the rtree
		MT_lock_set(&pb->batIdxLock);

		//Try to load it from disk
		if (BATcheckrtree(pb) == GDK_SUCCEED && pb->T.rtree != NULL) {
			MT_lock_unset(&pb->batIdxLock);
			return GDK_SUCCEED;
		}

		//First arg are dimensions: we only allow x, y
		//Second arg are flags: split strategy and nodes-per-page
		if ((rtree = rtree_new(2, RTREE_DEFAULT)) == NULL) {
			GDKerror("rtree_new failed\n");
			return GDK_FAIL;
		}
		bi = bat_iterator(mbr);
		canditer_init(&ci, mbr,NULL);

		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = canditer_next(&ci) - mbr->hseqbase;
			mbr_t *inMBR = (mbr_t *)BUNtail(bi, p);

			rtree_id_t rtree_id = i;
			rtree_coord_t rect[4];
			rect[0] = inMBR->xmin;
			rect[1] = inMBR->ymin;
			rect[2] = inMBR->xmax;
			rect[3] = inMBR->ymax;
			rtree_add_rect(rtree,rtree_id,rect);
		}
		bat_iterator_end(&bi);
		pb->T.rtree = rtree;
		persistRtree(pb);
		MT_lock_unset(&pb->batIdxLock);
	}
	return GDK_SUCCEED;
}

struct results_rtree {
	int results_next;
	int results_left;
	BUN* candidates;
};

static int
f (rtree_id_t id, void *context) {
	struct results_rtree *results_rtree = (struct results_rtree *) context;
	results_rtree->candidates[results_rtree->results_next++] = (BUN) id;
	results_rtree->results_left -= 1;
	return results_rtree->results_left <= 0;
}

BUN*
RTREEsearch(BAT *b, mbr_t *inMBR, int result_limit) {
	BAT *pb;
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}
	rtree_t *rtree = pb->T.rtree;
	if (rtree != NULL) {
		BUN *candidates = GDKmalloc(result_limit*sizeof(BUN));
		memset(candidates,BUN_NONE,result_limit*sizeof(BUN));

		rtree_coord_t rect[4];
		rect[0] = inMBR->xmin;
		rect[1] = inMBR->ymin;
		rect[2] = inMBR->xmax;
		rect[3] = inMBR->ymax;
		struct results_rtree results;
		results.results_next = 0;
		results.results_left = result_limit;
		results.candidates = candidates;
		rtree_search(rtree, (const rtree_coord_t*) rect, f, &results);
		return candidates;
	} else
		return NULL;
}
