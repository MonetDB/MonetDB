#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_rtree.h"

//TODO The check for hasrtree should look into the parent BAT, not just compare the BAT->rtree to NULL

// Persist rtree to disk if the conditions are right
/*static void
persistRtree (BAT *b)
{*/
	/* Conditions to persist the RTree:
	 * - BAT has to be persistent
	 * - No deleted rows (when does batInserted update?)
	 * - The heap is not dirty -> no new values
	 * - DB Farm is persistent i.e. not in memory
	 */
/*	if ((BBP_status(b->batCacheid) & BBPEXISTING)
	     && b->batInserted == b->batCount
	     && !b->theap->dirty
	     && !GDKinmemory(b->theap->farmid)) {
		BBPfix(b->batCacheid);
		//char name[MT_NAME_LEN];
		//snprintf(name, sizeof(name), "rtreesync%d", b->batCacheid);
	}
}*/

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

//Create the RTree index
gdk_return
RTREEcreate (BAT *b) {
	BAT *pb = NULL;
	//Check for a parent BAT of wkb, load if exists
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}
	//Check if rtree already exists
	//TODO Check if it is on disk
	if (pb->T.rtree == NULL) {
		//If it doesn't exist, take the lock to create/get the rtree
		MT_lock_set(&pb->batIdxLock);

		//Try to load it from disk
		//TODO BATcheckrtree

		//First arg are dimensions: we only allow x, y
		//Second arg are flags: split strategy and nodes-per-page
		if ((pb->T.rtree = rtree_new(2, RTREE_DEFAULT)) == NULL) {
			GDKerror("rtree_new failed\n");
			return GDK_FAIL;
		}
		//TODO persist rtree
		MT_lock_unset(&pb->batIdxLock);
	}
	return GDK_SUCCEED;
}

//Add a rectangle to the previously created RTree index
gdk_return
RTREEaddmbr (BAT *b, mbr_t *inMBR, BUN i) {
	BAT *pb = NULL;
	if (VIEWtparent(b))
		pb = BBP_cache(VIEWtparent(b));
	else
		pb = b;
	//Check if rtree already exists
	//TODO Check if it is on disk
	if (pb->T.rtree != NULL) {
		rtree_id_t rtree_id = i;
		rtree_coord_t rect[4];
		rect[0] = inMBR->xmin;
		rect[1] = inMBR->ymin;
		rect[2] = inMBR->xmax;
		rect[3] = inMBR->ymax;
		rtree_add_rect(pb->T.rtree,rtree_id,rect);
	}
	else {
		GDKerror("Tried to insert mbr into RTree that was not initialized\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

//TODO Make this multi-thread safe? -> Only allow one thread to do rtree_new and persist the BAT, but multiple threads can add new rects to the tree
//MBR bat
gdk_return
BATrtree(BAT *wkb, BAT *mbr)
{
	BAT *pb;
	BATiter bi;
	rtree_t *rtree = NULL;
	struct canditer ci;

	//TODO check for MBR type

	//Check for a parent BAT of wkb, load if exists
	if (VIEWtparent(wkb)) {
		pb = BBP_cache(VIEWtparent(wkb));
		assert(pb);
	} else {
		pb = wkb;
	}

	//Check if rtree already exists
	//TODO Check if it is on disk
	if (pb->T.rtree == NULL) {
		//If it doesn't exist, take the lock to create/get the rtree
		MT_lock_set(&pb->batIdxLock);

		//Try to load it from disk
		//TODO BATcheckrtree

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
		//TODO persist rtree
		MT_lock_unset(&pb->batIdxLock);
	}
	//TODO Check if the rtree is complete in case of already existing rtree (not NULL)
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
		memset(candidates,BUN_NONE,result_limit*sizeof(BUN*));

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
