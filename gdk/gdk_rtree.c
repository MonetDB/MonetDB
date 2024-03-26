/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

//TODO Check if we need to input RTREEdestroy into drop_index func in sql_cat.c
//TODO Should you check if the parent BAT is null?

//TODO Re-check the conditions
/* Conditions to persist the RTree:
 * - BAT has to be persistent
 * - No deleted rows (when does batInserted update?)
 * - The heap is not dirty -> no new values
 * - DB Farm is persistent i.e. not in memory
 */
#ifdef HAVE_RTREE
static bool
RTREEpersistcheck (BAT *b) {
	return ((BBP_status(b->batCacheid) & BBPEXISTING)
	     	&& b->batInserted == b->batCount
	     	&& !b->theap->dirty
	     	&& !GDKinmemory(b->theap->farmid));
}

void
RTREEdecref(BAT *b)
{
	ATOMIC_BASE_TYPE refs = ATOMIC_DEC(&b->trtree->refs);
	//If RTree is marked for destruction and there are no refs, destroy the RTree
	if (b->trtree->destroy && refs == 0) {
		ATOMIC_DESTROY(&b->trtree->refs);
		rtree_destroy(b->trtree->rtree);
		b->trtree->rtree = NULL;
		GDKfree(b->trtree);
		b->trtree = NULL;
	}

}

void
RTREEincref(BAT *b)
{
	(void) ATOMIC_INC(&b->trtree->refs);
}

// Persist rtree to disk if the conditions are right
static gdk_return
persistRtree (BAT *b)
{
	if (RTREEpersistcheck(b)) {
		//TODO Necessary?
		BBPfix(b->batCacheid);
		rtree_t *rtree = b->trtree->rtree;

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
		b->trtree = GDKmalloc(sizeof(struct RTree));
		b->trtree->rtree = rtree;
		b->trtree->destroy = false;
		ATOMIC_INIT(&b->trtree->refs, 1);
		fclose(file_stream);
		return GDK_SUCCEED;
	}
	else {
		GDKerror("%s",strerror(errno));
		close(fd);
		return GDK_FAIL;
	}
}

//Check if RTree exists on file (previously created index)
static bool
RTREEexistsonfile(BAT *b) {
	const char * filename = BBP_physical(b->batCacheid);

	if (!b->theap) return false;

	int farmid = b->theap->farmid;
	int fd = GDKfdlocate(farmid, filename, "r", "bsrt");

	//Do we have the rtree on file?
	if (fd == -1) {
		return false;
	} else {
		close(fd);
		return true;
	}
}

//Check if RTree exists
//We also check if it exists on file. If the index is not loaded, it will be
//TODO Check for destroy -> it does not exist if destroy
bool
RTREEexists(BAT *b)
{
	BAT *pb;
	bool ret;
	if (VIEWtparent(b)) {
		pb = BBP_desc(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	MT_lock_set(&pb->batIdxLock);
	ret = (pb->trtree != NULL || RTREEexistsonfile(pb));
	MT_lock_unset(&pb->batIdxLock);

	return ret;

}

bool
RTREEexists_bid(bat *bid)
{
	BAT *b;
	bool ret;
	if ((b = BATdescriptor(*bid)) == NULL)
		return false;
	ret = RTREEexists(b);
	BBPunfix(b->batCacheid);
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
		pb = BBP_desc(VIEWtparent(wkb));
		assert(pb);
	} else {
		pb = wkb;
	}

	//Check if rtree already exists
	if (pb->trtree == NULL) {
		//If it doesn't exist, take the lock to create/get the rtree
		MT_lock_set(&pb->batIdxLock);

		//Try to load it from disk
		if (BATcheckrtree(pb) == GDK_SUCCEED && pb->trtree != NULL) {
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
		pb->trtree = GDKmalloc(sizeof(struct RTree));
		pb->trtree->rtree = rtree;
		pb->trtree->destroy = false;
		ATOMIC_INIT(&pb->trtree->refs, 1);
		persistRtree(pb);
		MT_lock_unset(&pb->batIdxLock);
	}
	//TODO What do we do when the conditions are not right for creating the index?
	return GDK_SUCCEED;
}

//Free the RTree from memory,
void
RTREEfree(BAT *b)
{
	BAT *pb;
	if (VIEWtparent(b)) {
		pb = BBP_desc(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	if (pb && pb->trtree) {
		MT_lock_set(&pb->batIdxLock);
		//Mark the RTree for destruction
		pb->trtree->destroy = true;
		RTREEdecref(pb);
		MT_lock_unset(&b->batIdxLock);
	}
}

//Free the RTree from memory, unlink the file associated with it
void
RTREEdestroy(BAT *b)
{
	BAT *pb;
	if (VIEWtparent(b)) {
		pb = BBP_desc(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	MT_lock_set(&pb->batIdxLock);
	if (pb->trtree) {
		//Mark the RTree for destruction
		pb->trtree->destroy = true;
		RTREEdecref(pb);
		//If the farm is in-memory, don't unlink the file (there is no file in that case)
		if (pb->theap && !GDKinmemory(pb->theap->farmid)) {
			GDKunlink(pb->theap->farmid,
			  	BATDIR,
			  	BBP_physical(b->batCacheid),
			  	"bsrt");
		}
	}
	//If the rtree is not loaded (pb->trtree is null), but there is a file with the index (from previous execution),
	//we should remove the file
	else if (RTREEexistsonfile(pb)) {
		GDKunlink(pb->theap->farmid,
			BATDIR,
			BBP_physical(b->batCacheid),
			"bsrt");
	}
	MT_lock_unset(&b->batIdxLock);
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
	--results_rtree->results_left;
	return results_rtree->results_left <= 0;
}

BUN*
RTREEsearch(BAT *b, mbr_t *inMBR, int result_limit) {
	BAT *pb;
	if (VIEWtparent(b)) {
		pb = BBP_desc(VIEWtparent(b));
		assert(pb);
	} else {
		pb = b;
	}

	//Try to load if there is an RTree index on file
	MT_lock_set(&pb->batIdxLock);
	if (pb->trtree == NULL) {
		if (BATcheckrtree(pb) != GDK_SUCCEED) {
			MT_lock_unset(&pb->batIdxLock);
			return NULL;
		}
	}
	MT_lock_unset(&pb->batIdxLock);

	rtree_t *rtree = pb->trtree->rtree;
	if (rtree != NULL) {
		//Increase ref, we're gonna use the index
		RTREEincref(pb);
		BUN *candidates = GDKmalloc((result_limit + 1) * SIZEOF_BUN);
		memset(candidates, 0, (result_limit + 1) * SIZEOF_BUN);

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
		candidates[result_limit - results.results_left] = BUN_NONE;

		//Finished using the index, decrease ref
		RTREEdecref(pb);

		return candidates;
	} else
		return NULL;
}
#else
void
RTREEdestroy(BAT *b) {
	(void) b;
}

void
RTREEfree(BAT *b) {
	(void) b;
}
#endif
