/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. J. Nes
 *
 * @* Transaction management
 * The Transaction Manager maintains the buffer of (permanent) BATS
 * held resident.  Entries from the BAT buffer are always accessed by
 * BAT id.  A BAT becomes permanent by assigning a name with
 * @%BBPrename@.  Access to the transaction table is regulated by a
 * semaphore.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * The physical (disk) commit protocol is handled mostly by
 * BBPsync. Once a commit succeeded, there is the task of removing
 * ex-persistent bats (those that still were persistent in the
 * previous commit, but were made transient in this transaction).
 * Notice that such ex- (i.e. non-) persistent bats are not backed up
 * by the BBPsync protocol, so we cannot start deleting after we know
 * the commit will succeed.
 *
 * Another hairy issue are the delta statuses in BATs. These provide a
 * fast way to perform a transaction abort (HOT-abort, instead of
 * COLD-abort, which is achieved by the BBP recovery in a database
 * restart). Hot-abort functionality has not been important in MonetDB
 * for now, so it is not well-tested. The problem here is that if a
 * commit fails in the physical part (BBPsync), we have not sufficient
 * information to roll back the delta statuses.
 *
 * So a 'feature' of the abort is that after a failed commit,
 * in-memory we *will* commit the transaction. Subsequent commits can
 * retry to achieve a physical commit. The only way to abort in such a
 * situation is COLD-abort: quit the server and restart, so you get
 * the recovered disk images.
 */

/* in the commit epilogue, the BBP-status of the bats is changed to
 * reflect their presence in the succeeded checkpoint.  Also bats from
 * the previous checkpoint that were deleted now are physically
 * destroyed.
 */
static void
epilogue(int cnt, bat *subcommit, bool locked)
{
	int i = 0;

	while (++i < cnt) {
		bat bid = subcommit ? subcommit[i] : i;
		BAT *b;

		if (BBP_status(bid) & BBPPERSISTENT) {
			BBP_status_on(bid, BBPEXISTING);
		} else if (BBP_status(bid) & BBPDELETED) {
			/* check mmap modes of bats that are now
			 * transient. this has to be done after the
			 * commit succeeded, because the mmap modes
			 * allowed on transient bats would be
			 * dangerous on persistent bats. If the commit
			 * failed, the already processed bats that
			 * would become transient after the commit,
			 * but didn't due to the failure, would be a
			 * consistency risk.
			 */
			b = BBP_cache(bid);
			if (b) {
				/* check mmap modes */
				MT_lock_set(&b->theaplock);
				if (BATcheckmodes(b, true) != GDK_SUCCEED)
					GDKwarning("BATcheckmodes failed\n");
				MT_lock_unset(&b->theaplock);
			}
		}
		b = BBP_desc(bid);
		if (b && b->ttype >= 0 && ATOMvarsized(b->ttype)) {
			MT_lock_set(&b->theaplock);
			ValPtr p = BATgetprop_nolock(b, (enum prop_t) 20);
			if (p != NULL) {
				Heap *tail = p->val.pval;
				assert(b->oldtail != NULL);
				BATrmprop_nolock(b, (enum prop_t) 20);
				if (b->oldtail != (Heap *) 1)
					HEAPdecref(b->oldtail, true);
				if (tail == b->theap ||
				    strcmp(tail->filename,
					   b->theap->filename) == 0) {
					/* no upgrades done since saving
					 * started */
					b->oldtail = NULL;
					HEAPdecref(tail, false);
				} else {
					b->oldtail = tail;
					ATOMIC_OR(&tail->refs, DELAYEDREMOVE);
				}
			}
			MT_lock_unset(&b->theaplock);
		}
		if (!locked)
			MT_lock_set(&GDKswapLock(bid));
		if ((BBP_status(bid) & BBPDELETED) && BBP_refs(bid) <= 0 && BBP_lrefs(bid) <= 0) {
			if (!locked)
				MT_lock_unset(&GDKswapLock(bid));
			b = BBPquickdesc(bid);

			/* the unloaded ones are deleted without
			 * loading deleted disk images */
			if (b) {
				BATdelete(b);
			}
			BBPclear(bid); /* also clears BBP_status */
		} else {
			BBP_status_off(bid, BBPDELETED | BBPSWAPPED | BBPNEW);
			if (!locked)
				MT_lock_unset(&GDKswapLock(bid));
		}
	}
	GDKclrerr();
}

/*
 * @- TMcommit
 * global commit without any multi-threaded access assumptions, thus
 * taking all BBP locks.  It creates a new database checkpoint.
 */
gdk_return
TMcommit(void)
{
	gdk_return ret = GDK_FAIL;

	/* commit with the BBP globally locked */
	BBPlock();
	if (BBPsync(getBBPsize(), NULL, NULL, getBBPlogno(), getBBPtransid()) == GDK_SUCCEED) {
		epilogue(getBBPsize(), NULL, true);
		ret = GDK_SUCCEED;
	}
	BBPunlock();
	return ret;
}

/*
 * @- TMsubcommit
 *
 * Create a new checkpoint that is equal to the previous, with the
 * exception that for the passed list of batnames, the current state
 * will be reflected in the new checkpoint.
 *
 * On the bats in this list we assume exclusive access during the
 * operation.
 *
 * This operation is useful for e.g. adding a new XQuery document or
 * SQL table to the committed state (after bulk-load). Or for dropping
 * a table or doc, without forcing the total database to be clean,
 * which may require a lot of I/O.
 *
 * We expect the globally locked phase (BBPsync) to take little time
 * (<100ms) as only the BBP.dir is written out; and for the existing
 * bats that were modified, only some heap moves are done (moved from
 * BAKDIR to SUBDIR).  The atomic commit for sub-commit is the rename
 * of SUBDIR to DELDIR.
 *
 * As it does not take the BBP-locks (thanks to the assumption that
 * access is exclusive), the concurrency impact of subcommit is also
 * much lighter to ongoing concurrent query and update facilities than
 * a real global TMcommit.
 */
gdk_return
TMsubcommit_list(bat *restrict subcommit, BUN *restrict sizes, int cnt, lng logno, lng transid)
{
	int xx;
	gdk_return ret = GDK_FAIL;

	assert(cnt > 0);
	assert(subcommit[0] == 0); /* BBP artifact: slot 0 in the array will be ignored */

	if (GDKinmemory(0))
		return GDK_SUCCEED;

	/* sort the list on BAT id */
	GDKqsort(subcommit + 1, sizes ? sizes + 1 : NULL, NULL, cnt - 1, sizeof(bat), sizes ? sizeof(BUN) : 0, TYPE_bat, false, false);

	assert(cnt == 1 || subcommit[1] > 0);  /* all values > 0 */
	/* de-duplication of BAT ids in subcommit list
	 * this is needed because of legacy reasons (database
	 * upgrade) */
	for (xx = 2; xx < cnt; xx++) {
		if (subcommit[xx-1] == subcommit[xx]) {
			int i;
			cnt--;
			for (i = xx; i < cnt; i++)
				subcommit[i] = subcommit[i+1];
			if (sizes) {
				for (i = xx; i < cnt; i++)
					sizes[i] = sizes[i+1];
			}
		}
	}
	/* lock just prevents other global (sub-)commits */
	BBPtmlock();
	if (BBPsync(cnt, subcommit, sizes, logno, transid) == GDK_SUCCEED) { /* write BBP.dir (++) */
		epilogue(cnt, subcommit, false);
		ret = GDK_SUCCEED;
	}
	BBPtmunlock();
	return ret;
}

gdk_return
TMsubcommit(BAT *b)
{
	int cnt = 1;
	gdk_return ret = GDK_FAIL;
	bat *subcommit;
	BUN p, q;

	subcommit = GDKmalloc((BATcount(b) + 1) * sizeof(bat));
	if (subcommit == NULL)
		return GDK_FAIL;

	BATiter bi = bat_iterator(b);
	subcommit[0] = 0;	/* BBP artifact: slot 0 in the array will be ignored */
	/* collect the list and save the new bats outside any
	 * locking */
	BATloop(b, p, q) {
		bat bid = BBPindex((str) BUNtvar(bi, p));

		if (bid)
			subcommit[cnt++] = bid;
	}
	bat_iterator_end(&bi);

	ret = TMsubcommit_list(subcommit, NULL, cnt, getBBPlogno(), getBBPtransid());
	GDKfree(subcommit);
	return ret;
}
