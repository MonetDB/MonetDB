/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "gdk_tm.h"

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
/* in the commit prelude, the delta status in the memory image of all
 * bats is commited */
static gdk_return
prelude(int cnt, bat *subcommit)
{
	int i = 0;

	while (++i < cnt) {
		bat bid = subcommit ? subcommit[i] : i;

		if (BBP_status(bid) & BBPPERSISTENT) {
			BAT *b = BBP_cache(bid);

			if (b == NULL && (BBP_status(bid) & BBPSWAPPED)) {
				b = BBPquickdesc(bid, TRUE);
				if (b == NULL)
					return GDK_FAIL;
			}
			if (b) {
				assert(!isVIEW(b));
				assert(b->batRole == PERSISTENT);
				BATcommit(b);
			}
		}
	}
	return GDK_SUCCEED;
}

/* in the commit epilogue, the BBP-status of the bats is changed to
 * reflect their presence in the succeeded checkpoint.  Also bats from
 * the previous checkpoint that were deleted now are physically
 * destroyed.
 */
static void
epilogue(int cnt, bat *subcommit)
{
	int i = 0;

	while (++i < cnt) {
		bat bid = subcommit ? subcommit[i] : i;

		if (BBP_status(bid) & BBPPERSISTENT) {
			BBP_status_on(bid, BBPEXISTING, subcommit ? "TMsubcommit" : "TMcommit");
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
			BAT *b = BBP_cache(bid);
			if (b) {
				/* check mmap modes */
				if (BATcheckmodes(b, true) != GDK_SUCCEED)
					fprintf(stderr, "#epilogue: BATcheckmodes failed\n");
			}
		}
		if ((BBP_status(bid) & BBPDELETED) && BBP_refs(bid) <= 0 && BBP_lrefs(bid) <= 0) {
			BAT *b = BBPquickdesc(bid, TRUE);

			/* the unloaded ones are deleted without
			 * loading deleted disk images */
			if (b) {
				BATdelete(b);
				if (BBP_cache(bid)) {
					/* those that quickdesc
					 * decides to load => free
					 * memory */
					BATfree(b);
				}
			}
			BBPclear(bid);	/* clear with locking */
		}
		BBP_status_off(bid, BBPDELETED | BBPSWAPPED | BBPNEW, subcommit ? "TMsubcommit" : "TMcommit");
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
	if (prelude(getBBPsize(), NULL) == GDK_SUCCEED &&
	    BBPsync(getBBPsize(), NULL) == GDK_SUCCEED) {
		epilogue(getBBPsize(), NULL);
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
TMsubcommit_list(bat *subcommit, int cnt)
{
	int xx;
	gdk_return ret = GDK_FAIL;

	assert(cnt > 0);
	assert(subcommit[0] == 0); /* BBP artifact: slot 0 in the array will be ignored */

	/* sort the list on BAT id */
	GDKqsort(subcommit + 1, NULL, NULL, cnt - 1, sizeof(bat), 0, TYPE_bat);

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
		}
	}
	if (prelude(cnt, subcommit) == GDK_SUCCEED) {	/* save the new bats outside the lock */
		/* lock just prevents BBPtrims, and other global
		 * (sub-)commits */
		for (xx = 0; xx <= BBP_THREADMASK; xx++)
			MT_lock_set(&GDKtrimLock(xx));
		if (BBPsync(cnt, subcommit) == GDK_SUCCEED) { /* write BBP.dir (++) */
			epilogue(cnt, subcommit);
			ret = GDK_SUCCEED;
		}
		for (xx = BBP_THREADMASK; xx >= 0; xx--)
			MT_lock_unset(&GDKtrimLock(xx));
	}
	return ret;
}

gdk_return
TMsubcommit(BAT *b)
{
	int cnt = 1;
	gdk_return ret = GDK_FAIL;
	bat *subcommit;
	BUN p, q;
	BATiter bi = bat_iterator(b);

	subcommit = (bat *) GDKmalloc((BATcount(b) + 1) * sizeof(bat));
	if (subcommit == NULL)
		return GDK_FAIL;

	subcommit[0] = 0;	/* BBP artifact: slot 0 in the array will be ignored */
	/* collect the list and save the new bats outside any
	 * locking */
	BATloop(b, p, q) {
		bat bid = BBPindex((str) BUNtail(bi, p));

		if (bid < 0)
			bid = -bid;
		if (bid)
			subcommit[cnt++] = bid;
	}

	ret = TMsubcommit_list(subcommit, cnt);
	GDKfree(subcommit);
	return ret;
}

/*
 * @- TMabort
 * Transaction abort is cheap. We use the delta statuses to go back to
 * the previous version of each BAT. Also for BATs that are currently
 * swapped out. Persistent BATs that were made transient in this
 * transaction become persistent again.
 */
void
TMabort(void)
{
	int i;

	BBPlock();
	for (i = 1; i < getBBPsize(); i++) {
		if (BBP_status(i) & BBPNEW) {
			BAT *b = BBPquickdesc(i, FALSE);

			if (b) {
				if (b->batPersistence == PERSISTENT)
					BBPrelease(i);
				b->batPersistence = TRANSIENT;
				b->batDirtydesc = 1;
			}
		}
	}
	for (i = 1; i < getBBPsize(); i++) {
		if (BBP_status(i) & (BBPPERSISTENT | BBPDELETED | BBPSWAPPED)) {
			BAT *b = BBPquickdesc(i, TRUE);

			if (b == NULL)
				continue;

			BBPfix(i);
			if (BATdirty(b) || DELTAdirty(b)) {
				/* BUN move-backes need a real BAT! */
				/* Stefan:
				 * Actually, in case DELTAdirty(b),
				 * i.e., a BAT with differences that
				 * is saved/swapped-out but not yet
				 * committed, we (AFAIK) don't have to
				 * load the BAT and apply the undo,
				 * but rather could simply discard the
				 * delta and revive the backup;
				 * however, I don't know how to do
				 * this (yet), hence we stick with
				 * this solution for the time being
				 * --- it should be correct though it
				 * might not be the most efficient
				 * way...
				 */
				b = BBPdescriptor(i);
				BATundo(b);
			}
			if (BBP_status(i) & BBPDELETED) {
				BBP_status_on(i, BBPEXISTING, "TMabort");
				if (b->batPersistence != PERSISTENT)
					BBPretain(i);
				b->batPersistence = PERSISTENT;
				b->batDirtydesc = 1;
			}
			BBPunfix(i);
		}
		BBP_status_off(i, BBPDELETED | BBPSWAPPED | BBPNEW, "TMabort");
	}
	BBPunlock();
	GDKclrerr();
}
