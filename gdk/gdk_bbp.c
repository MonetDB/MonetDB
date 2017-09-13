/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. J. Nes
 * @* BAT Buffer Pool (BBP)
 * The BATs created and loaded are collected in a BAT buffer pool.
 * The Bat Buffer Pool has a number of functions:
 * @table @code
 *
 * @item administration and lookup
 * The BBP is a directory which contains status information about all
 * known BATs.  This interface may be used very heavily, by
 * data-intensive applications.  To eliminate all overhead, read-only
 * access to the BBP may be done by table-lookups. The integer index
 * type for these lookups is @emph{bat}, as retrieved by
 * @emph{BBPcacheid(b)}. The @emph{bat} zero is reserved for the nil
 * bat.
 *
 * @item persistence
 * The BBP is made persistent by saving it to the dictionary file
 * called @emph{BBP.dir} in the database.
 *
 * When the number of BATs rises, having all files in one directory
 * becomes a bottleneck.  The BBP therefore implements a scheme that
 * distributes all BATs in a growing directory tree with at most 64
 * BATs stored in one node.
 *
 * @item buffer management
 * The BBP is responsible for loading and saving of BATs to disk. It
 * also contains routines to unload BATs from memory when memory
 * resources get scarce. For this purpose, it administers BAT memory
 * reference counts (to know which BATs can be unloaded) and BAT usage
 * statistics (it unloads the least recently used BATs).
 *
 * @item recovery
 * When the database is closed or during a run-time syncpoint, the
 * system tables must be written to disk in a safe way, that is immune
 * for system failures (like disk full). To do so, the BBP implements
 * an atomic commit and recovery protocol: first all files to be
 * overwritten are moved to a BACKUP/ dir. If that succeeds, the
 * writes are done. If that also fully succeeds the BACKUP/ dir is
 * renamed to DELETE_ME/ and subsequently deleted.  If not, all files
 * in BACKUP/ are moved back to their original location.
 *
 * @item unloading
 * Bats which have a logical reference (ie. a lrefs > 0) but no memory
 * reference (refcnt == 0) can be unloaded. Unloading dirty bats
 * means, moving the original (committed version) to the BACKUP/ dir
 * and saving the bat. This complicates the commit and recovery/abort
 * issues.  The commit has to check if the bat is already moved. And
 * The recovery has to always move back the files from the BACKUP/
 * dir.
 *
 * @item reference counting
 * Bats use have two kinds of references: logical and physical
 * (pointer) ones.  The logical references are administered by
 * BBPretain/BBPrelease, the physical ones by BBPfix/BBPunfix.
 *
 * @item share counting
 * Views use the heaps of there parent bats. To save guard this, the
 * parent has a shared counter, which is incremented and decremented
 * using BBPshare and BBPunshare. These functions make sure the parent
 * is memory resident as required because of the 'pointer' sharing.
 * @end table
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_storage.h"
#include "mutils.h"

#ifndef F_OK
#define F_OK 0
#endif
#ifdef _MSC_VER
#define access(f, m)	_access(f, m)
#endif

/*
 * The BBP has a fixed address, so re-allocation due to a growing BBP
 * caused by one thread does not disturb reads to the old entries by
 * another.  This is implemented using anonymous virtual memory;
 * extensions on the same address are guaranteed because a large
 * non-committed VM area is requested initially. New slots in the BBP
 * are found in O(1) by keeping a freelist that uses the 'next' field
 * in the BBPrec records.
 */
BBPrec *BBP[N_BBPINIT];		/* fixed base VM address of BBP array */
bat BBPlimit = 0;		/* current committed VM BBP array */
#ifdef ATOMIC_LOCK
static MT_Lock BBPsizeLock MT_LOCK_INITIALIZER("BBPsizeLock");
#endif
static volatile ATOMIC_TYPE BBPsize = 0; /* current used size of BBP array */

struct BBPfarm_t BBPfarms[MAXFARMS];

#define KITTENNAP 4 	/* used to suspend processing */
#define BBPNONAME "."		/* filler for no name in BBP.dir */
/*
 * The hash index uses a bucket index (int array) of size mask that is
 * tuned for perfect hashing (1 lookup). The bucket chain uses the
 * 'next' field in the BBPrec records.
 */
bat *BBP_hash = NULL;		/* BBP logical name hash buckets */
bat BBP_mask = 0;		/* number of buckets = & mask */

static void BBPspin(bat bid, const char *debug, int event);
static gdk_return BBPfree(BAT *b, const char *calledFrom);
static void BBPdestroy(BAT *b);
static void BBPuncacheit(bat bid, int unloaddesc);
static gdk_return BBPprepare(bit subcommit);
static BAT *getBBPdescriptor(bat i, int lock);
static gdk_return BBPbackup(BAT *b, bit subcommit);
static gdk_return BBPdir(int cnt, bat *subcommit);

#ifdef HAVE_HGE
/* start out by saying we have no hge, but as soon as we've seen one,
 * we'll always say we do have it */
static int havehge = 0;
#endif

#define BBPnamecheck(s) (BBPtmpcheck(s) ? strtol((s) + 4, NULL, 8) : 0)

static void
BBP_insert(bat i)
{
	bat idx = (bat) (strHash(BBP_logical(i)) & BBP_mask);

	BBP_next(i) = BBP_hash[idx];
	BBP_hash[idx] = i;
}

static void
BBP_delete(bat i)
{
	bat *h = BBP_hash;
	const char *s = BBP_logical(i);
	bat idx = (bat) (strHash(s) & BBP_mask);

	for (h += idx; (i = *h) != 0; h = &BBP_next(i)) {
		if (strcmp(BBP_logical(i), s) == 0) {
			*h = BBP_next(i);
			break;
		}
	}
}

bat
getBBPsize(void)
{
	return (bat) ATOMIC_GET(BBPsize, BBPsizeLock);
}


/*
 * other globals
 */
int BBP_dirty = 0;		/* BBP structures modified? */
int BBPin = 0;			/* bats loaded statistic */
int BBPout = 0;			/* bats saved statistic */

/*
 * @+ BBP Consistency and Concurrency
 * While GDK provides the basic building blocks for an ACID system, in
 * itself it is not such a system, as we this would entail too much
 * overhead that is often not needed. Hence, some consistency control
 * is left to the user. The first important user constraint is that if
 * a user updates a BAT, (s)he himself must assure that no-one else
 * accesses this BAT.
 *
 * Concerning buffer management, the BBP carries out a swapping
 * policy.  BATs are kept in memory till the memory is full. If the
 * memory is full, the malloc functions initiate BBP trim actions,
 * that unload the coldest BATs that have a zero reference count. The
 * second important user constraint is therefore that a user may only
 * manipulate live BAT data in memory if it is sure that there is at
 * least one reference count to that BAT.
 *
 * The main BBP array is protected by two locks:
 * @table @code
 * @item GDKcacheLock]
 * this lock guards the free slot management in the BBP array.  The
 * BBP operations that allocate a new slot for a new BAT
 * (@emph{BBPinit},@emph{BBPcacheit}), delete the slot of a destroyed
 * BAT (@emph{BBPreclaim}), or rename a BAT (@emph{BBPrename}), hold
 * this lock. It also protects all BAT (re)naming actions include
 * (read and write) in the hash table with BAT names.
 * @item GDKswapLock
 * this lock guards the swap (loaded/unloaded) status of the
 * BATs. Hence, all BBP routines that influence the swapping policy,
 * or actually carry out the swapping policy itself, acquire this lock
 * (e.g. @emph{BBPfix},@emph{BBPunfix}).  Note that this also means
 * that updates to the BBP_status indicator array must be protected by
 * GDKswapLock.
 *
 * To reduce contention GDKswapLock was split into multiple locks; it
 * is now an array of lock pointers which is accessed by
 * GDKswapLock(bat)
 * @end table
 *
 * Routines that need both locks should first acquire the locks in the
 * GDKswapLock array (in ascending order) and then GDKcacheLock (and
 * release them in reverse order).
 *
 * To obtain maximum speed, read operations to existing elements in
 * the BBP are unguarded. As said, it is the users responsibility that
 * the BAT that is being read is not being modified. BBP update
 * actions that modify the BBP data structure itself are locked by the
 * BBP functions themselves. Hence, multiple concurrent BBP read
 * operations may be ongoing while at the same time at most one BBP
 * write operation @strong{on a different BAT} is executing.  This
 * holds for accesses to the public (quasi-) arrays @emph{BBPcache},
 * @emph{BBPstatus}, @emph{BBPrefs}, @emph{BBPlogical} and
 * @emph{BBPphysical}. These arrays are called quasi as now they are
 * actually stored together in one big BBPrec array called BBP, that
 * is allocated in anonymous VM space, so we can reallocate this
 * structure without changing the base address (a crucial feature if
 * read actions are to go on unlocked while other entries in the BBP
 * may be modified).
 */
static volatile MT_Id locked_by = 0;

#define BBP_unload_inc(bid, nme)		\
	do {					\
		MT_lock_set(&GDKunloadLock);	\
		BBPunloadCnt++;			\
		MT_lock_unset(&GDKunloadLock);	\
	} while (0)

#define BBP_unload_dec(bid, nme)		\
	do {					\
		MT_lock_set(&GDKunloadLock);	\
		--BBPunloadCnt;			\
		assert(BBPunloadCnt >= 0);	\
		MT_lock_unset(&GDKunloadLock);	\
	} while (0)

static int BBPunloadCnt = 0;
static MT_Lock GDKunloadLock MT_LOCK_INITIALIZER("GDKunloadLock");

void
BBPlock(void)
{
	int i;

	/* wait for all pending unloads to finish */
	MT_lock_set(&GDKunloadLock);
	while (BBPunloadCnt > 0) {
		MT_lock_unset(&GDKunloadLock);
		MT_sleep_ms(1);
		MT_lock_set(&GDKunloadLock);
	}

	for (i = 0; i <= BBP_THREADMASK; i++)
		MT_lock_set(&GDKtrimLock(i));
	for (i = 0; i <= BBP_THREADMASK; i++)
		MT_lock_set(&GDKcacheLock(i));
	for (i = 0; i <= BBP_BATMASK; i++)
		MT_lock_set(&GDKswapLock(i));
	locked_by = MT_getpid();

	MT_lock_unset(&GDKunloadLock);
}

void
BBPunlock(void)
{
	int i;

	for (i = BBP_BATMASK; i >= 0; i--)
		MT_lock_unset(&GDKswapLock(i));
	for (i = BBP_THREADMASK; i >= 0; i--)
		MT_lock_unset(&GDKcacheLock(i));
	locked_by = 0;
	for (i = BBP_THREADMASK; i >= 0; i--)
		MT_lock_unset(&GDKtrimLock(i));
}


static gdk_return
BBPinithash(int j)
{
	bat i = (bat) ATOMIC_GET(BBPsize, BBPsizeLock);

	assert(j >= 0 && j <= BBP_THREADMASK);
	for (BBP_mask = 1; (BBP_mask << 1) <= BBPlimit; BBP_mask <<= 1)
		;
	BBP_hash = (bat *) GDKzalloc(BBP_mask * sizeof(bat));
	if (BBP_hash == NULL) {
		GDKerror("BBPinithash: cannot allocate memory\n");
		return GDK_FAIL;
	}
	BBP_mask--;

	while (--i > 0) {
		const char *s = BBP_logical(i);

		if (s) {
			if (*s != '.' && BBPtmpcheck(s) == 0) {
				BBP_insert(i);
			}
		} else {
			BBP_next(i) = BBP_free(j);
			BBP_free(j) = i;
			if (++j > BBP_THREADMASK)
				j = 0;
		}
	}
	return GDK_SUCCEED;
}

int
BBPselectfarm(int role, int type, enum heaptype hptype)
{
	int i;

	(void) type;		/* may use in future */
	(void) hptype;		/* may use in future */

	assert(role >= 0 && role < 32);
#ifndef PERSISTENTHASH
	if (hptype == hashheap)
		role = TRANSIENT;
#endif
#ifndef PERSISTENTIDX
	if (hptype == orderidxheap)
		role = TRANSIENT;
#endif
	for (i = 0; i < MAXFARMS; i++)
		if (BBPfarms[i].dirname && BBPfarms[i].roles & (1 << role))
			return i;
	/* must be able to find farms for TRANSIENT and PERSISTENT */
	assert(role != TRANSIENT && role != PERSISTENT);
	return -1;
}

/*
 * BBPextend must take the trimlock, as it is called when other BBP
 * locks are held and it will allocate memory.
 */
static gdk_return
BBPextend(int idx, int buildhash)
{
	if ((bat) ATOMIC_GET(BBPsize, BBPsizeLock) >= N_BBPINIT * BBPINIT) {
		GDKerror("BBPextend: trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);
		return GDK_FAIL;
	}

	/* make sure the new size is at least BBPsize large */
	while (BBPlimit < (bat) ATOMIC_GET(BBPsize, BBPsizeLock)) {
		assert(BBP[BBPlimit >> BBPINITLOG] == NULL);
		BBP[BBPlimit >> BBPINITLOG] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (BBP[BBPlimit >> BBPINITLOG] == NULL) {
			GDKerror("BBPextend: failed to extend BAT pool\n");
			return GDK_FAIL;
		}
		BBPlimit += BBPINIT;
	}

	if (buildhash) {
		int i;

		GDKfree(BBP_hash);
		BBP_hash = NULL;
		for (i = 0; i <= BBP_THREADMASK; i++)
			BBP_free(i) = 0;
		if (BBPinithash(idx) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static inline char *
BBPtmpname(char *s, size_t len, bat i)
{
	snprintf(s, len, "tmp_%o", (int) i);
	return s;
}

static inline str
BBPphysicalname(str s, int len, bat i)
{
	s[--len] = 0;
	while (i > 0) {
		s[--len] = '0' + (i & 7);
		i >>= 3;
	}
	return s + len;
}

static gdk_return
recover_dir(int farmid, int direxists)
{
	if (direxists) {
		/* just try; don't care about these non-vital files */
		if (GDKunlink(farmid, BATDIR, "BBP", "bak") != GDK_SUCCEED)
			fprintf(stderr, "#recover_dir: unlink of BBP.bak failed\n");
		if (GDKmove(farmid, BATDIR, "BBP", "dir", BATDIR, "BBP", "bak") != GDK_SUCCEED)
			fprintf(stderr, "#recover_dir: rename of BBP.dir to BBP.bak failed\n");
	}
	return GDKmove(farmid, BAKDIR, "BBP", "dir", BATDIR, "BBP", "dir");
}

static gdk_return BBPrecover(int farmid);
static gdk_return BBPrecover_subdir(void);
static int BBPdiskscan(const char *, size_t);

#ifdef GDKLIBRARY_SORTEDPOS
static void
fixsorted(void)
{
	bat bid;
	BAT *b;
	BATiter bi;
	int dbg = GDKdebug;
	int loaded;

	GDKdebug &= ~(CHECKMASK | PROPMASK);
	for (bid = 1; bid < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); bid++) {
		if ((b = BBP_desc(bid)) == NULL)
			continue; /* not a valid BAT */
		loaded = 0;
		if (b->tnosorted != 0) {
			if (b->tsorted) {
				/* position should not be set */
				b->batDirtydesc = 1;
				b->tnosorted = 0;
			} else if (b->tnosorted == 0 ||
				   b->tnosorted >= b->batCount ||
				   b->ttype < 0) {
				/* out of range */
				b->batDirtydesc = 1;
				b->tnosorted = 0;
			} else if (b->ttype == TYPE_void) {
				/* void is always sorted */
				b->batDirtydesc = 1;
				b->tnosorted = 0;
				b->tsorted = 1;
			} else {
				if (!loaded) {
					b = BATdescriptor(bid);
					bi = bat_iterator(b);
					if (b == NULL)
						b = BBP_desc(bid);
					else
						loaded = 1;
				}
				if (!loaded ||
				    ATOMcmp(b->ttype,
					    BUNtail(bi, b->tnosorted - 1),
					    BUNtail(bi, b->tnosorted)) <= 0) {
					/* incorrect hint */
					b->batDirtydesc = 1;
					b->tnosorted = 0;
				}
			}
		}
		if (b->tnorevsorted != 0) {
			if (b->trevsorted) {
				/* position should not be set */
				b->batDirtydesc = 1;
				b->tnorevsorted = 0;
			} else if (b->tnorevsorted == 0 ||
				   b->tnorevsorted >= b->batCount ||
				   b->ttype < 0) {
				/* out of range */
				b->batDirtydesc = 1;
				b->tnorevsorted = 0;
			} else if (b->ttype == TYPE_void) {
				/* void is only revsorted if nil */
				b->batDirtydesc = 1;
				if (b->tseqbase == oid_nil ||
				    b->batCount <= 1) {
					b->tnorevsorted = 0;
					b->trevsorted = 1;
				} else {
					b->tnorevsorted = 1;
				}
			} else {
				if (!loaded) {
					b = BATdescriptor(bid);
					bi = bat_iterator(b);
					if (b == NULL)
						b = BBP_desc(bid);
					else
						loaded = 1;
				}
				if (!loaded ||
				    ATOMcmp(b->ttype,
					    BUNtail(bi, b->tnorevsorted - 1),
					    BUNtail(bi, b->tnorevsorted)) >= 0) {
					/* incorrect hint */
					b->batDirtydesc = 1;
					b->tnorevsorted = 0;
				}
			}
		}
		if (loaded)
			BBPunfix(bid);
	}
	GDKdebug = dbg;
}
#endif

#ifdef GDKLIBRARY_OLDWKB
/* "Danger, Will Robinson".
 *
 * Upgrade the Well-known Binary (WKB) from older geom versions to the
 * one in current use.  This function must be called before the SQL
 * Write-ahead Log (WAL) is processed, and in order to be able to
 * recover safely, we call it here.  The WAL may create new BATs with
 * the WKB type, or append values to an existing BAT.  In the first
 * case it is hard, and in the second impossible, to upgrade the BAT
 * later.
 *
 * This function is located here, since it needs to be called early
 * (as discussed), and because it calls functions that are GDK only.
 * There is a little knowledge about the MonetDB WKB type, but nothing
 * about the internals of the type.  The only knowledge is the layout
 * of the old and new structures.
 *
 * All errors are fatal.
 */
static void
fixwkbheap(void)
{
	bat bid, bbpsize = getBBPsize();
	BAT *b;
	int utypewkb = ATOMunknown_find("wkb");
	const char *nme, *bnme;
	char filename[64];
	Heap h1, h2;
	const var_t *restrict old;
	var_t *restrict new;
	BUN i;
	struct old_wkb {
		int len;
		char data[FLEXIBLE_ARRAY_MEMBER];
	} *owkb;
	struct new_wkb {
		int len;
		int srid;
		char data[FLEXIBLE_ARRAY_MEMBER];
	} *nwkb;
	char *oldname, *newname;

	if (utypewkb == 0)
		GDKfatal("fixwkbheap: no space for wkb atom");

	for (bid = 1; bid < bbpsize; bid++) {
		if ((b = BBP_desc(bid)) == NULL)
			continue; /* not a valid BAT */

		if (b->ttype != utypewkb || b->batCount == 0)
			continue; /* nothing to do for this BAT */
		assert(b->tvheap);
		assert(b->twidth == SIZEOF_VAR_T);

		nme = BBP_physical(bid);
		if ((bnme = strrchr(nme, DIR_SEP)) == NULL)
			bnme = nme;
		else
			bnme++;
		snprintf(filename, sizeof(filename), "BACKUP%c%s", DIR_SEP, bnme);
		if ((oldname = GDKfilepath(b->theap.farmid, BATDIR, nme, "tail")) == NULL ||
		    (newname = GDKfilepath(b->theap.farmid, BAKDIR, bnme, "tail")) == NULL ||
		    GDKcreatedir(newname) != GDK_SUCCEED ||
		    rename(oldname, newname) < 0)
			GDKfatal("fixwkbheap: cannot make backup of %s.tail\n", nme);
		GDKfree(oldname);
		GDKfree(newname);
		if ((oldname = GDKfilepath(b->tvheap->farmid, BATDIR, nme, "theap")) == NULL ||
		    (newname = GDKfilepath(b->tvheap->farmid, BAKDIR, bnme, "theap")) == NULL ||
		    rename(oldname, newname) < 0)
			GDKfatal("fixwkbheap: cannot make backup of %s.theap\n", nme);
		GDKfree(oldname);
		GDKfree(newname);

		h1 = b->theap;
		h1.filename = NULL;
		h1.base = NULL;
		h1.dirty = 0;
		h2 = *b->tvheap;
		h2.filename = NULL;
		h2.base = NULL;
		h2.dirty = 0;

		/* load old heaps */
		if (HEAPload(&h1, filename, "tail", 0) != GDK_SUCCEED ||
		    HEAPload(&h2, filename, "theap", 0) != GDK_SUCCEED)
			GDKfatal("fixwkbheap: cannot load old heaps for BAT %d\n", bid);
		/* create new heaps */
		if ((b->theap.filename = GDKfilepath(NOFARM, NULL, nme, "tail")) == NULL ||
		    (b->tvheap->filename = GDKfilepath(NOFARM, NULL, nme, "theap")) == NULL)
			GDKfatal("fixwkbheap: out of memory\n");
		if (HEAPalloc(&b->theap, b->batCapacity, SIZEOF_VAR_T) != GDK_SUCCEED)
			GDKfatal("fixwkbheap: cannot allocate heap\n");
		b->theap.dirty = TRUE;
		b->theap.free = h1.free;
		HEAP_initialize(b->tvheap, b->batCapacity, 0, (int) sizeof(var_t));
		if (b->tvheap->base == NULL)
			GDKfatal("fixwkbheap: cannot allocate heap\n");
		b->tvheap->parentid = bid;

		/* do the conversion */
		b->theap.dirty = TRUE;
		b->tvheap->dirty = TRUE;
		old = (const var_t *) h1.base;
		new = (var_t *) Tloc(b, 0);
		for (i = 0; i < b->batCount; i++) {
			int len;
			owkb = (struct old_wkb *) (h2.base + old[i]);
			if ((len = owkb->len) == ~0)
				len = 0;
			if ((new[i] = HEAP_malloc(b->tvheap, offsetof(struct new_wkb, data) + len)) == 0)
				GDKfatal("fixwkbheap: cannot allocate heap space\n");
			nwkb = (struct new_wkb *) (b->tvheap->base + new[i]);
			nwkb->len = owkb->len;
			nwkb->srid = 0;
			if (len > 0)
				memcpy(nwkb->data, owkb->data, len);
		}
		HEAPfree(&h1, 0);
		HEAPfree(&h2, 0);
		if (HEAPsave(&b->theap, nme, "tail") != GDK_SUCCEED ||
		    HEAPsave(b->tvheap, nme, "theap") != GDK_SUCCEED)
			GDKfatal("fixwkbheap: saving heap failed\n");
		HEAPfree(&b->theap, 0);
		HEAPfree(b->tvheap, 0);
	}
}
#endif

#ifdef GDKLIBRARY_BADEMPTY
/* There was a bug (fixed in changeset 1f5498568a24) which could
 * result in empty strings not being double-eliminated.  This code
 * fixes the affected bats.
 * Note that we only fix BATs whose string heap is still fully double
 * eliminated. */
static inline int
offsearch(const int *restrict offsets, int noffsets, int val)
{
	/* binary search on offsets for val, return whether present */
	int lo = 0, hi = noffsets - 1, mid;

	while (hi > lo) {
		mid = (lo + hi) / 2;
		if (offsets[mid] == val)
			return 1;
		if (offsets[mid] < val)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return offsets[lo] == val;
}

static void
fixstroffheap(BAT *b, int *restrict offsets)
{
	long_str filename;
	Heap h1;		/* old offset heap */
	Heap h2;		/* new string heap */
	Heap h3;		/* new offset heap */
	Heap *h;		/* string heap */
	int noffsets = 0;
	const size_t extralen = b->tvheap->hashash ? EXTRALEN : 0;
	size_t pos;
	var_t emptyoff = 0;
	const char *nme, *bnme;
	char *srcdir;
	BUN i;
	int width;
	int nofix = 1;

	assert(GDK_ELIMDOUBLES(b->tvheap));

	nme = BBP_physical(b->batCacheid);
	srcdir = GDKfilepath(NOFARM, BATDIR, nme, NULL);
	if (srcdir == NULL)
		GDKfatal("fixstroffheap: GDKmalloc failed\n");
	*strrchr(srcdir, DIR_SEP) = 0;

	/* load string heap */
	if (HEAPload(b->tvheap, nme, "theap", 0) != GDK_SUCCEED)
		GDKfatal("fixstroffheap: loading string (theap) heap "
			 "for BAT %d failed\n", b->batCacheid);
	h = b->tvheap;		/* abbreviation */
	/* collect valid offsets */
	pos = GDK_STRHASHSIZE;
	while (pos < h->free) {
		const char *s;
		size_t pad;

		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		s = h->base + pos;
		if (*s == '\0')
			emptyoff = (var_t) pos;
		offsets[noffsets++] = (int) pos; /* < 65536, i.e. fits */
		pos += GDK_STRLEN(s);
	}
	HEAPfree(b->tvheap, 0);

	if ((bnme = strrchr(nme, DIR_SEP)) != NULL)
		bnme++;
	else
		bnme = nme;
	sprintf(filename, "BACKUP%c%s", DIR_SEP, bnme);

	width = b->twidth;
	h2.dirty = 0;
	if (emptyoff == 0) {
		/* no legitimate empty string in the string heap; we
		 * now make a backup of the old string heap and create
		 * a new one to which we add an empty string */
		h2 = *b->tvheap;
		if (GDKmove(h2.farmid, srcdir, bnme, "theap", BAKDIR, bnme, "theap") != GDK_SUCCEED)
			GDKfatal("fixstroffheap: cannot make backup of %s.theap\n", nme);
		h2.filename = GDKfilepath(NOFARM, NULL, nme, "theap");
		if (h2.filename == NULL)
			GDKfatal("fixstroffheap: GDKmalloc failed\n");
		h2.base = NULL;
		if (HEAPalloc(&h2, h2.size, 1) != GDK_SUCCEED)
			GDKfatal("fixstroffheap: allocating new string heap "
				 "for BAT %d failed\n", b->batCacheid);
		h2.cleanhash = b->tvheap->cleanhash;
		h2.hashash = b->tvheap->hashash;
		h2.free = b->tvheap->free;
		/* load old offset heap and copy contents to new heap */
		h1 = *b->tvheap;
		h1.filename = NULL;
		h1.base = NULL;
		h1.dirty = 0;
		if (HEAPload(&h1, filename, "theap", 0) != GDK_SUCCEED)
			GDKfatal("fixstroffheap: loading old tail heap "
				 "for BAT %d failed\n", b->batCacheid);
		memcpy(h2.base, h1.base, h2.free);
		HEAPfree(&h1, 0);
		h2.dirty = 1;
		if ((*BATatoms[TYPE_str].atomPut)(&h2, &emptyoff, "") == 0)
			GDKfatal("fixstroffheap: cannot insert empty string "
				 "in BAT %d failed\n", b->batCacheid);
		/* if the offset of the new empty string doesn't fit
		 * in the offset heap (too many bits for the current
		 * width), we will also make the new offset heap
		 * wider */
		if ((width <= 2 ? emptyoff - GDK_VAROFFSET : emptyoff) >= (var_t) (1 << (width * 8))) {
			width <<= 1;
			assert((width <= 2 ? emptyoff - GDK_VAROFFSET : emptyoff) < (var_t) (1 << (width * 8)));
		}
	}

	/* make backup of offset heap */
	if (GDKmove(b->theap.farmid, srcdir, bnme, "tail", BAKDIR, bnme, "tail") != GDK_SUCCEED)
		GDKfatal("fixstroffheap: cannot make backup of %s.tail\n", nme);
	/* load old offset heap */
	h1 = b->theap;
	h1.filename = NULL;
	h1.base = NULL;
	h1.dirty = 0;
	if (HEAPload(&h1, filename, "tail", 0) != GDK_SUCCEED)
		GDKfatal("fixstroffheap: loading old tail heap "
			 "for BAT %d failed\n", b->batCacheid);

	/* create new offset heap */
	h3 = b->theap;
	h3.filename = GDKfilepath(NOFARM, NULL, nme, "tail");
	if (h3.filename == NULL)
		GDKfatal("fixstroffheap: GDKmalloc failed\n");
	if (HEAPalloc(&h3, b->batCapacity, width) != GDK_SUCCEED)
		GDKfatal("fixstroffheap: allocating new tail heap "
			 "for BAT %d failed\n", b->batCacheid);
	h3.dirty = TRUE;
	h3.free = h1.free;

	switch (b->twidth) {
	case 1:
		for (i = 0; i < b->batCount; i++) {
			pos = (var_t) ((unsigned char *) h1.base)[i] + GDK_VAROFFSET;
			if (!offsearch(offsets, noffsets, (int) pos)) {
				pos = emptyoff;
				nofix = 0;
			}
			if (width == 1)
				((unsigned char *) h3.base)[i] = (unsigned char) (pos - GDK_VAROFFSET);
			else
				((unsigned short *) h3.base)[i] = (unsigned short) (pos - GDK_VAROFFSET);
		}
		break;
	case 2:
		for (i = 0; i < b->batCount; i++) {
			pos = (var_t) ((unsigned short *) h1.base)[i] + GDK_VAROFFSET;
			if (!offsearch(offsets, noffsets, (int) pos)) {
				pos = emptyoff;
				nofix = 0;
			}
			if (width == 2)
				((unsigned short *) h3.base)[i] = (unsigned short) (pos - GDK_VAROFFSET);
			else
				((unsigned int *) h3.base)[i] = (unsigned int) (pos - GDK_VAROFFSET);
		}
		break;
	case 4:
		for (i = 0; i < b->batCount; i++) {
			pos = (var_t) ((unsigned int *) h1.base)[i];
			if (!offsearch(offsets, noffsets, (int) pos)) {
				pos = emptyoff;
				nofix = 0;
			}
			((unsigned int *) h3.base)[i] = (unsigned int) pos;
		}
		break;
#if SIZEOF_VAR_T == 8
	case 8:
		for (i = 0; i < b->batCount; i++) {
			pos = (var_t) ((ulng *) h1.base)[i];
			if (!offsearch(offsets, noffsets, (int) pos)) {
				pos = emptyoff;
				nofix = 0;
			}
			((ulng *) h3.base)[i] = (ulng) pos;
		}
		break;
#endif
	default:
		/* cannot happen */
		assert(0);
	}

	/* cleanup */
	HEAPfree(&h1, 0);
	if (nofix) {
		/* didn't fix anything, move backups back */
		if (h2.dirty) {
			HEAPfree(&h2, 1);
			if (GDKmove(b->tvheap->farmid, BAKDIR, bnme, "theap", srcdir, bnme, "theap") != GDK_SUCCEED)
				GDKfatal("fixstroffheap: cannot restore backup of %s.theap\n", nme);
		}
		HEAPfree(&h3, 1);
		if (GDKmove(b->theap.farmid, BAKDIR, bnme, "tail", srcdir, bnme, "tail") != GDK_SUCCEED)
			GDKfatal("fixstroffheap: cannot restore backup of %s.tail\n", nme);
	} else {
		/* offset heap was fixed */
		b->twidth = width;
		b->batDirtydesc = 1;
		if (h2.dirty) {
			/* in addition, we added an empty string to
			 * the string heap */
			if (HEAPsave(&h2, nme, "theap") != GDK_SUCCEED)
				GDKfatal("fixstroffheap: saving heap failed\n");
			HEAPfree(&h2, 0);
			*b->tvheap = h2;
		}
		if (HEAPsave(&h3, nme, "tail") != GDK_SUCCEED)
			GDKfatal("fixstroffheap: saving heap failed\n");
		HEAPfree(&h3, 0);
		b->theap = h3;
	}
	GDKfree(srcdir);
}

static void
fixstrbats(void)
{
	bat bid;
	BAT *b;
	int *offsets;
	int tt;

	fprintf(stderr,
		"# fixing string offset heaps\n");
	fflush(stderr);

	/* The minimum size a string occupies in the double-eliminated
	 * part of a string heap is SIZEOF_VAR_T (due to padding to
	 * multiples of this value) plus the size of the chain pointer
	 * (another var_t), and if hashes are stored, plus the size of
	 * a hash value (yet another var_t).  In total, 2 or 3 var_t
	 * sizes.  The hash table itself is 1024 times the size of a
	 * var_t.  So on 32 bit architectures, 8000 is plenty, and on
	 * 64 bit architectures, 4000 is plenty.  But we need less if
	 * the heap is not fully occupied.  This results in the
	 * following calculation. */
	offsets = GDKmalloc((GDK_ELIMLIMIT / (2 * SIZEOF_VAR_T)) * sizeof(int));
	if (offsets == NULL)
		GDKfatal("fixstroffheap: cannot allocate memory\n");

	for (bid = 1; bid < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); bid++) {
		if ((b = BBP_desc(bid)) == NULL || b->batCount == 0)
			continue;	/* not a valid BAT, or an empty one */
		if ((tt = b->ttype) < 0) {
			const char *anme;

			/* as yet unknown tail column type */
			anme = ATOMunknown_name(tt);
			/* known string types */
			if (strcmp(anme, "url") == 0 ||
			    strcmp(anme, "json") == 0 ||
			    strcmp(anme, "xml") == 0 ||
			    strcmp(anme, "identifier") == 0)
				tt = TYPE_str;
		}

		if (tt != TYPE_str || !GDK_ELIMDOUBLES(b->tvheap))
			continue;	/* nothing to do for this BAT */

		fixstroffheap(b, offsets);
	}

	GDKfree(offsets);
}
#endif

/*
 * A read only BAT can be shared in a file system by reading its
 * descriptor separately.  The default src=0 is to read the full
 * BBPdir file.
 */
static int
headheapinit(oid *hseq, const char *buf, bat bid)
{
	char type[11];
	unsigned short width;
	unsigned short var;
	unsigned short properties;
	lng nokey0;
	lng nokey1;
	lng nosorted;
	lng norevsorted;
	lng base;
	lng align;
	lng free;
	lng size;
	unsigned short storage;
	int n;

	if (sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (strcmp(type, "void") != 0)
		GDKfatal("BBPinit: head column must be VOID (ID = %d).", (int) bid);
	if (base < 0
#if SIZEOF_OID < SIZEOF_LNG
	    || base >= (lng) oid_nil
#endif
		)
		GDKfatal("BBPinit: head seqbase out of range (ID = %d, seq = "LLFMT").", (int) bid, base);
	*hseq = (oid) base;
	return n;
}

static int
heapinit(BAT *b, const char *buf, int *hashash, const char *HT, int bbpversion, bat bid)
{
	int t;
	char type[11];
	unsigned short width;
	unsigned short var;
	unsigned short properties;
	lng nokey0;
	lng nokey1;
	lng nosorted;
	lng norevsorted;
	lng base;
	lng align;
	lng free;
	lng size;
	unsigned short storage;
	int n;

	(void) bbpversion;	/* could be used to implement compatibility */

	norevsorted = 0; /* default for first case */
	if (bbpversion <= GDKLIBRARY_TALIGN ?
	    sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13 :
		sscanf(buf,
		   " %10s %hu %hu %hu "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &size, &storage,
		   &n) < 12)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (properties & ~0x0F81)
		GDKfatal("BBPinit: unknown properties are set: incompatible database\n");
	*hashash = var & 2;
	var &= ~2;
	/* silently convert chr columns to bte */
	if (strcmp(type, "chr") == 0)
		strcpy(type, "bte");
	/* silently convert wrd columns to int or lng */
	else if (strcmp(type, "wrd") == 0)
		strcpy(type, width == SIZEOF_INT ? "int" : "lng");
#ifdef HAVE_HGE
	else if (strcmp(type, "hge") == 0)
		havehge = 1;
#endif
	if ((t = ATOMindex(type)) < 0) {
		if ((t = ATOMunknown_find(type)) == 0)
			GDKfatal("BBPinit: no space for atom %s", type);
	} else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL))
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.varsized mismatch for BAT %d\n", HT, (int) bid);
	else if (var && t != 0 ?
		 ATOMsize(t) < width ||
		 (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		  && width != 8
#endif
			 ) :
		 ATOMsize(t) != width)
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.size mismatch for BAT %d\n", HT, (int) bid);
	b->ttype = t;
	b->twidth = width;
	b->tvarsized = var != 0;
	b->tshift = ATOMelmshift(width);
	assert_shift_width(b->tshift,b->twidth);
	b->tnokey[0] = (BUN) nokey0;
	b->tnokey[1] = (BUN) nokey1;
	b->tsorted = (bit) ((properties & 0x0001) != 0);
	b->trevsorted = (bit) ((properties & 0x0080) != 0);
	b->tkey = (properties & 0x0100) != 0;
	b->tdense = (properties & 0x0200) != 0;
	b->tnonil = (properties & 0x0400) != 0;
	b->tnil = (properties & 0x0800) != 0;
	b->tnosorted = (BUN) nosorted;
	b->tnorevsorted = (BUN) norevsorted;
	b->tseqbase = base < 0 ? oid_nil : (oid) base;
	b->theap.free = (size_t) free;
	b->theap.size = (size_t) size;
	b->theap.base = NULL;
	b->theap.filename = NULL;
	b->theap.storage = (storage_t) storage;
	b->theap.copied = 0;
	b->theap.newstorage = (storage_t) storage;
	b->theap.farmid = BBPselectfarm(PERSISTENT, b->ttype, offheap);
	b->theap.dirty = 0;
	if (b->theap.free > b->theap.size)
		GDKfatal("BBPinit: \"free\" value larger than \"size\" in heap of bat %d\n", (int) bid);
	return n;
}

static int
vheapinit(BAT *b, const char *buf, int hashash, bat bid)
{
	int n = 0;
	lng free, size;
	unsigned short storage;

	if (b->tvarsized && b->ttype != TYPE_void) {
		b->tvheap = GDKzalloc(sizeof(Heap));
		if (b->tvheap == NULL)
			GDKfatal("BBPinit: cannot allocate memory for heap.");
		if (sscanf(buf,
			   " "LLFMT" "LLFMT" %hu"
			   "%n",
			   &free, &size, &storage, &n) < 3)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		b->tvheap->free = (size_t) free;
		b->tvheap->size = (size_t) size;
		b->tvheap->base = NULL;
		b->tvheap->filename = NULL;
		b->tvheap->storage = (storage_t) storage;
		b->tvheap->copied = 0;
		b->tvheap->hashash = hashash != 0;
		b->tvheap->cleanhash = 1;
		b->tvheap->newstorage = (storage_t) storage;
		b->tvheap->dirty = 0;
		b->tvheap->parentid = bid;
		b->tvheap->farmid = BBPselectfarm(PERSISTENT, b->ttype, varheap);
		if (b->tvheap->free > b->tvheap->size)
			GDKfatal("BBPinit: \"free\" value larger than \"size\" in var heap of bat %d\n", (int) bid);
	}
	return n;
}

static void
BBPreadEntries(FILE *fp, int bbpversion)
{
	bat bid = 0;
	char buf[4096];
	BAT *bn;

	/* read the BBP.dir and insert the BATs into the BBP */
	while (fgets(buf, sizeof(buf), fp) != NULL) {
		lng batid;
		unsigned short status;
		char headname[129];
		char filename[129];
		unsigned int properties;
		int lastused;
		int nread;
		char *s, *options = NULL;
		char logical[1024];
		lng inserted = 0, deleted = 0, first = 0, count, capacity, base = 0;
#ifdef GDKLIBRARY_HEADED
		/* these variables are not used in later versions */
		char tailname[129];
		unsigned short map_head = 0, map_tail = 0, map_hheap = 0, map_theap = 0;
#endif
		int Thashash;

		if ((s = strchr(buf, '\r')) != NULL) {
			/* convert \r\n into just \n */
			if (s[1] != '\n')
				GDKfatal("BBPinit: invalid format for BBP.dir");
			*s++ = '\n';
			*s = 0;
		}

		if (bbpversion <= GDKLIBRARY_INSERTED ?
		    sscanf(buf,
			   LLFMT" %hu %128s %128s %128s %d %u "LLFMT" "LLFMT" "LLFMT" "LLFMT" "LLFMT" %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &inserted, &deleted, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 16 :
		    bbpversion <= GDKLIBRARY_HEADED ?
		    sscanf(buf,
			   LLFMT" %hu %128s %128s %128s %d %u "LLFMT" "LLFMT" "LLFMT" %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 14 :
		    sscanf(buf,
			   LLFMT" %hu %128s %128s %u "LLFMT" "LLFMT" "LLFMT
			   "%n",
			   &batid, &status, headname, filename,
			   &properties,
			   &count, &capacity, &base,
			   &nread) < 8)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

		/* convert both / and \ path separators to our own DIR_SEP */
#if DIR_SEP != '/'
		s = filename;
		while ((s = strchr(s, '/')) != NULL)
			*s++ = DIR_SEP;
#endif
#if DIR_SEP != '\\'
		s = filename;
		while ((s = strchr(s, '\\')) != NULL)
			*s++ = DIR_SEP;
#endif

		if (first != 0)
			GDKfatal("BBPinit: first != 0 (ID = "LLFMT").", batid);

		bid = (bat) batid;
		if (batid >= (lng) ATOMIC_GET(BBPsize, BBPsizeLock)) {
			ATOMIC_SET(BBPsize, (ATOMIC_TYPE) (batid + 1), BBPsizeLock);
			if ((bat) ATOMIC_GET(BBPsize, BBPsizeLock) >= BBPlimit)
				BBPextend(0, FALSE);
		}
		if (BBP_desc(bid) != NULL)
			GDKfatal("BBPinit: duplicate entry in BBP.dir (ID = "LLFMT").", batid);
		bn = GDKzalloc(sizeof(BAT));
		if (bn == NULL)
			GDKfatal("BBPinit: cannot allocate memory for BAT.");
		bn->batCacheid = bid;
		BATroles(bn, NULL);
		bn->batPersistence = PERSISTENT;
		bn->batCopiedtodisk = 1;
		bn->batRestricted = (properties & 0x06) >> 1;
		bn->batCount = (BUN) count;
		bn->batInserted = bn->batCount;
		bn->batCapacity = (BUN) capacity;

		if (bbpversion <= GDKLIBRARY_HEADED) {
			nread += headheapinit(&bn->hseqbase, buf + nread, bid);
		} else {
			if (base < 0
#if SIZEOF_OID < SIZEOF_LNG
			    || base >= (lng) oid_nil
#endif
				)
				GDKfatal("BBPinit: head seqbase out of range (ID = "LLFMT", seq = "LLFMT").", batid, base);
			bn->hseqbase = (oid) base;
		}
		nread += heapinit(bn, buf + nread, &Thashash, "T", bbpversion, bid);
		nread += vheapinit(bn, buf + nread, Thashash, bid);

		if (bbpversion <= GDKLIBRARY_NOKEY &&
		    (bn->tnokey[0] != 0 || bn->tnokey[1] != 0)) {
			/* we don't trust the nokey values */
			bn->tnokey[0] = bn->tnokey[1] = 0;
			bn->batDirtydesc = 1;
		}

		if (buf[nread] != '\n' && buf[nread] != ' ')
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		if (buf[nread] == ' ')
			options = buf + nread + 1;

		BBP_desc(bid) = bn;
		BBP_status(bid) = BBPEXISTING;	/* do we need other status bits? */
		if ((s = strchr(headname, '~')) != NULL && s == headname) {
			s = BBPtmpname(logical, sizeof(logical), bid);
		} else {
			if (s)
				*s = 0;
			strncpy(logical, headname, sizeof(logical));
			s = logical;
		}
		BBP_logical(bid) = GDKstrdup(s);
		/* tailname is ignored */
		BBP_physical(bid) = GDKstrdup(filename);
		BBP_options(bid) = NULL;
		if (options)
			BBP_options(bid) = GDKstrdup(options);
		BBP_refs(bid) = 0;
		BBP_lrefs(bid) = 1;	/* any BAT we encounter here is persistent, so has a logical reference */
	}
}

#ifdef HAVE_HGE
#define SIZEOF_MAX_INT SIZEOF_HGE
#else
#define SIZEOF_MAX_INT SIZEOF_LNG
#endif

static int
BBPheader(FILE *fp)
{
	char buf[BUFSIZ];
	int sz, bbpversion, ptrsize, oidsize, intsize;
	char *s;

	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: BBP.dir is empty");
	}
	if (sscanf(buf, "BBP.dir, GDKversion %d\n", &bbpversion) != 1) {
		GDKerror("BBPinit: old BBP without version number");
		GDKerror("dump the database using a compatible version,");
		GDKerror("then restore into new database using this version.\n");
		exit(1);
	}
	if (bbpversion != GDKLIBRARY &&
	    bbpversion != GDKLIBRARY_BADEMPTY &&
	    bbpversion != GDKLIBRARY_NOKEY &&
	    bbpversion != GDKLIBRARY_SORTEDPOS &&
	    bbpversion != GDKLIBRARY_OLDWKB &&
	    bbpversion != GDKLIBRARY_INSERTED &&
	    bbpversion != GDKLIBRARY_HEADED &&
	    bbpversion != GDKLIBRARY_TALIGN) {
		GDKfatal("BBPinit: incompatible BBP version: expected 0%o, got 0%o.\n"
			 "This database was probably created by %s version of MonetDB.",
			 GDKLIBRARY, bbpversion,
			 bbpversion > GDKLIBRARY ? "a newer" : "a too old");
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	if (sscanf(buf, "%d %d %d", &ptrsize, &oidsize, &intsize) != 3) {
		GDKfatal("BBPinit: BBP.dir has incompatible format: pointer, OID, and max. integer sizes are missing");
	}
	if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_OID) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected pointer size %d, got %d, expected OID size %d, got %d.",
			 SIZEOF_SIZE_T, ptrsize, SIZEOF_OID, oidsize);
	}
	if (intsize > SIZEOF_MAX_INT) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected max. integer size %d, got %d.",
			 SIZEOF_MAX_INT, intsize);
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	/* when removing GDKLIBRARY_TALIGN, also remove the strstr
	 * call and just sscanf from buf */
	if ((s = strstr(buf, "BBPsize")) != NULL) {
		sscanf(s, "BBPsize=%d", &sz);
		sz = (int) (sz * BATMARGIN);
		if (sz > (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
			ATOMIC_SET(BBPsize, sz, BBPsizeLock);
	}
	return bbpversion;
}

/* all errors are fatal */
void
BBPaddfarm(const char *dirname, int rolemask)
{
	struct stat st;
	int i;

	if (strchr(dirname, '\n') != NULL) {
		GDKfatal("BBPaddfarm: no newline allowed in directory name\n");
	}
	if (rolemask == 0 || (rolemask & 1 && BBPfarms[0].dirname != NULL)) {
		GDKfatal("BBPaddfarm: bad rolemask\n");
	}
	if (mkdir(dirname, 0755) < 0) {
		if (errno == EEXIST) {
			if (stat(dirname, &st) == -1 || !S_ISDIR(st.st_mode)) {
				GDKfatal("BBPaddfarm: %s: not a directory\n", dirname);
			}
		} else {
			GDKfatal("BBPaddfarm: %s: cannot create directory\n", dirname);
		}
	}
	for (i = 0; i < MAXFARMS; i++) {
		if (BBPfarms[i].dirname == NULL) {
			BBPfarms[i].dirname = GDKstrdup(dirname);
			BBPfarms[i].roles = rolemask;
			if ((rolemask & 1) == 0) {
				char *bbpdir;
				int j;

				for (j = 0; j < i; j++)
					if (strcmp(BBPfarms[i].dirname,
						   BBPfarms[j].dirname) == 0)
						return;
				/* if an extra farm, make sure we
				 * don't find a BBP.dir there that
				 * might belong to an existing
				 * database */
				bbpdir = GDKfilepath(i, BATDIR, "BBP", "dir");
				if (bbpdir == NULL)
					GDKfatal("BBPaddfarm: malloc failed\n");
				if (stat(bbpdir, &st) != -1 || errno != ENOENT)
					GDKfatal("BBPaddfarm: %s is a database\n", dirname);
				GDKfree(bbpdir);
				bbpdir = GDKfilepath(i, BAKDIR, "BBP", "dir");
				if (bbpdir == NULL)
					GDKfatal("BBPaddfarm: malloc failed\n");
				if (stat(bbpdir, &st) != -1 || errno != ENOENT)
					GDKfatal("BBPaddfarm: %s is a database\n", dirname);
				GDKfree(bbpdir);
			}
			return;
		}
	}
	GDKfatal("BBPaddfarm: too many farms\n");
}

void
BBPresetfarms(void)
{
	BBPexit();
	BBPunlock();
	BBPsize = 0;
	if (BBPfarms[0].dirname != NULL) {
		GDKfree((void*) BBPfarms[0].dirname);
	}
	BBPfarms[0].dirname = NULL;
	BBPfarms[0].roles = 0;
}


void
BBPinit(void)
{
	FILE *fp = NULL;
	struct stat st;
	int bbpversion;
	str bbpdirstr = GDKfilepath(0, BATDIR, "BBP", "dir");
	str backupbbpdirstr = GDKfilepath(0, BAKDIR, "BBP", "dir");
	int i;

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&GDKunloadLock, "GDKunloadLock");
	ATOMIC_INIT(BBPsizeLock);
#endif

	if (BBPfarms[0].dirname == NULL) {
		BBPaddfarm(".", 1 << PERSISTENT);
		BBPaddfarm(".", 1 << TRANSIENT);
	}

	if (GDKremovedir(0, DELDIR) != GDK_SUCCEED)
		GDKfatal("BBPinit: cannot remove directory %s\n", DELDIR);

	/* first move everything from SUBDIR to BAKDIR (its parent) */
	if (BBPrecover_subdir() != GDK_SUCCEED)
		GDKfatal("BBPinit: cannot properly recover_subdir process %s. Please check whether your disk is full or write-protected", SUBDIR);

	/* try to obtain a BBP.dir from bakdir */
	if (stat(backupbbpdirstr, &st) == 0) {
		/* backup exists; *must* use it */
		if (recover_dir(0, stat(bbpdirstr, &st) == 0) != GDK_SUCCEED)
			goto bailout;
		if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL)
			GDKfatal("BBPinit: cannot open recovered BBP.dir.");
	} else if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
		/* there was no BBP.dir either. Panic! try to use a
		 * BBP.bak */
		if (stat(backupbbpdirstr, &st) < 0) {
			/* no BBP.bak (nor BBP.dir or BACKUP/BBP.dir):
			 * create a new one */
			IODEBUG fprintf(stderr, "#BBPdir: initializing BBP.\n");	/* BBPdir instead of BBPinit for backward compatibility of error messages */
			if (BBPdir(0, NULL) != GDK_SUCCEED)
				goto bailout;
		} else if (GDKmove(0, BATDIR, "BBP", "bak", BATDIR, "BBP", "dir") == GDK_SUCCEED)
			IODEBUG fprintf(stderr, "#BBPinit: reverting to dir saved in BBP.bak.\n");

		if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL)
			goto bailout;
	}
	assert(fp != NULL);

	/* scan the BBP.dir to obtain current size */
	BBPlimit = 0;
	memset(BBP, 0, sizeof(BBP));
	ATOMIC_SET(BBPsize, 1, BBPsizeLock);
	BBPdirty(1);

	bbpversion = BBPheader(fp);

	BBPextend(0, FALSE);		/* allocate BBP records */
	ATOMIC_SET(BBPsize, 1, BBPsizeLock);

	BBPreadEntries(fp, bbpversion);
	fclose(fp);

	if (BBPinithash(0) != GDK_SUCCEED)
		GDKfatal("BBPinit: BBPinithash failed");

	/* will call BBPrecover if needed */
	if (BBPprepare(FALSE) != GDK_SUCCEED)
		GDKfatal("BBPinit: cannot properly prepare process %s. Please check whether your disk is full or write-protected", BAKDIR);

	/* cleanup any leftovers (must be done after BBPrecover) */
	for (i = 0; i < MAXFARMS && BBPfarms[i].dirname != NULL; i++) {
		int j;
		for (j = 0; j < i; j++) {
			/* don't clean a directory twice */
			if (BBPfarms[j].dirname &&
			    strcmp(BBPfarms[i].dirname,
				   BBPfarms[j].dirname) == 0)
				break;
		}
		if (j == i) {
			char *d = GDKfilepath(i, NULL, BATDIR, NULL);
			if (d == NULL)
				GDKfatal("BBPinit: malloc failed\n");
			BBPdiskscan(d, strlen(d) - strlen(BATDIR));
			GDKfree(d);
		}
	}

#ifdef GDKLIBRARY_SORTEDPOS
	if (bbpversion <= GDKLIBRARY_SORTEDPOS)
		fixsorted();
#endif
#ifdef GDKLIBRARY_OLDWKB
	if (bbpversion <= GDKLIBRARY_OLDWKB)
		fixwkbheap();
#endif
#ifdef GDKLIBRARY_BADEMPTY
	if (bbpversion <= GDKLIBRARY_BADEMPTY)
		fixstrbats();
#endif
	if (bbpversion < GDKLIBRARY)
		TMcommit();
	GDKfree(bbpdirstr);
	GDKfree(backupbbpdirstr);
	return;

      bailout:
	/* now it is time for real panic */
	GDKfatal("BBPinit: could not write %s%cBBP.dir. Please check whether your disk is full or write-protected", BATDIR, DIR_SEP);
}

/*
 * During the exit phase all non-persistent BATs are removed.  Upon
 * exit the status of the BBP tables is saved on disk.  This function
 * is called once and during the shutdown of the server. Since
 * shutdown may be issued from any thread (dangerous) it may lead to
 * interference in a parallel session.
 */

static int backup_files = 0, backup_dir = 0, backup_subdir = 0;

void
BBPexit(void)
{
	bat i;
	int skipped;

	BBPlock();	/* stop all threads ever touching more descriptors */

	/* free all memory (just for leak-checking in Purify) */
	do {
		skipped = 0;
		for (i = 0; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
			if (BBPvalid(i)) {
				BAT *b = BBP_desc(i);

				if (b) {
					if (b->batSharecnt > 0) {
						skipped = 1;
						continue;
					}
					if (isVIEW(b)) {
						/* "manually"
						 * decrement parent
						 * references, since
						 * VIEWdestroy doesn't
						 * (and can't here due
						 * to locks) do it */
						bat tp = VIEWtparent(b);
						bat vtp = VIEWvtparent(b);
						if (tp) {
							BBP_desc(tp)->batSharecnt--;
							--BBP_lrefs(tp);
						}
						if (vtp) {
							BBP_desc(vtp)->batSharecnt--;
							--BBP_lrefs(vtp);
						}
						VIEWdestroy(b);
					} else {
						BATfree(b);
					}
				}
				BBPuncacheit(i, TRUE);
				if (BBP_logical(i) != BBP_bak(i))
					GDKfree(BBP_bak(i));
				BBP_bak(i) = NULL;
				GDKfree(BBP_logical(i));
				BBP_logical(i) = NULL;
			}
			if (BBP_physical(i)) {
				GDKfree(BBP_physical(i));
				BBP_physical(i) = NULL;
			}
			if (BBP_bak(i))
				GDKfree(BBP_bak(i));
			BBP_bak(i) = NULL;
		}
	} while (skipped);
	GDKfree(BBP_hash);
	BBP_hash = 0;
	// these need to be NULL, otherwise no new ones get created
	backup_files = 0;
	backup_dir = 0;
	backup_subdir = 0;

}

/*
 * The routine BBPdir creates the BAT pool dictionary file.  It
 * includes some information about the current state of affair in the
 * pool.  The location in the buffer pool is saved for later use as
 * well.  This is merely done for ease of debugging and of no
 * importance to front-ends.  The tail of non-used entries is
 * reclaimed as well.
 */
static inline int
heap_entry(FILE *fp, BAT *b)
{
	return fprintf(fp, " %s %d %d %d " BUNFMT " " BUNFMT " " BUNFMT " "
		       BUNFMT " " OIDFMT " " SZFMT " " SZFMT " %d",
		       b->ttype >= 0 ? BATatoms[b->ttype].name : ATOMunknown_name(b->ttype),
		       b->twidth,
		       b->tvarsized | (b->tvheap ? b->tvheap->hashash << 1 : 0),
		       (unsigned short) b->tsorted |
			   ((unsigned short) b->trevsorted << 7) |
			   (((unsigned short) b->tkey & 0x01) << 8) |
			   ((unsigned short) b->tdense << 9) |
			   ((unsigned short) b->tnonil << 10) |
			   ((unsigned short) b->tnil << 11),
		       b->tnokey[0],
		       b->tnokey[1],
		       b->tnosorted,
		       b->tnorevsorted,
		       b->tseqbase,
		       b->theap.free,
		       b->theap.size,
		       (int) b->theap.newstorage);
}

static inline int
vheap_entry(FILE *fp, Heap *h)
{
	if (h == NULL)
		return 0;
	return fprintf(fp, " " SZFMT " " SZFMT " %d",
		       h->free, h->size, (int) h->newstorage);
}

static gdk_return
new_bbpentry(FILE *fp, bat i, const char *prefix)
{
#ifndef NDEBUG
	assert(i > 0);
	assert(i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock));
	assert(BBP_desc(i));
	assert(BBP_desc(i)->batCacheid == i);
	assert(BBP_desc(i)->batRole == PERSISTENT);
	assert(0 <= BBP_desc(i)->theap.farmid && BBP_desc(i)->theap.farmid < MAXFARMS);
	assert(BBPfarms[BBP_desc(i)->theap.farmid].roles & (1 << PERSISTENT));
	if (BBP_desc(i)->tvheap) {
		assert(0 <= BBP_desc(i)->tvheap->farmid && BBP_desc(i)->tvheap->farmid < MAXFARMS);
		assert(BBPfarms[BBP_desc(i)->tvheap->farmid].roles & (1 << PERSISTENT));
	}
#endif

	if (fprintf(fp, "%s" SSZFMT " %d %s %s %d " BUNFMT " "
		    BUNFMT " " OIDFMT, prefix,
		    /* BAT info */
		    (ssize_t) i,
		    BBP_status(i) & BBPPERSISTENT,
		    BBP_logical(i),
		    BBP_physical(i),
		    BBP_desc(i)->batRestricted << 1,
		    BBP_desc(i)->batCount,
		    BBP_desc(i)->batCapacity,
		    BBP_desc(i)->hseqbase) < 0 ||
	    heap_entry(fp, BBP_desc(i)) < 0 ||
	    vheap_entry(fp, BBP_desc(i)->tvheap) < 0 ||
	    (BBP_options(i) &&
	     fprintf(fp, " %s", BBP_options(i)) < 0) ||
	    fprintf(fp, "\n") < 0) {
		GDKsyserror("new_bbpentry: Writing BBP.dir entry failed\n");
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

static gdk_return
BBPdir_header(FILE *f, int n)
{
	if (fprintf(f, "BBP.dir, GDKversion %d\n%d %d %d\nBBPsize=%d\n",
		    GDKLIBRARY, SIZEOF_SIZE_T, SIZEOF_OID,
#ifdef HAVE_HGE
		    havehge ? SIZEOF_HGE :
#endif
		    SIZEOF_LNG, n) < 0 ||
	    ferror(f)) {
		GDKsyserror("BBPdir_header: Writing BBP.dir header failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static gdk_return
BBPdir_subcommit(int cnt, bat *subcommit)
{
	FILE *obbpf, *nbbpf;
	bat j = 1;
	char buf[3000];
	int n;

#ifndef NDEBUG
	assert(subcommit != NULL);
	for (n = 2; n < cnt; n++)
		assert(subcommit[n - 1] < subcommit[n]);
#endif

	if ((nbbpf = GDKfilelocate(0, "BBP", "w", "dir")) == NULL)
		return GDK_FAIL;

	n = (bat) ATOMIC_GET(BBPsize, BBPsizeLock);

	/* we need to copy the backup BBP.dir to the new, but
	 * replacing the entries for the subcommitted bats */
	if ((obbpf = GDKfileopen(0, SUBDIR, "BBP", "dir", "r")) == NULL) {
		if ((obbpf = GDKfileopen(0, BAKDIR, "BBP", "dir", "r")) == NULL)
			GDKfatal("BBPdir: subcommit attempted without backup BBP.dir.");
	}
	/* read first three lines */
	if (fgets(buf, sizeof(buf), obbpf) == NULL || /* BBP.dir, GDKversion %d */
	    fgets(buf, sizeof(buf), obbpf) == NULL || /* SIZEOF_SIZE_T SIZEOF_OID SIZEOF_MAX_INT */
	    fgets(buf, sizeof(buf), obbpf) == NULL) /* BBPsize=%d */
		GDKfatal("BBPdir: subcommit attempted with invalid backup BBP.dir.");
	/* third line contains BBPsize */
	sscanf(buf, "BBPsize=%d", &n);
	if (n < (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
		n = (bat) ATOMIC_GET(BBPsize, BBPsizeLock);

	if (GDKdebug & (IOMASK | THRDMASK))
		fprintf(stderr, "#BBPdir: writing BBP.dir (%d bats).\n", n);

	if (BBPdir_header(nbbpf, n) != GDK_SUCCEED) {
		goto bailout;
	}
	n = 0;
	for (;;) {
		/* but for subcommits, all except the bats in the list
		 * retain their existing mode */
		if (n == 0 && obbpf != NULL) {
			if (fgets(buf, sizeof(buf), obbpf) == NULL) {
				fclose(obbpf);
				obbpf = NULL;
			} else if (sscanf(buf, "%d", &n) != 1 || n <= 0)
				GDKfatal("BBPdir: subcommit attempted with invalid backup BBP.dir.");
			/* at this point, obbpf == NULL, or n > 0 */
		}
		if (j == cnt && n == 0) {
			assert(obbpf == NULL);
			break;
		}
		if (j < cnt && (n == 0 || subcommit[j] <= n || obbpf == NULL)) {
			bat i = subcommit[j];
			/* BBP.dir consists of all persistent bats only */
			if (BBP_status(i) & BBPPERSISTENT) {
				if (new_bbpentry(nbbpf, i, "") != GDK_SUCCEED) {
					goto bailout;
				}
				IODEBUG new_bbpentry(stderr, i, "#");
			}
			if (i == n)
				n = 0;	/* read new entry (i.e. skip this one from old BBP.dir */
			do
				/* go to next, skipping duplicates */
				j++;
			while (j < cnt && subcommit[j] == i);
		} else {
			if (fprintf(nbbpf, "%s", buf) < 0) {
				GDKsyserror("BBPdir_subcommit: Copying BBP.dir entry failed\n");
				goto bailout;
			}
			IODEBUG fprintf(stderr, "#%s", buf);
			n = 0;
		}
	}

	if (fflush(nbbpf) == EOF ||
	    (!(GDKdebug & FORCEMITOMASK) &&
#ifdef NATIVE_WIN32
	     _commit(_fileno(nbbpf)) < 0
#else
#ifdef HAVE_FDATASYNC
	     fdatasync(fileno(nbbpf)) < 0
#else
#ifdef HAVE_FSYNC
	     fsync(fileno(nbbpf)) < 0
#endif
#endif
#endif
		    )) {
		GDKsyserror("BBPdir_subcommit: Syncing BBP.dir file failed\n");
		goto bailout;
	}
	if (fclose(nbbpf) == EOF) {
		GDKsyserror("BBPdir_subcommit: Closing BBP.dir file failed\n");
		goto bailout;
	}

	IODEBUG fprintf(stderr, "#BBPdir end\n");

	return GDK_SUCCEED;

      bailout:
	if (obbpf != NULL)
		fclose(obbpf);
	if (nbbpf != NULL)
		fclose(nbbpf);
	return GDK_FAIL;
}

gdk_return
BBPdir(int cnt, bat *subcommit)
{
	FILE *fp;
	bat i;

	if (subcommit)
		return BBPdir_subcommit(cnt, subcommit);

	if (GDKdebug & (IOMASK | THRDMASK))
		fprintf(stderr, "#BBPdir: writing BBP.dir (%d bats).\n", (int) (bat) ATOMIC_GET(BBPsize, BBPsizeLock));
	if ((fp = GDKfilelocate(0, "BBP", "w", "dir")) == NULL) {
		goto bailout;
	}

	if (BBPdir_header(fp, (bat) ATOMIC_GET(BBPsize, BBPsizeLock)) != GDK_SUCCEED) {
		goto bailout;
	}

	for (i = 1; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
		/* write the entry
		 * BBP.dir consists of all persistent bats */
		if (BBP_status(i) & BBPPERSISTENT) {
			if (new_bbpentry(fp, i, "") != GDK_SUCCEED) {
				goto bailout;
			}
			IODEBUG new_bbpentry(stderr, i, "#");
		}
	}

	if (fflush(fp) == EOF ||
#ifdef NATIVE_WIN32
	    _commit(_fileno(fp)) < 0
#else
#ifdef HAVE_FDATASYNC
	    fdatasync(fileno(fp)) < 0
#else
#ifdef HAVE_FSYNC
	    fsync(fileno(fp)) < 0
#endif
#endif
#endif
		) {
		GDKsyserror("BBPdir: Syncing BBP.dir file failed\n");
		goto bailout;
	}
	if (fclose(fp) == EOF) {
		GDKsyserror("BBPdir: Closing BBP.dir file failed\n");
		return GDK_FAIL;
	}

	IODEBUG fprintf(stderr, "#BBPdir end\n");

	if (i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
		return GDK_FAIL;

	return GDK_SUCCEED;

      bailout:
	if (fp != NULL)
		fclose(fp);
	return GDK_FAIL;
}

/* function used for debugging */
void
BBPdump(void)
{
	bat i;
	size_t mem = 0, vm = 0;
	size_t cmem = 0, cvm = 0;
	int n = 0, nc = 0;

	for (i = 0; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
		BAT *b = BBP_cache(i);
		if (b == NULL)
			continue;
		fprintf(stderr,
			"# %d[%s]: nme='%s' refs=%d lrefs=%d "
			"status=%d count=" BUNFMT,
			i,
			ATOMname(b->ttype),
			BBP_logical(i) ? BBP_logical(i) : "<NULL>",
			BBP_refs(i),
			BBP_lrefs(i),
			BBP_status(i),
			b->batCount);
		if (b->batSharecnt > 0)
			fprintf(stderr, " shares=%d", b->batSharecnt);
		if (b->batDirty)
			fprintf(stderr, " Dirty");
		if (b->batDirtydesc)
			fprintf(stderr, " DirtyDesc");
		if (b->theap.parentid) {
			fprintf(stderr, " Theap -> %d", b->theap.parentid);
		} else {
			fprintf(stderr,
				" Theap=[" SZFMT "," SZFMT "]%s",
				HEAPmemsize(&b->theap),
				HEAPvmsize(&b->theap),
				b->theap.dirty ? "(Dirty)" : "");
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(&b->theap);
				cvm += HEAPvmsize(&b->theap);
				nc++;
			} else {
				mem += HEAPmemsize(&b->theap);
				vm += HEAPvmsize(&b->theap);
				n++;
			}
		}
		if (b->tvheap) {
			if (b->tvheap->parentid != b->batCacheid) {
				fprintf(stderr,
					" Tvheap -> %d",
					b->tvheap->parentid);
			} else {
				fprintf(stderr,
					" Tvheap=[" SZFMT "," SZFMT "]%s",
					HEAPmemsize(b->tvheap),
					HEAPvmsize(b->tvheap),
				b->tvheap->dirty ? "(Dirty)" : "");
				if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
					cmem += HEAPmemsize(b->tvheap);
					cvm += HEAPvmsize(b->tvheap);
				} else {
					mem += HEAPmemsize(b->tvheap);
					vm += HEAPvmsize(b->tvheap);
				}
			}
		}
		if (b->thash && b->thash != (Hash *) -1) {
			fprintf(stderr,
				" Thash=[" SZFMT "," SZFMT "]",
				HEAPmemsize(b->thash->heap),
				HEAPvmsize(b->thash->heap));
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(b->thash->heap);
				cvm += HEAPvmsize(b->thash->heap);
			} else {
				mem += HEAPmemsize(b->thash->heap);
				vm += HEAPvmsize(b->thash->heap);
			}
		}
		fprintf(stderr, " role: %s, persistence: %s\n",
			b->batRole == PERSISTENT ? "persistent" : "transient",
			b->batPersistence == PERSISTENT ? "persistent" : "transient");
	}
	fprintf(stderr,
		"# %d bats: mem=" SZFMT ", vm=" SZFMT " %d cached bats: mem=" SZFMT ", vm=" SZFMT "\n",
		n, mem, vm, nc, cmem, cvm);
	fflush(stderr);
}

/*
 * @+ BBP Readonly Interface
 *
 * These interface functions do not change the BBP tables. If they
 * only access one specific BAT, the caller must have ensured that no
 * other thread is modifying that BAT, therefore such functions do not
 * need locking.
 *
 * BBP index lookup by BAT name:
 */
static inline bat
BBP_find(const char *nme, int lock)
{
	bat i = BBPnamecheck(nme);

	if (i != 0) {
		/* for tmp_X BATs, we already know X */
		const char *s;

		if (i >= (bat) ATOMIC_GET(BBPsize, BBPsizeLock) || (s = BBP_logical(i)) == NULL || strcmp(s, nme)) {
			i = 0;
		}
	} else if (*nme != '.') {
		/* must lock since hash-lookup traverses other BATs */
		if (lock)
			MT_lock_set(&GDKnameLock);
		for (i = BBP_hash[strHash(nme) & BBP_mask]; i; i = BBP_next(i)) {
			if (strcmp(BBP_logical(i), nme) == 0)
				break;
		}
		if (lock)
			MT_lock_unset(&GDKnameLock);
	}
	return i;
}

bat
BBPindex(const char *nme)
{
	return BBP_find(nme, TRUE);
}

BAT *
BBPgetdesc(bat i)
{
	if (i == bat_nil)
		return NULL;
	if (i < 0)
		i = -i;
	if (i != 0 && i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock) && i && BBP_logical(i)) {
		return BBP_desc(i);
	}
	return NULL;
}

str
BBPlogical(bat bid, str buf)
{
	if (buf == NULL) {
		return NULL;
	} else if (BBPcheck(bid, "BBPlogical")) {
		strcpy(buf, BBP_logical(bid));
	} else {
		*buf = 0;
	}
	return buf;
}

str
BBPphysical(bat bid, str buf)
{
	if (buf == NULL) {
		return NULL;
	} else if (BBPcheck(bid, "BBPphysical")) {
		strcpy(buf, BBP_physical(bid));
	} else {
		*buf = 0;
	}
	return buf;
}

/*
 * @+ BBP Update Interface
 * Operations to insert, delete, clear, and modify BBP entries.
 * Our policy for the BBP is to provide unlocked BBP access for
 * speed, but still write operations have to be locked.
 * #ifdef DEBUG_THREADLOCAL_BATS
 * Create the shadow version (reversed) of a bat.
 *
 * An existing BAT is inserted into the BBP
 */
static inline str
BBPsubdir_recursive(str s, bat i)
{
	i >>= 6;
	if (i >= 0100) {
		s = BBPsubdir_recursive(s, i);
		*s++ = DIR_SEP;
	}
	i &= 077;
	*s++ = '0' + (i >> 3);
	*s++ = '0' + (i & 7);
	return s;
}

static inline void
BBPgetsubdir(str s, bat i)
{
	if (i >= 0100) {
		s = BBPsubdir_recursive(s, i);
	}
	*s = 0;
}

/* There are BBP_THREADMASK+1 (64) free lists, and ours (idx) is
 * empty.  Here we find a longish free list (at least 20 entries), and
 * if we can find one, we take one entry from that list.  If no long
 * enough list can be found, we create a new entry by either just
 * increasing BBPsize (up to BBPlimit) or extending the BBP (which
 * increases BBPlimit).  Every time this function is called we start
 * searching in a following free list (variable "last"). */
static gdk_return
maybeextend(int idx)
{
	int t, m;
	int n, l;
	bat i;
	static int last = 0;

	l = 0;			/* length of longest list */
	m = 0;			/* index of longest list */
	/* find a longish free list */
	for (t = 0; t <= BBP_THREADMASK && l <= 20; t++) {
		n = 0;
		for (i = BBP_free((t + last) & BBP_THREADMASK);
		     i != 0 && n <= 20;
		     i = BBP_next(i))
			n++;
		if (n > l) {
			m = (t + last) & BBP_THREADMASK;
			l = n;
		}
	}
	if (l > 20) {
		/* list is long enough, get an entry from there */
		i = BBP_free(m);
		BBP_free(m) = BBP_next(i);
		BBP_next(i) = 0;
		BBP_free(idx) = i;
	} else {
		/* let the longest list alone, get a fresh entry */
		if ((bat) ATOMIC_ADD(BBPsize, 1, BBPsizeLock) >= BBPlimit) {
			if (BBPextend(idx, TRUE) != GDK_SUCCEED) {
				/* undo add */
				ATOMIC_SUB(BBPsize, 1, BBPsizeLock);
				/* couldn't extend; if there is any
				 * free entry, take it from the
				 * longest list after all */
				if (l > 0) {
					i = BBP_free(m);
					BBP_free(m) = BBP_next(i);
					BBP_next(i) = 0;
					BBP_free(idx) = i;
				} else {
					/* nothing available */
					return GDK_FAIL;
				}
			}
		} else {
			BBP_free(idx) = (bat) ATOMIC_GET(BBPsize, BBPsizeLock) - 1;
		}
	}
	last = (last + 1) & BBP_THREADMASK;
	return GDK_SUCCEED;
}

/* return new BAT id (> 0); return 0 on failure */
bat
BBPinsert(BAT *bn)
{
	MT_Id pid = MT_getpid();
	int lock = locked_by ? pid != locked_by : 1;
	const char *s;
	long_str dirname;
	bat i;
	int idx = threadmask(pid);

	/* critical section: get a new BBP entry */
	if (lock) {
		MT_lock_set(&GDKtrimLock(idx));
		MT_lock_set(&GDKcacheLock(idx));
	}

	/* find an empty slot */
	if (BBP_free(idx) <= 0) {
		/* we need to extend the BBP */
		gdk_return r = GDK_SUCCEED;
		if (lock) {
			/* we must take all locks in a consistent
			 * order so first unset the one we've already
			 * got */
			MT_lock_unset(&GDKcacheLock(idx));
			for (i = 0; i <= BBP_THREADMASK; i++)
				MT_lock_set(&GDKcacheLock(i));
		}
		MT_lock_set(&GDKnameLock);
		/* check again in case some other thread extended
		 * while we were waiting */
		if (BBP_free(idx) <= 0) {
			r = maybeextend(idx);
		}
		MT_lock_unset(&GDKnameLock);
		if (lock)
			for (i = BBP_THREADMASK; i >= 0; i--)
				if (i != idx)
					MT_lock_unset(&GDKcacheLock(i));
		if (r != GDK_SUCCEED) {
			if (lock) {
				MT_lock_unset(&GDKcacheLock(idx));
				MT_lock_unset(&GDKtrimLock(idx));
			}
			return 0;
		}
	}
	i = BBP_free(idx);
	assert(i > 0);
	BBP_free(idx) = BBP_next(i);

	if (lock) {
		MT_lock_unset(&GDKcacheLock(idx));
		MT_lock_unset(&GDKtrimLock(idx));
	}
	/* rest of the work outside the lock */

	/* fill in basic BBP fields for the new bat */

	bn->batCacheid = i;
	bn->creator_tid = MT_getpid();

	BBP_status_set(i, BBPDELETING, "BBPinsert");
	BBP_cache(i) = NULL;
	BBP_desc(i) = NULL;
	BBP_refs(i) = 1;	/* new bats have 1 pin */
	BBP_lrefs(i) = 0;	/* ie. no logical refs */

#ifdef HAVE_HGE
	if (bn->ttype == TYPE_hge)
		havehge = 1;
#endif

	if (BBP_bak(i) == NULL) {
		s = BBPtmpname(dirname, 64, i);
		BBP_logical(i) = GDKstrdup(s);
		BBP_bak(i) = BBP_logical(i);
	} else
		BBP_logical(i) = BBP_bak(i);

	/* Keep the physical location around forever */
	if (BBP_physical(i) == NULL) {
		char name[64], *nme;

		BBPgetsubdir(dirname, i);
		nme = BBPphysicalname(name, 64, i);

		BBP_physical(i) = GDKfilepath(NOFARM, dirname, nme, NULL);

		BATDEBUG fprintf(stderr, "#%d = new %s(%s)\n", (int) i, BBPname(i), ATOMname(bn->ttype));
	}

	BBPdirty(1);

	return i;
}

gdk_return
BBPcacheit(BAT *bn, int lock)
{
	bat i = bn->batCacheid;
	int mode;

	if (lock)
		lock = locked_by ? MT_getpid() != locked_by : 1;

	if (i) {
		assert(i > 0);
	} else {
		i = BBPinsert(bn);	/* bat was not previously entered */
		if (i == 0)
			return GDK_FAIL;
		if (bn->tvheap)
			bn->tvheap->parentid = i;
	}
	assert(bn->batCacheid > 0);

	if (lock)
		MT_lock_set(&GDKswapLock(i));
	mode = (BBP_status(i) | BBPLOADED) & ~(BBPLOADING | BBPDELETING);
	BBP_status_set(i, mode, "BBPcacheit");
	BBP_desc(i) = bn;

	/* cache it! */
	BBP_cache(i) = bn;

	if (lock)
		MT_lock_unset(&GDKswapLock(i));
	return GDK_SUCCEED;
}

/*
 * BBPuncacheit changes the BBP status to swapped out.  Currently only
 * used in BBPfree (bat swapped out) and BBPclear (bat destroyed
 * forever).
 */

static void
BBPuncacheit(bat i, int unloaddesc)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(i, "BBPuncacheit")) {
		BAT *b = BBP_desc(i);

		if (b) {
			if (BBP_cache(i)) {
				BATDEBUG fprintf(stderr, "#uncache %d (%s)\n", (int) i, BBPname(i));

				BBP_cache(i) = NULL;

				/* clearing bits can be done without the lock */
				BBP_status_off(i, BBPLOADED, "BBPuncacheit");
			}
			if (unloaddesc) {
				BBP_desc(i) = NULL;
				BATdestroy(b);
			}
		}
	}
}

/*
 * @- BBPclear
 * BBPclear removes a BAT from the BBP directory forever.
 */
static inline void
bbpclear(bat i, int idx, const char *lock)
{
	BATDEBUG {
		fprintf(stderr, "#clear %d (%s)\n", (int) i, BBPname(i));
	}
	BBPuncacheit(i, TRUE);
	BATDEBUG {
		fprintf(stderr, "#BBPclear set to unloading %d\n", i);
	}
	BBP_status_set(i, BBPUNLOADING, "BBPclear");
	BBP_refs(i) = 0;
	BBP_lrefs(i) = 0;
	if (lock)
		MT_lock_set(&GDKcacheLock(idx));

	if (BBPtmpcheck(BBP_logical(i)) == 0) {
		MT_lock_set(&GDKnameLock);
		BBP_delete(i);
		MT_lock_unset(&GDKnameLock);
	}
	if (BBP_logical(i) != BBP_bak(i))
		GDKfree(BBP_logical(i));
	BBP_status_set(i, 0, "BBPclear");
	BBP_logical(i) = NULL;
	BBP_next(i) = BBP_free(idx);
	BBP_free(idx) = i;
	if (lock)
		MT_lock_unset(&GDKcacheLock(idx));
}

void
BBPclear(bat i)
{
	MT_Id pid = MT_getpid();
	int lock = locked_by ? pid != locked_by : 1;

	if (BBPcheck(i, "BBPclear")) {
		bbpclear(i, threadmask(pid), lock ? "BBPclear" : NULL);
	}
}

/*
 * @- BBP rename
 *
 * Each BAT has a logical name that is globally unique. Its reverse
 * view can also be assigned a name, that also has to be globally
 * unique.  The batId is the same as the logical BAT name.
 *
 * The default logical name of a BAT is tmp_X, where X is the
 * batCacheid.  Apart from being globally unique, new logical bat
 * names cannot be of the form tmp_X, unless X is the batCacheid.
 *
 * Physical names consist of a directory name followed by a logical
 * name suffix.  The directory name is derived from the batCacheid,
 * and is currently organized in a hierarchy that puts max 64 bats in
 * each directory (see BBPgetsubdir).
 *
 * Concerning the physical suffix: it is almost always bat_X. This
 * saves us a whole lot of trouble, as bat_X is always unique and no
 * conflicts can occur.  Other suffixes are only supported in order
 * just for backward compatibility with old repositories (you won't
 * see them anymore in new repositories).
 */
int
BBPrename(bat bid, const char *nme)
{
	BAT *b = BBPdescriptor(bid);
	long_str dirname;
	bat tmpid = 0, i;
	int idx;

	if (b == NULL)
		return 0;

	/* If name stays same, do nothing */
	if (BBP_logical(bid) && strcmp(BBP_logical(bid), nme) == 0)
		return 0;

	BBPgetsubdir(dirname, bid);

	if ((tmpid = BBPnamecheck(nme)) && tmpid != bid) {
		GDKerror("BBPrename: illegal temporary name: '%s'\n", nme);
		return BBPRENAME_ILLEGAL;
	}
	if (strlen(dirname) + strLen(nme) + 1 >= IDLENGTH) {
		GDKerror("BBPrename: illegal temporary name: '%s'\n", nme);
		return BBPRENAME_LONG;
	}
	idx = threadmask(MT_getpid());
	MT_lock_set(&GDKtrimLock(idx));
	MT_lock_set(&GDKnameLock);
	i = BBP_find(nme, FALSE);
	if (i != 0) {
		MT_lock_unset(&GDKnameLock);
		MT_lock_unset(&GDKtrimLock(idx));
		GDKerror("BBPrename: name is in use: '%s'.\n", nme);
		return BBPRENAME_ALREADY;
	}

	/* carry through the name change */
	if (BBP_logical(bid) && BBPtmpcheck(BBP_logical(bid)) == 0) {
		BBP_delete(bid);
	}
	if (BBP_logical(bid) != BBP_bak(bid))
		GDKfree(BBP_logical(bid));
	BBP_logical(bid) = GDKstrdup(nme);
	if (tmpid == 0) {
		BBP_insert(bid);
	}
	b->batDirtydesc = 1;
	if (b->batPersistence == PERSISTENT) {
		int lock = locked_by ? MT_getpid() != locked_by : 1;

		if (lock)
			MT_lock_set(&GDKswapLock(i));
		BBP_status_on(bid, BBPRENAMED, "BBPrename");
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		BBPdirty(1);
	}
	MT_lock_unset(&GDKnameLock);
	MT_lock_unset(&GDKtrimLock(idx));
	return 0;
}

/*
 * @+ BBP swapping Policy
 * The BAT can be moved back to disk using the routine BBPfree.  It
 * frees the storage for other BATs. After this call BAT* references
 * maintained for the BAT are wrong.  We should keep track of dirty
 * unloaded BATs. They may have to be committed later on, which may
 * include reading them in again.
 *
 * BBPswappable: may this bat be unloaded?  Only real bats without
 * memory references can be unloaded.
 */
static inline void
BBPspin(bat i, const char *s, int event)
{
	if (BBPcheck(i, "BBPspin") && (BBP_status(i) & event)) {
		lng spin = LL_CONSTANT(0);

		while (BBP_status(i) & event) {
			MT_sleep_ms(KITTENNAP);
			spin++;
		}
		BATDEBUG fprintf(stderr, "#BBPspin(%d,%s,%d): " LLFMT " loops\n", (int) i, s, event, spin);
	}
}

static inline int
incref(bat i, int logical, int lock)
{
	int refs;
	bat tp, tvp;
	BAT *b;
	int load = 0;

	if (i == bat_nil) {
		/* Stefan: May this happen? Or should we better call
		 * GDKerror(), here? */
		/* GDKerror("BBPincref() called with bat_nil!\n"); */
		return 0;
	}

	if (!BBPcheck(i, logical ? "BBPretain" : "BBPfix"))
		return 0;

	if (lock) {
		for (;;) {
			MT_lock_set(&GDKswapLock(i));
			if (!(BBP_status(i) & (BBPUNSTABLE|BBPLOADING)))
				break;
			/* the BATs is "unstable", try again */
			MT_lock_unset(&GDKswapLock(i));
			MT_sleep_ms(KITTENNAP);
		}
	}
	/* we have the lock */

	b = BBP_desc(i);
	if (b == NULL) {
		/* should not have happened */
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		return 0;
	}

	assert(BBP_refs(i) + BBP_lrefs(i) ||
	       BBP_status(i) & (BBPDELETED | BBPSWAPPED));
	if (logical) {
		/* parent BATs are not relevant for logical refs */
		tp = tvp = 0;
		refs = ++BBP_lrefs(i);
	} else {
		tp = b->theap.parentid;
		assert(tp >= 0);
		tvp = b->tvheap == 0 || b->tvheap->parentid == i ? 0 : b->tvheap->parentid;
		refs = ++BBP_refs(i);
		if (refs == 1 && (tp || tvp)) {
			/* If this is a view, we must load the parent
			 * BATs, but we must do that outside of the
			 * lock.  Set the BBPLOADING flag so that
			 * other threads will wait until we're
			 * done. */
			BBP_status_on(i, BBPLOADING, "BBPfix");
			load = 1;
		}
	}
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	if (load) {
		/* load the parent BATs and set the heap base pointers
		 * to the correct values */
		assert(!logical);
		if (tp) {
			BAT *pb;
			incref(tp, 0, lock);
			pb = getBBPdescriptor(tp, lock);
			b->theap.base = pb->theap.base + (size_t) b->theap.base;
			/* if we shared the hash before, share it
			 * again note that if the parent's hash is
			 * destroyed, we also don't have a hash
			 * anymore */
			if (b->thash == (Hash *) -1)
				b->thash = pb->thash;
		}
		if (tvp) {
			incref(tvp, 0, lock);
			(void) getBBPdescriptor(tvp, lock);
		}
		/* done loading, release descriptor */
		BBP_status_off(i, BBPLOADING, "BBPfix");
	}
	return refs;
}

int
BBPfix(bat i)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	return incref(i, FALSE, lock);
}

int
BBPretain(bat i)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	return incref(i, TRUE, lock);
}

void
BBPshare(bat parent)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	assert(parent > 0);
	if (lock)
		MT_lock_set(&GDKswapLock(parent));
	(void) incref(parent, TRUE, 0);
	++BBP_cache(parent)->batSharecnt;
	assert(BBP_refs(parent) > 0);
	(void) incref(parent, FALSE, 0);
	if (lock)
		MT_lock_unset(&GDKswapLock(parent));
}

static inline int
decref(bat i, int logical, int releaseShare, int lock, const char *func)
{
	int refs = 0, swap = 0;
	bat tp = 0, tvp = 0;
	BAT *b;

	assert(i > 0);
	if (lock)
		MT_lock_set(&GDKswapLock(i));
	if (releaseShare) {
		--BBP_desc(i)->batSharecnt;
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		return refs;
	}

	while (BBP_status(i) & BBPUNLOADING) {
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		BBPspin(i, func, BBPUNLOADING);
		if (lock)
			MT_lock_set(&GDKswapLock(i));
	}

	b = BBP_cache(i);

	/* decrement references by one */
	if (logical) {
		if (BBP_lrefs(i) == 0) {
			GDKerror("%s: %s does not have logical references.\n", func, BBPname(i));
			assert(0);
		} else {
			refs = --BBP_lrefs(i);
		}
	} else {
		if (BBP_refs(i) == 0) {
			GDKerror("%s: %s does not have pointer fixes.\n", func, BBPname(i));
			assert(0);
		} else {
			assert(b == NULL || b->theap.parentid == 0 || BBP_refs(b->theap.parentid) > 0);
			assert(b == NULL || b->tvheap == NULL || b->tvheap->parentid == 0 || BBP_refs(b->tvheap->parentid) > 0);
			refs = --BBP_refs(i);
			if (b && refs == 0) {
				if ((tp = b->theap.parentid) != 0)
					b->theap.base = (char *) (b->theap.base - BBP_cache(tp)->theap.base);
				/* if a view shared the hash with its
				 * parent, indicate this, but only if
				 * view isn't getting destroyed */
				if (tp && b->thash &&
				    b->thash == BBP_cache(tp)->thash)
					b->thash = (Hash *) -1;
				tvp = VIEWvtparent(b);
			}
		}
	}

	/* we destroy transients asap and unload persistent bats only
	 * if they have been made cold or are not dirty */
	if (BBP_refs(i) > 0 ||
	    (BBP_lrefs(i) > 0 &&
	     (b == NULL || BATdirty(b) || !(BBP_status(i) & BBPPERSISTENT)))) {
		/* bat cannot be swapped out */
	} else if (b || (BBP_status(i) & BBPTMP)) {
		/* bat will be unloaded now. set the UNLOADING bit
		 * while locked so no other thread thinks it's
		 * available anymore */
		assert((BBP_status(i) & BBPUNLOADING) == 0);
		BATDEBUG {
			fprintf(stderr, "#%s set to unloading BAT %d\n", func, i);
		}
		BBP_status_on(i, BBPUNLOADING, func);
		swap = TRUE;
	}

	/* unlock before re-locking in unload; as saving a dirty
	 * persistent bat may take a long time */
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	if (swap && b != NULL) {
		if (BBP_lrefs(i) == 0 && (BBP_status(i) & BBPDELETED) == 0) {
			/* free memory (if loaded) and delete from
			 * disk (if transient but saved) */
			BBPdestroy(b);
		} else {
			BATDEBUG {
				fprintf(stderr, "#%s unload and free bat %d\n", func, i);
			}
			BBP_unload_inc(i, func);
			/* free memory of transient */
			if (BBPfree(b, func) != GDK_SUCCEED)
				return -1;	/* indicate failure */
		}
	}
	if (tp)
		decref(tp, FALSE, FALSE, lock, func);
	if (tvp)
		decref(tvp, FALSE, FALSE, lock, func);
	return refs;
}

int
BBPunfix(bat i)
{
	if (BBPcheck(i, "BBPunfix") == 0) {
		return -1;
	}
	return decref(i, FALSE, FALSE, TRUE, "BBPunfix");
}

int
BBPrelease(bat i)
{
	if (BBPcheck(i, "BBPrelease") == 0) {
		return -1;
	}
	return decref(i, TRUE, FALSE, TRUE, "BBPrelease");
}

/*
 * M5 often changes the physical ref into a logical reference.  This
 * state change consist of the sequence BBPretain(b);BBPunfix(b).
 * A faster solution is given below, because it does not trigger the
 * BBP management actions, such as garbage collecting the bats.
 * [first step, initiate code change]
 */
void
BBPkeepref(bat i)
{
	if (i == bat_nil)
		return;
	if (BBPcheck(i, "BBPkeepref")) {
		int lock = locked_by ? MT_getpid() != locked_by : 1;
		BAT *b;

		if ((b = BBPdescriptor(i)) != NULL) {
			BATsettrivprop(b);
			if (GDKdebug & (CHECKMASK | PROPMASK))
				BATassertProps(b);
		}

		incref(i, TRUE, lock);
		assert(BBP_refs(i));
		decref(i, FALSE, FALSE, lock, "BBPkeepref");
	}
}

static inline void
GDKunshare(bat parent)
{
	(void) decref(parent, FALSE, TRUE, TRUE, "GDKunshare");
	(void) decref(parent, TRUE, FALSE, TRUE, "GDKunshare");
}

void
BBPunshare(bat parent)
{
	GDKunshare(parent);
}

/*
 * BBPreclaim is a user-exported function; the common way to destroy a
 * BAT the hard way.
 *
 * Return values:
 * -1 = bat cannot be unloaded (it has more than your own memory fix)
 *  0 = unloaded successfully
 *  1 = unload failed (due to write-to-disk failure)
 */
int
BBPreclaim(BAT *b)
{
	bat i;
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	if (b == NULL)
		return -1;
	i = b->batCacheid;

	assert(BBP_refs(i) == 1);

	return decref(i, 0, 0, lock, "BBPreclaim") <0;
}

/*
 * BBPdescriptor checks whether BAT needs loading and does so if
 * necessary. You must have at least one fix on the BAT before calling
 * this.
 */
static BAT *
getBBPdescriptor(bat i, int lock)
{
	int load = FALSE;
	BAT *b = NULL;

	assert(i > 0);
	if (!BBPcheck(i, "BBPdescriptor")) {
		return NULL;
	}
	assert(BBP_refs(i));
	if ((b = BBP_cache(i)) == NULL) {

		if (lock)
			MT_lock_set(&GDKswapLock(i));
		while (BBP_status(i) & BBPWAITING) {	/* wait for bat to be loaded by other thread */
			if (lock)
				MT_lock_unset(&GDKswapLock(i));
			MT_sleep_ms(KITTENNAP);
			if (lock)
				MT_lock_set(&GDKswapLock(i));
		}
		if (BBPvalid(i)) {
			b = BBP_cache(i);
			if (b == NULL) {
				load = TRUE;
				BATDEBUG {
					fprintf(stderr, "#BBPdescriptor set to unloading BAT %d\n", i);
				}
				BBP_status_on(i, BBPLOADING, "BBPdescriptor");
			}
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
	}
	if (load) {
		IODEBUG fprintf(stderr, "#load %s\n", BBPname(i));

		b = BATload_intern(i, lock);
		BBPin++;

		/* clearing bits can be done without the lock */
		BBP_status_off(i, BBPLOADING, "BBPdescriptor");
		CHECKDEBUG if (b != NULL)
			BATassertProps(b);
	}
	return b;
}

BAT *
BBPdescriptor(bat i)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	return getBBPdescriptor(i, lock);
}

/*
 * In BBPsave executes unlocked; it just marks the BBP_status of the
 * BAT to BBPsaving, so others that want to save or unload this BAT
 * must spin lock on the BBP_status field.
 */
gdk_return
BBPsave(BAT *b)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;
	bat bid = b->batCacheid;
	gdk_return ret = GDK_SUCCEED;

	if (BBP_lrefs(bid) == 0 || isVIEW(b) || !BATdirty(b))
		/* do nothing */
		return GDK_SUCCEED;

	if (lock)
		MT_lock_set(&GDKswapLock(bid));

	if (BBP_status(bid) & BBPSAVING) {
		/* wait until save in other thread completes */
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));
		BBPspin(bid, "BBPsave", BBPSAVING);
	} else {
		/* save it */
		int flags = BBPSAVING;

		if (DELTAdirty(b)) {
			flags |= BBPSWAPPED;
			BBPdirty(1);
		}
		if (b->batPersistence != PERSISTENT) {
			flags |= BBPTMP;
		}
		BBP_status_on(bid, flags, "BBPsave");
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));

		IODEBUG fprintf(stderr, "#save %s\n", BATgetId(b));

		/* do the time-consuming work unlocked */
		if (BBP_status(bid) & BBPEXISTING)
			ret = BBPbackup(b, FALSE);
		if (ret == GDK_SUCCEED) {
			BBPout++;
			ret = BATsave(b);
		}
		/* clearing bits can be done without the lock */
		BBP_status_off(bid, BBPSAVING, "BBPsave");
	}
	return ret;
}

/*
 * TODO merge BBPfree with BATfree? Its function is to prepare a BAT
 * for being unloaded (or even destroyed, if the BAT is not
 * persistent).
 */
static void
BBPdestroy(BAT *b)
{
	bat tp = b->theap.parentid;
	bat vtp = VIEWvtparent(b);

	if (isVIEW(b)) {	/* a physical view */
		VIEWdestroy(b);
	} else {
		/* bats that get destroyed must unfix their atoms */
		int (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		BUN p, q;
		BATiter bi = bat_iterator(b);

		assert(b->batSharecnt == 0);
		if (tunfix) {
			BATloop(b, p, q) {
				(*tunfix) (BUNtail(bi, p));
			}
		}
		BATdelete(b);	/* handles persistent case also (file deletes) */
	}
	BBPclear(b->batCacheid);	/* if destroyed; de-register from BBP */

	/* parent released when completely done with child */
	if (tp)
		GDKunshare(tp);
	if (vtp)
		GDKunshare(vtp);
}

static gdk_return
BBPfree(BAT *b, const char *calledFrom)
{
	bat bid = b->batCacheid, tp = VIEWtparent(b), vtp = VIEWvtparent(b);
	gdk_return ret;

	assert(bid > 0);
	assert(BBPswappable(b));
	(void) calledFrom;

	/* write dirty BATs before being unloaded */
	ret = BBPsave(b);
	if (ret == GDK_SUCCEED) {
		if (isVIEW(b)) {	/* physical view */
			VIEWdestroy(b);
		} else {
			if (BBP_cache(bid))
				BATfree(b);	/* free memory */
		}
		BBPuncacheit(bid, FALSE);
	}
	/* clearing bits can be done without the lock */
	BATDEBUG {
		fprintf(stderr, "#BBPfree turn off unloading %d\n", bid);
	}
	BBP_status_off(bid, BBPUNLOADING, calledFrom);
	BBP_unload_dec(bid, calledFrom);

	/* parent released when completely done with child */
	if (ret == GDK_SUCCEED && tp)
		GDKunshare(tp);
	if (ret == GDK_SUCCEED && vtp)
		GDKunshare(vtp);
	return ret;
}

/*
 * BBPquickdesc loads a BAT descriptor without loading the entire BAT,
 * of which the result be used only for a *limited* number of
 * purposes. Specifically, during the global sync/commit, we do not
 * want to load any BATs that are not already loaded, both because
 * this costs performance, and because getting into memory shortage
 * during a commit is extremely dangerous. Loading a BAT tends not to
 * be required, since the commit actions mostly involve moving some
 * pointers in the BAT descriptor. However, some column types do
 * require loading the full bat. This is tested by the complexatom()
 * routine. Such columns are those of which the type has a fix/unfix
 * method, or those that have HeapDelete methods. The HeapDelete
 * actions are not always required and therefore the BBPquickdesc is
 * parametrized.
 */
static int
complexatom(int t, int delaccess)
{
	if (t >= 0 && (BATatoms[t].atomFix || (delaccess && BATatoms[t].atomDel))) {
		return TRUE;
	}
	return FALSE;
}

BAT *
BBPquickdesc(bat bid, int delaccess)
{
	BAT *b;

	if (bid == bat_nil || bid == 0)
		return NULL;
	if (bid < 0) {
		GDKerror("BBPquickdesc: called with negative batid.\n");
		assert(0);
		return NULL;
	}
	if ((b = BBP_cache(bid)) != NULL)
		return b;	/* already cached */
	b = (BAT *) BBPgetdesc(bid);
	if (b == NULL ||
	    complexatom(b->ttype, delaccess)) {
		b = BATload_intern(bid, 1);
		BBPin++;
	}
	return b;
}

/*
 * @+ Global Commit
 */
static BAT *
dirty_bat(bat *i, int subcommit)
{
	if (BBPvalid(*i)) {
		BAT *b;
		BBPspin(*i, "dirty_bat", BBPSAVING);
		b = BBP_cache(*i);
		if (b != NULL) {
			if ((BBP_status(*i) & BBPNEW) &&
			    BATcheckmodes(b, FALSE) != GDK_SUCCEED) /* check mmap modes */
				*i = 0;	/* error */
			if ((BBP_status(*i) & BBPPERSISTENT) &&
			    (subcommit || BATdirty(b)))
				return b;	/* the bat is loaded, persistent and dirty */
		} else if (BBP_status(*i) & BBPSWAPPED) {
			b = (BAT *) BBPquickdesc(*i, TRUE);
			if (b && (subcommit || b->batDirtydesc))
				return b;	/* only the desc is loaded & dirty */
		}
	}
	return NULL;
}

/*
 * @- backup-bat
 * Backup-bat moves all files of a BAT to a backup directory. Only
 * after this succeeds, it may be saved. If some failure occurs
 * halfway saving, we can thus always roll back.
 */
static gdk_return
file_move(int farmid, const char *srcdir, const char *dstdir, const char *name, const char *ext)
{
	if (GDKmove(farmid, srcdir, name, ext, dstdir, name, ext) == GDK_SUCCEED) {
		return GDK_SUCCEED;
	} else {
		char *path;
		struct stat st;

		path = GDKfilepath(farmid, srcdir, name, ext);
		if (path == NULL)
			return GDK_FAIL;
		if (stat(path, &st)) {
			/* source file does not exist; the best
			 * recovery is to give an error but continue
			 * by considering the BAT as not saved; making
			 * sure that this time it does get saved.
			 */
			GDKsyserror("file_move: cannot stat %s\n", path);
			GDKfree(path);
			return GDK_FAIL;	/* fishy, but not fatal */
		}
		GDKfree(path);
	}
	return GDK_FAIL;
}

/* returns 1 if the file exists */
static int
file_exists(int farmid, const char *dir, const char *name, const char *ext)
{
	char *path;
	struct stat st;
	int ret = -1;

	path = GDKfilepath(farmid, dir, name, ext);
	if (path) {
		ret = stat(path, &st);
		IODEBUG fprintf(stderr, "#stat(%s) = %d\n", path, ret);
		GDKfree(path);
	}
	return (ret == 0);
}

static gdk_return
heap_move(Heap *hp, const char *srcdir, const char *dstdir, const char *nme, const char *ext)
{
	/* see doc at BATsetaccess()/gdk_bat.c for an expose on mmap
	 * heap modes */
	if (file_exists(hp->farmid, dstdir, nme, ext)) {
		/* dont overwrite heap with the committed state
		 * already in dstdir */
		return GDK_SUCCEED;
	} else if (hp->filename &&
		   hp->newstorage == STORE_PRIV &&
		   !file_exists(hp->farmid, srcdir, nme, ext)) {

		/* In order to prevent half-saved X.new files
		 * surviving a recover we create a dummy file in the
		 * BACKUP(dstdir) whose presence will trigger
		 * BBPrecover to remove them.  Thus, X will prevail
		 * where it otherwise wouldn't have.  If X already has
		 * a saved X.new, that one is backed up as normal.
		 */

		FILE *fp;
		long_str kill_ext;
		char *path;

		snprintf(kill_ext, sizeof(kill_ext), "%s.kill", ext);
		path = GDKfilepath(hp->farmid, dstdir, nme, kill_ext);
		if (path == NULL)
			return GDK_FAIL;
		fp = fopen(path, "w");
		if (fp == NULL)
			GDKsyserror("heap_move: cannot open file %s\n", path);
		IODEBUG fprintf(stderr, "#open %s = %d\n", path, fp ? 0 : -1);
		GDKfree(path);

		if (fp != NULL) {
			fclose(fp);
			return GDK_SUCCEED;
		} else {
			return GDK_FAIL;
		}
	}
	return file_move(hp->farmid, srcdir, dstdir, nme, ext);
}

/*
 * @- BBPprepare
 *
 * this routine makes sure there is a BAKDIR/, and initiates one if
 * not.  For subcommits, it does the same with SUBDIR.
 *
 * It is now locked, to get proper file counters, and also to prevent
 * concurrent BBPrecovers, etc.
 *
 * backup_dir == 0 => no backup BBP.dir
 * backup_dir == 1 => BBP.dir saved in BACKUP/
 * backup_dir == 2 => BBP.dir saved in SUBCOMMIT/
 */

static gdk_return
BBPprepare(bit subcommit)
{
	int start_subcommit, set = 1 + subcommit;
	str bakdirpath = GDKfilepath(0, NULL, BAKDIR, NULL);
	str subdirpath = GDKfilepath(0, NULL, SUBDIR, NULL);

	gdk_return ret = GDK_SUCCEED;

	/* tmLock is only used here, helds usually very shortly just
	 * to protect the file counters */
	MT_lock_set(&GDKtmLock);

	start_subcommit = (subcommit && backup_subdir == 0);
	if (start_subcommit) {
		/* starting a subcommit. Make sure SUBDIR and DELDIR
		 * are clean */
		ret = BBPrecover_subdir();
	}
	if (backup_files == 0) {
		backup_dir = 0;
		ret = BBPrecover(0);
		if (ret == GDK_SUCCEED) {
			if (mkdir(bakdirpath, 0755) < 0 && errno != EEXIST) {
				GDKsyserror("BBPprepare: cannot create directory %s\n", bakdirpath);
				ret = GDK_FAIL;
			}
			/* if BAKDIR already exists, don't signal error */
			IODEBUG fprintf(stderr, "#mkdir %s = %d\n", bakdirpath, (int) ret);
		}
	}
	if (ret == GDK_SUCCEED && start_subcommit) {
		/* make a new SUBDIR (subdir of BAKDIR) */
		if (mkdir(subdirpath, 0755) < 0) {
			GDKsyserror("BBPprepare: cannot create directory %s\n", subdirpath);
			ret = GDK_FAIL;
		}
		IODEBUG fprintf(stderr, "#mkdir %s = %d\n", subdirpath, (int) ret);
	}
	if (ret == GDK_SUCCEED && backup_dir != set) {
		/* a valid backup dir *must* at least contain BBP.dir */
		if ((ret = GDKmove(0, backup_dir ? BAKDIR : BATDIR, "BBP", "dir", subcommit ? SUBDIR : BAKDIR, "BBP", "dir")) == GDK_SUCCEED) {
			backup_dir = set;
		}
	}
	/* increase counters */
	if (ret == GDK_SUCCEED) {
		backup_subdir += subcommit;
		backup_files++;
	}
	MT_lock_unset(&GDKtmLock);
	GDKfree(bakdirpath);
	GDKfree(subdirpath);
	return ret;
}

static gdk_return
do_backup(const char *srcdir, const char *nme, const char *ext,
	  Heap *h, int dirty, bit subcommit)
{
	gdk_return ret = GDK_SUCCEED;

	 /* direct mmap is unprotected (readonly usage, or has WAL
	  * protection); however, if we're backing up for subcommit
	  * and a backup already exists in the main backup directory
	  * (see GDKupgradevarheap), move the file */
	if (subcommit && file_exists(h->farmid, BAKDIR, nme, ext)) {
		if (file_move(h->farmid, BAKDIR, SUBDIR, nme, ext) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	if (h->storage != STORE_MMAP) {
		/* STORE_PRIV saves into X.new files. Two cases could
		 * happen. The first is when a valid X.new exists
		 * because of an access change or a previous
		 * commit. This X.new should be backed up as
		 * usual. The second case is when X.new doesn't
		 * exist. In that case we could have half written
		 * X.new files (after a crash). To protect against
		 * these we write X.new.kill files in the backup
		 * directory (see heap_move). */
		char extnew[16];
		gdk_return mvret = GDK_SUCCEED;

		snprintf(extnew, sizeof(extnew), "%s.new", ext);
		if (dirty &&
		    !file_exists(h->farmid, BAKDIR, nme, extnew) &&
		    !file_exists(h->farmid, BAKDIR, nme, ext)) {
			/* if the heap is dirty and there is no heap
			 * file (with or without .new extension) in
			 * the BAKDIR, move the heap (preferably with
			 * .new extension) to the correct backup
			 * directory */
			if (file_exists(h->farmid, srcdir, nme, extnew))
				mvret = heap_move(h, srcdir,
						  subcommit ? SUBDIR : BAKDIR,
						  nme, extnew);
			else
				mvret = heap_move(h, srcdir,
						  subcommit ? SUBDIR : BAKDIR,
						  nme, ext);
		} else if (subcommit) {
			/* if subcommit, wqe may need to move an
			 * already made backup from BAKDIR to
			 * SUBSIR */
			if (file_exists(h->farmid, BAKDIR, nme, extnew))
				mvret = file_move(h->farmid, BAKDIR, SUBDIR, nme, extnew);
			else if (file_exists(h->farmid, BAKDIR, nme, ext))
				mvret = file_move(h->farmid, BAKDIR, SUBDIR, nme, ext);
		}
		/* there is a situation where the move may fail,
		 * namely if this heap was not supposed to be existing
		 * before, i.e. after a BATmaterialize on a persistent
		 * bat as a workaround, do not complain about move
		 * failure if the source file is nonexistent
		 */
		if (mvret != GDK_SUCCEED && file_exists(h->farmid, srcdir, nme, ext)) {
			ret = GDK_FAIL;
		}
		if (subcommit &&
		    (h->storage == STORE_PRIV || h->newstorage == STORE_PRIV)) {
			long_str kill_ext;

			snprintf(kill_ext, sizeof(kill_ext), "%s.new.kill", ext);
			if (file_exists(h->farmid, BAKDIR, nme, kill_ext) &&
			    file_move(h->farmid, BAKDIR, SUBDIR, nme, kill_ext) != GDK_SUCCEED) {
				ret = GDK_FAIL;
			}
		}
	}
	return ret;
}

static gdk_return
BBPbackup(BAT *b, bit subcommit)
{
	char *srcdir;
	long_str nme;
	const char *s = BBP_physical(b->batCacheid);

	if (BBPprepare(subcommit) != GDK_SUCCEED) {
		return GDK_FAIL;
	}
	if (b->batCopiedtodisk == 0 || b->batPersistence != PERSISTENT) {
		return GDK_SUCCEED;
	}
	/* determine location dir and physical suffix */
	srcdir = GDKfilepath(NOFARM, BATDIR, s, NULL);
	s = strrchr(srcdir, DIR_SEP);
	if (!s)
		goto fail;
	strncpy(nme, ++s, sizeof(nme));
	nme[sizeof(nme) - 1] = 0;
	srcdir[s - srcdir] = 0;

	if (b->ttype != TYPE_void &&
	    do_backup(srcdir, nme, "tail", &b->theap,
		      b->batDirty || b->theap.dirty, subcommit) != GDK_SUCCEED)
		goto fail;
	if (b->tvheap &&
	    do_backup(srcdir, nme, "theap", b->tvheap,
		      b->batDirty || b->tvheap->dirty, subcommit) != GDK_SUCCEED)
		goto fail;
	GDKfree(srcdir);
	return GDK_SUCCEED;
  fail:
	GDKfree(srcdir);
	return GDK_FAIL;
}

/*
 * @+ Atomic Write
 * The atomic BBPsync() function first safeguards the old images of
 * all files to be written in BAKDIR. It then saves all files. If that
 * succeeds fully, BAKDIR is renamed to DELDIR. The rename is
 * considered an atomic action. If it succeeds, the DELDIR is removed.
 * If something fails, the pre-sync status can be obtained by moving
 * back all backed up files; this is done by BBPrecover().
 *
 * The BBP.dir is also moved into the BAKDIR.
 */
gdk_return
BBPsync(int cnt, bat *subcommit)
{
	gdk_return ret = GDK_SUCCEED;
	int bbpdirty = 0;
	int t0 = 0, t1 = 0;

	str bakdir = GDKfilepath(0, NULL, subcommit ? SUBDIR : BAKDIR, NULL);
	str deldir = GDKfilepath(0, NULL, DELDIR, NULL);

	PERFDEBUG t0 = t1 = GDKms();

	ret = BBPprepare(subcommit != NULL);

	/* PHASE 1: safeguard everything in a backup-dir */
	bbpdirty = BBP_dirty;
	if (ret == GDK_SUCCEED) {
		int idx = 0;

		while (++idx < cnt) {
			bat i = subcommit ? subcommit[idx] : idx;
			BAT *b = dirty_bat(&i, subcommit != NULL);
			if (i <= 0)
				break;
			if (BBP_status(i) & BBPEXISTING) {
				if (b != NULL && BBPbackup(b, subcommit != NULL) != GDK_SUCCEED)
					break;
			} else if (subcommit && (b = BBP_desc(i)) && BBP_status(i) & BBPDELETED) {
				char o[10];
				char *f;
				snprintf(o, sizeof(o), "%o", b->batCacheid);
				f = GDKfilepath(b->theap.farmid, BAKDIR, o, "tail");
				if (access(f, F_OK) == 0)
					file_move(b->theap.farmid, BAKDIR, SUBDIR, o, "tail");
				GDKfree(f);
				f = GDKfilepath(b->theap.farmid, BAKDIR, o, "theap");
				if (access(f, F_OK) == 0)
					file_move(b->theap.farmid, BAKDIR, SUBDIR, o, "theap");
				GDKfree(f);
			}
		}
		if (idx < cnt)
			ret = GDK_FAIL;
	}
	PERFDEBUG fprintf(stderr, "#BBPsync (move time %d) %d files\n", (t1 = GDKms()) - t0, backup_files);

	/* PHASE 2: save the repository */
	if (ret == GDK_SUCCEED) {
		int idx = 0;

		while (++idx < cnt) {
			bat i = subcommit ? subcommit[idx] : idx;

			if (BBP_status(i) & BBPPERSISTENT) {
				BAT *b = dirty_bat(&i, subcommit != NULL);
				if (i <= 0)
					break;
				if (b != NULL && BATsave(b) != GDK_SUCCEED)
					break;	/* write error */
			}
		}
		if (idx < cnt)
			ret = GDK_FAIL;
	}

	PERFDEBUG fprintf(stderr, "#BBPsync (write time %d)\n", (t0 = GDKms()) - t1);

	if (ret == GDK_SUCCEED) {
		if (bbpdirty) {
			ret = BBPdir(cnt, subcommit);
		} else if (backup_dir && GDKmove(0, (backup_dir == 1) ? BAKDIR : SUBDIR, "BBP", "dir", BATDIR, "BBP", "dir") != GDK_SUCCEED) {
			ret = GDK_FAIL;	/* tried a cheap way to get BBP.dir; but it failed */
		} else {
			/* commit might still fail; we must remember
			 * that we moved BBP.dir out of BAKDIR */
			backup_dir = 0;
		}
	}

	PERFDEBUG fprintf(stderr, "#BBPsync (dir time %d) %d bats\n", (t1 = GDKms()) - t0, (bat) ATOMIC_GET(BBPsize, BBPsizeLock));

	if (bbpdirty || backup_files > 0) {
		if (ret == GDK_SUCCEED) {

			/* atomic switchover */
			/* this is the big one: this call determines
			 * whether the operation of this function
			 * succeeded, so no changing of ret after this
			 * call anymore */

			if (rename(bakdir, deldir) < 0)
				ret = GDK_FAIL;
			if (ret != GDK_SUCCEED &&
			    GDKremovedir(0, DELDIR) == GDK_SUCCEED && /* maybe there was an old deldir */
			    rename(bakdir, deldir) < 0)
				ret = GDK_FAIL;
			if (ret != GDK_SUCCEED)
				GDKsyserror("BBPsync: rename(%s,%s) failed.\n", bakdir, deldir);
			IODEBUG fprintf(stderr, "#BBPsync: rename %s %s = %d\n", bakdir, deldir, (int) ret);
		}

		/* AFTERMATH */
		if (ret == GDK_SUCCEED) {
			BBP_dirty = 0;
			backup_files = subcommit ? (backup_files - backup_subdir) : 0;
			backup_dir = backup_subdir = 0;
			if (GDKremovedir(0, DELDIR) != GDK_SUCCEED)
				fprintf(stderr, "#BBPsync: cannot remove directory %s\n", DELDIR);
			(void) BBPprepare(0);	/* (try to) remove DELDIR and set up new BAKDIR */
			if (backup_files > 1) {
				PERFDEBUG fprintf(stderr, "#BBPsync (backup_files %d > 1)\n", backup_files);
				backup_files = 1;
			}
		}
	}
	PERFDEBUG fprintf(stderr, "#BBPsync (ready time %d)\n", (t0 = GDKms()) - t1);
	GDKfree(bakdir);
	GDKfree(deldir);
	return ret;
}

/*
 * Recovery just moves all files back to their original location. this
 * is an incremental process: if something fails, just stop with still
 * files left for moving in BACKUP/.  The recovery process can resume
 * later with the left over files.
 */
static gdk_return
force_move(int farmid, const char *srcdir, const char *dstdir, const char *name)
{
	const char *p;
	char *dstpath, *killfile;
	gdk_return ret = GDK_SUCCEED;

	if ((p = strrchr(name, '.')) != NULL && strcmp(p, ".kill") == 0) {
		/* Found a X.new.kill file, ie remove the X.new file */
		ptrdiff_t len = p - name;
		long_str srcpath;

		strncpy(srcpath, name, len);
		srcpath[len] = '\0';
		dstpath = GDKfilepath(farmid, dstdir, srcpath, NULL);

		/* step 1: remove the X.new file that is going to be
		 * overridden by X */
		if (unlink(dstpath) < 0 && errno != ENOENT) {
			/* if it exists and cannot be removed, all
			 * this is going to fail */
			GDKsyserror("force_move: unlink(%s)\n", dstpath);
			GDKfree(dstpath);
			return GDK_FAIL;
		}
		GDKfree(dstpath);

		/* step 2: now remove the .kill file. This one is
		 * crucial, otherwise we'll never finish recovering */
		killfile = GDKfilepath(farmid, srcdir, name, NULL);
		if (unlink(killfile) < 0) {
			ret = GDK_FAIL;
			GDKsyserror("force_move: unlink(%s)\n", killfile);
		}
		GDKfree(killfile);
		return ret;
	}
	/* try to rename it */
	ret = GDKmove(farmid, srcdir, name, NULL, dstdir, name, NULL);

	if (ret != GDK_SUCCEED) {
		char *srcpath;

		/* two legal possible causes: file exists or dir
		 * doesn't exist */
		dstpath = GDKfilepath(farmid, dstdir, name, NULL);
		srcpath = GDKfilepath(farmid, srcdir, name, NULL);
		if (unlink(dstpath) < 0)	/* clear destination */
			ret = GDK_FAIL;
		IODEBUG fprintf(stderr, "#unlink %s = %d\n", dstpath, (int) ret);

		(void) GDKcreatedir(dstdir); /* if fails, move will fail */
		ret = GDKmove(farmid, srcdir, name, NULL, dstdir, name, NULL);
		IODEBUG fprintf(stderr, "#link %s %s = %d\n", srcpath, dstpath, (int) ret);
		GDKfree(dstpath);
		GDKfree(srcpath);
	}
	return ret;
}

gdk_return
BBPrecover(int farmid)
{
	str bakdirpath;
	str leftdirpath;
	DIR *dirp;
	struct dirent *dent;
	long_str path, dstpath;
	bat i;
	size_t j = strlen(BATDIR);
	gdk_return ret = GDK_SUCCEED;
	int dirseen = FALSE;
	str dstdir;

	bakdirpath = GDKfilepath(farmid, NULL, BAKDIR, NULL);
	leftdirpath = GDKfilepath(farmid, NULL, LEFTDIR, NULL);
	if (bakdirpath == NULL || leftdirpath == NULL) {
		GDKfree(bakdirpath);
		GDKfree(leftdirpath);
		return GDK_FAIL;
	}
	dirp = opendir(bakdirpath);
	if (dirp == NULL) {
		GDKfree(bakdirpath);
		GDKfree(leftdirpath);
		return GDK_SUCCEED;	/* nothing to do */
	}
	memcpy(dstpath, BATDIR, j);
	dstpath[j] = DIR_SEP;
	dstpath[++j] = 0;
	dstdir = dstpath + j;
	IODEBUG fprintf(stderr, "#BBPrecover(start)\n");

	if (mkdir(leftdirpath, 0755) < 0 && errno != EEXIST) {
		GDKsyserror("BBPrecover: cannot create directory %s\n", leftdirpath);
		closedir(dirp);
		GDKfree(bakdirpath);
		GDKfree(leftdirpath);
		return GDK_FAIL;
	}

	/* move back all files */
	while ((dent = readdir(dirp)) != NULL) {
		const char *q = strchr(dent->d_name, '.');

		if (q == dent->d_name) {
			char *fn;

			if (strcmp(dent->d_name, ".") == 0 ||
			    strcmp(dent->d_name, "..") == 0)
				continue;
			fn = GDKfilepath(farmid, BAKDIR, dent->d_name, NULL);
			if (fn) {
				int uret = unlink(fn);
				IODEBUG fprintf(stderr, "#unlink %s = %d\n",
						fn, uret);
				GDKfree(fn);
			}
			continue;
		} else if (strcmp(dent->d_name, "BBP.dir") == 0) {
			dirseen = TRUE;
			continue;
		}
		if (q == NULL)
			q = dent->d_name + strlen(dent->d_name);
		if ((j = q - dent->d_name) + 1 > sizeof(path)) {
			/* name too long: ignore */
			continue;
		}
		strncpy(path, dent->d_name, j);
		path[j] = 0;
		if (GDKisdigit(*path)) {
			i = strtol(path, NULL, 8);
		} else {
			i = BBP_find(path, FALSE);
			if (i < 0)
				i = -i;
		}
		if (i == 0 || i >= (bat) ATOMIC_GET(BBPsize, BBPsizeLock) || !BBPvalid(i)) {
			force_move(farmid, BAKDIR, LEFTDIR, dent->d_name);
		} else {
			BBPgetsubdir(dstdir, i);
			if (force_move(farmid, BAKDIR, dstpath, dent->d_name) != GDK_SUCCEED)
				ret = GDK_FAIL;
		}
	}
	closedir(dirp);
	if (dirseen && ret == GDK_SUCCEED) {	/* we have a saved BBP.dir; it should be moved back!! */
		struct stat st;
		char *fn;

		fn = GDKfilepath(farmid, BATDIR, "BBP", "dir");
		ret = recover_dir(farmid, stat(fn, &st) == 0);
		GDKfree(fn);
	}

	if (ret == GDK_SUCCEED) {
		if (rmdir(bakdirpath) < 0) {
			GDKsyserror("BBPrecover: cannot remove directory %s\n", bakdirpath);
			ret = GDK_FAIL;
		}
		IODEBUG fprintf(stderr, "#rmdir %s = %d\n", bakdirpath, (int) ret);
	}
	if (ret != GDK_SUCCEED)
		GDKerror("BBPrecover: recovery failed. Please check whether your disk is full or write-protected.\n");

	IODEBUG fprintf(stderr, "#BBPrecover(end)\n");
	GDKfree(bakdirpath);
	GDKfree(leftdirpath);
	return ret;
}

/*
 * SUBDIR recovery is quite mindlessly moving all files back to the
 * parent (BAKDIR).  We do recognize moving back BBP.dir and set
 * backed_up_subdir accordingly.
 */
gdk_return
BBPrecover_subdir(void)
{
	str subdirpath;
	DIR *dirp;
	struct dirent *dent;
	gdk_return ret = GDK_SUCCEED;

	subdirpath = GDKfilepath(0, NULL, SUBDIR, NULL);
	if (subdirpath == NULL)
		return GDK_FAIL;
	dirp = opendir(subdirpath);
	GDKfree(subdirpath);
	if (dirp == NULL) {
		return GDK_SUCCEED;	/* nothing to do */
	}
	IODEBUG fprintf(stderr, "#BBPrecover_subdir(start)\n");

	/* move back all files */
	while ((dent = readdir(dirp)) != NULL) {
		if (dent->d_name[0] == '.')
			continue;
		ret = GDKmove(0, SUBDIR, dent->d_name, NULL, BAKDIR, dent->d_name, NULL);
		if (ret == GDK_SUCCEED && strcmp(dent->d_name, "BBP.dir") == 0)
			backup_dir = 1;
		if (ret != GDK_SUCCEED)
			break;
	}
	closedir(dirp);

	/* delete the directory */
	if (ret == GDK_SUCCEED) {
		ret = GDKremovedir(0, SUBDIR);
		if (backup_dir == 2) {
			IODEBUG fprintf(stderr, "#BBPrecover_subdir: %s%cBBP.dir had disappeared!", SUBDIR, DIR_SEP);
			backup_dir = 0;
		}
	}
	IODEBUG fprintf(stderr, "#BBPrecover_subdir(end) = %d\n", (int) ret);

	if (ret != GDK_SUCCEED)
		GDKerror("BBPrecover_subdir: recovery failed. Please check whether your disk is full or write-protected.\n");
	return ret;
}

/*
 * @- The diskscan
 * The BBPdiskscan routine walks through the BAT dir, cleans up
 * leftovers, and measures disk occupancy.  Leftovers are files that
 * cannot belong to a BAT. in order to establish this for [ht]heap
 * files, the BAT descriptor is loaded in order to determine whether
 * these files are still required.
 *
 * The routine gathers all bat sizes in a bat that contains bat-ids
 * and bytesizes. The return value is the number of bytes of space
 * freed.
 */
static int
persistent_bat(bat bid)
{
	if (bid >= 0 && bid < (bat) ATOMIC_GET(BBPsize, BBPsizeLock) && BBPvalid(bid)) {
		BAT *b = BBP_cache(bid);

		if (b == NULL || b->batCopiedtodisk) {
			return TRUE;
		}
	}
	return FALSE;
}

static BAT *
getdesc(bat bid)
{
	BAT *b = BBPgetdesc(bid);

	if (b == NULL)
		BBPclear(bid);
	return b;
}

static int
BBPdiskscan(const char *parent, size_t baseoff)
{
	DIR *dirp = opendir(parent);
	struct dirent *dent;
	char fullname[PATHLENGTH];
	str dst = fullname;
	size_t dstlen = sizeof(fullname);
	const char *src = parent;

	if (dirp == NULL)
		return -1;	/* nothing to do */

	while (*src) {
		*dst++ = *src++;
		dstlen--;
	}
	if (dst > fullname && dst[-1] != DIR_SEP) {
		*dst++ = DIR_SEP;
		dstlen--;
	}

	while ((dent = readdir(dirp)) != NULL) {
		const char *p;
		bat bid;
		int ok, delete;

		if (dent->d_name[0] == '.')
			continue;	/* ignore .dot files and directories (. ..) */

		if (strncmp(dent->d_name, "BBP.", 4) == 0 &&
		    (strcmp(parent + baseoff, BATDIR) == 0 ||
		     strncmp(parent + baseoff, BAKDIR, strlen(BAKDIR)) == 0 ||
		     strncmp(parent + baseoff, SUBDIR, strlen(SUBDIR)) == 0))
			continue;

		p = strchr(dent->d_name, '.');

		if (strlen(dent->d_name) >= dstlen) {
			/* found a file with too long a name
			   (i.e. unknown); stop pruning in this
			   subdir */
			fprintf(stderr, "BBPdiskscan: unexpected file %s, leaving %s.\n", dent->d_name, parent);
			break;
		}
		strncpy(dst, dent->d_name, dstlen);
		fullname[sizeof(fullname) - 1] = 0;

		if (p == NULL && BBPdiskscan(fullname, baseoff) == 0) {
			/* it was a directory */
			continue;
		}

		if (p && strcmp(p + 1, "tmp") == 0) {
			delete = TRUE;
			ok = TRUE;
			bid = 0;
		} else {
			bid = strtol(dent->d_name, NULL, 8);
			ok = p && bid;
			delete = FALSE;

			if (ok == FALSE || !persistent_bat(bid)) {
				delete = TRUE;
			} else if (strncmp(p + 1, "tail", 4) == 0) {
				BAT *b = getdesc(bid);
				delete = (b == NULL || !b->ttype || b->batCopiedtodisk == 0);
			} else if (strncmp(p + 1, "theap", 5) == 0) {
				BAT *b = getdesc(bid);
				delete = (b == NULL || !b->tvheap || b->batCopiedtodisk == 0);
			} else if (strncmp(p + 1, "thash", 5) == 0) {
#ifdef PERSISTENTHASH
				BAT *b = getdesc(bid);
				delete = b == NULL;
				if (!delete)
					b->thash = (Hash *) 1;
#else
				delete = TRUE;
#endif
			} else if (strncmp(p + 1, "timprints", 9) == 0) {
				BAT *b = getdesc(bid);
				delete = b == NULL;
				if (!delete)
					b->timprints = (Imprints *) 1;
			} else if (strncmp(p + 1, "torderidx", 9) == 0) {
#ifdef PERSISTENTIDX
				BAT *b = getdesc(bid);
				delete = b == NULL;
				if (!delete)
					b->torderidx = (Heap *) 1;
#else
				delete = TRUE;
#endif
			} else if (strncmp(p + 1, "priv", 4) != 0 &&
				   strncmp(p + 1, "new", 3) != 0 &&
				   strncmp(p + 1, "head", 4) != 0 &&
				   strncmp(p + 1, "tail", 4) != 0) {
				ok = FALSE;
			} else if (strncmp(p + 1, "head", 4) == 0 ||
				   strncmp(p + 1, "hheap", 5) == 0 ||
				   strncmp(p + 1, "hhash", 5) == 0 ||
				   strncmp(p + 1, "himprints", 9) == 0 ||
				   strncmp(p + 1, "horderidx", 9) == 0) {
				/* head is VOID, so no head, hheap files, and
				 * we do not support any indexes on the
				 * head */
				delete = 1;
			}
		}
		if (!ok) {
			/* found an unknown file; stop pruning in this
			 * subdir */
			fprintf(stderr, "BBPdiskscan: unexpected file %s, leaving %s.\n", dent->d_name, parent);
			break;
		}
		if (delete) {
			if (unlink(fullname) < 0 && errno != ENOENT) {
				GDKsyserror("BBPdiskscan: unlink(%s)", fullname);
				continue;
			}
			IODEBUG fprintf(stderr, "#BBPcleanup: unlink(%s) = 0\n", fullname);
		}
	}
	closedir(dirp);
	return 0;
}

void
gdk_bbp_reset(void)
{
	int i;

	while (BBPlimit > 0) {
		BBPlimit -= BBPINIT;
		assert(BBPlimit >= 0);
		GDKfree(BBP[BBPlimit >> BBPINITLOG]);
	}
	memset(BBP, 0, sizeof(BBP));
	BBPlimit = 0;
	BBPsize = 0;
	for (i = 0; i < MAXFARMS; i++)
		GDKfree((void *) BBPfarms[i].dirname); /* loose "const" */
	memset(BBPfarms, 0, sizeof(BBPfarms));
	BBP_hash = 0;
	BBP_mask = 0;

	BBP_dirty = 0;
	BBPin = 0;
	BBPout = 0;

	locked_by = 0;
	BBPunloadCnt = 0;
	backup_files = 0;
	backup_dir = 0;
	backup_subdir = 0;
}
