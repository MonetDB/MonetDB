/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
 * @emph{b->batCacheid}. The @emph{bat} zero is reserved for the nil
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
#ifndef S_ISDIR
#define S_ISDIR(mode)	(((mode) & _S_IFMT) == _S_IFDIR)
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
static ATOMIC_TYPE BBPsize = ATOMIC_VAR_INIT(0); /* current used size of BBP array */

struct BBPfarm_t BBPfarms[MAXFARMS];

#define KITTENNAP 1		/* used to suspend processing */
#define BBPNONAME "."		/* filler for no name in BBP.dir */
/*
 * The hash index uses a bucket index (int array) of size mask that is
 * tuned for perfect hashing (1 lookup). The bucket chain uses the
 * 'next' field in the BBPrec records.
 */
bat *BBP_hash = NULL;		/* BBP logical name hash buckets */
bat BBP_mask = 0;		/* number of buckets = & mask */

static gdk_return BBPfree(BAT *b, const char *calledFrom);
static void BBPdestroy(BAT *b);
static void BBPuncacheit(bat bid, bool unloaddesc);
static gdk_return BBPprepare(bool subcommit);
static BAT *getBBPdescriptor(bat i, bool lock);
static gdk_return BBPbackup(BAT *b, bool subcommit);
static gdk_return BBPdir(int cnt, bat *restrict subcommit, BUN *restrict sizes, lng logno, lng transid);

static lng BBPlogno;		/* two lngs of extra info in BBP.dir */
static lng BBPtransid;

#ifdef HAVE_HGE
/* start out by saying we have no hge, but as soon as we've seen one,
 * we'll always say we do have it */
static bool havehge = false;
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
	return (bat) ATOMIC_GET(&BBPsize);
}

lng
getBBPlogno(void)
{
	return BBPlogno;
}

lng
getBBPtransid(void)
{
	return BBPtransid;
}


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
 * @emph{BBPstatus} and @emph{BBPrefs}.
 * These arrays are called quasi as now they are
 * actually stored together in one big BBPrec array called BBP, that
 * is allocated in anonymous VM space, so we can reallocate this
 * structure without changing the base address (a crucial feature if
 * read actions are to go on unlocked while other entries in the BBP
 * may be modified).
 */
static volatile MT_Id locked_by = 0;

#define BBP_unload_inc()			\
	do {					\
		MT_lock_set(&GDKunloadLock);	\
		BBPunloadCnt++;			\
		MT_lock_unset(&GDKunloadLock);	\
	} while (0)

#define BBP_unload_dec()			\
	do {					\
		MT_lock_set(&GDKunloadLock);	\
		--BBPunloadCnt;			\
		assert(BBPunloadCnt >= 0);	\
		MT_lock_unset(&GDKunloadLock);	\
	} while (0)

static int BBPunloadCnt = 0;
static MT_Lock GDKunloadLock = MT_LOCK_INITIALIZER(GDKunloadLock);

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
	bat i = (bat) ATOMIC_GET(&BBPsize);

	assert(j >= 0 && j <= BBP_THREADMASK);
	for (BBP_mask = 1; (BBP_mask << 1) <= BBPlimit; BBP_mask <<= 1)
		;
	BBP_hash = (bat *) GDKzalloc(BBP_mask * sizeof(bat));
	if (BBP_hash == NULL) {
		GDKerror("cannot allocate memory\n");
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
BBPselectfarm(role_t role, int type, enum heaptype hptype)
{
	int i;

	(void) type;		/* may use in future */
	(void) hptype;		/* may use in future */

	if (GDKinmemory(0))
		return 0;

#ifndef PERSISTENTHASH
	if (hptype == hashheap)
		role = TRANSIENT;
#endif
#ifndef PERSISTENTIDX
	if (hptype == orderidxheap)
		role = TRANSIENT;
#endif
	for (i = 0; i < MAXFARMS; i++)
		if (BBPfarms[i].roles & (1U << (int) role))
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
BBPextend(int idx, bool buildhash)
{
	if ((bat) ATOMIC_GET(&BBPsize) >= N_BBPINIT * BBPINIT) {
		GDKerror("trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);
		return GDK_FAIL;
	}

	/* make sure the new size is at least BBPsize large */
	while (BBPlimit < (bat) ATOMIC_GET(&BBPsize)) {
		assert(BBP[BBPlimit >> BBPINITLOG] == NULL);
		BBP[BBPlimit >> BBPINITLOG] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (BBP[BBPlimit >> BBPINITLOG] == NULL) {
			GDKerror("failed to extend BAT pool\n");
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

static gdk_return
recover_dir(int farmid, bool direxists)
{
	if (direxists) {
		/* just try; don't care about these non-vital files */
		if (GDKunlink(farmid, BATDIR, "BBP", "bak") != GDK_SUCCEED)
			TRC_WARNING(GDK, "unlink of BBP.bak failed\n");
		if (GDKmove(farmid, BATDIR, "BBP", "dir", BATDIR, "BBP", "bak") != GDK_SUCCEED)
			TRC_WARNING(GDK, "rename of BBP.dir to BBP.bak failed\n");
	}
	return GDKmove(farmid, BAKDIR, "BBP", "dir", BATDIR, "BBP", "dir");
}

static gdk_return BBPrecover(int farmid);
static gdk_return BBPrecover_subdir(void);
static bool BBPdiskscan(const char *, size_t);

static int
heapinit(BAT *b, const char *buf, int *hashash, unsigned bbpversion, bat bid, const char *filename, int lineno)
{
	int t;
	char type[33];
	uint16_t width;
	uint16_t var;
	uint16_t properties;
	uint64_t nokey0;
	uint64_t nokey1;
	uint64_t nosorted;
	uint64_t norevsorted;
	uint64_t base;
	uint64_t free;
	uint64_t size;
	uint16_t storage;
	uint64_t minpos, maxpos;
	int n;

	(void) bbpversion;	/* could be used to implement compatibility */

	minpos = maxpos = (uint64_t) oid_nil; /* for GDKLIBRARY_MINMAX_POS case */
	if (bbpversion <= GDKLIBRARY_MINMAX_POS ?
	    sscanf(buf,
		   " %10s %" SCNu16 " %" SCNu16 " %" SCNu16 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu16
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &size, &storage,
		   &n) < 12 :
	    sscanf(buf,
		   " %10s %" SCNu16 " %" SCNu16 " %" SCNu16 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu16 " %" SCNu64 " %" SCNu64
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &size, &storage, &minpos, &maxpos,
		   &n) < 14) {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
		return -1;
	}

	if (properties & ~0x0F81) {
		TRC_CRITICAL(GDK, "unknown properties are set: incompatible database on line %d of BBP.dir\n", lineno);
		return -1;
	}
	*hashash = var & 2;
	var &= ~2;
#ifdef HAVE_HGE
	if (strcmp(type, "hge") == 0)
		havehge = true;
#endif
	if ((t = ATOMindex(type)) < 0) {
		if ((t = ATOMunknown_find(type)) == 0) {
			TRC_CRITICAL(GDK, "no space for atom %s", type);
			return -1;
		}
	} else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL)) {
		TRC_CRITICAL(GDK, "inconsistent entry in BBP.dir: tvarsized mismatch for BAT %d on line %d\n", (int) bid, lineno);
		return -1;
	} else if (var && t != 0 ?
		   ATOMsize(t) < width ||
		   (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		    && width != 8
#endif
			   ) :
		   ATOMsize(t) != width) {
		TRC_CRITICAL(GDK, "inconsistent entry in BBP.dir: tsize mismatch for BAT %d on line %d\n", (int) bid, lineno);
		return -1;
	}
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
	b->tnonil = (properties & 0x0400) != 0;
	b->tnil = (properties & 0x0800) != 0;
	b->tnosorted = (BUN) nosorted;
	b->tnorevsorted = (BUN) norevsorted;
	/* (properties & 0x0200) is the old tdense flag */
	b->tseqbase = (properties & 0x0200) == 0 || base >= (uint64_t) oid_nil ? oid_nil : (oid) base;
	b->theap->free = (size_t) free;
	b->theap->size = (size_t) size;
	b->theap->base = NULL;
	settailname(b->theap, filename, t, width);
	b->theap->storage = (storage_t) storage;
	b->theap->newstorage = (storage_t) storage;
	b->theap->farmid = BBPselectfarm(PERSISTENT, b->ttype, offheap);
	b->theap->dirty = false;
	b->theap->parentid = b->batCacheid;
	if (minpos < b->batCount)
		BATsetprop(b, GDK_MIN_POS, TYPE_oid, &(oid){(oid)minpos});
	if (maxpos < b->batCount)
		BATsetprop(b, GDK_MAX_POS, TYPE_oid, &(oid){(oid)maxpos});
	if (b->theap->free > b->theap->size) {
		TRC_CRITICAL(GDK, "\"free\" value larger than \"size\" in heap of bat %d on line %d\n", (int) bid, lineno);
		return -1;
	}
	return n;
}

static int
vheapinit(BAT *b, const char *buf, int hashash, bat bid, const char *filename, int lineno)
{
	int n = 0;
	uint64_t free, size;
	uint16_t storage;

	if (b->tvarsized && b->ttype != TYPE_void) {
		b->tvheap = GDKzalloc(sizeof(Heap));
		if (b->tvheap == NULL) {
			TRC_CRITICAL(GDK, "cannot allocate memory for heap.");
			return -1;
		}
		if (sscanf(buf,
			   " %" SCNu64 " %" SCNu64 " %" SCNu16
			   "%n",
			   &free, &size, &storage, &n) < 3) {
			TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
			return -1;
		}
		b->tvheap->free = (size_t) free;
		b->tvheap->size = (size_t) size;
		b->tvheap->base = NULL;
		strconcat_len(b->tvheap->filename, sizeof(b->tvheap->filename),
			      filename, ".theap", NULL);
		b->tvheap->storage = (storage_t) storage;
		b->tvheap->hashash = hashash != 0;
		b->tvheap->cleanhash = true;
		b->tvheap->newstorage = (storage_t) storage;
		b->tvheap->dirty = false;
		b->tvheap->parentid = bid;
		b->tvheap->farmid = BBPselectfarm(PERSISTENT, b->ttype, varheap);
		ATOMIC_INIT(&b->tvheap->refs, 1);
		if (b->tvheap->free > b->tvheap->size) {
			TRC_CRITICAL(GDK, "\"free\" value larger than \"size\" in var heap of bat %d on line %d\n", (int) bid, lineno);
			return -1;
		}
	}
	return n;
}

static gdk_return
BBPreadEntries(FILE *fp, unsigned bbpversion, int lineno)
{
	bat bid = 0;
	char buf[4096];

	/* read the BBP.dir and insert the BATs into the BBP */
	while (fgets(buf, sizeof(buf), fp) != NULL) {
		BAT *bn;
		uint64_t batid;
		uint16_t status;
		char headname[129];
		char filename[sizeof(BBP_physical(0))];
		unsigned int properties;
		int nread, n;
		char *s, *options = NULL;
		char logical[1024];
		uint64_t count, capacity, base = 0;
		int Thashash;

		lineno++;
		if ((s = strchr(buf, '\r')) != NULL) {
			/* convert \r\n into just \n */
			if (s[1] != '\n') {
				TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
				return GDK_FAIL;
			}
			*s++ = '\n';
			*s = 0;
		}

		if (sscanf(buf,
			   "%" SCNu64 " %" SCNu16 " %128s %19s %u %" SCNu64
			   " %" SCNu64 " %" SCNu64
			   "%n",
			   &batid, &status, headname, filename,
			   &properties,
			   &count, &capacity, &base,
			   &nread) < 8) {
			TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
			return GDK_FAIL;
		}

		if (batid >= N_BBPINIT * BBPINIT) {
			TRC_CRITICAL(GDK, "bat ID (%" PRIu64 ") too large to accomodate (max %d), on line %d.", batid, N_BBPINIT * BBPINIT - 1, lineno);
			return GDK_FAIL;
		}

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

		bid = (bat) batid;
		if (batid >= (uint64_t) ATOMIC_GET(&BBPsize)) {
			ATOMIC_SET(&BBPsize, batid + 1);
			if ((bat) ATOMIC_GET(&BBPsize) >= BBPlimit)
				BBPextend(0, false);
		}
		if (BBP_desc(bid) != NULL) {
			TRC_CRITICAL(GDK, "duplicate entry in BBP.dir (ID = "
				     "%" PRIu64 ") on line %d.", batid, lineno);
			return GDK_FAIL;
		}
		if ((bn = GDKzalloc(sizeof(BAT))) == NULL ||
		    (bn->theap = GDKzalloc(sizeof(Heap))) == NULL) {
			GDKfree(bn);
			TRC_CRITICAL(GDK, "cannot allocate memory for BAT.");
			return GDK_FAIL;
		}
		bn->batCacheid = bid;
		if (BATroles(bn, NULL) != GDK_SUCCEED) {
			GDKfree(bn->theap);
			GDKfree(bn);
			TRC_CRITICAL(GDK, "BATroles failed.");
			return GDK_FAIL;
		}
		bn->batTransient = false;
		bn->batCopiedtodisk = true;
		bn->batRestricted = (properties & 0x06) >> 1;
		bn->batCount = (BUN) count;
		bn->batInserted = bn->batCount;
		bn->batCapacity = (BUN) capacity;
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "heaplock%d", bn->batCacheid); /* fits */
		MT_lock_init(&bn->theaplock, name);
		snprintf(name, sizeof(name), "BATlock%d", bn->batCacheid); /* fits */
		MT_lock_init(&bn->batIdxLock, name);
		snprintf(name, sizeof(name), "hashlock%d", bn->batCacheid); /* fits */
		MT_rwlock_init(&bn->thashlock, name);
		ATOMIC_INIT(&bn->theap->refs, 1);

		if (base > (uint64_t) GDK_oid_max) {
			BATdestroy(bn);
			TRC_CRITICAL(GDK, "head seqbase out of range (ID = %" PRIu64 ", seq = %" PRIu64 ") on line %d.", batid, base, lineno);
			return GDK_FAIL;
		}
		bn->hseqbase = (oid) base;
		n = heapinit(bn, buf + nread, &Thashash, bbpversion, bid, filename, lineno);
		if (n < 0) {
			BATdestroy(bn);
			return GDK_FAIL;
		}
		nread += n;
		n = vheapinit(bn, buf + nread, Thashash, bid, filename, lineno);
		if (n < 0) {
			BATdestroy(bn);
			return GDK_FAIL;
		}
		nread += n;

		if (buf[nread] != '\n' && buf[nread] != ' ') {
			BATdestroy(bn);
			TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
			return GDK_FAIL;
		}
		if (buf[nread] == ' ')
			options = buf + nread + 1;

		if (snprintf(BBP_bak(bid), sizeof(BBP_bak(bid)), "tmp_%o", (unsigned) bid) >= (int) sizeof(BBP_bak(bid))) {
			BATdestroy(bn);
			TRC_CRITICAL(GDK, "BBP logical filename directory is too large, on line %d\n", lineno);
			return GDK_FAIL;
		}
		if ((s = strchr(headname, '~')) != NULL && s == headname) {
			/* sizeof(logical) > sizeof(BBP_bak(bid)), so
			 * this fits */
			strcpy(logical, BBP_bak(bid));
		} else {
			if (s)
				*s = 0;
			strcpy_len(logical, headname, sizeof(logical));
		}
		if (strcmp(logical, BBP_bak(bid)) == 0) {
			BBP_logical(bid) = BBP_bak(bid);
		} else {
			BBP_logical(bid) = GDKstrdup(logical);
			if (BBP_logical(bid) == NULL) {
				BATdestroy(bn);
				TRC_CRITICAL(GDK, "GDKstrdup failed\n");
				return GDK_FAIL;
			}
		}
		/* tailname is ignored */
		strcpy_len(BBP_physical(bid), filename, sizeof(BBP_physical(bid)));
#ifdef __COVERITY__
		/* help coverity */
		BBP_physical(bid)[sizeof(BBP_physical(bid)) - 1] = 0;
#endif
		BBP_options(bid) = NULL;
		if (options) {
			BBP_options(bid) = GDKstrdup(options);
			if (BBP_options(bid) == NULL) {
				BATdestroy(bn);
				TRC_CRITICAL(GDK, "GDKstrdup failed\n");
				return GDK_FAIL;
			}
		}
		BBP_refs(bid) = 0;
		BBP_lrefs(bid) = 1;	/* any BAT we encounter here is persistent, so has a logical reference */
		BBP_desc(bid) = bn;
		BBP_status(bid) = BBPEXISTING;	/* do we need other status bits? */
	}
	return GDK_SUCCEED;
}

/* check that the necessary files for all BATs exist and are large
 * enough */
static gdk_return
BBPcheckbats(unsigned bbpversion)
{
	(void) bbpversion;
	for (bat bid = 1; bid < (bat) ATOMIC_GET(&BBPsize); bid++) {
		struct stat statb;
		BAT *b;
		char *path;

		if ((b = BBP_desc(bid)) == NULL) {
			/* not a valid BAT */
			continue;
		}
		if (b->ttype == TYPE_void) {
			/* no files needed */
			continue;
		}
		path = GDKfilepath(0, BATDIR, b->theap->filename, NULL);
		if (path == NULL)
			return GDK_FAIL;
#ifdef GDKLIBRARY_TAILN
		/* if bbpversion > GDKLIBRARY_TAILN, the offset heap can
		 * exist with either name .tail1 (etc) or .tail, if <=
		 * GDKLIBRARY_TAILN, only with .tail */
		char tailsave = 0;
		size_t taillen = 0;
		if (b->ttype == TYPE_str &&
		    b->twidth < SIZEOF_VAR_T) {
			/* old version: .tail, not .tail1, .tail2, .tail4 */
			taillen = strlen(path) - 1;
			tailsave = path[taillen];
			path[taillen] = 0;
		}
#endif
		if (MT_stat(path, &statb) < 0
#ifdef GDKLIBRARY_TAILN
		    && bbpversion > GDKLIBRARY_TAILN
		    && b->ttype == TYPE_str
		    && b->twidth < SIZEOF_VAR_T
		    && (path[taillen] = tailsave) != 0
		    && MT_stat(path, &statb) < 0
#endif
			) {

			GDKsyserror("cannot stat file %s (expected size %zu)\n",
				    path, b->theap->free);
			GDKfree(path);
			return GDK_FAIL;
		}
		if ((size_t) statb.st_size < b->theap->free) {
			GDKerror("file %s too small (expected %zu, actual %zu)\n", path, b->theap->free, (size_t) statb.st_size);
			GDKfree(path);
			return GDK_FAIL;
		}
		GDKfree(path);
		if (b->tvheap != NULL) {
			path = GDKfilepath(0, BATDIR, BBP_physical(b->batCacheid), "theap");
			if (path == NULL)
				return GDK_FAIL;
			if (MT_stat(path, &statb) < 0) {
				GDKsyserror("cannot stat file %s\n",
					    path);
				GDKfree(path);
				return GDK_FAIL;
			}
			if ((size_t) statb.st_size < b->tvheap->free) {
				GDKerror("file %s too small (expected %zu, actual %zu)\n", path, b->tvheap->free, (size_t) statb.st_size);
				GDKfree(path);
				return GDK_FAIL;
			}
			GDKfree(path);
		}
	}
	return GDK_SUCCEED;
}

#ifdef HAVE_HGE
#define SIZEOF_MAX_INT SIZEOF_HGE
#else
#define SIZEOF_MAX_INT SIZEOF_LNG
#endif

static unsigned
BBPheader(FILE *fp, int *lineno)
{
	char buf[BUFSIZ];
	int sz, ptrsize, oidsize, intsize;
	unsigned bbpversion;

	if (fgets(buf, sizeof(buf), fp) == NULL) {
		TRC_CRITICAL(GDK, "BBP.dir is empty");
		return 0;
	}
	++*lineno;
	if (sscanf(buf, "BBP.dir, GDKversion %u\n", &bbpversion) != 1) {
		GDKerror("old BBP without version number; "
			 "dump the database using a compatible version, "
			 "then restore into new database using this version.\n");
		return 0;
	}
	if (bbpversion != GDKLIBRARY &&
	    bbpversion != GDKLIBRARY_TAILN &&
	    bbpversion != GDKLIBRARY_MINMAX_POS) {
		TRC_CRITICAL(GDK, "incompatible BBP version: expected 0%o, got 0%o. "
			     "This database was probably created by a %s version of MonetDB.",
			     GDKLIBRARY, bbpversion,
			     bbpversion > GDKLIBRARY ? "newer" : "too old");
		return 0;
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		TRC_CRITICAL(GDK, "short BBP");
		return 0;
	}
	++*lineno;
	if (sscanf(buf, "%d %d %d", &ptrsize, &oidsize, &intsize) != 3) {
		TRC_CRITICAL(GDK, "BBP.dir has incompatible format: pointer, OID, and max. integer sizes are missing on line %d", *lineno);
		return 0;
	}
	if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_OID) {
		TRC_CRITICAL(GDK, "database created with incompatible server: "
			     "expected pointer size %d, got %d, expected OID size %d, got %d.",
			     SIZEOF_SIZE_T, ptrsize, SIZEOF_OID, oidsize);
		return 0;
	}
	if (intsize > SIZEOF_MAX_INT) {
		TRC_CRITICAL(GDK, "database created with incompatible server: "
			     "expected max. integer size %d, got %d.",
			     SIZEOF_MAX_INT, intsize);
		return 0;
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		TRC_CRITICAL(GDK, "short BBP");
		return 0;
	}
	++*lineno;
	if (sscanf(buf, "BBPsize=%d", &sz) != 1) {
		TRC_CRITICAL(GDK, "no BBPsize value found\n");
		return 0;
	}
	sz = (int) (sz * BATMARGIN);
	if (sz > (bat) ATOMIC_GET(&BBPsize))
		ATOMIC_SET(&BBPsize, sz);
	if (bbpversion > GDKLIBRARY_MINMAX_POS) {
		if (fgets(buf, sizeof(buf), fp) == NULL) {
			TRC_CRITICAL(GDK, "short BBP");
			return 0;
		}
		if (sscanf(buf, "BBPinfo=" LLSCN " " LLSCN, &BBPlogno, &BBPtransid) != 2) {
			TRC_CRITICAL(GDK, "no info value found\n");
			return 0;
		}
	}
	return bbpversion;
}

bool
GDKinmemory(int farmid)
{
	if (farmid == NOFARM)
		farmid = 0;
	assert(farmid >= 0 && farmid < MAXFARMS);
	return BBPfarms[farmid].dirname == NULL;
}

/* all errors are fatal */
gdk_return
BBPaddfarm(const char *dirname, uint32_t rolemask, bool logerror)
{
	struct stat st;
	int i;

	if (dirname == NULL) {
		assert(BBPfarms[0].dirname == NULL);
		assert(rolemask & 1);
		assert(BBPfarms[0].roles == 0);
		BBPfarms[0].roles = rolemask;
		return GDK_SUCCEED;
	}
	if (strchr(dirname, '\n') != NULL) {
		if (logerror)
			GDKerror("no newline allowed in directory name\n");
		return GDK_FAIL;
	}
	if (rolemask == 0 || (rolemask & 1 && BBPfarms[0].dirname != NULL)) {
		if (logerror)
			GDKerror("bad rolemask\n");
		return GDK_FAIL;
	}
	if (strcmp(dirname, ":memory:") == 0) {
		dirname = NULL;
	} else if (MT_mkdir(dirname) < 0) {
		if (errno == EEXIST) {
			if (MT_stat(dirname, &st) == -1 || !S_ISDIR(st.st_mode)) {
				if (logerror)
					GDKerror("%s: not a directory\n", dirname);
				return GDK_FAIL;
			}
		} else {
			if (logerror)
				GDKsyserror("%s: cannot create directory\n", dirname);
			return GDK_FAIL;
		}
	}
	for (i = 0; i < MAXFARMS; i++) {
		if (BBPfarms[i].roles == 0) {
			if (dirname) {
				BBPfarms[i].dirname = GDKstrdup(dirname);
				if (BBPfarms[i].dirname == NULL)
					return GDK_FAIL;
			}
			BBPfarms[i].roles = rolemask;
			if ((rolemask & 1) == 0 && dirname != NULL) {
				char *bbpdir;
				int j;

				for (j = 0; j < i; j++)
					if (BBPfarms[j].dirname != NULL &&
					    strcmp(BBPfarms[i].dirname,
						   BBPfarms[j].dirname) == 0)
						return GDK_SUCCEED;
				/* if an extra farm, make sure we
				 * don't find a BBP.dir there that
				 * might belong to an existing
				 * database */
				bbpdir = GDKfilepath(i, BATDIR, "BBP", "dir");
				if (bbpdir == NULL) {
					return GDK_FAIL;
				}
				if (MT_stat(bbpdir, &st) != -1 || errno != ENOENT) {
					GDKfree(bbpdir);
					if (logerror)
						GDKerror("%s is a database\n", dirname);
					return GDK_FAIL;
				}
				GDKfree(bbpdir);
				bbpdir = GDKfilepath(i, BAKDIR, "BBP", "dir");
				if (bbpdir == NULL) {
					return GDK_FAIL;
				}
				if (MT_stat(bbpdir, &st) != -1 || errno != ENOENT) {
					GDKfree(bbpdir);
					if (logerror)
						GDKerror("%s is a database\n", dirname);
					return GDK_FAIL;
				}
				GDKfree(bbpdir);
			}
			return GDK_SUCCEED;
		}
	}
	if (logerror)
		GDKerror("too many farms\n");
	return GDK_FAIL;
}

#ifdef GDKLIBRARY_TAILN
static gdk_return
movestrbats(void)
{
	for (bat bid = 1, nbat = (bat) ATOMIC_GET(&BBPsize); bid < nbat; bid++) {
		BAT *b = BBP_desc(bid);
		if (b == NULL) {
			/* not a valid BAT */
			continue;
		}
		if (b->ttype != TYPE_str || b->twidth == SIZEOF_VAR_T)
			continue;
		char *oldpath = GDKfilepath(0, BATDIR, BBP_physical(b->batCacheid), "tail");
		char *newpath = GDKfilepath(0, BATDIR, b->theap->filename, NULL);
		int ret = MT_rename(oldpath, newpath);
		GDKfree(oldpath);
		GDKfree(newpath);
		if (ret < 0)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}
#endif

gdk_return
BBPinit(void)
{
	FILE *fp = NULL;
	struct stat st;
	unsigned bbpversion = 0;
	int i;
	int lineno = 0;

	/* the maximum number of BATs allowed in the system and the
	 * size of the "physical" array are linked in a complicated
	 * manner.  The expression below shows the relationship */
	static_assert((uint64_t) N_BBPINIT * BBPINIT < (UINT64_C(1) << (3 * ((sizeof(BBP[0][0].physical) + 2) * 2 / 5))), "\"physical\" array in BBPrec is too small");
	/* similarly, the maximum number of BATs allowed also has a
	 * (somewhat simpler) relation with the size of the "bak"
	 * array */
	static_assert((uint64_t) N_BBPINIT * BBPINIT < (UINT64_C(1) << (3 * (sizeof(BBP[0][0].bak) - 5))), "\"bak\" array in BBPrec is too small");

	if (!GDKinmemory(0)) {
		str bbpdirstr, backupbbpdirstr;

		if (!(bbpdirstr = GDKfilepath(0, BATDIR, "BBP", "dir"))) {
			TRC_CRITICAL(GDK, "GDKmalloc failed\n");
			return GDK_FAIL;
		}

		if (!(backupbbpdirstr = GDKfilepath(0, BAKDIR, "BBP", "dir"))) {
			GDKfree(bbpdirstr);
			TRC_CRITICAL(GDK, "GDKmalloc failed\n");
			return GDK_FAIL;
		}

		if (GDKremovedir(0, TEMPDIR) != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot remove directory %s\n", TEMPDIR);
			return GDK_FAIL;
		}

		if (GDKremovedir(0, DELDIR) != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot remove directory %s\n", DELDIR);
			return GDK_FAIL;
		}

		/* first move everything from SUBDIR to BAKDIR (its parent) */
		if (BBPrecover_subdir() != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot properly recover_subdir process %s. Please check whether your disk is full or write-protected", SUBDIR);
			return GDK_FAIL;
		}

		/* try to obtain a BBP.dir from bakdir */
		if (MT_stat(backupbbpdirstr, &st) == 0) {
			/* backup exists; *must* use it */
			if (recover_dir(0, MT_stat(bbpdirstr, &st) == 0) != GDK_SUCCEED) {
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				goto bailout;
			}
			if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				TRC_CRITICAL(GDK, "cannot open recovered BBP.dir.");
				return GDK_FAIL;
			}
		} else if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
			/* there was no BBP.dir either. Panic! try to use a
			 * BBP.bak */
			if (MT_stat(backupbbpdirstr, &st) < 0) {
				/* no BBP.bak (nor BBP.dir or BACKUP/BBP.dir):
				 * create a new one */
				TRC_DEBUG(IO_, "initializing BBP.\n");
				if (BBPdir(0, NULL, NULL, LL_CONSTANT(0), LL_CONSTANT(0)) != GDK_SUCCEED) {
					GDKfree(bbpdirstr);
					GDKfree(backupbbpdirstr);
					goto bailout;
				}
			} else if (GDKmove(0, BATDIR, "BBP", "bak", BATDIR, "BBP", "dir") == GDK_SUCCEED)
				TRC_DEBUG(IO_, "reverting to dir saved in BBP.bak.\n");

			if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				goto bailout;
			}
		}
		assert(fp != NULL);
		GDKfree(bbpdirstr);
		GDKfree(backupbbpdirstr);
	}

	/* scan the BBP.dir to obtain current size */
	BBPlimit = 0;
	memset(BBP, 0, sizeof(BBP));
	ATOMIC_SET(&BBPsize, 1);

	if (GDKinmemory(0)) {
		bbpversion = GDKLIBRARY;
	} else {
		bbpversion = BBPheader(fp, &lineno);
		if (bbpversion == 0)
			return GDK_FAIL;
	}

	BBPextend(0, false);		/* allocate BBP records */

	if (!GDKinmemory(0)) {
		ATOMIC_SET(&BBPsize, 1);
		if (BBPreadEntries(fp, bbpversion, lineno) != GDK_SUCCEED)
			return GDK_FAIL;
		fclose(fp);
	}

	if (BBPinithash(0) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "BBPinithash failed");
		return GDK_FAIL;
	}

	/* will call BBPrecover if needed */
	if (!GDKinmemory(0) && BBPprepare(false) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "cannot properly prepare process %s. Please check whether your disk is full or write-protected", BAKDIR);
		return GDK_FAIL;
	}

	if (BBPcheckbats(bbpversion) != GDK_SUCCEED)
		return GDK_FAIL;

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
			if (d == NULL) {
				return GDK_FAIL;
			}
			BBPdiskscan(d, strlen(d) - strlen(BATDIR));
			GDKfree(d);
		}
	}

	if (bbpversion < GDKLIBRARY && TMcommit() != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "TMcommit failed\n");
		return GDK_FAIL;
	}
#ifdef GDKLIBRARY_TAILN
	/* we rename the offset heaps after the above commit: in this
	 * version we accept both the old and new names, but we want to
	 * convert so that future versions only have the new name */
	if (bbpversion <= GDKLIBRARY_TAILN) {
		/* note, if renaming fails, nothing is lost: a next
		 * invocation will just try again; an older version of
		 * mserver will not work because of the TMcommit
		 * above */
		if (movestrbats() != GDK_SUCCEED)
			return GDK_FAIL;
	}
#endif
	return GDK_SUCCEED;

  bailout:
	/* now it is time for real panic */
	TRC_CRITICAL(GDK, "could not write %s%cBBP.dir. Please check whether your disk is full or write-protected", BATDIR, DIR_SEP);
	return GDK_FAIL;
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
	bool skipped;

	BBPlock();	/* stop all threads ever touching more descriptors */

	/* free all memory (just for leak-checking in Purify) */
	do {
		skipped = false;
		for (i = 0; i < (bat) ATOMIC_GET(&BBPsize); i++) {
			if (BBPvalid(i)) {
				BAT *b = BBP_desc(i);

				if (b) {
					if (b->batSharecnt > 0) {
						skipped = true;
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
						PROPdestroy(b);
						BATfree(b);
					}
				}
				BBPuncacheit(i, true);
				if (BBP_logical(i) != BBP_bak(i))
					GDKfree(BBP_logical(i));
				BBP_logical(i) = NULL;
			}
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
heap_entry(FILE *fp, BAT *b, BUN size)
{
	const ValRecord *minprop, *maxprop;
	minprop = BATgetprop(b, GDK_MIN_POS);
	maxprop = BATgetprop(b, GDK_MAX_POS);
	size_t free = b->theap->free;
	if (size < BUN_NONE) {
		if ((b->ttype >= 0 && ATOMstorage(b->ttype) == TYPE_msk)) {
			BUN bytes = ((size + 31) / 32) * 4;
			if (free > bytes)
				free = bytes;
		} else if (b->twidth > 0 && free / b->twidth > size)
			free = size * b->twidth;
	}
	return fprintf(fp, " %s %d %d %d " BUNFMT " " BUNFMT " " BUNFMT " "
		       BUNFMT " " OIDFMT " %zu %zu %d " OIDFMT " " OIDFMT,
		       b->ttype >= 0 ? BATatoms[b->ttype].name : ATOMunknown_name(b->ttype),
		       b->twidth,
		       b->tvarsized | (b->tvheap ? b->tvheap->hashash << 1 : 0),
		       (unsigned short) b->tsorted |
			   ((unsigned short) b->trevsorted << 7) |
			   (((unsigned short) b->tkey & 0x01) << 8) |
		           ((unsigned short) BATtdense(b) << 9) |
			   ((unsigned short) b->tnonil << 10) |
			   ((unsigned short) b->tnil << 11),
		       b->tnokey[0] >= size || b->tnokey[1] >= size ? 0 : b->tnokey[0],
		       b->tnokey[0] >= size || b->tnokey[1] >= size ? 0 : b->tnokey[1],
		       b->tnosorted >= size ? 0 : b->tnosorted,
		       b->tnorevsorted >= size ? 0 : b->tnorevsorted,
		       b->tseqbase,
		       free,
		       b->theap->size,
		       (int) b->theap->newstorage,
		       minprop ? minprop->val.oval : oid_nil,
		       maxprop ? maxprop->val.oval : oid_nil);
}

static inline int
vheap_entry(FILE *fp, Heap *h)
{
	if (h == NULL)
		return 0;
	return fprintf(fp, " %zu %zu %d",
		       h->free, h->size, (int) h->newstorage);
}

static gdk_return
new_bbpentry(FILE *fp, bat i, BUN size)
{
#ifndef NDEBUG
	assert(i > 0);
	assert(i < (bat) ATOMIC_GET(&BBPsize));
	assert(BBP_desc(i));
	assert(BBP_desc(i)->batCacheid == i);
	assert(BBP_desc(i)->batRole == PERSISTENT);
	assert(0 <= BBP_desc(i)->theap->farmid && BBP_desc(i)->theap->farmid < MAXFARMS);
	assert(BBPfarms[BBP_desc(i)->theap->farmid].roles & (1U << PERSISTENT));
	if (BBP_desc(i)->tvheap) {
		assert(0 <= BBP_desc(i)->tvheap->farmid && BBP_desc(i)->tvheap->farmid < MAXFARMS);
		assert(BBPfarms[BBP_desc(i)->tvheap->farmid].roles & (1U << PERSISTENT));
	}
#endif

	if (size > BBP_desc(i)->batCount)
		size = BBP_desc(i)->batCount;
	if (fprintf(fp, "%d %u %s %s %d " BUNFMT " " BUNFMT " " OIDFMT,
		    /* BAT info */
		    (int) i,
		    BBP_status(i) & BBPPERSISTENT,
		    BBP_logical(i),
		    BBP_physical(i),
		    BBP_desc(i)->batRestricted << 1,
		    size,
		    BBP_desc(i)->batCapacity,
		    BBP_desc(i)->hseqbase) < 0 ||
	    heap_entry(fp, BBP_desc(i), size) < 0 ||
	    vheap_entry(fp, BBP_desc(i)->tvheap) < 0 ||
	    (BBP_options(i) && fprintf(fp, " %s", BBP_options(i)) < 0) ||
	    fprintf(fp, "\n") < 0) {
		GDKsyserror("new_bbpentry: Writing BBP.dir entry failed\n");
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

static gdk_return
BBPdir_header(FILE *f, int n, lng logno, lng transid)
{
	if (fprintf(f, "BBP.dir, GDKversion %u\n%d %d %d\nBBPsize=%d\nBBPinfo=" LLFMT " " LLFMT "\n",
		    GDKLIBRARY, SIZEOF_SIZE_T, SIZEOF_OID,
#ifdef HAVE_HGE
		    havehge ? SIZEOF_HGE :
#endif
		    SIZEOF_LNG, n, logno, transid) < 0 ||
	    ferror(f)) {
		GDKsyserror("Writing BBP.dir header failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static gdk_return
BBPdir_subcommit(int cnt, bat *restrict subcommit, BUN *restrict sizes, lng logno, lng transid)
{
	FILE *obbpf, *nbbpf;
	bat j = 1;
	char buf[3000];
	int n;
	lng ologno, otransid;

#ifndef NDEBUG
	assert(subcommit != NULL);
	for (n = 2; n < cnt; n++)
		assert(subcommit[n - 1] < subcommit[n]);
#endif

	if ((nbbpf = GDKfilelocate(0, "BBP", "w", "dir")) == NULL)
		return GDK_FAIL;

	/* we need to copy the backup BBP.dir to the new, but
	 * replacing the entries for the subcommitted bats */
	if ((obbpf = GDKfileopen(0, SUBDIR, "BBP", "dir", "r")) == NULL &&
	    (obbpf = GDKfileopen(0, BAKDIR, "BBP", "dir", "r")) == NULL) {
		GDKsyserror("subcommit attempted without backup BBP.dir.");
		goto bailout;
	}
	/* read first three lines */
	if (fgets(buf, sizeof(buf), obbpf) == NULL || /* BBP.dir, GDKversion %d */
	    fgets(buf, sizeof(buf), obbpf) == NULL || /* SIZEOF_SIZE_T SIZEOF_OID SIZEOF_MAX_INT */
	    fgets(buf, sizeof(buf), obbpf) == NULL) { /* BBPsize=%d */
		GDKerror("subcommit attempted with invalid backup BBP.dir.");
		goto bailout;
	}
	/* third line contains BBPsize */
	if (sscanf(buf, "BBPsize=%d", &n) != 1) {
		GDKerror("cannot read BBPsize in backup BBP.dir.");
		goto bailout;
	}
	if (n < (bat) ATOMIC_GET(&BBPsize))
		n = (bat) ATOMIC_GET(&BBPsize);
	/* fourth line contains BBPinfo */
	if (fgets(buf, sizeof(buf), obbpf) == NULL ||
	    sscanf(buf, "BBPinfo=" LLSCN " " LLSCN, &ologno, &otransid) != 2) {
		GDKerror("cannot read BBPinfo in backup BBP.dir.");
		goto bailout;
	}

	TRC_DEBUG(IO_, "writing BBP.dir (%d bats).\n", n);

	if (BBPdir_header(nbbpf, n, logno, transid) != GDK_SUCCEED) {
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
			} else if (sscanf(buf, "%d", &n) != 1 || n <= 0) {
				GDKerror("subcommit attempted with invalid backup BBP.dir.");
				goto bailout;
			}
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
				if (new_bbpentry(nbbpf, i, sizes ? sizes[j] : BUN_NONE) != GDK_SUCCEED) {
					goto bailout;
				}
			}
			if (i == n)
				n = 0;	/* read new entry (i.e. skip this one from old BBP.dir */
			do
				/* go to next, skipping duplicates */
				j++;
			while (j < cnt && subcommit[j] == i);
		} else {
			if (fprintf(nbbpf, "%s", buf) < 0) {
				GDKsyserror("Copying BBP.dir entry failed\n");
				goto bailout;
			}
			TRC_DEBUG(IO_, "%s", buf);
			n = 0;
		}
	}

	if (fflush(nbbpf) == EOF ||
	    (!(GDKdebug & NOSYNCMASK)
#if defined(NATIVE_WIN32)
	     && _commit(_fileno(nbbpf)) < 0
#elif defined(HAVE_FDATASYNC)
	     && fdatasync(fileno(nbbpf)) < 0
#elif defined(HAVE_FSYNC)
	     && fsync(fileno(nbbpf)) < 0
#endif
		    )) {
		GDKsyserror("Syncing BBP.dir file failed\n");
		goto bailout;
	}
	if (fclose(nbbpf) == EOF) {
		GDKsyserror("Closing BBP.dir file failed\n");
		goto bailout;
	}

	TRC_DEBUG(IO_, "end\n");

	return GDK_SUCCEED;

  bailout:
	if (obbpf != NULL)
		fclose(obbpf);
	if (nbbpf != NULL)
		fclose(nbbpf);
	return GDK_FAIL;
}

gdk_return
BBPdir(int cnt, bat *restrict subcommit, BUN *restrict sizes, lng logno, lng transid)
{
	FILE *fp;
	bat i;

	if (subcommit)
		return BBPdir_subcommit(cnt, subcommit, sizes, logno, transid);

	TRC_DEBUG(IO_, "writing BBP.dir (%d bats).\n", (int) (bat) ATOMIC_GET(&BBPsize));
	if ((fp = GDKfilelocate(0, "BBP", "w", "dir")) == NULL) {
		goto bailout;
	}

	if (BBPdir_header(fp, (bat) ATOMIC_GET(&BBPsize), logno, transid) != GDK_SUCCEED) {
		goto bailout;
	}

	for (i = 1; i < (bat) ATOMIC_GET(&BBPsize); i++) {
		/* write the entry
		 * BBP.dir consists of all persistent bats */
		if (BBP_status(i) & BBPPERSISTENT) {
			if (new_bbpentry(fp, i, BUN_NONE) != GDK_SUCCEED) {
				goto bailout;
			}
		}
	}

	if (fflush(fp) == EOF ||
	    (!(GDKdebug & NOSYNCMASK)
#if defined(NATIVE_WIN32)
	     && _commit(_fileno(fp)) < 0
#elif defined(HAVE_FDATASYNC)
	     && fdatasync(fileno(fp)) < 0
#elif defined(HAVE_FSYNC)
	     && fsync(fileno(fp)) < 0
#endif
		    )) {
		GDKsyserror("Syncing BBP.dir file failed\n");
		goto bailout;
	}
	if (fclose(fp) == EOF) {
		GDKsyserror("Closing BBP.dir file failed\n");
		return GDK_FAIL;
	}

	TRC_DEBUG(IO_, "end\n");

	if (i < (bat) ATOMIC_GET(&BBPsize))
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

	for (i = 0; i < (bat) ATOMIC_GET(&BBPsize); i++) {
		if (BBP_refs(i) == 0 && BBP_lrefs(i) == 0)
			continue;
		BAT *b = BBP_desc(i);
		fprintf(stderr,
			"# %d: " ALGOOPTBATFMT " "
			"refs=%d lrefs=%d "
			"status=%u%s",
			i,
			ALGOOPTBATPAR(b),
			BBP_refs(i),
			BBP_lrefs(i),
			BBP_status(i),
			BBP_cache(i) ? "" : " not cached");
		if (b->batSharecnt > 0)
			fprintf(stderr, " shares=%d", b->batSharecnt);
		if (b->batDirtydesc)
			fprintf(stderr, " DirtyDesc");
		if (b->theap) {
			if (b->theap->parentid != b->batCacheid) {
				fprintf(stderr, " Theap -> %d", b->theap->parentid);
			} else {
				fprintf(stderr,
					" Theap=[%zu,%zu,f=%d]%s",
					HEAPmemsize(b->theap),
					HEAPvmsize(b->theap),
					b->theap->farmid,
					b->theap->dirty ? "(Dirty)" : "");
				if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
					cmem += HEAPmemsize(b->theap);
					cvm += HEAPvmsize(b->theap);
					nc++;
				} else {
					mem += HEAPmemsize(b->theap);
					vm += HEAPvmsize(b->theap);
					n++;
				}
			}
		}
		if (b->tvheap) {
			if (b->tvheap->parentid != b->batCacheid) {
				fprintf(stderr,
					" Tvheap -> %d",
					b->tvheap->parentid);
			} else {
				fprintf(stderr,
					" Tvheap=[%zu,%zu,f=%d]%s",
					HEAPmemsize(b->tvheap),
					HEAPvmsize(b->tvheap),
					b->tvheap->farmid,
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
		if (b->thash && b->thash != (Hash *) 1) {
			size_t m = HEAPmemsize(&b->thash->heaplink) + HEAPmemsize(&b->thash->heapbckt);
			size_t v = HEAPvmsize(&b->thash->heaplink) + HEAPvmsize(&b->thash->heapbckt);
			fprintf(stderr, " Thash=[%zu,%zu,f=%d/%d]", m, v,
				b->thash->heaplink.farmid,
				b->thash->heapbckt.farmid);
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += m;
				cvm += v;
			} else {
				mem += m;
				vm += v;
			}
		}
		fprintf(stderr, " role: %s\n",
			b->batRole == PERSISTENT ? "persistent" : "transient");
	}
	fprintf(stderr,
		"# %d bats: mem=%zu, vm=%zu %d cached bats: mem=%zu, vm=%zu\n",
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
BBP_find(const char *nme, bool lock)
{
	bat i = BBPnamecheck(nme);

	if (i != 0) {
		/* for tmp_X BATs, we already know X */
		const char *s;

		if (i >= (bat) ATOMIC_GET(&BBPsize) || (s = BBP_logical(i)) == NULL || strcmp(s, nme)) {
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
	return BBP_find(nme, true);
}

BAT *
BBPgetdesc(bat i)
{
	if (is_bat_nil(i))
		return NULL;
	if (i < 0)
		i = -i;
	if (i != 0 && i < (bat) ATOMIC_GET(&BBPsize) && i && BBP_logical(i)) {
		return BBP_desc(i);
	}
	return NULL;
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
		if ((bat) ATOMIC_ADD(&BBPsize, 1) >= BBPlimit) {
			if (BBPextend(idx, true) != GDK_SUCCEED) {
				/* undo add */
				ATOMIC_SUB(&BBPsize, 1);
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
			BBP_free(idx) = (bat) ATOMIC_GET(&BBPsize) - 1;
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
	bool lock = locked_by == 0 || locked_by != pid;
	char dirname[24];
	bat i;
	int idx = threadmask(pid), len = 0;

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
		havehge = true;
#endif

	if (*BBP_bak(i) == 0)
		len = snprintf(BBP_bak(i), sizeof(BBP_bak(i)), "tmp_%o", (unsigned) i);
	if (len == -1 || len >= FILENAME_MAX)
		return 0;
	BBP_logical(i) = BBP_bak(i);

	/* Keep the physical location around forever */
	if (!GDKinmemory(0) && *BBP_physical(i) == 0) {
		BBPgetsubdir(dirname, i);

		if (*dirname)	/* i.e., i >= 0100 */
			len = snprintf(BBP_physical(i), sizeof(BBP_physical(i)),
				       "%s%c%o", dirname, DIR_SEP, (unsigned) i);
		else
			len = snprintf(BBP_physical(i), sizeof(BBP_physical(i)),
				       "%o", (unsigned) i);
		if (len == -1 || len >= FILENAME_MAX)
			return 0;

		TRC_DEBUG(BAT_, "%d = new %s(%s)\n", (int) i, BBPname(i), ATOMname(bn->ttype));
	}

	return i;
}

gdk_return
BBPcacheit(BAT *bn, bool lock)
{
	bat i = bn->batCacheid;
	unsigned mode;

	if (lock)
		lock = locked_by == 0 || locked_by != MT_getpid();

	if (i) {
		assert(i > 0);
	} else {
		i = BBPinsert(bn);	/* bat was not previously entered */
		if (i == 0)
			return GDK_FAIL;
		if (bn->theap)
			bn->theap->parentid = i;
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
BBPuncacheit(bat i, bool unloaddesc)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(i, "BBPuncacheit")) {
		BAT *b = BBP_desc(i);

		if (b) {
			if (BBP_cache(i)) {
				TRC_DEBUG(BAT_, "uncache %d (%s)\n", (int) i, BBPname(i));

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
bbpclear(bat i, int idx, bool lock)
{
	TRC_DEBUG(BAT_, "clear %d (%s)\n", (int) i, BBPname(i));
	BBPuncacheit(i, true);
	TRC_DEBUG(BAT_, "set to unloading %d\n", i);
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
	bool lock = locked_by == 0 || locked_by != pid;

	if (BBPcheck(i, "BBPclear")) {
		bbpclear(i, threadmask(pid), lock);
	}
}

/*
 * @- BBP rename
 *
 * Each BAT has a logical name that is globally unique.
 * The batId is the same as the logical BAT name.
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
	char dirname[24];
	bat tmpid = 0, i;
	int idx;

	if (b == NULL)
		return 0;

	if (nme == NULL) {
		if (BBP_bak(bid)[0] == 0 &&
		    snprintf(BBP_bak(bid), sizeof(BBP_bak(bid)), "tmp_%o", (unsigned) bid) >= (int) sizeof(BBP_bak(bid))) {
			/* cannot happen */
			TRC_CRITICAL(GDK, "BBP default filename too long\n");
			return BBPRENAME_LONG;
		}
		nme = BBP_bak(bid);
	}

	/* If name stays same, do nothing */
	if (BBP_logical(bid) && strcmp(BBP_logical(bid), nme) == 0)
		return 0;

	BBPgetsubdir(dirname, bid);

	if ((tmpid = BBPnamecheck(nme)) && tmpid != bid) {
		GDKerror("illegal temporary name: '%s'\n", nme);
		return BBPRENAME_ILLEGAL;
	}
	if (strlen(dirname) + strLen(nme) + 1 >= IDLENGTH) {
		GDKerror("illegal temporary name: '%s'\n", nme);
		return BBPRENAME_LONG;
	}

	idx = threadmask(MT_getpid());
	MT_lock_set(&GDKtrimLock(idx));
	MT_lock_set(&GDKnameLock);
	i = BBP_find(nme, false);
	if (i != 0) {
		MT_lock_unset(&GDKnameLock);
		MT_lock_unset(&GDKtrimLock(idx));
		GDKerror("name is in use: '%s'.\n", nme);
		return BBPRENAME_ALREADY;
	}

	char *nnme;
	if (nme == BBP_bak(bid) || strcmp(nme, BBP_bak(bid)) == 0) {
		nnme = BBP_bak(bid);
	} else {
		nnme = GDKstrdup(nme);
		if (nnme == NULL) {
			MT_lock_unset(&GDKnameLock);
			MT_lock_unset(&GDKtrimLock(idx));
			return BBPRENAME_MEMORY;
		}
	}

	/* carry through the name change */
	if (BBP_logical(bid) && BBPtmpcheck(BBP_logical(bid)) == 0) {
		BBP_delete(bid);
	}
	if (BBP_logical(bid) != BBP_bak(bid))
		GDKfree(BBP_logical(bid));
	BBP_logical(bid) = nnme;
	if (tmpid == 0) {
		BBP_insert(bid);
	}
	b->batDirtydesc = true;
	if (!b->batTransient) {
		bool lock = locked_by == 0 || locked_by != MT_getpid();

		if (lock)
			MT_lock_set(&GDKswapLock(i));
		BBP_status_on(bid, BBPRENAMED, "BBPrename");
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
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
BBPspin(bat i, const char *s, unsigned event)
{
	if (BBPcheck(i, "BBPspin") && (BBP_status(i) & event)) {
		lng spin = LL_CONSTANT(0);

		do {
			MT_sleep_ms(KITTENNAP);
			spin++;
		} while (BBP_status(i) & event);
		TRC_DEBUG(BAT_, "%d,%s,%u: " LLFMT " loops\n", (int) i, s, event, spin);
	}
}

/* This function can fail if the input parameter (i) is incorrect
 * (unlikely), of if the bat is a view, this is a physical (not
 * logical) incref (i.e. called through BBPfix(), and it is the first
 * reference (refs was 0 and should become 1).  It can fail in this
 * case if the parent bat cannot be loaded.
 * This means the return value of BBPfix should be checked in these
 * circumstances, but not necessarily in others. */
static inline int
incref(bat i, bool logical, bool lock)
{
	int refs;
	bat tp, tvp;
	BAT *b, *pb = NULL, *pvb = NULL;
	bool load = false;

	if (!BBPcheck(i, logical ? "BBPretain" : "BBPfix"))
		return 0;

	/* Before we get the lock and before we do all sorts of
	 * things, make sure we can load the parent bats if there are
	 * any.  If we can't load them, we can still easily fail.  If
	 * this is indeed a view, but not the first physical
	 * reference, getting the parent BAT descriptor is
	 * superfluous, but not too expensive, so we do it anyway. */
	if (!logical && (b = BBP_desc(i)) != NULL) {
		if (b->theap->parentid != i) {
			pb = BATdescriptor(b->theap->parentid);
			if (pb == NULL)
				return 0;
		}
		if (b->tvheap && b->tvheap->parentid != i) {
			pvb = BATdescriptor(b->tvheap->parentid);
			if (pvb == NULL) {
				if (pb)
					BBPunfix(pb->batCacheid);
				return 0;
			}
		}
	}

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
		tp = b->theap->parentid == i ? 0 : b->theap->parentid;
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
			load = true;
		}
	}
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	if (load) {
		/* load the parent BATs */
		assert(!logical);
		if (tp) {
			assert(pb != NULL);
			if (b->theap != pb->theap) {
				HEAPincref(pb->theap);
				HEAPdecref(b->theap, false);
			}
			b->theap = pb->theap;
		}
		/* done loading, release descriptor */
		BBP_status_off(i, BBPLOADING, "BBPfix");
	} else if (!logical) {
		/* this wasn't the first physical reference, so undo
		 * the fixes on the parent bats */
		if (pb)
			BBPunfix(pb->batCacheid);
		if (pvb)
			BBPunfix(pvb->batCacheid);
	}
	return refs;
}

/* see comment for incref */
int
BBPfix(bat i)
{
	bool lock = locked_by == 0 || locked_by != MT_getpid();

	return incref(i, false, lock);
}

int
BBPretain(bat i)
{
	bool lock = locked_by == 0 || locked_by != MT_getpid();

	return incref(i, true, lock);
}

void
BBPshare(bat parent)
{
	bool lock = locked_by == 0 || locked_by != MT_getpid();

	assert(parent > 0);
	(void) incref(parent, true, lock);
	if (lock)
		MT_lock_set(&GDKswapLock(parent));
	++BBP_cache(parent)->batSharecnt;
	assert(BBP_refs(parent) > 0);
	if (lock)
		MT_lock_unset(&GDKswapLock(parent));
	(void) incref(parent, false, lock);
}

static inline int
decref(bat i, bool logical, bool releaseShare, bool lock, const char *func)
{
	int refs = 0;
	bool swap = false;
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
			assert(b == NULL || b->theap == NULL || BBP_refs(b->theap->parentid) > 0);
			assert(b == NULL || b->tvheap == NULL || BBP_refs(b->tvheap->parentid) > 0);
			refs = --BBP_refs(i);
			if (b && refs == 0) {
				tp = VIEWtparent(b);
				tvp = VIEWvtparent(b);
			}
		}
	}

	/* Make sure we do not unload bats which have more rows than marked persistent */
	if (b && BBP_lrefs(i) > 0 && DELTAdirty(b)) {
		b->batDirtydesc = true;
		b->theap->dirty = true;
		if (b->tvheap)
			b->tvheap->dirty = true;
	}

	/* we destroy transients asap and unload persistent bats only
	 * if they have been made cold or are not dirty */
	if (BBP_refs(i) > 0 ||
	    (BBP_lrefs(i) > 0 &&
	     (b == NULL || BATdirty(b) || !(BBP_status(i) & BBPPERSISTENT) || GDKinmemory(b->theap->farmid)))) {
		/* bat cannot be swapped out */
	} else if (b ? b->batSharecnt == 0 : (BBP_status(i) & BBPTMP)) {
		/* bat will be unloaded now. set the UNLOADING bit
		 * while locked so no other thread thinks it's
		 * available anymore */
		assert((BBP_status(i) & BBPUNLOADING) == 0);
		TRC_DEBUG(BAT_, "%s set to unloading BAT %d\n", func, i);
		BBP_status_on(i, BBPUNLOADING, func);
		assert(!b || BBP_lrefs(i) == 0 || !DELTAdirty(b));
		swap = true;
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
			TRC_DEBUG(BAT_, "%s unload and free bat %d\n", func, i);
			/* free memory of transient */
			if (BBPfree(b, func) != GDK_SUCCEED)
				return -1;	/* indicate failure */
		}
	}
	if (tp)
		decref(tp, false, false, lock, func);
	if (tvp)
		decref(tvp, false, false, lock, func);
	return refs;
}

int
BBPunfix(bat i)
{
	if (BBPcheck(i, "BBPunfix") == 0) {
		return -1;
	}
	return decref(i, false, false, true, "BBPunfix");
}

int
BBPrelease(bat i)
{
	if (BBPcheck(i, "BBPrelease") == 0) {
		return -1;
	}
	return decref(i, true, false, true, "BBPrelease");
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
	if (is_bat_nil(i))
		return;
	if (BBPcheck(i, "BBPkeepref")) {
		bool lock = locked_by == 0 || locked_by != MT_getpid();
		BAT *b;

		if ((b = BBPdescriptor(i)) != NULL) {
			BATsetaccess(b, BAT_READ);
			BATsettrivprop(b);
			if (GDKdebug & (CHECKMASK | PROPMASK))
				BATassertProps(b);
		}

		incref(i, true, lock);
		assert(BBP_refs(i));
		decref(i, false, false, lock, "BBPkeepref");
	}
}

static inline void
GDKunshare(bat parent)
{
	(void) decref(parent, false, true, true, "GDKunshare");
	(void) decref(parent, true, false, true, "GDKunshare");
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
	bool lock = locked_by == 0 || locked_by != MT_getpid();

	if (b == NULL)
		return -1;
	i = b->batCacheid;

	assert(BBP_refs(i) == 1);

	return decref(i, false, false, lock, "BBPreclaim") <0;
}

/*
 * BBPdescriptor checks whether BAT needs loading and does so if
 * necessary. You must have at least one fix on the BAT before calling
 * this.
 */
static BAT *
getBBPdescriptor(bat i, bool lock)
{
	bool load = false;
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
				load = true;
				TRC_DEBUG(BAT_, "set to loading BAT %d\n", i);
				BBP_status_on(i, BBPLOADING, "BBPdescriptor");
			}
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
	}
	if (load) {
		TRC_DEBUG(IO_, "load %s\n", BBPname(i));

		b = BATload_intern(i, lock);

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
	bool lock = locked_by == 0 || locked_by != MT_getpid();

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
	bool lock = locked_by == 0 || locked_by != MT_getpid();
	bat bid = b->batCacheid;
	gdk_return ret = GDK_SUCCEED;

	if (BBP_lrefs(bid) == 0 || isVIEW(b) || !BATdirtydata(b)) {
		/* do nothing */
		if (b->thash && b->thash != (Hash *) 1 &&
		    (b->thash->heaplink.dirty || b->thash->heapbckt.dirty))
			BAThashsave(b, (BBP_status(bid) & BBPPERSISTENT) != 0);
		return GDK_SUCCEED;
	}
	if (lock)
		MT_lock_set(&GDKswapLock(bid));

	if (BBP_status(bid) & BBPSAVING) {
		/* wait until save in other thread completes */
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));
		BBPspin(bid, "BBPsave", BBPSAVING);
	} else {
		/* save it */
		unsigned flags = BBPSAVING;

		if (DELTAdirty(b)) {
			flags |= BBPSWAPPED;
		}
		if (b->batTransient) {
			flags |= BBPTMP;
		}
		BBP_status_on(bid, flags, "BBPsave");
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));

		TRC_DEBUG(IO_, "save %s\n", BATgetId(b));

		/* do the time-consuming work unlocked */
		if (BBP_status(bid) & BBPEXISTING)
			ret = BBPbackup(b, false);
		if (ret == GDK_SUCCEED) {
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
	bat tp = VIEWtparent(b);
	bat vtp = VIEWvtparent(b);

	if (isVIEW(b)) {	/* a physical view */
		VIEWdestroy(b);
	} else {
		/* bats that get destroyed must unfix their atoms */
		gdk_return (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		assert(b->batSharecnt == 0);
		if (tunfix) {
			BUN p, q;
			BATiter bi = bat_iterator(b);

			BATloop(b, p, q) {
				/* ignore errors */
				(void) (*tunfix)(BUNtail(bi, p));
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

	BBP_unload_inc();
	/* write dirty BATs before being unloaded */
	ret = BBPsave(b);
	if (ret == GDK_SUCCEED) {
		if (isVIEW(b)) {	/* physical view */
			VIEWdestroy(b);
		} else {
			if (BBP_cache(bid))
				BATfree(b);	/* free memory */
		}
		BBPuncacheit(bid, false);
	}
	/* clearing bits can be done without the lock */
	TRC_DEBUG(BAT_, "turn off unloading %d\n", bid);
	assert(!b || BBP_lrefs(bid) == 0 || !DELTAdirty(b));
	BBP_status_off(bid, BBPUNLOADING, calledFrom);
	BBP_unload_dec();

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
static bool
complexatom(int t, bool delaccess)
{
	if (t >= 0 && (BATatoms[t].atomFix || (delaccess && BATatoms[t].atomDel))) {
		return true;
	}
	return false;
}

BAT *
BBPquickdesc(bat bid, bool delaccess)
{
	BAT *b;

	if (is_bat_nil(bid))
		return NULL;
	if (bid < 0) {
		GDKerror("called with negative batid.\n");
		assert(0);
		return NULL;
	}
	if ((b = BBP_cache(bid)) != NULL)
		return b;	/* already cached */
	b = (BAT *) BBPgetdesc(bid);
	if (b == NULL ||
	    complexatom(b->ttype, delaccess)) {
		b = BATload_intern(bid, true);
	}
	return b;
}

/*
 * @+ Global Commit
 */
static BAT *
dirty_bat(bat *i, bool subcommit)
{
	if (BBPvalid(*i)) {
		BAT *b;
		BBPspin(*i, "dirty_bat", BBPSAVING);
		b = BBP_cache(*i);
		if (b != NULL) {
			if ((BBP_status(*i) & BBPNEW) &&
			    BATcheckmodes(b, false) != GDK_SUCCEED) /* check mmap modes */
				*i = 0;	/* error */
			if ((BBP_status(*i) & BBPPERSISTENT) &&
			    (subcommit || BATdirty(b)))
				return b;	/* the bat is loaded, persistent and dirty */
		} else if (BBP_status(*i) & BBPSWAPPED) {
			b = (BAT *) BBPquickdesc(*i, true);
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
		if (MT_stat(path, &st)) {
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

/* returns true if the file exists */
static bool
file_exists(int farmid, const char *dir, const char *name, const char *ext)
{
	char *path;
	struct stat st;
	int ret = -1;

	path = GDKfilepath(farmid, dir, name, ext);
	if (path) {
		ret = MT_stat(path, &st);
		TRC_DEBUG(IO_, "stat(%s) = %d\n", path, ret);
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
	} else if (hp->newstorage == STORE_PRIV &&
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

		strconcat_len(kill_ext, sizeof(kill_ext), ext, ".kill", NULL);
		path = GDKfilepath(hp->farmid, dstdir, nme, kill_ext);
		if (path == NULL)
			return GDK_FAIL;
		fp = MT_fopen(path, "w");
		if (fp == NULL)
			GDKsyserror("heap_move: cannot open file %s\n", path);
		TRC_DEBUG(IO_, "open %s = %d\n", path, fp ? 0 : -1);
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
BBPprepare(bool subcommit)
{
	bool start_subcommit;
	int set = 1 + subcommit;
	str bakdirpath, subdirpath;
	gdk_return ret = GDK_SUCCEED;

	if(!(bakdirpath = GDKfilepath(0, NULL, BAKDIR, NULL)))
		return GDK_FAIL;
	if(!(subdirpath = GDKfilepath(0, NULL, SUBDIR, NULL))) {
		GDKfree(bakdirpath);
		return GDK_FAIL;
	}

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
			if (MT_mkdir(bakdirpath) < 0 && errno != EEXIST) {
				GDKsyserror("cannot create directory %s\n", bakdirpath);
				ret = GDK_FAIL;
			}
			/* if BAKDIR already exists, don't signal error */
			TRC_DEBUG(IO_, "mkdir %s = %d\n", bakdirpath, (int) ret);
		}
	}
	if (ret == GDK_SUCCEED && start_subcommit) {
		/* make a new SUBDIR (subdir of BAKDIR) */
		if (MT_mkdir(subdirpath) < 0) {
			GDKsyserror("cannot create directory %s\n", subdirpath);
			ret = GDK_FAIL;
		}
		TRC_DEBUG(IO_, "mkdir %s = %d\n", subdirpath, (int) ret);
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
	  Heap *h, bool dirty, bool subcommit)
{
	gdk_return ret = GDK_SUCCEED;
	char extnew[16];
	bool istail = strncmp(ext, "tail", 4) == 0;

	/* direct mmap is unprotected (readonly usage, or has WAL
	 * protection); however, if we're backing up for subcommit
	 * and a backup already exists in the main backup directory
	 * (see GDKupgradevarheap), move the file */
	if (subcommit) {
		strcpy_len(extnew, ext, sizeof(extnew));
		char *p = extnew + strlen(extnew) - 1;
		if (*p == 'l') {
			p++;
			p[1] = 0;
		}
		bool exists;
		for (;;) {
			exists = file_exists(h->farmid, BAKDIR, nme, extnew);
			if (exists)
				break;
			if (!istail)
				break;
			if (*p == '1')
				break;
			if (*p == '2')
				*p = '1';
#if SIZEOF_VAR_T == 8
			else if (*p != '4')
				*p = '4';
#endif
			else
				*p = '2';
		}
		if (exists &&
		    file_move(h->farmid, BAKDIR, SUBDIR, nme, extnew) != GDK_SUCCEED)
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
		gdk_return mvret = GDK_SUCCEED;
		bool exists;

		if (istail) {
			exists = file_exists(h->farmid, BAKDIR, nme, "tail.new") ||
#if SIZEOF_VAR_T == 8
				file_exists(h->farmid, BAKDIR, nme, "tail4.new") ||
#endif
				file_exists(h->farmid, BAKDIR, nme, "tail2.new") ||
				file_exists(h->farmid, BAKDIR, nme, "tail1.new") ||
				file_exists(h->farmid, BAKDIR, nme, "tail") ||
#if SIZEOF_VAR_T == 8
				file_exists(h->farmid, BAKDIR, nme, "tail4") ||
#endif
				file_exists(h->farmid, BAKDIR, nme, "tail2") ||
				file_exists(h->farmid, BAKDIR, nme, "tail1");
		} else {
			exists = file_exists(h->farmid, BAKDIR, nme, "theap.new") ||
				file_exists(h->farmid, BAKDIR, nme, "theap");
		}

		strconcat_len(extnew, sizeof(extnew), ext, ".new", NULL);
		if (dirty && !exists) {
			/* if the heap is dirty and there is no heap
			 * file (with or without .new extension) in
			 * the BAKDIR, move the heap (preferably with
			 * .new extension) to the correct backup
			 * directory */
			if (istail) {
				if (file_exists(h->farmid, srcdir, nme, "tail.new"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail.new");
#if SIZEOF_VAR_T == 8
				else if (file_exists(h->farmid, srcdir, nme, "tail4.new"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail4.new");
#endif
				else if (file_exists(h->farmid, srcdir, nme, "tail2.new"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail2.new");
				else if (file_exists(h->farmid, srcdir, nme, "tail1.new"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail1.new");
				else if (file_exists(h->farmid, srcdir, nme, "tail"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail");
#if SIZEOF_VAR_T == 8
				else if (file_exists(h->farmid, srcdir, nme, "tail4"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail4");
#endif
				else if (file_exists(h->farmid, srcdir, nme, "tail2"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail2");
				else if (file_exists(h->farmid, srcdir, nme, "tail1"))
					mvret = heap_move(h, srcdir,
							  subcommit ? SUBDIR : BAKDIR,
							  nme, "tail1");
			} else if (file_exists(h->farmid, srcdir, nme, extnew))
				mvret = heap_move(h, srcdir,
						  subcommit ? SUBDIR : BAKDIR,
						  nme, extnew);
			else
				mvret = heap_move(h, srcdir,
						  subcommit ? SUBDIR : BAKDIR,
						  nme, ext);
		} else if (subcommit) {
			/* if subcommit, we may need to move an
			 * already made backup from BAKDIR to
			 * SUBDIR */
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

			strconcat_len(kill_ext, sizeof(kill_ext),
				      ext, ".new.kill", NULL);
			if (file_exists(h->farmid, BAKDIR, nme, kill_ext) &&
			    file_move(h->farmid, BAKDIR, SUBDIR, nme, kill_ext) != GDK_SUCCEED) {
				ret = GDK_FAIL;
			}
		}
	}
	return ret;
}

static gdk_return
BBPbackup(BAT *b, bool subcommit)
{
	char *srcdir;
	long_str nme;
	const char *s = BBP_physical(b->batCacheid);
	size_t slen;

	if (BBPprepare(subcommit) != GDK_SUCCEED) {
		return GDK_FAIL;
	}
	if (!b->batCopiedtodisk || b->batTransient) {
		return GDK_SUCCEED;
	}
	/* determine location dir and physical suffix */
	if (!(srcdir = GDKfilepath(NOFARM, BATDIR, s, NULL)))
		goto fail;
	s = strrchr(srcdir, DIR_SEP);
	if (!s)
		goto fail;

	slen = strlen(++s);
	if (slen >= sizeof(nme))
		goto fail;
	memcpy(nme, s, slen + 1);
	srcdir[s - srcdir] = 0;

	if (b->ttype != TYPE_void &&
	    do_backup(srcdir, nme, gettailname(b), b->theap,
		      b->batDirtydesc || b->theap->dirty,
		      subcommit) != GDK_SUCCEED)
		goto fail;
	if (b->tvheap &&
	    do_backup(srcdir, nme, "theap", b->tvheap,
		      b->batDirtydesc || b->tvheap->dirty,
		      subcommit) != GDK_SUCCEED)
		goto fail;
	GDKfree(srcdir);
	return GDK_SUCCEED;
  fail:
	if(srcdir)
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
BBPsync(int cnt, bat *restrict subcommit, BUN *restrict sizes, lng logno, lng transid)
{
	gdk_return ret = GDK_SUCCEED;
	int t0 = 0, t1 = 0;
	str bakdir, deldir;

	if(!(bakdir = GDKfilepath(0, NULL, subcommit ? SUBDIR : BAKDIR, NULL)))
		return GDK_FAIL;
	if(!(deldir = GDKfilepath(0, NULL, DELDIR, NULL))) {
		GDKfree(bakdir);
		return GDK_FAIL;
	}

	TRC_DEBUG_IF(PERF) t0 = t1 = GDKms();

	ret = BBPprepare(subcommit != NULL);

	/* PHASE 1: safeguard everything in a backup-dir */
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
				snprintf(o, sizeof(o), "%o", (unsigned) b->batCacheid);
				f = GDKfilepath(b->theap->farmid, BAKDIR, o, gettailname(b));
				if (f == NULL) {
					ret = GDK_FAIL;
					goto bailout;
				}
				if (MT_access(f, F_OK) == 0)
					file_move(b->theap->farmid, BAKDIR, SUBDIR, o, gettailname(b));
				GDKfree(f);
				f = GDKfilepath(b->theap->farmid, BAKDIR, o, "theap");
				if (f == NULL) {
					ret = GDK_FAIL;
					goto bailout;
				}
				if (MT_access(f, F_OK) == 0)
					file_move(b->theap->farmid, BAKDIR, SUBDIR, o, "theap");
				GDKfree(f);
			}
		}
		if (idx < cnt)
			ret = GDK_FAIL;
	}
	TRC_DEBUG(PERF, "move time %d, %d files\n", (t1 = GDKms()) - t0, backup_files);

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

	TRC_DEBUG(PERF, "write time %d\n", (t0 = GDKms()) - t1);

	if (ret == GDK_SUCCEED) {
		ret = BBPdir(cnt, subcommit, sizes, logno, transid);
	}

	TRC_DEBUG(PERF, "dir time %d, %d bats\n", (t1 = GDKms()) - t0, (bat) ATOMIC_GET(&BBPsize));

	if (ret == GDK_SUCCEED) {
		/* atomic switchover */
		/* this is the big one: this call determines
		 * whether the operation of this function
		 * succeeded, so no changing of ret after this
		 * call anymore */

		if (MT_rename(bakdir, deldir) < 0 &&
		    /* maybe there was an old deldir, so remove and try again */
		    (GDKremovedir(0, DELDIR) != GDK_SUCCEED ||
		     MT_rename(bakdir, deldir) < 0))
			ret = GDK_FAIL;
		if (ret != GDK_SUCCEED)
			GDKsyserror("rename(%s,%s) failed.\n", bakdir, deldir);
		TRC_DEBUG(IO_, "rename %s %s = %d\n", bakdir, deldir, (int) ret);
	}

	/* AFTERMATH */
	if (ret == GDK_SUCCEED) {
		BBPlogno = logno;	/* the new value */
		BBPtransid = transid;
		backup_files = subcommit ? (backup_files - backup_subdir) : 0;
		backup_dir = backup_subdir = 0;
		if (GDKremovedir(0, DELDIR) != GDK_SUCCEED)
			fprintf(stderr, "#BBPsync: cannot remove directory %s\n", DELDIR);
		(void) BBPprepare(false); /* (try to) remove DELDIR and set up new BAKDIR */
		if (backup_files > 1) {
			TRC_DEBUG(PERF, "backup_files %d > 1\n", backup_files);
			backup_files = 1;
		}
	}
	TRC_DEBUG(PERF, "%s (ready time %d)\n",
		  ret == GDK_SUCCEED ? "" : " failed",
		  (t0 = GDKms()) - t1);
  bailout:
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
		if(!(dstpath = GDKfilepath(farmid, dstdir, srcpath, NULL))) {
			return GDK_FAIL;
		}

		/* step 1: remove the X.new file that is going to be
		 * overridden by X */
		if (MT_remove(dstpath) != 0 && errno != ENOENT) {
			/* if it exists and cannot be removed, all
			 * this is going to fail */
			GDKsyserror("force_move: remove(%s)\n", dstpath);
			GDKfree(dstpath);
			return GDK_FAIL;
		}
		GDKfree(dstpath);

		/* step 2: now remove the .kill file. This one is
		 * crucial, otherwise we'll never finish recovering */
		if(!(killfile = GDKfilepath(farmid, srcdir, name, NULL))) {
			return GDK_FAIL;
		}
		if (MT_remove(killfile) != 0) {
			ret = GDK_FAIL;
			GDKsyserror("force_move: remove(%s)\n", killfile);
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
		if(!(dstpath = GDKfilepath(farmid, dstdir, name, NULL)))
			return GDK_FAIL;
		if(!(srcpath = GDKfilepath(farmid, srcdir, name, NULL))) {
			GDKfree(dstpath);
			return GDK_FAIL;
		}
		if (MT_remove(dstpath) != 0)	/* clear destination */
			ret = GDK_FAIL;
		TRC_DEBUG(IO_, "remove %s = %d\n", dstpath, (int) ret);

		(void) GDKcreatedir(dstdir); /* if fails, move will fail */
		ret = GDKmove(farmid, srcdir, name, NULL, dstdir, name, NULL);
		TRC_DEBUG(IO_, "link %s %s = %d\n", srcpath, dstpath, (int) ret);
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
	bool dirseen = false;
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
		if (errno != ENOENT)
			GDKsyserror("cannot open directory %s\n", bakdirpath);
		GDKfree(bakdirpath);
		GDKfree(leftdirpath);
		return GDK_SUCCEED;	/* nothing to do */
	}
	memcpy(dstpath, BATDIR, j);
	dstpath[j] = DIR_SEP;
	dstpath[++j] = 0;
	dstdir = dstpath + j;
	TRC_DEBUG(IO_, "start\n");

	if (MT_mkdir(leftdirpath) < 0 && errno != EEXIST) {
		GDKsyserror("cannot create directory %s\n", leftdirpath);
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
				int uret = MT_remove(fn);
				TRC_DEBUG(IO_, "remove %s = %d\n",
					  fn, uret);
				GDKfree(fn);
			}
			continue;
		} else if (strcmp(dent->d_name, "BBP.dir") == 0) {
			dirseen = true;
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
			i = BBP_find(path, false);
			if (i < 0)
				i = -i;
		}
		if (i == 0 || i >= (bat) ATOMIC_GET(&BBPsize) || !BBPvalid(i)) {
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
		if (fn == NULL) {
			ret = GDK_FAIL;
		} else {
			ret = recover_dir(farmid, MT_stat(fn, &st) == 0);
			GDKfree(fn);
		}
	}

	if (ret == GDK_SUCCEED) {
		if (MT_rmdir(bakdirpath) < 0) {
			GDKsyserror("cannot remove directory %s\n", bakdirpath);
			ret = GDK_FAIL;
		}
		TRC_DEBUG(IO_, "rmdir %s = %d\n", bakdirpath, (int) ret);
	}
	if (ret != GDK_SUCCEED)
		GDKerror("recovery failed. Please check whether your disk is full or write-protected.\n");

	TRC_DEBUG(IO_, "end\n");
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
	if (dirp == NULL && errno != ENOENT)
		GDKsyserror("cannot open directory %s\n", subdirpath);
	GDKfree(subdirpath);
	if (dirp == NULL) {
		return GDK_SUCCEED;	/* nothing to do */
	}
	TRC_DEBUG(IO_, "start\n");

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
			TRC_DEBUG(IO_, "%s%cBBP.dir had disappeared!\n", SUBDIR, DIR_SEP);
			backup_dir = 0;
		}
	}
	TRC_DEBUG(IO_, "end = %d\n", (int) ret);

	if (ret != GDK_SUCCEED)
		GDKerror("recovery failed. Please check whether your disk is full or write-protected.\n");
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
static bool
persistent_bat(bat bid)
{
	if (bid >= 0 && bid < (bat) ATOMIC_GET(&BBPsize) && BBPvalid(bid)) {
		BAT *b = BBP_cache(bid);

		if (b == NULL || b->batCopiedtodisk) {
			return true;
		}
	}
	return false;
}

static BAT *
getdesc(bat bid)
{
	BAT *b = BBPgetdesc(bid);

	if (b == NULL)
		BBPclear(bid);
	return b;
}

static bool
BBPdiskscan(const char *parent, size_t baseoff)
{
	DIR *dirp = opendir(parent);
	struct dirent *dent;
	char fullname[FILENAME_MAX];
	str dst = fullname;
	size_t dstlen = sizeof(fullname);
	const char *src = parent;

	if (dirp == NULL) {
		if (errno != ENOENT)
			GDKsyserror("cannot open directory %s\n", parent);
		return true;	/* nothing to do */
	}

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
		bool ok, delete;

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
			fprintf(stderr, "unexpected file %s, leaving %s.\n", dent->d_name, parent);
			break;
		}
		strncpy(dst, dent->d_name, dstlen);
		fullname[sizeof(fullname) - 1] = 0;

		if (p == NULL && !BBPdiskscan(fullname, baseoff)) {
			/* it was a directory */
			continue;
		}

		if (p && strcmp(p + 1, "tmp") == 0) {
			delete = true;
			ok = true;
			bid = 0;
		} else {
			bid = strtol(dent->d_name, NULL, 8);
			ok = p && bid;
			delete = false;

			if (!ok || !persistent_bat(bid)) {
				delete = true;
			} else if (strncmp(p + 1, "tail", 4) == 0) {
				BAT *b = getdesc(bid);
				delete = (b == NULL || !b->ttype || !b->batCopiedtodisk);
			} else if (strncmp(p + 1, "theap", 5) == 0) {
				BAT *b = getdesc(bid);
				delete = (b == NULL || !b->tvheap || !b->batCopiedtodisk);
			} else if (strncmp(p + 1, "thashl", 6) == 0 ||
				   strncmp(p + 1, "thashb", 6) == 0) {
#ifdef PERSISTENTHASH
				BAT *b = getdesc(bid);
				delete = b == NULL;
				if (!delete)
					b->thash = (Hash *) 1;
#else
				delete = true;
#endif
			} else if (strncmp(p + 1, "thash", 5) == 0) {
				/* older versions used .thash which we
				 * can simply ignore */
				delete = true;
			} else if (strncmp(p + 1, "thsh", 4) == 0) {
				/* temporary hash files which we can
				 * simply ignore */
				delete = true;
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
				delete = true;
#endif
			} else if (strncmp(p + 1, "new", 3) != 0) {
				ok = false;
			}
		}
		if (!ok) {
			/* found an unknown file; stop pruning in this
			 * subdir */
			fprintf(stderr, "unexpected file %s, leaving %s.\n", dent->d_name, parent);
			break;
		}
		if (delete) {
			if (MT_remove(fullname) != 0 && errno != ENOENT) {
				GDKsyserror("remove(%s)", fullname);
				continue;
			}
			TRC_DEBUG(IO_, "remove(%s) = 0\n", fullname);
		}
	}
	closedir(dirp);
	return false;
}

void
gdk_bbp_reset(void)
{
	int i;

	while (BBPlimit > 0) {
		BBPlimit -= BBPINIT;
		assert(BBPlimit >= 0);
		GDKfree(BBP[BBPlimit >> BBPINITLOG]);
		BBP[BBPlimit >> BBPINITLOG] = NULL;
	}
	ATOMIC_SET(&BBPsize, 0);
	for (i = 0; i < MAXFARMS; i++)
		GDKfree((void *) BBPfarms[i].dirname); /* loose "const" */
	memset(BBPfarms, 0, sizeof(BBPfarms));
	BBP_hash = 0;
	BBP_mask = 0;

	locked_by = 0;
	BBPunloadCnt = 0;
	backup_files = 0;
	backup_dir = 0;
	backup_subdir = 0;
}
