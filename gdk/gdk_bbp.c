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
#include "mutils.h"
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifndef F_OK
#define F_OK 0
#endif
#ifndef S_ISDIR
#define S_ISDIR(mode)	(((mode) & _S_IFMT) == _S_IFDIR)
#endif
#ifndef O_CLOEXEC
#ifdef _O_NOINHERIT
#define O_CLOEXEC _O_NOINHERIT	/* Windows */
#else
#define O_CLOEXEC 0
#endif
#endif
#ifndef O_BINARY
#define O_BINARY 0
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
static MT_Lock BBPnameLock = MT_LOCK_INITIALIZER(BBPnameLock);
static bat *BBP_hash = NULL;		/* BBP logical name hash buckets */
static bat BBP_mask = 0;		/* number of buckets = & mask */
static MT_Lock GDKcacheLock = MT_LOCK_INITIALIZER(GDKcacheLock);
static bat BBP_free;

static gdk_return BBPfree(BAT *b);
static void BBPdestroy(BAT *b);
static void BBPuncacheit(bat bid, bool unloaddesc);
static gdk_return BBPprepare(bool subcommit);
static BAT *getBBPdescriptor(bat i);
static gdk_return BBPbackup(BAT *b, bool subcommit);
static gdk_return BBPdir_init(void);
static void BBPcallbacks(void);

/* two lngs of extra info in BBP.dir */
/* these two need to be atomic because of their use in AUTHcommit() */
static ATOMIC_TYPE BBPlogno = ATOMIC_VAR_INIT(0);
static ATOMIC_TYPE BBPtransid = ATOMIC_VAR_INIT(0);

#define BBPtmpcheck(s)	(strncmp(s, "tmp_", 4) == 0)

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
	return (lng) ATOMIC_GET(&BBPlogno);
}

lng
getBBPtransid(void)
{
	return (lng) ATOMIC_GET(&BBPtransid);
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

/* use a lock instead of atomic instructions so that we wait for
 * BBPlock/BBPunlock */
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

	BBPtmlock();
	MT_lock_set(&GDKcacheLock);
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
	MT_lock_unset(&GDKcacheLock);
	locked_by = 0;
	BBPtmunlock();
}

static gdk_return
BBPinithash(bat size)
{
	for (BBP_mask = 1; (BBP_mask << 1) <= BBPlimit; BBP_mask <<= 1)
		;
	BBP_hash = (bat *) GDKzalloc(BBP_mask * sizeof(bat));
	if (BBP_hash == NULL) {
		return GDK_FAIL;
	}
	BBP_mask--;

	while (--size > 0) {
		const char *s = BBP_logical(size);

		if (s) {
			if (*s != '.' && !BBPtmpcheck(s)) {
				BBP_insert(size);
			}
		} else {
			BBP_next(size) = BBP_free;
			BBP_free = size;
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

static gdk_return
BBPextend(bool buildhash, bat newsize)
{
	if (newsize >= N_BBPINIT * BBPINIT) {
		GDKerror("trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);
		return GDK_FAIL;
	}

	/* make sure the new size is at least BBPsize large */
	while (BBPlimit < newsize) {
		BUN limit = BBPlimit >> BBPINITLOG;
		assert(BBP[limit] == NULL);
		BBP[limit] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (BBP[limit] == NULL) {
			GDKerror("failed to extend BAT pool\n");
			return GDK_FAIL;
		}
		for (BUN i = 0; i < BBPINIT; i++) {
			ATOMIC_INIT(&BBP[limit][i].status, 0);
			BBP[limit][i].pid = ~(MT_Id)0;
		}
		BBPlimit += BBPINIT;
	}

	if (buildhash) {
		GDKfree(BBP_hash);
		BBP_hash = NULL;
		BBP_free = 0;
		if (BBPinithash(newsize) != GDK_SUCCEED)
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
			GDKwarning("unlink of BBP.bak failed\n");
		if (GDKmove(farmid, BATDIR, "BBP", "dir", BATDIR, "BBP", "bak", false) != GDK_SUCCEED)
			GDKwarning("rename of BBP.dir to BBP.bak failed\n");
	}
	return GDKmove(farmid, BAKDIR, "BBP", "dir", BATDIR, "BBP", "dir", true);
}

static gdk_return BBPrecover(int farmid);
static gdk_return BBPrecover_subdir(void);
static bool BBPdiskscan(const char *, size_t);

static int
vheapinit(BAT *b, const char *buf, unsigned bbpversion, const char *filename, int lineno)
{
	int n = 0;
	uint64_t free, size;
	uint16_t storage;

	(void) bbpversion;	/* could be used to implement compatibility */

	size = 0;			      /* for GDKLIBRARY_HSIZE case */
	storage = STORE_INVALID;	      /* for GDKLIBRARY_HSIZE case */
	if (bbpversion <= GDKLIBRARY_HSIZE ?
	    sscanf(buf,
		   " %" SCNu64 " %" SCNu64 " %" SCNu16
		   "%n",
		   &free, &size, &storage, &n) < 3 :
	    sscanf(buf,
		   " %" SCNu64
		   "%n",
		   &free, &n) < 1) {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
		return -1;
	}
	if (b->batCount == 0)
		free = 0;
	if (b->ttype >= 0 &&
	    ATOMstorage(b->ttype) == TYPE_str &&
	    free < GDK_STRHASHTABLE * sizeof(stridx_t) + BATTINY * GDK_VARALIGN)
		size = GDK_STRHASHTABLE * sizeof(stridx_t) + BATTINY * GDK_VARALIGN;
	else if (free < 512)
		size = 512;
	else
		size = free;
	*b->tvheap = (Heap) {
		.free = (size_t) free,
		.size = (size_t) size,
		.base = NULL,
		.storage = STORE_INVALID,
		.cleanhash = true,
		.newstorage = STORE_INVALID,
		.dirty = false,
		.parentid = b->batCacheid,
		.farmid = BBPselectfarm(PERSISTENT, b->ttype, varheap),
	};
	strconcat_len(b->tvheap->filename, sizeof(b->tvheap->filename),
		      filename, ".theap", NULL);
	return n;
}

static int
heapinit(BAT *b, const char *buf,
#ifdef GDKLIBRARY_HASHASH
	 int *hashash,
#endif
	 unsigned bbpversion, const char *filename, int lineno)
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
	size = 0;			      /* for GDKLIBRARY_HSIZE case */
	storage = STORE_INVALID;	      /* for GDKLIBRARY_HSIZE case */
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
	    bbpversion <= GDKLIBRARY_HSIZE ?
	    sscanf(buf,
		   " %10s %" SCNu16 " %" SCNu16 " %" SCNu16 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu16 " %" SCNu64 " %" SCNu64
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &size, &storage, &minpos, &maxpos,
		   &n) < 14 :
	    sscanf(buf,
		   " %10s %" SCNu16 " %" SCNu16 " %" SCNu16 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
		   " %" SCNu64 " %" SCNu64 " %" SCNu64
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &free, &minpos, &maxpos,
		   &n) < 12) {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", lineno);
		return -1;
	}

	if (strcmp(type, "wkba") == 0)
		GDKwarning("type wkba (SQL name: GeometryA) is deprecated\n");

	if (properties & ~0x0F81) {
		TRC_CRITICAL(GDK, "unknown properties are set: incompatible database on line %d of BBP.dir\n", lineno);
		return -1;
	}
#ifdef GDKLIBRARY_HASHASH
	*hashash = var & 2;
#endif
	var &= ~2;
	if ((t = ATOMindex(type)) < 0) {
		if ((t = ATOMunknown_find(type)) == 0) {
			TRC_CRITICAL(GDK, "no space for atom %s", type);
			return -1;
		}
	} else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL)) {
		TRC_CRITICAL(GDK, "inconsistent entry in BBP.dir: tvarsized mismatch for BAT %d on line %d\n", (int) b->batCacheid, lineno);
		return -1;
	} else if (var && t != 0 ?
		   ATOMsize(t) < width ||
		   (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		    && width != 8
#endif
			   ) :
		   ATOMsize(t) != width) {
		TRC_CRITICAL(GDK, "inconsistent entry in BBP.dir: tsize mismatch for BAT %d on line %d\n", (int) b->batCacheid, lineno);
		return -1;
	}
	b->ttype = t;
	b->twidth = width;
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
	b->tunique_est = 0.0;
	/* (properties & 0x0200) is the old tdense flag */
	b->tseqbase = (properties & 0x0200) == 0 || base >= (uint64_t) oid_nil ? oid_nil : (oid) base;
	b->theap->free = (size_t) free;
	/* set heap size to match capacity */
	if (b->ttype == TYPE_msk) {
		/* round up capacity to multiple of 32 */
		b->batCapacity = (b->batCapacity + 31) & ~((BUN) 31);
		b->theap->size = b->batCapacity / 8;
	} else {
		b->theap->size = (size_t) b->batCapacity << b->tshift;
	}
	b->theap->base = NULL;
	settailname(b->theap, filename, t, width);
	b->theap->storage = STORE_INVALID;
	b->theap->newstorage = STORE_INVALID;
	b->theap->farmid = BBPselectfarm(PERSISTENT, b->ttype, offheap);
	b->theap->dirty = false;
	b->theap->parentid = b->batCacheid;
	if (minpos < b->batCount)
		b->tminpos = (BUN) minpos;
	else
		b->tminpos = BUN_NONE;
	if (maxpos < b->batCount)
		b->tmaxpos = (BUN) maxpos;
	else
		b->tmaxpos = BUN_NONE;
	if (t && var) {
		t = vheapinit(b, buf + n, bbpversion, filename, lineno);
		if (t < 0)
			return t;
		n += t;
	} else {
		b->tvheap = NULL;
	}
	return n;
}

/* read a single line from the BBP.dir file (file pointer fp) and fill
 * in the structure pointed to by bn and extra information through the
 * other pointers; this function does not allocate any memory; return 0
 * on end of file, 1 on success, and -1 on failure */
/* set to true during initialization, else always false; if false, do
 * not return any options (set pointer to NULL as if there aren't any);
 * if true and there are options, return them in freshly allocated
 * memory through *options */
static bool return_options = false;
int
BBPreadBBPline(FILE *fp, unsigned bbpversion, int *lineno, BAT *bn,
#ifdef GDKLIBRARY_HASHASH
	       int *hashash,
#endif
	       char *batname, char *filename, char **options)
{
	char buf[4096];
	uint64_t batid;
	unsigned int status;
	unsigned int properties;
	int nread, n;
	char *s;
	uint64_t count, capacity = 0, base = 0;

	if (fgets(buf, sizeof(buf), fp) == NULL) {
		if (ferror(fp)) {
			TRC_CRITICAL(GDK, "error reading BBP.dir on line %d\n", *lineno);
			return -1;
		}
		return 0;	/* end of file */
	}
	(*lineno)++;
	if ((s = strpbrk(buf, "\r\n")) != NULL) {
		if (s[0] == '\r' && s[1] != '\n') {
			TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", *lineno);
			return -1;
		}
		/* zap the newline */
		*s = '\0';
	} else {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d: line too long\n", *lineno);
		return -1;
	}

	if (bbpversion <= GDKLIBRARY_HSIZE ?
	    sscanf(buf,
		   "%" SCNu64 " %u %128s %23s %u %" SCNu64
		   " %" SCNu64 " %" SCNu64
		   "%n",
		   &batid, &status, batname, filename,
		   &properties, &count, &capacity, &base,
		   &nread) < 8 :
	    sscanf(buf,
		   "%" SCNu64 " %u %128s %23s %u %" SCNu64
		   " %" SCNu64
		   "%n",
		   &batid, &status, batname, filename,
		   &properties, &count, &base,
		   &nread) < 7) {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", *lineno);
		return -1;
	}

	if (batid >= N_BBPINIT * BBPINIT) {
		TRC_CRITICAL(GDK, "bat ID (%" PRIu64 ") too large to accomodate (max %d), on line %d.", batid, N_BBPINIT * BBPINIT - 1, *lineno);
		return -1;
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

	bn->batCacheid = (bat) batid;
	BATinit_idents(bn);
	bn->batTransient = false;
	bn->batCopiedtodisk = true;
	switch ((properties & 0x06) >> 1) {
	case 0:
		bn->batRestricted = BAT_WRITE;
		break;
	case 1:
		bn->batRestricted = BAT_READ;
		break;
	case 2:
		bn->batRestricted = BAT_APPEND;
		break;
	default:
		TRC_CRITICAL(GDK, "incorrect batRestricted value");
		return -1;
	}
	bn->batCount = (BUN) count;
	bn->batInserted = bn->batCount;
	/* set capacity to at least count */
	bn->batCapacity = (BUN) count <= BATTINY ? BATTINY : (BUN) count;

	if (base > (uint64_t) GDK_oid_max) {
		TRC_CRITICAL(GDK, "head seqbase out of range (ID = %" PRIu64 ", seq = %" PRIu64 ") on line %d.", batid, base, *lineno);
		return -1;
	}
	bn->hseqbase = (oid) base;
	n = heapinit(bn, buf + nread,
#ifdef GDKLIBRARY_HASHASH
		     hashash,
#endif
		     bbpversion, filename, *lineno);
	if (n < 0) {
		return -1;
	}
	nread += n;

	if (nread >= (int) sizeof(buf) || (buf[nread] != '\0' && buf[nread] != ' ')) {
		TRC_CRITICAL(GDK, "invalid format for BBP.dir on line %d", *lineno);
		return -1;
	}
	if (options) {
		if (return_options && buf[nread] == ' ') {
			if ((*options = GDKstrdup(buf + nread + 1)) == NULL) {
				TRC_CRITICAL(GDK, "GDKstrdup failed\n");
				return -1;
			}
		} else {
			*options = NULL;
		}
	}
	return 1;
}

static gdk_return
BBPreadEntries(FILE *fp, unsigned bbpversion, int lineno
#ifdef GDKLIBRARY_HASHASH
	       , bat **hashbats, bat *nhashbats
#endif
	)
{
#ifdef GDKLIBRARY_HASHASH
	bat *hbats = NULL;
	bat nhbats = 0;
#endif

	/* read the BBP.dir and insert the BATs into the BBP */
	return_options = true;
	for (;;) {
		BAT b;
		Heap h;
		Heap vh;
		vh = h = (Heap) {
			.free = 0,
		};
		b = (BAT) {
			.theap = &h,
			.tvheap = &vh,
		};
		char *options;
		char headname[129];
		char filename[sizeof(BBP_physical(0))];
		char logical[1024];
#ifdef GDKLIBRARY_HASHASH
		int Thashash;
#endif

		switch (BBPreadBBPline(fp, bbpversion, &lineno, &b,
#ifdef GDKLIBRARY_HASHASH
				       &Thashash,
#endif
				       headname, filename, &options)) {
		case 0:
			/* end of file */
#ifdef GDKLIBRARY_HASHASH
			*hashbats = hbats;
			*nhashbats = nhbats;
#endif
			return_options = false;
			return GDK_SUCCEED;
		case 1:
			/* successfully read an entry */
			break;
		default:
			/* error */
			goto bailout;
		}

		if (b.batCacheid >= N_BBPINIT * BBPINIT) {
			GDKfree(options);
			TRC_CRITICAL(GDK, "bat ID (%d) too large to accommodate (max %d), on line %d.", b.batCacheid, N_BBPINIT * BBPINIT - 1, lineno);
			goto bailout;
		}

		if (b.batCacheid >= (bat) ATOMIC_GET(&BBPsize)) {
			if ((bat) ATOMIC_GET(&BBPsize) + 1 >= BBPlimit &&
			    BBPextend(false, b.batCacheid + 1) != GDK_SUCCEED) {
				GDKfree(options);
				goto bailout;
			}
			ATOMIC_SET(&BBPsize, b.batCacheid + 1);
		}
		if (BBP_desc(b.batCacheid) != NULL) {
			GDKfree(options);
			TRC_CRITICAL(GDK, "duplicate entry in BBP.dir (ID = "
				     "%d) on line %d.", b.batCacheid, lineno);
			goto bailout;
		}

#ifdef GDKLIBRARY_HASHASH
		if (Thashash) {
			assert(bbpversion <= GDKLIBRARY_HASHASH);
			bat *sb = GDKrealloc(hbats, ++nhbats * sizeof(bat));
			if (sb == NULL) {
				GDKfree(options);
				goto bailout;
			}
			hbats = sb;
			hbats[nhbats - 1] = b.batCacheid;
		}
#endif

		BAT *bn;
		Heap *hn;
		if ((bn = GDKzalloc(sizeof(BAT))) == NULL ||
		    (hn = GDKzalloc(sizeof(Heap))) == NULL) {
			GDKfree(bn);
			GDKfree(options);
			TRC_CRITICAL(GDK, "cannot allocate memory for BAT.");
			goto bailout;
		}
		*bn = b;
		*hn = h;
		bn->theap = hn;
		if (b.tvheap) {
			Heap *vhn;
			assert(b.tvheap == &vh);
			if ((vhn = GDKmalloc(sizeof(Heap))) == NULL) {
				GDKfree(hn);
				GDKfree(bn);
				GDKfree(options);
				TRC_CRITICAL(GDK, "cannot allocate memory for BAT.");
				goto bailout;
			}
			*vhn = vh;
			bn->tvheap = vhn;
			ATOMIC_INIT(&bn->tvheap->refs, 1);
		}

		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "heaplock%d", bn->batCacheid); /* fits */
		MT_lock_init(&bn->theaplock, name);
		snprintf(name, sizeof(name), "BATlock%d", bn->batCacheid); /* fits */
		MT_lock_init(&bn->batIdxLock, name);
		snprintf(name, sizeof(name), "hashlock%d", bn->batCacheid); /* fits */
		MT_rwlock_init(&bn->thashlock, name);
		ATOMIC_INIT(&bn->theap->refs, 1);

		if (snprintf(BBP_bak(b.batCacheid), sizeof(BBP_bak(b.batCacheid)), "tmp_%o", (unsigned) b.batCacheid) >= (int) sizeof(BBP_bak(b.batCacheid))) {
			BATdestroy(bn);
			GDKfree(options);
			TRC_CRITICAL(GDK, "BBP logical filename directory is too large, on line %d\n", lineno);
			goto bailout;
		}
		char *s;
		if ((s = strchr(headname, '~')) != NULL && s == headname) {
			/* sizeof(logical) > sizeof(BBP_bak(b.batCacheid)), so
			 * this fits */
			strcpy(logical, BBP_bak(b.batCacheid));
		} else {
			if (s)
				*s = 0;
			strcpy_len(logical, headname, sizeof(logical));
		}
		if (strcmp(logical, BBP_bak(b.batCacheid)) == 0) {
			BBP_logical(b.batCacheid) = BBP_bak(b.batCacheid);
		} else {
			BBP_logical(b.batCacheid) = GDKstrdup(logical);
			if (BBP_logical(b.batCacheid) == NULL) {
				BATdestroy(bn);
				GDKfree(options);
				TRC_CRITICAL(GDK, "GDKstrdup failed\n");
				goto bailout;
			}
		}
		strcpy_len(BBP_physical(b.batCacheid), filename, sizeof(BBP_physical(b.batCacheid)));
#ifdef __COVERITY__
		/* help coverity */
		BBP_physical(b.batCacheid)[sizeof(BBP_physical(b.batCacheid)) - 1] = 0;
#endif
		BBP_options(b.batCacheid) = options;
		BBP_refs(b.batCacheid) = 0;
		BBP_lrefs(b.batCacheid) = 1;	/* any BAT we encounter here is persistent, so has a logical reference */
		BBP_desc(b.batCacheid) = bn;
		BBP_pid(b.batCacheid) = 0;
		BBP_status_set(b.batCacheid, BBPEXISTING);	/* do we need other status bits? */
	}

  bailout:
	return_options = false;
#ifdef GDKLIBRARY_HASHASH
	GDKfree(hbats);
#endif
	return GDK_FAIL;
}

/* check that the necessary files for all BATs exist and are large
 * enough */
static gdk_return
BBPcheckbats(unsigned bbpversion)
{
	(void) bbpversion;
	for (bat bid = 1, size = (bat) ATOMIC_GET(&BBPsize); bid < size; bid++) {
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
		if (b->theap->free > 0) {
			path = GDKfilepath(0, BATDIR, b->theap->filename, NULL);
			if (path == NULL)
				return GDK_FAIL;
			/* first check string offset heap with width,
			 * then without */
			if (MT_stat(path, &statb) < 0) {
#ifdef GDKLIBRARY_TAILN
				if (b->ttype == TYPE_str &&
				    b->twidth < SIZEOF_VAR_T) {
					size_t taillen = strlen(path) - 1;
					char tailsave = path[taillen];
					path[taillen] = 0;
					if (MT_stat(path, &statb) < 0) {
						GDKsyserror("cannot stat file %s%c or %s (expected size %zu)\n",
							    path, tailsave, path, b->theap->free);
						GDKfree(path);
						return GDK_FAIL;
					}
				} else
#endif
				{
					GDKsyserror("cannot stat file %s (expected size %zu)\n",
						    path, b->theap->free);
					GDKfree(path);
					return GDK_FAIL;
				}
			}
			if ((size_t) statb.st_size < b->theap->free) {
				GDKerror("file %s too small (expected %zu, actual %zu)\n", path, b->theap->free, (size_t) statb.st_size);
				GDKfree(path);
				return GDK_FAIL;
			}
			size_t hfree = b->theap->free;
			hfree = (hfree + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
			if (hfree == 0)
				hfree = GDK_mmap_pagesize;
			if (statb.st_size > (off_t) hfree) {
				int fd;
				if ((fd = MT_open(path, O_RDWR | O_CLOEXEC | O_BINARY)) >= 0) {
					if (ftruncate(fd, hfree) == -1)
						perror("ftruncate");
					(void) close(fd);
				}
			}
			GDKfree(path);
		}
		if (b->tvheap != NULL && b->tvheap->free > 0) {
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
			size_t hfree = b->tvheap->free;
			hfree = (hfree + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);
			if (hfree == 0)
				hfree = GDK_mmap_pagesize;
			if (statb.st_size > (off_t) hfree) {
				int fd;
				if ((fd = MT_open(path, O_RDWR | O_CLOEXEC | O_BINARY)) >= 0) {
					if (ftruncate(fd, hfree) == -1)
						perror("ftruncate");
					(void) close(fd);
				}
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

unsigned
BBPheader(FILE *fp, int *lineno, bat *bbpsize, lng *logno, lng *transid)
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
	    bbpversion != GDKLIBRARY_HSIZE &&
	    bbpversion != GDKLIBRARY_HASHASH &&
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
	if (sz > *bbpsize)
		*bbpsize = sz;
	if (bbpversion > GDKLIBRARY_MINMAX_POS) {
		if (fgets(buf, sizeof(buf), fp) == NULL) {
			TRC_CRITICAL(GDK, "short BBP");
			return 0;
		}
		if (sscanf(buf, "BBPinfo=" LLSCN " " LLSCN, logno, transid) != 2) {
			TRC_CRITICAL(GDK, "no info value found\n");
			return 0;
		}
	} else {
		*logno = *transid = 0;
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
	if (strcmp(dirname, "in-memory") == 0 ||
	    /* backward compatibility: */ strcmp(dirname, ":memory:") == 0) {
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

#ifdef GDKLIBRARY_HASHASH
static gdk_return
fixhashashbat(BAT *b)
{
	const char *nme = BBP_physical(b->batCacheid);
	char *srcdir = GDKfilepath(NOFARM, BATDIR, nme, NULL);
	if (srcdir == NULL) {
		TRC_CRITICAL(GDK, "GDKfilepath failed\n");
		return GDK_FAIL;
	}
	char *s;
	if ((s = strrchr(srcdir, DIR_SEP)) != NULL)
		*s = 0;
	const char *bnme;
	if ((bnme = strrchr(nme, DIR_SEP)) != NULL)
		bnme++;
	else
		bnme = nme;
	long_str filename;
	snprintf(filename, sizeof(filename), "BACKUP%c%s", DIR_SEP, bnme);

	/* we don't maintain index structures */
	HASHdestroy(b);
	IMPSdestroy(b);
	OIDXdestroy(b);
	PROPdestroy(b);
	STRMPdestroy(b);

	/* make backup of heaps */
	const char *t;
	if (GDKmove(b->theap->farmid, srcdir, bnme, "tail1",
		    BAKDIR, bnme, "tail1", false) == GDK_SUCCEED)
		t = "tail1";
	else if (GDKmove(b->theap->farmid, srcdir, bnme, "tail2",
			 BAKDIR, bnme, "tail2", false) == GDK_SUCCEED)
		t = "tail2";
#if SIZEOF_VAR_T == 8
	else if (GDKmove(b->theap->farmid, srcdir, bnme, "tail4",
			 BAKDIR, bnme, "tail4", false) == GDK_SUCCEED)
		t = "tail4";
#endif
	else if (GDKmove(b->theap->farmid, srcdir, bnme, "tail",
			 BAKDIR, bnme, "tail", true) == GDK_SUCCEED)
		t = "tail";
	else {
		GDKfree(srcdir);
		TRC_CRITICAL(GDK, "cannot make backup of %s.tail\n", nme);
		return GDK_FAIL;
	}
	GDKclrerr();
	if (GDKmove(b->theap->farmid, srcdir, bnme, "theap",
		    BAKDIR, bnme, "theap", true) != GDK_SUCCEED) {
		GDKfree(srcdir);
		TRC_CRITICAL(GDK, "cannot make backup of %s.theap\n", nme);
		return GDK_FAIL;
	}
	/* load old heaps */
	Heap h1 = *b->theap;	/* old heap */
	h1.base = NULL;
	h1.dirty = false;
	strconcat_len(h1.filename, sizeof(h1.filename), filename, ".", t, NULL);
	if (HEAPload(&h1, filename, t, false) != GDK_SUCCEED) {
		GDKfree(srcdir);
		TRC_CRITICAL(GDK, "loading old tail heap "
			     "for BAT %d failed\n", b->batCacheid);
		return GDK_FAIL;
	}
	Heap vh1 = *b->tvheap;	/* old heap */
	vh1.base = NULL;
	vh1.dirty = false;
	strconcat_len(vh1.filename, sizeof(vh1.filename), filename, ".theap", NULL);
	if (HEAPload(&vh1, filename, "theap", false) != GDK_SUCCEED) {
		GDKfree(srcdir);
		HEAPfree(&h1, false);
		TRC_CRITICAL(GDK, "loading old string heap "
			     "for BAT %d failed\n", b->batCacheid);
		return GDK_FAIL;
	}

	/* create new heaps */
	Heap *h2 = GDKmalloc(sizeof(Heap));
	Heap *vh2 = GDKmalloc(sizeof(Heap));
	if (h2 == NULL || vh2 == NULL) {
		GDKfree(h2);
		GDKfree(vh2);
		GDKfree(srcdir);
		HEAPfree(&h1, false);
		HEAPfree(&vh1, false);
		TRC_CRITICAL(GDK, "allocating new heaps "
			     "for BAT %d failed\n", b->batCacheid);
		return GDK_FAIL;
	}
	*h2 = *b->theap;
	if (HEAPalloc(h2, b->batCapacity, b->twidth) != GDK_SUCCEED) {
		GDKfree(h2);
		GDKfree(vh2);
		GDKfree(srcdir);
		HEAPfree(&h1, false);
		HEAPfree(&vh1, false);
		TRC_CRITICAL(GDK, "allocating new tail heap "
			     "for BAT %d failed\n", b->batCacheid);
		return GDK_FAIL;
	}
	h2->dirty = true;
	h2->free = h1.free;

	*vh2 = *b->tvheap;
	strconcat_len(vh2->filename, sizeof(vh2->filename), nme, ".theap", NULL);
	strHeap(vh2, b->batCapacity);
	if (vh2->base == NULL) {
		GDKfree(srcdir);
		HEAPfree(&h1, false);
		HEAPfree(&vh1, false);
		HEAPfree(h2, false);
		GDKfree(h2);
		GDKfree(vh2);
		TRC_CRITICAL(GDK, "allocating new string heap "
			     "for BAT %d failed\n", b->batCacheid);
		return GDK_FAIL;
	}
	vh2->dirty = true;
	ATOMIC_INIT(&h2->refs, 1);
	ATOMIC_INIT(&vh2->refs, 1);
	Heap *ovh = b->tvheap;
	b->tvheap = vh2;
	vh2 = NULL;		/* no longer needed */
	for (BUN i = 0; i < b->batCount; i++) {
		var_t o;
		switch (b->twidth) {
		case 1:
			o = (var_t) ((uint8_t *) h1.base)[i] + GDK_VAROFFSET;
			break;
		case 2:
			o = (var_t) ((uint16_t *) h1.base)[i] + GDK_VAROFFSET;
			break;
#if SIZEOF_VAR_T == 8
		case 4:
			o = (var_t) ((uint32_t *) h1.base)[i];
			break;
#endif
		default:
			o = ((var_t *) h1.base)[i];
			break;
		}
		const char *s = vh1.base + o;
		var_t no = strPut(b, &o, s);
		if (no == 0) {
			HEAPfree(&h1, false);
			HEAPfree(&vh1, false);
			HEAPdecref(h2, false);
			HEAPdecref(b->tvheap, false);
			b->tvheap = ovh;
			GDKfree(srcdir);
			TRC_CRITICAL(GDK, "storing string value "
				     "for BAT %d failed\n", b->batCacheid);
			return GDK_FAIL;
		}
		assert(no >= GDK_VAROFFSET);
		switch (b->twidth) {
		case 1:
			no -= GDK_VAROFFSET;
			assert(no <= 0xFF);
			((uint8_t *) h2->base)[i] = (uint8_t) no;
			break;
		case 2:
			no -= GDK_VAROFFSET;
			assert(no <= 0xFFFF);
			((uint16_t *) h2->base)[i] = (uint16_t) no;
			break;
#if SIZEOF_VAR_T == 8
		case 4:
			assert(no <= 0xFFFFFFFF);
			((uint32_t *) h2->base)[i] = (uint32_t) no;
			break;
#endif
		default:
			((var_t *) h2->base)[i] = no;
			break;
		}
	}

	/* cleanup */
	HEAPfree(&h1, false);
	HEAPfree(&vh1, false);
	if (HEAPsave(h2, nme, BATtailname(b), true, h2->free, NULL) != GDK_SUCCEED) {
		HEAPdecref(h2, false);
		HEAPdecref(b->tvheap, false);
		b->tvheap = ovh;
		GDKfree(srcdir);
		TRC_CRITICAL(GDK, "saving heap failed\n");
		return GDK_FAIL;
	}
	if (HEAPsave(b->tvheap, nme, "theap", true, b->tvheap->free, &b->theaplock) != GDK_SUCCEED) {
		HEAPfree(b->tvheap, false);
		b->tvheap = ovh;
		GDKfree(srcdir);
		TRC_CRITICAL(GDK, "saving string heap failed\n");
		return GDK_FAIL;
	}
	HEAPdecref(b->theap, false);
	b->theap = h2;
	HEAPfree(h2, false);
	HEAPdecref(ovh, false);
	HEAPfree(b->tvheap, false);
	GDKfree(srcdir);
	return GDK_SUCCEED;
}

static gdk_return
fixhashash(bat *hashbats, bat nhashbats)
{
	for (bat i = 0; i < nhashbats; i++) {
		bat bid = hashbats[i];
		BAT *b;
		if ((b = BBP_desc(bid)) == NULL) {
			/* not a valid BAT (shouldn't happen) */
			continue;
		}
		if (fixhashashbat(b) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}
#endif

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
		if (b->ttype != TYPE_str || b->twidth == SIZEOF_VAR_T || b->batCount == 0)
			continue;
		char *oldpath = GDKfilepath(0, BATDIR, BBP_physical(b->batCacheid), "tail");
		char *newpath = GDKfilepath(0, BATDIR, b->theap->filename, NULL);
		int ret = -1;
		if (oldpath != NULL && newpath != NULL) {
			struct stat oldst, newst;
			bool oldexist = MT_stat(oldpath, &oldst) == 0;
			bool newexist = MT_stat(newpath, &newst) == 0;
			if (newexist) {
				if (oldexist) {
					if (oldst.st_mtime > newst.st_mtime) {
						GDKerror("both %s and %s exist with %s unexpectedly newer: manual intervention required\n", oldpath, newpath, oldpath);
						ret = -1;
					} else {
						GDKwarning("both %s and %s exist, removing %s\n", oldpath, newpath, oldpath);
						ret = MT_remove(oldpath);
					}
				} else {
					/* already good */
					ret = 0;
				}
			} else if (oldexist) {
				TRC_DEBUG(IO_, "rename %s to %s\n", oldpath, newpath);
				ret = MT_rename(oldpath, newpath);
			} else {
				/* neither file exists: may be ok, but
				 * will be checked later */
				ret = 0;
			}
		}
		GDKfree(oldpath);
		GDKfree(newpath);
		if (ret == -1)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}
#endif

static void
BBPtrim(bool aggressive)
{
	int n = 0;
	unsigned flag = BBPUNLOADING | BBPSYNCING | BBPSAVING;
	if (!aggressive)
		flag |= BBPHOT;
	for (bat bid = 1, nbat = (bat) ATOMIC_GET(&BBPsize); bid < nbat; bid++) {
		/* don't do this during a (sub)commit */
		BBPtmlock();
		MT_lock_set(&GDKswapLock(bid));
		BAT *b = NULL;
		bool swap = false;
		if (!(BBP_status(bid) & flag) &&
		    BBP_refs(bid) == 0 &&
		    BBP_lrefs(bid) != 0 &&
		    (b = BBP_cache(bid)) != NULL) {
			MT_lock_set(&b->theaplock);
			if (b->batSharecnt == 0 &&
			    !isVIEW(b) &&
			    (!BATdirty(b) || (aggressive && b->theap->storage == STORE_MMAP && (b->tvheap == NULL || b->tvheap->storage == STORE_MMAP))) /*&&
			    (BBP_status(bid) & BBPPERSISTENT ||
			     (b->batRole == PERSISTENT && BBP_lrefs(bid) == 1)) */) {
				BBP_status_on(bid, BBPUNLOADING);
				swap = true;
			}
			MT_lock_unset(&b->theaplock);
		}
		MT_lock_unset(&GDKswapLock(bid));
		if (swap) {
			TRC_DEBUG(BAT_, "unload and free bat %d\n", bid);
			if (BBPfree(b) != GDK_SUCCEED)
				GDKerror("unload failed for bat %d", bid);
			n++;
		}
		BBPtmunlock();
	}
	TRC_DEBUG(BAT_, "unloaded %d bats%s\n", n, aggressive ? " (also hot)" : "");
}

static void
BBPmanager(void *dummy)
{
	(void) dummy;

	for (;;) {
		int n = 0;
		for (bat bid = 1, nbat = (bat) ATOMIC_GET(&BBPsize); bid < nbat; bid++) {
			MT_lock_set(&GDKswapLock(bid));
			if (BBP_refs(bid) == 0 && BBP_lrefs(bid) != 0) {
				n += (BBP_status(bid) & BBPHOT) != 0;
				BBP_status_off(bid, BBPHOT);
			}
			MT_lock_unset(&GDKswapLock(bid));
		}
		TRC_DEBUG(BAT_, "cleared HOT bit from %d bats\n", n);
		size_t cur = GDKvm_cursize();
		for (int i = 0, n = cur > GDK_vm_maxsize / 2 ? 1 : cur > GDK_vm_maxsize / 4 ? 10 : 100; i < n; i++) {
			MT_sleep_ms(100);
			if (GDKexiting())
				return;
		}
		BBPtrim(false);
		BBPcallbacks();
		if (GDKexiting())
			return;
	}
}

static MT_Id manager;

gdk_return
BBPinit(void)
{
	FILE *fp = NULL;
	struct stat st;
	unsigned bbpversion = 0;
	int i;
	int lineno = 0;
#ifdef GDKLIBRARY_HASHASH
	bat *hashbats = NULL;
	bat nhashbats = 0;
	gdk_return res = GDK_SUCCEED;
#endif
	int dbg = GDKdebug;

	GDKdebug &= ~TAILCHKMASK;

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

		BBPtmlock();

		if (!(bbpdirstr = GDKfilepath(0, BATDIR, "BBP", "dir"))) {
			TRC_CRITICAL(GDK, "GDKmalloc failed\n");
			BBPtmunlock();
			GDKdebug = dbg;
			return GDK_FAIL;
		}

		if (!(backupbbpdirstr = GDKfilepath(0, BAKDIR, "BBP", "dir"))) {
			GDKfree(bbpdirstr);
			TRC_CRITICAL(GDK, "GDKmalloc failed\n");
			BBPtmunlock();
			GDKdebug = dbg;
			return GDK_FAIL;
		}

		if (GDKremovedir(0, TEMPDIR) != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot remove directory %s\n", TEMPDIR);
			BBPtmunlock();
			GDKdebug = dbg;
			return GDK_FAIL;
		}

		if (GDKremovedir(0, DELDIR) != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot remove directory %s\n", DELDIR);
			BBPtmunlock();
			GDKdebug = dbg;
			return GDK_FAIL;
		}

		/* first move everything from SUBDIR to BAKDIR (its parent) */
		if (BBPrecover_subdir() != GDK_SUCCEED) {
			GDKfree(bbpdirstr);
			GDKfree(backupbbpdirstr);
			TRC_CRITICAL(GDK, "cannot properly recover_subdir process %s.", SUBDIR);
			BBPtmunlock();
			GDKdebug = dbg;
			return GDK_FAIL;
		}

		/* try to obtain a BBP.dir from bakdir */
		if (MT_stat(backupbbpdirstr, &st) == 0) {
			/* backup exists; *must* use it */
			if (recover_dir(0, MT_stat(bbpdirstr, &st) == 0) != GDK_SUCCEED) {
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				BBPtmunlock();
				goto bailout;
			}
			if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				TRC_CRITICAL(GDK, "cannot open recovered BBP.dir.");
				BBPtmunlock();
				GDKdebug = dbg;
				return GDK_FAIL;
			}
		} else if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
			/* there was no BBP.dir either. Panic! try to use a
			 * BBP.bak */
			if (MT_stat(backupbbpdirstr, &st) < 0) {
				/* no BBP.bak (nor BBP.dir or BACKUP/BBP.dir):
				 * create a new one */
				TRC_DEBUG(IO_, "initializing BBP.\n");
				if (BBPdir_init() != GDK_SUCCEED) {
					GDKfree(bbpdirstr);
					GDKfree(backupbbpdirstr);
					BBPtmunlock();
					goto bailout;
				}
			} else if (GDKmove(0, BATDIR, "BBP", "bak", BATDIR, "BBP", "dir", true) == GDK_SUCCEED)
				TRC_DEBUG(IO_, "reverting to dir saved in BBP.bak.\n");

			if ((fp = GDKfilelocate(0, "BBP", "r", "dir")) == NULL) {
				GDKsyserror("cannot open BBP.dir");
				GDKfree(bbpdirstr);
				GDKfree(backupbbpdirstr);
				BBPtmunlock();
				goto bailout;
			}
		}
		assert(fp != NULL);
		GDKfree(bbpdirstr);
		GDKfree(backupbbpdirstr);
		BBPtmunlock();
	}

	/* scan the BBP.dir to obtain current size */
	BBPlimit = 0;
	memset(BBP, 0, sizeof(BBP));

	bat bbpsize;
	bbpsize = 1;
	if (GDKinmemory(0)) {
		bbpversion = GDKLIBRARY;
	} else {
		lng logno, transid;
		bbpversion = BBPheader(fp, &lineno, &bbpsize, &logno, &transid);
		if (bbpversion == 0) {
			GDKdebug = dbg;
			return GDK_FAIL;
		}
		assert(bbpversion > GDKLIBRARY_MINMAX_POS || logno == 0);
		assert(bbpversion > GDKLIBRARY_MINMAX_POS || transid == 0);
		ATOMIC_SET(&BBPlogno, logno);
		ATOMIC_SET(&BBPtransid, transid);
	}

	/* allocate BBP records */
	if (BBPextend(false, bbpsize) != GDK_SUCCEED) {
		GDKdebug = dbg;
		return GDK_FAIL;
	}
	ATOMIC_SET(&BBPsize, bbpsize);

	if (!GDKinmemory(0)) {
		if (BBPreadEntries(fp, bbpversion, lineno
#ifdef GDKLIBRARY_HASHASH
				   , &hashbats, &nhashbats
#endif
			    ) != GDK_SUCCEED) {
			GDKdebug = dbg;
			return GDK_FAIL;
		}
		fclose(fp);
	}

	MT_lock_set(&BBPnameLock);
	if (BBPinithash((bat) ATOMIC_GET(&BBPsize)) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "BBPinithash failed");
		MT_lock_unset(&BBPnameLock);
#ifdef GDKLIBRARY_HASHASH
		GDKfree(hashbats);
#endif
		GDKdebug = dbg;
		return GDK_FAIL;
	}
	MT_lock_unset(&BBPnameLock);

	/* will call BBPrecover if needed */
	if (!GDKinmemory(0)) {
		BBPtmlock();
		gdk_return rc = BBPprepare(false);
		BBPtmunlock();
		if (rc != GDK_SUCCEED) {
#ifdef GDKLIBRARY_HASHASH
			GDKfree(hashbats);
#endif
			TRC_CRITICAL(GDK, "cannot properly prepare process %s.", BAKDIR);
			GDKdebug = dbg;
			return rc;
		}
	}

	if (BBPcheckbats(bbpversion) != GDK_SUCCEED) {
#ifdef GDKLIBRARY_HASHASH
		GDKfree(hashbats);
#endif
		GDKdebug = dbg;
		return GDK_FAIL;
	}

#ifdef GDKLIBRARY_TAILN
	char *needstrbatmove;
	if (GDKinmemory(0)) {
		needstrbatmove = NULL;
	} else {
		needstrbatmove = GDKfilepath(0, BATDIR, "needstrbatmove", NULL);
		if (bbpversion <= GDKLIBRARY_TAILN) {
			/* create signal file that we need to rename string
			 * offset heaps */
			int fd = MT_open(needstrbatmove, O_WRONLY | O_CREAT);
			if (fd < 0) {
				TRC_CRITICAL(GDK, "cannot create signal file needstrbatmove.\n");
				GDKfree(needstrbatmove);
#ifdef GDKLIBRARY_HASHASH
				GDKfree(hashbats);
#endif
				GDKdebug = dbg;
				return GDK_FAIL;
			}
			close(fd);
		} else {
			/* check signal file whether we need to rename string
			 * offset heaps */
			int fd = MT_open(needstrbatmove, O_RDONLY);
			if (fd >= 0) {
				/* yes, we do */
				close(fd);
			} else if (errno == ENOENT) {
				/* no, we don't: set var to NULL */
				GDKfree(needstrbatmove);
				needstrbatmove = NULL;
			} else {
				GDKsyserror("unexpected error opening %s\n", needstrbatmove);
				GDKfree(needstrbatmove);
#ifdef GDKLIBRARY_HASHASH
				GDKfree(hashbats);
#endif
				GDKdebug = dbg;
				return GDK_FAIL;
			}
		}
	}
#endif

#ifdef GDKLIBRARY_HASHASH
	if (nhashbats > 0)
		res = fixhashash(hashbats, nhashbats);
	GDKfree(hashbats);
	if (res != GDK_SUCCEED)
		return res;
#endif

	if (bbpversion < GDKLIBRARY && TMcommit() != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "TMcommit failed\n");
		GDKdebug = dbg;
		return GDK_FAIL;
	}

#ifdef GDKLIBRARY_TAILN
	/* we rename the offset heaps after the above commit: in this
	 * version we accept both the old and new names, but we want to
	 * convert so that future versions only have the new name */
	if (needstrbatmove) {
		/* note, if renaming fails, nothing is lost: a next
		 * invocation will just try again; an older version of
		 * mserver will not work because of the TMcommit
		 * above */
		if (movestrbats() != GDK_SUCCEED) {
			GDKfree(needstrbatmove);
			GDKdebug = dbg;
			return GDK_FAIL;
		}
		MT_remove(needstrbatmove);
		GDKfree(needstrbatmove);
		needstrbatmove = NULL;
	}
#endif
	GDKdebug = dbg;

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

	manager = THRcreate(BBPmanager, NULL, MT_THR_DETACHED, "BBPmanager");
	return GDK_SUCCEED;

  bailout:
	/* now it is time for real panic */
	TRC_CRITICAL(GDK, "could not write %s%cBBP.dir.", BATDIR, DIR_SEP);
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

	//BBPlock();	/* stop all threads ever touching more descriptors */

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
					MT_lock_set(&b->theaplock);
					bat tp = VIEWtparent(b);
					if (tp != 0) {
						BBP_desc(tp)->batSharecnt--;
						--BBP_lrefs(tp);
						HEAPdecref(b->theap, false);
						b->theap = NULL;
					}
					tp = VIEWvtparent(b);
					if (tp != 0) {
						BBP_desc(tp)->batSharecnt--;
						--BBP_lrefs(tp);
						HEAPdecref(b->tvheap, false);
						b->tvheap = NULL;
					}
					if (b->oldtail) {
						Heap *h = b->oldtail;
						b->oldtail = NULL;
						ATOMIC_AND(&h->refs, ~DELAYEDREMOVE);
						HEAPdecref(h, false);
					}
					PROPdestroy_nolock(b);
					MT_lock_unset(&b->theaplock);
					BATfree(b);
				}
				BBP_pid(i) = 0;
				BBPuncacheit(i, true);
				if (BBP_logical(i) != BBP_bak(i))
					GDKfree(BBP_logical(i));
				BBP_logical(i) = NULL;
			}
		}
	} while (skipped);
	GDKfree(BBP_hash);
	BBP_hash = NULL;
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
heap_entry(FILE *fp, BATiter *bi, BUN size)
{
	size_t free = bi->hfree;
	if (size < BUN_NONE) {
		if ((bi->type >= 0 && ATOMstorage(bi->type) == TYPE_msk))
			free = ((size + 31) / 32) * 4;
		else if (bi->width > 0)
			free = size << bi->shift;
		else
			free = 0;
	}

	return fprintf(fp, " %s %d %d %d " BUNFMT " " BUNFMT " " BUNFMT " "
		       BUNFMT " " OIDFMT " %zu %" PRIu64" %" PRIu64,
		       bi->type >= 0 ? BATatoms[bi->type].name : ATOMunknown_name(bi->type),
		       bi->width,
		       bi->type == TYPE_void || bi->vh != NULL,
		       (unsigned short) bi->sorted |
			   ((unsigned short) bi->revsorted << 7) |
			   ((unsigned short) bi->key << 8) |
		           ((unsigned short) BATtdensebi(bi) << 9) |
			   ((unsigned short) bi->nonil << 10) |
			   ((unsigned short) bi->nil << 11),
		       bi->nokey[0] >= size || bi->nokey[1] >= size ? 0 : bi->nokey[0],
		       bi->nokey[0] >= size || bi->nokey[1] >= size ? 0 : bi->nokey[1],
		       bi->nosorted >= size ? 0 : bi->nosorted,
		       bi->norevsorted >= size ? 0 : bi->norevsorted,
		       bi->tseq,
		       free,
		       bi->minpos < size ? (uint64_t) bi->minpos : (uint64_t) oid_nil,
		       bi->maxpos < size ? (uint64_t) bi->maxpos : (uint64_t) oid_nil);
}

static inline int
vheap_entry(FILE *fp, BATiter *bi, BUN size)
{
	(void) size;
	if (bi->vh == NULL)
		return 0;
	return fprintf(fp, " %zu", size == 0 ? 0 : bi->vhfree);
}

static gdk_return
new_bbpentry(FILE *fp, bat i, BUN size, BATiter *bi)
{
#ifndef NDEBUG
	assert(i > 0);
	assert(i < (bat) ATOMIC_GET(&BBPsize));
	assert(bi->b);
	assert(bi->b->batCacheid == i);
	assert(bi->b->batRole == PERSISTENT);
	assert(0 <= bi->h->farmid && bi->h->farmid < MAXFARMS);
	assert(BBPfarms[bi->h->farmid].roles & (1U << PERSISTENT));
	if (bi->vh) {
		assert(0 <= bi->vh->farmid && bi->vh->farmid < MAXFARMS);
		assert(BBPfarms[bi->vh->farmid].roles & (1U << PERSISTENT));
	}
	assert(size <= bi->count || size == BUN_NONE);
	assert(BBP_options(i) == NULL || strpbrk(BBP_options(i), "\r\n") == NULL);
#endif

	if (BBP_options(i) != NULL && strpbrk(BBP_options(i), "\r\n") != NULL) {
		GDKerror("options for bat %d contains a newline\n", i);
		return GDK_FAIL;
	}
	if (size > bi->count)
		size = bi->count;
	if (fprintf(fp, "%d %u %s %s %d " BUNFMT " " OIDFMT,
		    /* BAT info */
		    (int) i,
		    BBP_status(i) & BBPPERSISTENT,
		    BBP_logical(i),
		    BBP_physical(i),
		    (unsigned) bi->restricted << 1,
		    size,
		    bi->b->hseqbase) < 0 ||
	    heap_entry(fp, bi, size) < 0 ||
	    vheap_entry(fp, bi, size) < 0 ||
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
		    SIZEOF_HGE
#else
		    SIZEOF_LNG
#endif
		    , n, logno, transid) < 0 ||
	    ferror(f)) {
		GDKsyserror("Writing BBP.dir header failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static gdk_return
BBPdir_first(bool subcommit, lng logno, lng transid,
	     FILE **obbpfp, FILE **nbbpfp)
{
	FILE *obbpf = NULL, *nbbpf = NULL;
	int n = 0;
	lng ologno, otransid;

	if (obbpfp)
		*obbpfp = NULL;
	*nbbpfp = NULL;

	if ((nbbpf = GDKfilelocate(0, "BBP", "w", "dir")) == NULL) {
		return GDK_FAIL;
	}

	if (subcommit) {
		char buf[512];

		assert(obbpfp != NULL);
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
		/* fourth line contains BBPinfo */
		if (fgets(buf, sizeof(buf), obbpf) == NULL ||
		    sscanf(buf, "BBPinfo=" LLSCN " " LLSCN, &ologno, &otransid) != 2) {
			GDKerror("cannot read BBPinfo in backup BBP.dir.");
			goto bailout;
		}
	}

	if (n < (bat) ATOMIC_GET(&BBPsize))
		n = (bat) ATOMIC_GET(&BBPsize);

	TRC_DEBUG(IO_, "writing BBP.dir (%d bats).\n", n);

	if (BBPdir_header(nbbpf, n, logno, transid) != GDK_SUCCEED) {
		goto bailout;
	}

	if (obbpfp)
		*obbpfp = obbpf;
	*nbbpfp = nbbpf;

	return GDK_SUCCEED;

  bailout:
	if (obbpf != NULL)
		fclose(obbpf);
	if (nbbpf != NULL)
		fclose(nbbpf);
	return GDK_FAIL;
}

static bat
BBPdir_step(bat bid, BUN size, int n, char *buf, size_t bufsize,
	    FILE **obbpfp, FILE *nbbpf, BATiter *bi)
{
	if (n < -1)		/* safety catch */
		return n;
	while (n >= 0 && n < bid) {
		if (n > 0) {
			if (fputs(buf, nbbpf) == EOF) {
				GDKerror("Writing BBP.dir file failed.\n");
				goto bailout;
			}
		}
		if (fgets(buf, (int) bufsize, *obbpfp) == NULL) {
			if (ferror(*obbpfp)) {
				GDKerror("error reading backup BBP.dir.");
				goto bailout;
			}
			n = -1;
			if (fclose(*obbpfp) == EOF) {
				GDKsyserror("Closing backup BBP.dir file failed.\n");
				GDKclrerr(); /* ignore error */
			}
			*obbpfp = NULL;
		} else {
			if (sscanf(buf, "%d", &n) != 1 || n <= 0 || n >= N_BBPINIT * BBPINIT) {
				GDKerror("subcommit attempted with invalid backup BBP.dir.");
				goto bailout;
			}
		}
	}
	if (BBP_status(bid) & BBPPERSISTENT) {
		if (new_bbpentry(nbbpf, bid, size, bi) != GDK_SUCCEED)
			goto bailout;
	}
	return n == -1 ? -1 : n == bid ? 0 : n;

  bailout:
	if (*obbpfp)
		fclose(*obbpfp);
	fclose(nbbpf);
	return -2;
}

static gdk_return
BBPdir_last(int n, char *buf, size_t bufsize, FILE *obbpf, FILE *nbbpf)
{
	if (n > 0 && fputs(buf, nbbpf) == EOF) {
		GDKerror("Writing BBP.dir file failed.\n");
		goto bailout;
	}
	while (obbpf) {
		if (fgets(buf, (int) bufsize, obbpf) == NULL) {
			if (ferror(obbpf)) {
				GDKerror("error reading backup BBP.dir.");
				goto bailout;
			}
			if (fclose(obbpf) == EOF) {
				GDKsyserror("Closing backup BBP.dir file failed.\n");
				GDKclrerr(); /* ignore error */
			}
			obbpf = NULL;
		} else {
			if (fputs(buf, nbbpf) == EOF) {
				GDKerror("Writing BBP.dir file failed.\n");
				goto bailout;
			}
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
		nbbpf = NULL;	/* can't close again */
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
BBPdir_init(void)
{
	FILE *fp;
	gdk_return rc;

	rc = BBPdir_first(false, 0, 0, NULL, &fp);
	if (rc == GDK_SUCCEED)
		rc = BBPdir_last(-1, NULL, 0, NULL, fp);
	return rc;
}

/* function used for debugging */
void
BBPdump(void)
{
	size_t mem = 0, vm = 0;
	size_t cmem = 0, cvm = 0;
	int n = 0, nc = 0;

	for (bat i = 0; i < (bat) ATOMIC_GET(&BBPsize); i++) {
		if (BBP_refs(i) == 0 && BBP_lrefs(i) == 0)
			continue;
		BAT *b = BBP_desc(i);
		unsigned status = BBP_status(i);
		fprintf(stderr,
			"# %d: " ALGOOPTBATFMT " "
			"refs=%d lrefs=%d "
			"status=%u%s",
			i,
			ALGOOPTBATPAR(b),
			BBP_refs(i),
			BBP_lrefs(i),
			status,
			BBP_cache(i) ? "" : " not cached");
		if (b == NULL) {
			fprintf(stderr, ", no descriptor\n");
			continue;
		}
		if (b->batSharecnt > 0)
			fprintf(stderr, " shares=%d", b->batSharecnt);
		if (b->theap) {
			if (b->theap->parentid != b->batCacheid) {
				fprintf(stderr, " Theap -> %d", b->theap->parentid);
			} else {
				fprintf(stderr,
					" Theap=[%zu,%zu,f=%d]%s%s",
					b->theap->free,
					b->theap->size,
					b->theap->farmid,
					b->theap->base == NULL ? "X" : b->theap->storage == STORE_MMAP ? "M" : "",
					status & BBPSWAPPED ? "(Swapped)" : b->theap->dirty ? "(Dirty)" : "");
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
					" Tvheap=[%zu,%zu,f=%d]%s%s",
					b->tvheap->free,
					b->tvheap->size,
					b->tvheap->farmid,
					b->tvheap->base == NULL ? "X" : b->tvheap->storage == STORE_MMAP ? "M" : "",
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
		if (MT_rwlock_rdtry(&b->thashlock)) {
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
			MT_rwlock_rdunlock(&b->thashlock);
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
			MT_lock_set(&BBPnameLock);
		for (i = BBP_hash[strHash(nme) & BBP_mask]; i; i = BBP_next(i)) {
			if (strcmp(BBP_logical(i), nme) == 0)
				break;
		}
		if (lock)
			MT_lock_unset(&BBPnameLock);
	}
	return i;
}

bat
BBPindex(const char *nme)
{
	return BBP_find(nme, true);
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

/* The free list is empty.  We create a new entry by either just
 * increasing BBPsize (up to BBPlimit) or extending the BBP (which
 * increases BBPlimit).
 *
 * Note that this is the only place in normal, multi-threaded operation
 * where BBPsize is assigned a value (never decreasing), that the
 * assignment happens after any necessary memory was allocated and
 * initialized, and that this happens when the BBPnameLock is held. */
static gdk_return
maybeextend(void)
{
	bat size = (bat) ATOMIC_GET(&BBPsize);
	if (size >= BBPlimit &&
	    BBPextend(true, size + 1) != GDK_SUCCEED) {
		/* nothing available */
		return GDK_FAIL;
	} else {
		ATOMIC_SET(&BBPsize, size + 1);
		BBP_free = size;
	}
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
	int len = 0;

	/* critical section: get a new BBP entry */
	if (lock) {
		MT_lock_set(&GDKcacheLock);
	}

	/* find an empty slot */
	if (BBP_free <= 0) {
		/* we need to extend the BBP */
		gdk_return r = GDK_SUCCEED;
		MT_lock_set(&BBPnameLock);
		/* check again in case some other thread extended
		 * while we were waiting */
		if (BBP_free <= 0) {
			r = maybeextend();
		}
		MT_lock_unset(&BBPnameLock);
		if (r != GDK_SUCCEED) {
			if (lock) {
				MT_lock_unset(&GDKcacheLock);
			}
			return 0;
		}
	}
	i = BBP_free;
	assert(i > 0);
	BBP_free = BBP_next(i);

	if (lock) {
		MT_lock_unset(&GDKcacheLock);
	}
	/* rest of the work outside the lock */

	/* fill in basic BBP fields for the new bat */

	bn->batCacheid = i;
	bn->creator_tid = MT_getpid();

	MT_lock_set(&GDKswapLock(i));
	BBP_status_set(i, BBPDELETING|BBPHOT);
	BBP_cache(i) = NULL;
	BBP_desc(i) = bn;
	BBP_refs(i) = 1;	/* new bats have 1 pin */
	BBP_lrefs(i) = 0;	/* ie. no logical refs */
	BBP_pid(i) = MT_getpid();
	MT_lock_unset(&GDKswapLock(i));

	if (*BBP_bak(i) == 0)
		len = snprintf(BBP_bak(i), sizeof(BBP_bak(i)), "tmp_%o", (unsigned) i);
	if (len == -1 || len >= FILENAME_MAX) {
		GDKerror("impossible error\n");
		return 0;
	}
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

		TRC_DEBUG(BAT_, "%d = new %s(%s)\n", (int) i, BBP_logical(i), ATOMname(bn->ttype));
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

	assert(i > 0);

	if (lock)
		MT_lock_set(&GDKswapLock(i));
	mode = (BBP_status(i) | BBPLOADED) & ~(BBPLOADING | BBPDELETING | BBPSWAPPED);

	/* cache it! */
	BBP_cache(i) = bn;

	BBP_status_set(i, mode);

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
	if (BBPcheck(i)) {
		BAT *b = BBP_desc(i);

		assert(unloaddesc || BBP_refs(i) == 0);

		if (b) {
			if (BBP_cache(i)) {
				TRC_DEBUG(BAT_, "uncache %d (%s)\n", (int) i, BBP_logical(i));

				/* clearing bits can be done without the lock */
				BBP_status_off(i, BBPLOADED);

				BBP_cache(i) = NULL;
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
bbpclear(bat i, bool lock)
{
	TRC_DEBUG(BAT_, "clear %d (%s)\n", (int) i, BBP_logical(i));
	BBPuncacheit(i, true);
	TRC_DEBUG(BAT_, "set to unloading %d\n", i);
	if (lock) {
		MT_lock_set(&GDKcacheLock);
		MT_lock_set(&GDKswapLock(i));
	}

	BBP_status_set(i, BBPUNLOADING);
	BBP_refs(i) = 0;
	BBP_lrefs(i) = 0;
	if (lock)
		MT_lock_unset(&GDKswapLock(i));
	if (!BBPtmpcheck(BBP_logical(i))) {
		MT_lock_set(&BBPnameLock);
		BBP_delete(i);
		MT_lock_unset(&BBPnameLock);
	}
	if (BBP_logical(i) != BBP_bak(i))
		GDKfree(BBP_logical(i));
	BBP_status_set(i, 0);
	BBP_logical(i) = NULL;
	BBP_next(i) = BBP_free;
	BBP_free = i;
	BBP_pid(i) = ~(MT_Id)0; /* not zero, not a valid thread id */
	if (lock)
		MT_lock_unset(&GDKcacheLock);
}

void
BBPclear(bat i)
{
	if (BBPcheck(i)) {
		bool lock = locked_by == 0 || locked_by != MT_getpid();
		bbpclear(i, lock);
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
BBPrename(BAT *b, const char *nme)
{
	if (b == NULL)
		return 0;

	char dirname[24];
	bat bid = b->batCacheid;
	bat tmpid = 0, i;

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

	MT_lock_set(&BBPnameLock);
	i = BBP_find(nme, false);
	if (i != 0) {
		MT_lock_unset(&BBPnameLock);
		GDKerror("name is in use: '%s'.\n", nme);
		return BBPRENAME_ALREADY;
	}

	char *nnme;
	if (nme == BBP_bak(bid) || strcmp(nme, BBP_bak(bid)) == 0) {
		nnme = BBP_bak(bid);
	} else {
		nnme = GDKstrdup(nme);
		if (nnme == NULL) {
			MT_lock_unset(&BBPnameLock);
			return BBPRENAME_MEMORY;
		}
	}

	/* carry through the name change */
	if (BBP_logical(bid) && !BBPtmpcheck(BBP_logical(bid))) {
		BBP_delete(bid);
	}
	if (BBP_logical(bid) != BBP_bak(bid))
		GDKfree(BBP_logical(bid));
	BBP_logical(bid) = nnme;
	if (tmpid == 0) {
		BBP_insert(bid);
	}
	MT_lock_set(&b->theaplock);
	bool transient = b->batTransient;
	MT_lock_unset(&b->theaplock);
	if (!transient) {
		bool lock = locked_by == 0 || locked_by != MT_getpid();

		if (lock)
			MT_lock_set(&GDKswapLock(i));
		BBP_status_on(bid, BBPRENAMED);
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
	}
	MT_lock_unset(&BBPnameLock);
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
	if (BBPcheck(i) && (BBP_status(i) & event)) {
		lng spin = LL_CONSTANT(0);

		do {
			MT_sleep_ms(KITTENNAP);
			spin++;
		} while (BBP_status(i) & event);
		TRC_DEBUG(BAT_, "%d,%s,%u: " LLFMT " loops\n", (int) i, s, event, spin);
	}
}

void
BBPcold(bat i)
{
	if (!is_bat_nil(i)) {
		BAT *b = BBP_cache(i);
		if (b == NULL)
			b = BBP_desc(i);
		if (b == NULL || b->batRole == PERSISTENT)
			BBP_status_off(i, BBPHOT);
	}
}

/* This function can fail if the input parameter (i) is incorrect
 * (unlikely). */
static inline int
incref(bat i, bool logical, bool lock)
{
	int refs;
	BAT *b;

	if (!BBPcheck(i))
		return 0;

	if (lock) {
		for (;;) {
			MT_lock_set(&GDKswapLock(i));
			if (!(BBP_status(i) & (BBPUNSTABLE|BBPLOADING)))
				break;
			/* the BATs is "unstable", try again */
			MT_lock_unset(&GDKswapLock(i));
			BBPspin(i, __func__, BBPUNSTABLE|BBPLOADING);
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
		refs = ++BBP_lrefs(i);
		BBP_pid(i) = 0;
	} else {
		refs = ++BBP_refs(i);
		BBP_status_on(i, BBPHOT);
	}
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	return refs;
}

/* increment the physical reference counter for the given bat
 * returns the new reference count
 * also increments the physical reference count of the parent bat(s) (if
 * any) */
int
BBPfix(bat i)
{
	return BATdescriptor(i) ? 1 : 0;
}

/* increment the logical reference count for the given bat
 * returns the new reference count */
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
	(void) BATdescriptor(parent);
}

static inline int
decref(bat i, bool logical, bool releaseShare, bool recurse, bool lock, const char *func)
{
	int refs = 0, lrefs;
	bool swap = false;
	bool locked = false;
	bat tp = 0, tvp = 0;
	int farmid = 0;
	BAT *b;

	if (is_bat_nil(i))
		return -1;
	assert(i > 0);
	if (BBPcheck(i) == 0)
		return -1;

	if (lock)
		MT_lock_set(&GDKswapLock(i));
	if (releaseShare) {
		assert(BBP_lrefs(i) > 0);
		if (BBP_desc(i)->batSharecnt == 0) {
			GDKerror("%s: %s does not have any shares.\n", func, BBP_logical(i));
			assert(0);
		} else {
			--BBP_desc(i)->batSharecnt;
		}
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
			GDKerror("%s: %s does not have logical references.\n", func, BBP_logical(i));
			assert(0);
		} else {
			refs = --BBP_lrefs(i);
		}
		/* cannot release last logical ref if still shared */
		assert(BBP_desc(i)->batSharecnt == 0 || refs > 0);
	} else {
		if (BBP_refs(i) == 0) {
			GDKerror("%s: %s does not have pointer fixes.\n", func, BBP_logical(i));
			assert(0);
		} else {
#ifndef NDEBUG
			if (b) {
				MT_lock_set(&b->theaplock);
				locked = true;
				assert(b->theap == NULL || BBP_refs(b->theap->parentid) > 0);
				assert(b->tvheap == NULL || BBP_refs(b->tvheap->parentid) > 0);
			}
#endif
			refs = --BBP_refs(i);
			if (b && refs == 0) {
#ifdef NDEBUG
				/* if NDEBUG is not defined, we locked
				 * the heaplock above, so we only lock
				 * it here if NDEBUG *is* defined */
				MT_lock_set(&b->theaplock);
				locked = true;
#endif
				assert(locked); /* just to be clear */
				tp = VIEWtparent(b);
				tvp = VIEWvtparent(b);
				if (tp || tvp)
					BBP_status_on(i, BBPHOT);
			}
		}
	}
	if (b) {
		if (!locked) {
			MT_lock_set(&b->theaplock);
			locked = true;
		}
#if 0
		if (b->batCount > b->batInserted && !isVIEW(b)) {
			/* if batCount is larger than batInserted and
			 * the dirty bits are off, it may be that a
			 * (sub)commit happened in parallel to an
			 * update; we must undo the turning off of the
			 * dirty bits */
			if (b->theap && b->theap->parentid == i)
				b->theap->dirty = true;
			if (b->tvheap && b->tvheap->parentid == i)
				b->tvheap->dirty = true;
		}
#endif
		if (b->theap)
			farmid = b->theap->farmid;
	}

	/* we destroy transients asap and unload persistent bats only
	 * if they have been made cold or are not dirty */
	unsigned chkflag = BBPSYNCING;
	if (b && GDKvm_cursize() < GDK_vm_maxsize) {
		if (!locked) {
			MT_lock_set(&b->theaplock);
			locked = true;
		}
		if (((b->theap ? b->theap->size : 0) + (b->tvheap ? b->tvheap->size : 0)) < (GDK_vm_maxsize - GDKvm_cursize()) / 32)
			chkflag |= BBPHOT;
	}
	/* only consider unloading if refs is 0; if, in addition, lrefs
	 * is 0, we can definitely unload, else only if some more
	 * conditions are met */
	if (BBP_refs(i) == 0 &&
	    (BBP_lrefs(i) == 0 ||
	     (b != NULL && b->theap != NULL
	      ? (!BATdirty(b) &&
		 !(BBP_status(i) & chkflag) &&
		 (BBP_status(i) & BBPPERSISTENT) &&
		 /* cannot unload in-memory data */
		 !GDKinmemory(farmid) &&
		 /* do not unload views or parents of views */
		 b->batSharecnt == 0 &&
		 b->batCacheid == b->theap->parentid &&
		 (b->tvheap == NULL || b->batCacheid == b->tvheap->parentid))
	      : (BBP_status(i) & BBPTMP)))) {
		/* bat will be unloaded now. set the UNLOADING bit
		 * while locked so no other thread thinks it's
		 * available anymore */
		assert((BBP_status(i) & BBPUNLOADING) == 0);
		TRC_DEBUG(BAT_, "%s set to unloading BAT %d (status %u, lrefs %d)\n", func, i, BBP_status(i), BBP_lrefs(i));
		BBP_status_on(i, BBPUNLOADING);
		swap = true;
	} /* else: bat cannot be swapped out */
	lrefs = BBP_lrefs(i);
	if (locked)
		MT_lock_unset(&b->theaplock);

	/* unlock before re-locking in unload; as saving a dirty
	 * persistent bat may take a long time */
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	if (swap) {
		if (b != NULL) {
			if (lrefs == 0 && (BBP_status(i) & BBPDELETED) == 0) {
				/* free memory (if loaded) and delete from
				 * disk (if transient but saved) */
				BBPdestroy(b);
			} else {
				TRC_DEBUG(BAT_, "%s unload and free bat %d\n", func, i);
				/* free memory of transient */
				if (BBPfree(b) != GDK_SUCCEED)
					return -1;	/* indicate failure */
			}
		} else if (lrefs == 0 && (BBP_status(i) & BBPDELETED) == 0) {
			if ((b = BBP_desc(i)) != NULL)
				BATdelete(b);
			BBPclear(i);
		} else {
			BBP_status_off(i, BBPUNLOADING);
		}
	}
	if (recurse) {
		if (tp)
			decref(tp, false, false, true, lock, func);
		if (tvp)
			decref(tvp, false, false, true, lock, func);
	}
	return refs;
}

int
BBPunfix(bat i)
{
	return decref(i, false, false, true, true, __func__);
}

int
BBPrelease(bat i)
{
	return decref(i, true, false, true, true, __func__);
}

/*
 * M5 often changes the physical ref into a logical reference.  This
 * state change consist of the sequence BBPretain(b);BBPunfix(b).
 * A faster solution is given below, because it does not trigger the
 * BBP management actions, such as garbage collecting the bats.
 * [first step, initiate code change]
 */
void
BBPkeepref(BAT *b)
{
	assert(b != NULL);
	bool lock = locked_by == 0 || locked_by != MT_getpid();
	int i = b->batCacheid;
	int refs = incref(i, true, lock);
	if (refs == 1) {
		MT_lock_set(&b->theaplock);
		BATsettrivprop(b);
		MT_lock_unset(&b->theaplock);
	}
	if (GDKdebug & CHECKMASK)
		BATassertProps(b);
	if (BATsetaccess(b, BAT_READ) == NULL)
		return;		/* already decreffed */

	refs = decref(i, false, false, true, lock, __func__);
	(void) refs;
	assert(refs >= 0);
}

BAT *
BATdescriptor(bat i)
{
	BAT *b = NULL;

	if (BBPcheck(i)) {
		bool lock = locked_by == 0 || locked_by != MT_getpid();
		/* parent bats get a single fix for all physical
		 * references of a view and in order to do that
		 * properly, we must incref the parent bats always
		 * before our own incref, then after that decref them if
		 * we were not the first */
		int tp = 0, tvp = 0;
		if ((b = BBP_desc(i)) != NULL) {
			MT_lock_set(&b->theaplock);
			tp = b->theap->parentid;
			tvp = b->tvheap ? b->tvheap->parentid : 0;
			MT_lock_unset(&b->theaplock);
			if (tp != i) {
				if (BATdescriptor(tp) == NULL) {
					return NULL;
				}
			}
			if (tvp != 0 && tvp != i) {
				if (BATdescriptor(tvp) == NULL) {
					if (tp != i)
						BBPunfix(tp);
					return NULL;
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
				BBPspin(i, __func__, BBPUNSTABLE|BBPLOADING);
			}
		}
		int refs;
		if ((refs = incref(i, false, false)) > 0) {
			b = BBP_cache(i);
			if (b == NULL)
				b = getBBPdescriptor(i);
		} else {
			/* if incref fails, we must return NULL */
			b = NULL;
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		if (refs != 1) {
			/* unfix both in case of failure (<= 0) and when
			 * not the first (> 1) */
			if (tp != 0 && tp != i)
				BBPunfix(tp);
			if (tvp != 0 && tvp != i)
				BBPunfix(tvp);
		}
	}
	return b;
}

void
BBPunshare(bat parent)
{
	(void) decref(parent, false, true, true, true, __func__);
	(void) decref(parent, true, false, true, true, __func__);
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

	return decref(i, false, false, true, lock, __func__) < 0;
}

/*
 * BBPdescriptor checks whether BAT needs loading and does so if
 * necessary. You must have at least one fix on the BAT before calling
 * this.
 */
static BAT *
getBBPdescriptor(bat i)
{
	bool load = false;
	BAT *b = NULL;

	assert(i > 0);
	if (!BBPcheck(i)) {
		GDKerror("BBPcheck failed for bat id %d\n", i);
		return NULL;
	}
	assert(BBP_refs(i));
	if ((b = BBP_cache(i)) == NULL || BBP_status(i) & BBPWAITING) {

		while (BBP_status(i) & BBPWAITING) {	/* wait for bat to be loaded by other thread */
			MT_lock_unset(&GDKswapLock(i));
			BBPspin(i, __func__, BBPWAITING);
			MT_lock_set(&GDKswapLock(i));
		}
		if (BBPvalid(i)) {
			b = BBP_cache(i);
			if (b == NULL) {
				load = true;
				TRC_DEBUG(BAT_, "set to loading BAT %d\n", i);
				BBP_status_on(i, BBPLOADING);
			}
		}
	}
	if (load) {
		TRC_DEBUG(IO_, "load %s\n", BBP_logical(i));

		b = BATload_intern(i, false);

		/* clearing bits can be done without the lock */
		BBP_status_off(i, BBPLOADING);
		CHECKDEBUG if (b != NULL)
			BATassertProps(b);
	}
	return b;
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

	MT_lock_set(&b->theaplock);
	if (BBP_lrefs(bid) == 0 || isVIEW(b) || !BATdirty(b)) {
		/* do nothing */
		MT_lock_unset(&b->theaplock);
		MT_rwlock_rdlock(&b->thashlock);
		if (b->thash && b->thash != (Hash *) 1 &&
		    (b->thash->heaplink.dirty || b->thash->heapbckt.dirty))
			BAThashsave(b, (BBP_status(bid) & BBPPERSISTENT) != 0);
		MT_rwlock_rdunlock(&b->thashlock);
		return GDK_SUCCEED;
	}
	MT_lock_unset(&b->theaplock);
	if (lock)
		MT_lock_set(&GDKswapLock(bid));

	if (BBP_status(bid) & BBPSAVING) {
		/* wait until save in other thread completes */
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));
		BBPspin(bid, __func__, BBPSAVING);
	} else {
		/* save it */
		unsigned flags = BBPSAVING;

		MT_lock_set(&b->theaplock);
		if (DELTAdirty(b)) {
			flags |= BBPSWAPPED;
		}
		if (b->batTransient) {
			flags |= BBPTMP;
		}
		MT_lock_unset(&b->theaplock);
		BBP_status_on(bid, flags);
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
		BBP_status_off(bid, BBPSAVING);
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

	if (tp == 0) {
		/* bats that get destroyed must unfix their atoms */
		gdk_return (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		assert(b->batSharecnt == 0);
		if (tunfix) {
			BUN p, q;
			BATiter bi = bat_iterator_nolock(b);

			BATloop(b, p, q) {
				/* ignore errors */
				(void) (*tunfix)(BUNtail(bi, p));
			}
		}
	}
	if (tp != 0) {
		HEAPdecref(b->theap, false);
		b->theap = NULL;
	}
	if (vtp != 0) {
		HEAPdecref(b->tvheap, false);
		b->tvheap = NULL;
	}
	if (b->oldtail) {
		ATOMIC_AND(&b->oldtail->refs, ~DELAYEDREMOVE);
		HEAPdecref(b->oldtail, true);
		b->oldtail = NULL;
	}
	BATdelete(b);

	BBPclear(b->batCacheid);	/* if destroyed; de-register from BBP */

	/* parent released when completely done with child */
	if (tp)
		BBPunshare(tp);
	if (vtp)
		BBPunshare(vtp);
}

static gdk_return
BBPfree(BAT *b)
{
	bat bid = b->batCacheid, tp = VIEWtparent(b), vtp = VIEWvtparent(b);
	gdk_return ret;

	assert(bid > 0);
	assert(BBPswappable(b));
	assert(!isVIEW(b));

	BBP_unload_inc();
	/* write dirty BATs before unloading */
	ret = BBPsave(b);
	if (ret == GDK_SUCCEED) {
		if (BBP_cache(bid))
			BATfree(b);	/* free memory */
		BBPuncacheit(bid, false);
	}
	TRC_DEBUG(BAT_, "turn off unloading %d\n", bid);
	BBP_status_off(bid, BBPUNLOADING);
	BBP_unload_dec();

	/* parent released when completely done with child */
	if (ret == GDK_SUCCEED && tp)
		BBPunshare(tp);
	if (ret == GDK_SUCCEED && vtp)
		BBPunshare(vtp);
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
 * pointers in the BAT descriptor.
 */
BAT *
BBPquickdesc(bat bid)
{
	BAT *b;

	if (!BBPcheck(bid)) {
		if (!is_bat_nil(bid)) {
			GDKerror("called with invalid batid.\n");
			assert(0);
		}
		return NULL;
	}
	BBPspin(bid, __func__, BBPWAITING);
	b = BBP_desc(bid);
	if (b && b->ttype < 0) {
		const char *aname = ATOMunknown_name(b->ttype);
		int tt = ATOMindex(aname);
		if (tt < 0) {
			GDKwarning("atom '%s' unknown in bat '%s'.\n",
				   aname, BBP_physical(bid));
		} else {
			b->ttype = tt;
		}
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
		BBPspin(*i, __func__, BBPSAVING);
		b = BBP_cache(*i);
		if (b != NULL) {
			MT_lock_set(&b->theaplock);
			if ((BBP_status(*i) & BBPNEW) &&
			    BATcheckmodes(b, false) != GDK_SUCCEED) /* check mmap modes */
				*i = -*i;	/* error */
			else if ((BBP_status(*i) & BBPPERSISTENT) &&
			    (subcommit || BATdirty(b))) {
				MT_lock_unset(&b->theaplock);
				return b;	/* the bat is loaded, persistent and dirty */
			}
			MT_lock_unset(&b->theaplock);
		} else if (BBP_status(*i) & BBPSWAPPED) {
			b = (BAT *) BBPquickdesc(*i);
			if (b) {
				if (subcommit) {
					return b;	/* only the desc is loaded */
				}
			}
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
	if (GDKmove(farmid, srcdir, name, ext, dstdir, name, ext, false) == GDK_SUCCEED) {
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
	gdk_return ret = GDK_SUCCEED;

	start_subcommit = (subcommit && backup_subdir == 0);
	if (start_subcommit) {
		/* starting a subcommit. Make sure SUBDIR and DELDIR
		 * are clean */
		ret = BBPrecover_subdir();
		if (ret != GDK_SUCCEED)
			return ret;
	}
	if (backup_files == 0) {
		backup_dir = 0;
		ret = BBPrecover(0);
		if (ret != GDK_SUCCEED)
			return ret;
		str bakdirpath = GDKfilepath(0, NULL, BAKDIR, NULL);
		if (bakdirpath == NULL) {
			return GDK_FAIL;
		}

		if (MT_mkdir(bakdirpath) < 0 && errno != EEXIST) {
			GDKsyserror("cannot create directory %s\n", bakdirpath);
			GDKfree(bakdirpath);
			return GDK_FAIL;
		}
		/* if BAKDIR already exists, don't signal error */
		TRC_DEBUG(IO_, "mkdir %s = %d\n", bakdirpath, (int) ret);
		GDKfree(bakdirpath);
	}
	if (start_subcommit) {
		/* make a new SUBDIR (subdir of BAKDIR) */
		str subdirpath = GDKfilepath(0, NULL, SUBDIR, NULL);
		if (subdirpath == NULL) {
			return GDK_FAIL;
		}

		if (MT_mkdir(subdirpath) < 0) {
			GDKsyserror("cannot create directory %s\n", subdirpath);
			GDKfree(subdirpath);
			return GDK_FAIL;
		}
		TRC_DEBUG(IO_, "mkdir %s\n", subdirpath);
		GDKfree(subdirpath);
	}
	if (backup_dir != set) {
		/* a valid backup dir *must* at least contain BBP.dir */
		if ((ret = GDKmove(0, backup_dir ? BAKDIR : BATDIR, "BBP", "dir", subcommit ? SUBDIR : BAKDIR, "BBP", "dir", true)) != GDK_SUCCEED)
			return ret;
		backup_dir = set;
	}
	/* increase counters */
	backup_subdir += subcommit;
	backup_files++;

	return ret;
}

static gdk_return
do_backup(const char *srcdir, const char *nme, const char *ext,
	  Heap *h, bool dirty, bool subcommit)
{
	gdk_return ret = GDK_SUCCEED;
	char extnew[16];

	if (h->wasempty) {
		return GDK_SUCCEED;
	}

	/* direct mmap is unprotected (readonly usage, or has WAL
	 * protection) */
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

		strconcat_len(extnew, sizeof(extnew), ext, ".new", NULL);
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
			else if (file_exists(h->farmid, srcdir, nme, ext))
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
		 * bat; as a workaround, do not complain about move
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
	gdk_return rc;

	if ((rc = BBPprepare(subcommit)) != GDK_SUCCEED) {
		return rc;
	}
	BATiter bi = bat_iterator(b);
	if (!bi.copiedtodisk || bi.transient) {
		bat_iterator_end(&bi);
		return GDK_SUCCEED;
	}
	/* determine location dir and physical suffix */
	char *srcdir;
	if ((srcdir = GDKfilepath(NOFARM, BATDIR, BBP_physical(b->batCacheid), NULL)) != NULL) {
		char *nme = strrchr(srcdir, DIR_SEP);
		assert(nme != NULL);
		*nme++ = '\0';	/* split into directory and file name */

		if (bi.type != TYPE_void) {
			rc = do_backup(srcdir, nme, BATITERtailname(&bi), bi.h,
				       bi.hdirty, subcommit);
			if (rc == GDK_SUCCEED && bi.vh != NULL)
				rc = do_backup(srcdir, nme, "theap", bi.vh,
					       bi.vhdirty, subcommit);
		}
	} else {
		rc = GDK_FAIL;
	}
	bat_iterator_end(&bi);
	GDKfree(srcdir);
	return rc;
}

static inline void
BBPcheckHeap(Heap *h)
{
	struct stat statb;
	char *path;

	char *s = strrchr(h->filename, DIR_SEP);
	if (s)
		s++;
	else
		s = h->filename;
	path = GDKfilepath(0, BAKDIR, s, NULL);
	if (path == NULL)
		return;
	if (MT_stat(path, &statb) < 0) {
		GDKfree(path);
		path = GDKfilepath(0, BATDIR, h->filename, NULL);
		if (path == NULL)
			return;
		if (MT_stat(path, &statb) < 0) {
			GDKsyserror("cannot stat file %s (expected size %zu)\n",
				    path, h->free);
			assert(0);
			GDKfree(path);
			return;
		}
	}
	assert((statb.st_mode & S_IFMT) == S_IFREG);
	assert((size_t) statb.st_size >= h->free);
	if ((size_t) statb.st_size < h->free) {
		GDKerror("file %s too small (expected %zu, actual %zu)\n", path, h->free, (size_t) statb.st_size);
		GDKfree(path);
		return;
	}
	GDKfree(path);
}

static void
BBPcheckBBPdir(void)
{
	FILE *fp;
	int lineno = 0;
	bat bbpsize = 0;
	unsigned bbpversion;
	lng logno, transid;

	fp = GDKfileopen(0, BAKDIR, "BBP", "dir", "r");
	assert(fp != NULL);
	if (fp == NULL) {
		fp = GDKfileopen(0, BATDIR, "BBP", "dir", "r");
		assert(fp != NULL);
		if (fp == NULL)
			return;
	}
	bbpversion = BBPheader(fp, &lineno, &bbpsize, &logno, &transid);
	if (bbpversion == 0) {
		fclose(fp);
		return;		/* error reading file */
	}
	assert(bbpversion == GDKLIBRARY);

	for (;;) {
		BAT b;
		Heap h;
		Heap vh;
		vh = h = (Heap) {
			.free = 0,
		};
		b = (BAT) {
			.theap = &h,
			.tvheap = &vh,
		};
		char filename[sizeof(BBP_physical(0))];
		char batname[129];
#ifdef GDKLIBRARY_HASHASH
		int hashash;
#endif

		switch (BBPreadBBPline(fp, bbpversion, &lineno, &b,
#ifdef GDKLIBRARY_HASHASH
				       &hashash,
#endif
				       batname, filename, NULL)) {
		case 0:
			/* end of file */
			fclose(fp);
			/* don't leak errors, this is just debug code */
			GDKclrerr();
			return;
		case 1:
			/* successfully read an entry */
			break;
		default:
			/* error */
			fclose(fp);
			return;
		}
#ifdef GDKLIBRARY_HASHASH
		assert(hashash == 0);
#endif
		assert(b.batCacheid < (bat) ATOMIC_GET(&BBPsize));
		assert(BBP_desc(b.batCacheid) != NULL);
		assert(b.hseqbase <= GDK_oid_max);
		if (b.ttype == TYPE_void) {
			/* no files needed */
			continue;
		}
		if (b.theap->free > 0)
			BBPcheckHeap(b.theap);
		if (b.tvheap != NULL && b.tvheap->free > 0)
			BBPcheckHeap(b.tvheap);
	}
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
	const bool lock = locked_by == 0 || locked_by != MT_getpid();
	char buf[3000];
	int n = subcommit ? 0 : -1;
	FILE *obbpf, *nbbpf;

	if(!(bakdir = GDKfilepath(0, NULL, subcommit ? SUBDIR : BAKDIR, NULL)))
		return GDK_FAIL;
	if(!(deldir = GDKfilepath(0, NULL, DELDIR, NULL))) {
		GDKfree(bakdir);
		return GDK_FAIL;
	}

	TRC_DEBUG_IF(PERF) t0 = t1 = GDKms();

	if ((GDKdebug & TAILCHKMASK) && !GDKinmemory(0))
		BBPcheckBBPdir();

	ret = BBPprepare(subcommit != NULL);

	/* PHASE 1: safeguard everything in a backup-dir */
	for (int idx = 1; ret == GDK_SUCCEED && idx < cnt; idx++) {
		bat i = subcommit ? subcommit[idx] : idx;
		const bat bid = i;
		if (lock)
			MT_lock_set(&GDKswapLock(bid));
		/* set flag that we're syncing, i.e. that we'll
		 * be between moving heap to backup dir and
		 * saving the new version, in other words, the
		 * heap may not exist in the usual location */
		BBP_status_on(bid, BBPSYNCING);
		/* wait until unloading is finished before
		 * attempting to make a backup */
		while (BBP_status(bid) & BBPUNLOADING) {
			if (lock)
				MT_lock_unset(&GDKswapLock(bid));
			BBPspin(bid, __func__, BBPUNLOADING);
			if (lock)
				MT_lock_set(&GDKswapLock(bid));
		}
		BAT *b = dirty_bat(&i, subcommit != NULL);
		if (i <= 0 ||
		    (BBP_status(bid) & BBPEXISTING &&
		     b != NULL &&
		     b->batInserted > 0 &&
		     BBPbackup(b, subcommit != NULL) != GDK_SUCCEED)) {
			ret = GDK_FAIL;
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(bid));
	}
	TRC_DEBUG(PERF, "move time %d, %d files\n", (t1 = GDKms()) - t0, backup_files);

	/* PHASE 2: save the repository and write new BBP.dir file */
	if (ret == GDK_SUCCEED) {
		ret = BBPdir_first(subcommit != NULL, logno, transid,
				   &obbpf, &nbbpf);
	}

	for (int idx = 1; ret == GDK_SUCCEED && idx < cnt; idx++) {
		bat i = subcommit ? subcommit[idx] : idx;
		/* BBP_desc(i) may be NULL */
		BUN size = sizes ? sizes[idx] : BUN_NONE;
		BATiter bi;

		if (BBP_status(i) & BBPPERSISTENT) {
			BAT *b = dirty_bat(&i, subcommit != NULL);
			if (i <= 0) {
				ret = GDK_FAIL;
				break;
			}
			bi = bat_iterator(BBP_desc(i));
			assert(sizes == NULL || size <= bi.count);
			assert(sizes == NULL || bi.width == 0 || (bi.type == TYPE_msk ? ((size + 31) / 32) * 4 : size << bi.shift) <= bi.hfree);
			if (size > bi.count) /* includes sizes==NULL */
				size = bi.count;
			bi.b->batInserted = size;
			if (bi.b->ttype >= 0 && ATOMvarsized(bi.b->ttype)) {
				/* see epilogue() for other part of this */
				MT_lock_set(&bi.b->theaplock);
				/* remember the tail we're saving */
				if (BATsetprop_nolock(bi.b, (enum prop_t) 20, TYPE_ptr, &bi.h) == NULL) {
					GDKerror("setprop failed\n");
					ret = GDK_FAIL;
				} else {
					if (bi.b->oldtail == NULL)
						bi.b->oldtail = (Heap *) 1;
					HEAPincref(bi.h);
				}
				MT_lock_unset(&bi.b->theaplock);
			}
			if (ret == GDK_SUCCEED && b && size != 0) {
				/* wait for BBPSAVING so that we
				 * can set it, wait for
				 * BBPUNLOADING before
				 * attempting to save */
				for (;;) {
					if (lock)
						MT_lock_set(&GDKswapLock(i));
					if (!(BBP_status(i) & (BBPSAVING|BBPUNLOADING)))
						break;
					if (lock)
						MT_lock_unset(&GDKswapLock(i));
					BBPspin(i, __func__, BBPSAVING|BBPUNLOADING);
				}
				BBP_status_on(i, BBPSAVING);
				if (lock)
					MT_lock_unset(&GDKswapLock(i));
				ret = BATsave_iter(b, &bi, size);
				BBP_status_off(i, BBPSAVING);
			}
		} else {
			bi = bat_iterator(NULL);
		}
		if (ret == GDK_SUCCEED) {
			n = BBPdir_step(i, size, n, buf, sizeof(buf), &obbpf, nbbpf, &bi);
			if (n < -1)
				ret = GDK_FAIL;
		}
		bat_iterator_end(&bi);
		/* we once again have a saved heap */
	}

	TRC_DEBUG(PERF, "write time %d\n", (t0 = GDKms()) - t1);

	if (ret == GDK_SUCCEED) {
		ret = BBPdir_last(n, buf, sizeof(buf), obbpf, nbbpf);
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
		ATOMIC_SET(&BBPlogno, logno);	/* the new value */
		ATOMIC_SET(&BBPtransid, transid);
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

	if (ret != GDK_SUCCEED) {
		/* clean up extra refs we created */
		for (int idx = 1; idx < cnt; idx++) {
			bat i = subcommit ? subcommit[idx] : idx;
			BAT *b = BBP_desc(i);
			if (b && ATOMvarsized(b->ttype)) {
				MT_lock_set(&b->theaplock);
				ValPtr p = BATgetprop_nolock(b, (enum prop_t) 20);
				if (p != NULL) {
					HEAPdecref(p->val.pval, false);
					BATrmprop_nolock(b, (enum prop_t) 20);
				}
				MT_lock_unset(&b->theaplock);
			}
		}
	}

	/* turn off the BBPSYNCING bits for all bats, even when things
	 * didn't go according to plan (i.e., don't check for ret ==
	 * GDK_SUCCEED) */
	for (int idx = 1; idx < cnt; idx++) {
		bat i = subcommit ? subcommit[idx] : idx;
		BBP_status_off(i, BBPSYNCING);
	}

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
	ret = GDKmove(farmid, srcdir, name, NULL, dstdir, name, NULL, false);

	if (ret != GDK_SUCCEED) {
		char *srcpath;

		GDKclrerr();
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
		ret = GDKmove(farmid, srcdir, name, NULL, dstdir, name, NULL, true);
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
		GDKerror("recovery failed.\n");

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
		ret = GDKmove(0, SUBDIR, dent->d_name, NULL, BAKDIR, dent->d_name, NULL, true);
		if (ret != GDK_SUCCEED)
			break;
		if (strcmp(dent->d_name, "BBP.dir") == 0)
			backup_dir = 1;
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
		GDKerror("recovery failed.\n");
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
	BAT *b = NULL;

	if (is_bat_nil(bid))
		return NULL;
	assert(bid > 0);
	if (bid < (bat) ATOMIC_GET(&BBPsize) && BBP_logical(bid))
		b = BBP_desc(bid);
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
				delete = (b == NULL || !b->ttype || !b->batCopiedtodisk || b->batCount == 0);
				assert(b == NULL || b->batCount > 0 || b->theap->free == 0);
				if (!delete) {
					if (b->ttype == TYPE_str) {
						switch (b->twidth) {
						case 1:
							delete = strcmp(p + 1, "tail1") != 0;
							break;
						case 2:
							delete = strcmp(p + 1, "tail2") != 0;
							break;
#if SIZEOF_VAR_T == 8
						case 4:
							delete = strcmp(p + 1, "tail4") != 0;
							break;
#endif
						default:
							delete = strcmp(p + 1, "tail") != 0;
							break;
						}
					} else {
						delete = strcmp(p + 1, "tail") != 0;
					}
				}
			} else if (strncmp(p + 1, "theap", 5) == 0) {
				BAT *b = getdesc(bid);
				delete = (b == NULL || !b->tvheap || !b->batCopiedtodisk || b->tvheap->free == 0);
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
			} else if (strncmp(p + 1, "tstrimps", 8) == 0) {
				BAT *b = getdesc(bid);
				delete = b == NULL;
				if (!delete)
					b->tstrimps = (Strimps *)1;
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

	BBP_free = 0;
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
	GDKfree(BBP_hash);
	BBP_hash = NULL;
	BBP_mask = 0;

	locked_by = 0;
	BBPunloadCnt = 0;
	backup_files = 0;
	backup_dir = 0;
	backup_subdir = 0;
}

static MT_Lock GDKCallbackListLock = MT_LOCK_INITIALIZER(GDKCallbackListLock);

static struct {
	int cnt;
	gdk_callback *head;
} callback_list = {
	.cnt = 0,
	.head = NULL,
};

/*
 * @- Add a callback
 * Adds new callback to the callback list.
 */
gdk_return
gdk_add_callback(char *name, gdk_callback_func *f, int argc, void *argv[], int
		interval)
{

	gdk_callback *callback = NULL;
	gdk_callback *p = callback_list.head;

	if (!(callback = GDKmalloc(sizeof(gdk_callback) + sizeof(void *) * argc))) {
		TRC_CRITICAL(GDK, "Failed to allocate memory!");
		return GDK_FAIL;
	}

	*callback = (gdk_callback) {
		.name = name,
		.argc = argc,
		.interval = interval,
		.func = f,
	};

	for (int i=0; i < argc; i++) {
		callback->argv[i] = argv[i];
	}

	MT_lock_set(&GDKCallbackListLock);
	if (p) {
		int cnt = 1;
		do {
			// check if already added
			if (strcmp(callback->name, p->name) == 0) {
				MT_lock_unset(&GDKCallbackListLock);
				GDKfree(callback);
				return GDK_FAIL;
			}
			if (p->next == NULL) {
			   	p->next = callback;
				p = callback->next;
			} else {
				p = p->next;
			}
			cnt += 1;
		} while(p);
		callback_list.cnt = cnt;
	} else {
		callback_list.cnt = 1;
		callback_list.head = callback;
	}
	MT_lock_unset(&GDKCallbackListLock);
	return GDK_SUCCEED;
}

/*
 * @- Remove a callback
 * Removes a callback from the callback list with a given name as an argument.
 */
gdk_return
gdk_remove_callback(char *cb_name, gdk_callback_func *argsfree)
{
	gdk_callback *curr = callback_list.head;
	gdk_callback *prev = NULL;
	gdk_return res = GDK_FAIL;
	while(curr) {
		if (strcmp(cb_name, curr->name) == 0) {
			MT_lock_set(&GDKCallbackListLock);
			if (curr == callback_list.head && prev == NULL) {
				callback_list.head = curr->next;
			} else {
				prev->next = curr->next;
			}
			if (argsfree)
			       	argsfree(curr->argc, curr->argv);
			GDKfree(curr);
			curr = NULL;
			callback_list.cnt -=1;
			res = GDK_SUCCEED;
			MT_lock_unset(&GDKCallbackListLock);
		} else {
			prev = curr;
			curr = curr->next;
		}
	}
	return res;
}

static gdk_return
do_callback(gdk_callback *cb)
{
	cb->last_called = GDKusec();
	return cb->func(cb->argc, cb->argv);
}

static bool
should_call(gdk_callback *cb)
{
	if (cb->last_called && cb->interval) {
		return (cb->last_called + cb->interval * 1000 * 1000) <
			GDKusec();
	}
	return true;
}

static void
BBPcallbacks(void)
{
	gdk_callback *next = callback_list.head;

	MT_lock_set(&GDKCallbackListLock);
	while (next) {
		if(should_call(next))
			do_callback(next);
		next = next->next;
	}
	MT_lock_unset(&GDKCallbackListLock);
}

/* GDKtmLock protects all accesses and changes to BAKDIR and SUBDIR.
 * MUST use BBPtmlock()/BBPtmunlock() to set/unset the lock.
 *
 * This is at the end of the file on purpose: we don't want people to
 * accidentally use GDKtmLock directly. */
static MT_Lock GDKtmLock = MT_LOCK_INITIALIZER(GDKtmLock);
static char *lockfile;
static int lockfd;

void
BBPtmlock(void)
{
	MT_lock_set(&GDKtmLock);
	if (GDKinmemory(0))
		return;
	/* also use an external lock file to synchronize with external
	 * programs */
	if (lockfile == NULL) {
		lockfile = GDKfilepath(0, NULL, ".tm_lock", NULL);
		if (lockfile == NULL)
			return;
	}
	lockfd = MT_lockf(lockfile, F_LOCK);
}

void
BBPtmunlock(void)
{
	if (lockfile && lockfd >= 0) {
		assert(!GDKinmemory(0));
		MT_lockf(lockfile, F_ULOCK);
		close(lockfd);
		lockfd = -1;
	}
	MT_lock_unset(&GDKtmLock);
}
