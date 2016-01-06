/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * (pointer) ones.  Both are administered with the BBPincref/BBPdecref
 * routines. For backward compatibility, we maintain BBPfix/BBPunfix
 * as shorthands for the adjusting the pointer references.
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

#define BBPnamecheck(s) (BBPtmpcheck(s) ? ((s)[3] == '_' ? strtol((s) + 4, NULL, 8) : -strtol((s) + 5, NULL, 8)) : 0)

#ifdef ATOMIC_LOCK
static MT_Lock stampLock MT_LOCK_INITIALIZER("stampLock");
#endif
static volatile ATOMIC_TYPE stamp = 0;
static inline int
BBPstamp(void)
{
	return (int) ATOMIC_INC(stamp, stampLock);
}

static void
BBPsetstamp(int newstamp)
{
	ATOMIC_SET(stamp, newstamp, stampLock);
}


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
#ifdef ATOMIC_LOCK
static MT_Lock BBP_curstampLock MT_LOCK_INITIALIZER("BBP_curstampLock");
#endif
static volatile ATOMIC_TYPE BBP_curstamp = 0; /* unique stamp for creation of a bat */
MT_Id BBP_notrim = ~((MT_Id) 0);	/* avoids BBPtrim when we really do not want it */
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
 * GDKswapLock(abs(bat))
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
BBPlock(const char *nme)
{
	int i;

	/* wait for all pending unloads to finish */
	(void) nme;
	MT_lock_set(&GDKunloadLock);
	while (BBPunloadCnt > 0) {
		MT_lock_unset(&GDKunloadLock);
		MT_sleep_ms(1);
		MT_lock_set(&GDKunloadLock);
	}

	for (i = 0; i <= BBP_THREADMASK; i++)
		MT_lock_set(&GDKtrimLock(i));
	BBP_notrim = MT_getpid();
	for (i = 0; i <= BBP_THREADMASK; i++)
		MT_lock_set(&GDKcacheLock(i));
	for (i = 0; i <= BBP_BATMASK; i++)
		MT_lock_set(&GDKswapLock(i));
	locked_by = BBP_notrim;

	MT_lock_unset(&GDKunloadLock);
}

void
BBPunlock(const char *nme)
{
	int i;

	(void) nme;
	for (i = BBP_BATMASK; i >= 0; i--)
		MT_lock_unset(&GDKswapLock(i));
	for (i = BBP_THREADMASK; i >= 0; i--)
		MT_lock_unset(&GDKcacheLock(i));
	BBP_notrim = 0;
	locked_by = 0;
	for (i = BBP_THREADMASK; i >= 0; i--)
		MT_lock_unset(&GDKtrimLock(i));
}


static void
BBPinithash(int j)
{
	bat i = (bat) ATOMIC_GET(BBPsize, BBPsizeLock);

	assert(j >= 0 && j <= BBP_THREADMASK);
	for (BBP_mask = 1; (BBP_mask << 1) <= BBPlimit; BBP_mask <<= 1)
		;
	BBP_hash = (bat *) GDKzalloc(BBP_mask * sizeof(bat));
	if (BBP_hash == NULL)
		GDKfatal("BBPinithash: cannot allocate memory\n");
	BBP_mask--;

	while (--i > 0) {
		const char *s = BBP_logical(i);

		if (s) {
			const char *sm = BBP_logical(-i);

			if (*s != '.' && BBPtmpcheck(s) == 0) {
				BBP_insert(i);
			}
			if (sm && *sm != '.' && BBPtmpcheck(sm) == 0) {
				BBP_insert(-i);
			}
		} else {
			BBP_next(i) = BBP_free(j);
			BBP_free(j) = i;
			if (++j > BBP_THREADMASK)
				j = 0;
		}
	}
}

int
BBPselectfarm(int role, int type, enum heaptype hptype)
{
	int i;

	assert(role >= 0 && role < 32);
	(void) type;		/* may use in future */
	(void) hptype;		/* may use in future */
	for (i = 0; i < MAXFARMS; i++)
		if (BBPfarms[i].dirname && BBPfarms[i].roles & (1 << role))
			return i;
	/* must be able to find farms for TRANSIENT and PERSISTENT */
	assert(role != TRANSIENT && role != PERSISTENT);
	return -1;
}

/*
 * BBPextend must take the trimlock, as it is called when other BBP
 * locks are held and it will allocate memory. This could trigger a
 * BBPtrim, causing deadlock.
 */
static void
BBPextend(int idx, int buildhash)
{
	BBP_notrim = MT_getpid();

	if ((bat) ATOMIC_GET(BBPsize, BBPsizeLock) >= N_BBPINIT * BBPINIT)
		GDKfatal("BBPextend: trying to extend BAT pool beyond the "
			 "limit (%d)\n", N_BBPINIT * BBPINIT);

	/* make sure the new size is at least BBPsize large */
	while (BBPlimit < (bat) ATOMIC_GET(BBPsize, BBPsizeLock)) {
		assert(BBP[BBPlimit >> BBPINITLOG] == NULL);
		BBP[BBPlimit >> BBPINITLOG] = GDKzalloc(BBPINIT * sizeof(BBPrec));
		if (BBP[BBPlimit >> BBPINITLOG] == NULL)
			GDKfatal("BBPextend: failed to extend BAT pool\n");
		BBPlimit += BBPINIT;
	}

	if (buildhash) {
		int i;

		GDKfree(BBP_hash);
		BBP_hash = NULL;
		for (i = 0; i <= BBP_THREADMASK; i++)
			BBP_free(i) = 0;
		BBPinithash(idx);
	}
	BBP_notrim = 0;
}

static inline str
BBPtmpname(str s, int len, bat i)
{
	int reverse = i < 0;

	if (reverse)
		i = -i;
	s[--len] = 0;
	while (i > 0) {
		s[--len] = '0' + (i & 7);
		i >>= 3;
	}
	s[--len] = '_';
	if (reverse)
		s[--len] = 'r';
	s[--len] = 'p';
	s[--len] = 'm';
	s[--len] = 't';
	return s + len;
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
		(void) GDKunlink(farmid, BATDIR, "BBP", "bak");
		(void) GDKmove(farmid, BATDIR, "BBP", "dir", BATDIR, "BBP", "bak");
	}
	return GDKmove(farmid, BAKDIR, "BBP", "dir", BATDIR, "BBP", "dir");
}

static gdk_return BBPrecover(int farmid);
static gdk_return BBPrecover_subdir(void);
static int BBPdiskscan(const char *);

#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
/* Convert 32-bit OIDs to 64 bits.
 * This function must be called at the end of BBPinit(), just before
 * "normal processing" starts.  All errors that happen during the
 * conversion are fatal.  This function is "safe" in the sense that
 * when it is interrupted, recovery will just start over.  No
 * permanent changes are made to the database until all string heaps
 * have been converted, and then the changes are made atomically.
 *
 * In addition to doing the conversion to all BATs with OID or STR
 * columns (the latter since the format of the string heap is
 * different for 64/32 and 64/64 configurations), we also look out for
 * the *_catalog BATs that are used by gdk_logger.  If we encounter
 * such a BAT, we create a file based on the name of that BAT
 * (i.e. using the first part of the name) to inform the logger that
 * it too needs to convert 32 bit OIDs to 64 bits.  If the process
 * here gets interrupted, the logger is never called, and if we get
 * then restarted, we just create the file again, so this process is
 * safe. */

static void
fixoidheapcolumn(BAT *b, const char *srcdir, const char *nme,
		 const char *filename, const char *headtail,
		 const char *htheap)
{
	bat bid = abs(b->batCacheid);
	Heap h1, h2;
	int *old;
	oid *new;
	BUN i;
	char *s;
	unsigned short w;
	const char *bnme;
	int ht;

	if ((bnme = strrchr(nme, DIR_SEP)) != NULL)
		bnme++;
	else
		bnme = nme;

	if (GDKmove(b->H->heap.farmid, srcdir, bnme, headtail, BAKDIR, bnme, headtail) != GDK_SUCCEED)
		GDKfatal("fixoidheap: cannot make backup of %s.%s\n", nme, headtail);

	if ((ht = b->H->type) < 0) {
		const char *anme;

		/* as yet unknown head column type */
		anme = ATOMunknown_name(ht);
		if (strcmp(anme, "url") == 0)
			b->H->type = TYPE_str;
		else if (strcmp(anme, "sqlblob") == 0 ||
			 strcmp(anme, "wkb") == 0)
			b->H->type = TYPE_int;
		else
			GDKfatal("fixoidheap: unrecognized column "
				 "type %s for BAT %d\n", anme, bid);
	}

	if (b->H->type == TYPE_str) {
		if (GDKmove(b->H->vheap->farmid, srcdir, bnme, htheap, BAKDIR, bnme, htheap) != GDK_SUCCEED)
			GDKfatal("fixoidheap: cannot make backup of %s.%s\n", nme, htheap);

		h1 = b->H->heap;
		h1.filename = NULL;
		h1.base = NULL;
		h1.dirty = 0;
		h2 = *b->H->vheap;
		h2.filename = NULL;
		h2.base = NULL;
		h2.dirty = 0;

		/* load old string heap */
		if (HEAPload(&h1, filename, headtail, 0) != GDK_SUCCEED)
			GDKfatal("fixoidheap: loading old %s heap "
				 "for BAT %d failed\n", headtail, bid);
		if (HEAPload(&h2, filename, htheap, 0) != GDK_SUCCEED)
			GDKfatal("fixoidheap: loading old string heap "
				 "for BAT %d failed\n", bid);

		/* create new string heap */
		b->H->heap.filename = GDKfilepath(NOFARM, NULL, nme, headtail);
		if (b->H->heap.filename == NULL)
			GDKfatal("fixoidheap: GDKmalloc failed\n");
		w = b->H->width; /* remember old width */
		b->H->width = 1;
		b->H->shift = 0;
		if (HEAPalloc(&b->H->heap, b->batCapacity, SIZEOF_OID) != GDK_SUCCEED)
			GDKfatal("fixoidheap: allocating new %s heap "
				 "for BAT %d failed\n", headtail, bid);

		b->H->heap.dirty = TRUE;
		b->H->vheap->filename = GDKfilepath(NOFARM, NULL, nme, htheap);
		if (b->H->vheap->filename == NULL)
			GDKfatal("fixoidheap: GDKmalloc failed\n");
		if (ATOMheap(TYPE_str, b->H->vheap, b->batCapacity) != GDK_SUCCEED)
			GDKfatal("fixoidheap: initializing new string "
				 "heap for BAT %d failed\n", bid);
		b->H->vheap->parentid = bid;

		/* do the conversion */
		b->H->heap.dirty = TRUE;
		b->H->vheap->dirty = TRUE;
		for (i = 0; i < b->batCount; i++) {
			/* s = h2.base + VarHeapVal(h1.base, i, w); */
			switch (w) {
			case 1:
				s = h2.base + (((var_t) ((unsigned char *) h1.base)[i] + ((GDK_STRHASHTABLE * sizeof(unsigned short)) >> 3)) << 3);
				break;
			case 2:
				s = h2.base + (((var_t) ((unsigned short *) h1.base)[i] + ((GDK_STRHASHTABLE * sizeof(unsigned short)) >> 3)) << 3);
				break;
			case 4:
				s = h2.base + ((var_t) ((unsigned int *) h1.base)[i] << 3);
				break;
			default:
				assert(0);
				/* cannot happen, but compiler doesn't know */
				s = NULL;
			}
			b->H->heap.free += b->H->width;
			Hputvalue(b, Hloc(b, i), s, 0);
		}
		HEAPfree(&h1, 0);
		HEAPfree(&h2, 0);
		HEAPsave(b->H->vheap, nme, htheap);
		HEAPfree(b->H->vheap, 0);
	} else {
		assert(b->H->type == TYPE_oid ||
		       (b->H->type != TYPE_void && b->H->varsized));
		h1 = b->H->heap;
		h1.filename = NULL;
		h1.base = NULL;
		h1.dirty = 0;
		h1.parentid = 0;

		/* load old heap */
		if (HEAPload(&h1, filename, headtail, 0) != GDK_SUCCEED)
			GDKfatal("fixoidheap: loading old %s heap "
				 "for BAT %d failed\n", headtail, bid);

		/* create new heap */
		b->H->heap.filename = GDKfilepath(NOFARM, NULL, nme, headtail);
		if (b->H->heap.filename == NULL)
			GDKfatal("fixoidheap: GDKmalloc failed\n");
		b->H->width = SIZEOF_OID;
		b->H->shift = 3;
		assert(b->H->width == (1 << b->H->shift));
		if (HEAPalloc(&b->H->heap, b->batCapacity, SIZEOF_OID) != GDK_SUCCEED)
			GDKfatal("fixoidheap: allocating new %s heap "
				 "for BAT %d failed\n", headtail, bid);

		b->H->heap.dirty = TRUE;
		old = (int *) h1.base + b->batFirst;
		new = (oid *) b->H->heap.base + b->batFirst;
		if (b->H->varsized)
			for (i = 0; i < b->batCount; i++)
				new[i] = (oid) old[i] << 3;
		else
			for (i = 0; i < b->batCount; i++)
				new[i] = old[i] == int_nil ? oid_nil : (oid) old[i];
		b->H->heap.free = h1.free << 1;
		HEAPfree(&h1, 0);
	}
	HEAPsave(&b->H->heap, nme, headtail);
	HEAPfree(&b->H->heap, 0);

	if (ht < 0)
		b->H->type = ht;

	return;

  bunins_failed:
	GDKfatal("fixoidheap: memory allocation failed\n");
}

static void
fixoidheap(void)
{
	bat bid;
	BATstore *bs;
	const char *nme, *bnme;
	char *srcdir;
	long_str filename;
	size_t len;
	FILE *fp;

	fprintf(stderr,
		"# upgrading database from 32 bit OIDs to 64 bit OIDs\n");
	fflush(stderr);

	for (bid = 1; bid < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); bid++) {
		if ((bs = BBP_desc(bid)) == NULL)
			continue;	/* not a valid BAT */
		if (BBP_logical(bid) &&
		    (len = strlen(BBP_logical(bid))) > 8 &&
		    strcmp(BBP_logical(bid) + len - 8, "_catalog") == 0) {
			/* this is one of the files used by the
			 * logger.  We need to communicate to the
			 * logger that it also needs to do a
			 * conversion.  That is done by creating a
			 * file here based on the name of this BAT. */
			snprintf(filename, sizeof(filename),
				 "%.*s_32-64-convert",
				 (int) (len - 8), BBP_logical(bid));
			fp = fopen(filename, "w");
			if (fp == NULL)
				GDKfatal("fixoidheap: cannot create file %s\n",
					 filename);
			fclose(fp);
		}

		/* OID and (non-void) varsized columns have to be rewritten */
		if (bs->H.type != TYPE_oid &&
		    (bs->H.type == TYPE_void || !bs->H.varsized) &&
		    bs->T.type != TYPE_oid &&
		    (bs->T.type == TYPE_void || !bs->T.varsized))
			continue; /* nothing to do for this BAT */

		nme = BBP_physical(bid);
		if ((bnme = strrchr(nme, DIR_SEP)) == NULL)
			bnme = nme;
		else
			bnme++;
		sprintf(filename, "BACKUP%c%s", DIR_SEP, bnme);
		srcdir = GDKfilepath(bs->H.heap.farmid, BATDIR, nme, NULL);
		*strrchr(srcdir, DIR_SEP) = 0;

		if (bs->H.type == TYPE_oid ||
		    (bs->H.varsized && bs->H.type != TYPE_void)) {
			assert(bs->H.type != TYPE_oid || bs->H.width == 4);
			fixoidheapcolumn(&bs->B, srcdir, nme, filename, "head", "hheap");
		}
		GDKfree(srcdir);
		srcdir = GDKfilepath(bs->T.heap.farmid, BATDIR, nme, NULL);
		*strrchr(srcdir, DIR_SEP) = 0;
		if (bs->T.type == TYPE_oid ||
		    (bs->T.varsized && bs->T.type != TYPE_void)) {
			assert(bs->T.type != TYPE_oid || bs->T.width == 4);
			fixoidheapcolumn(&bs->BM, srcdir, nme, filename, "tail", "theap");
		}
		GDKfree(srcdir);
	}

	/* make permanent */
	if (TMcommit() != GDK_SUCCEED)
		GDKfatal("fixoidheap: commit failed\n");
}
#endif

/*
 * A read only BAT can be shared in a file system by reading its
 * descriptor separately.  The default src=0 is to read the full
 * BBPdir file.
 */
static int
heapinit(COLrec *col, const char *buf, int *hashash, const char *HT, int oidsize, int bbpversion, bat bid)
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

	(void) oidsize;		/* only used when SIZEOF_OID==8 */
	(void) bbpversion;	/* could be used to implement compatibility */

	norevsorted = 0; /* default for first case */
	if (sscanf(buf,
		   " %10s %hu %hu %hu %lld %lld %lld %lld %lld %lld %lld %lld %hu"
		   "%n",
		   type, &width, &var, &properties, &nokey0,
		   &nokey1, &nosorted, &norevsorted, &base,
		   &align, &free, &size, &storage,
		   &n) < 13)
		GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);

	if (properties & ~0x0F81)
		GDKfatal("BBPinit: unknown properties are set: incompatible database\n");
	*hashash = var & 2;
	var &= ~2;
	/* silently convert chr columns to bte */
	if (strcmp(type, "chr") == 0)
		strcpy(type, "bte");
	if ((t = ATOMindex(type)) < 0)
		t = ATOMunknown_find(type);
	else if (var != (t == TYPE_void || BATatoms[t].atomPut != NULL))
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.varsized mismatch for BAT %d\n", HT, (int) bid);
	else if (var && t != 0 ?
		 ATOMsize(t) < width ||
		 (width != 1 && width != 2 && width != 4
#if SIZEOF_VAR_T == 8
		  && width != 8
#endif
			 ) :
		 ATOMsize(t) != width
#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
		 && (t != TYPE_oid || oidsize == 0 || width != oidsize)
#endif
		)
		GDKfatal("BBPinit: inconsistent entry in BBP.dir: %s.size mismatch for BAT %d\n", HT, (int) bid);
	col->type = t;
	col->width = width;
	col->varsized = var != 0;
	col->shift = ATOMelmshift(width);
	assert_shift_width(col->shift,col->width);
	col->nokey[0] = (BUN) nokey0;
	col->nokey[1] = (BUN) nokey1;
	col->sorted = (bit) ((properties & 0x0001) != 0);
	col->revsorted = (bit) ((properties & 0x0080) != 0);
	col->key = (properties & 0x0100) != 0;
	col->dense = (properties & 0x0200) != 0;
	col->nonil = (properties & 0x0400) != 0;
	col->nil = (properties & 0x0800) != 0;
	col->nosorted = (BUN) nosorted;
	col->norevsorted = (BUN) norevsorted;
	col->seq = base < 0 ? oid_nil : (oid) base;
	col->align = (oid) align;
	col->heap.free = (size_t) free;
	col->heap.size = (size_t) size;
	col->heap.base = NULL;
	col->heap.filename = NULL;
	col->heap.storage = (storage_t) storage;
	col->heap.copied = 0;
	col->heap.newstorage = (storage_t) storage;
	col->heap.farmid = BBPselectfarm(PERSISTENT, col->type, offheap);
	col->heap.dirty = 0;
	if (bbpversion <= GDKLIBRARY_INET_COMPARE && strcmp(type, "inet") == 0) {
		/* don't trust ordering information on inet columns */
		col->sorted = 0;
		col->revsorted = 0;
	}
	if (col->heap.free > col->heap.size)
		GDKfatal("BBPinit: \"free\" value larger than \"size\" in heap of bat %d\n", (int) bid);
	return n;
}

static int
vheapinit(COLrec *col, const char *buf, int hashash, bat bid)
{
	int n = 0;
	lng free, size;
	unsigned short storage;

	if (col->varsized && col->type != TYPE_void) {
		col->vheap = GDKzalloc(sizeof(Heap));
		if (col->vheap == NULL)
			GDKfatal("BBPinit: cannot allocate memory for heap.");
		if (sscanf(buf,
			   " %lld %lld %hu"
			   "%n",
			   &free, &size, &storage, &n) < 3)
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		col->vheap->free = (size_t) free;
		col->vheap->size = (size_t) size;
		col->vheap->base = NULL;
		col->vheap->filename = NULL;
		col->vheap->storage = (storage_t) storage;
		col->vheap->copied = 0;
		col->vheap->hashash = hashash != 0;
		col->vheap->newstorage = (storage_t) storage;
		col->vheap->dirty = 0;
		col->vheap->parentid = bid;
		col->vheap->farmid = BBPselectfarm(PERSISTENT, col->type, varheap);
		if (col->vheap->free > col->vheap->size)
			GDKfatal("BBPinit: \"free\" value larger than \"size\" in var heap of bat %d\n", (int) bid);
	}
	return n;
}

static int
BBPreadEntries(FILE *fp, int *min_stamp, int *max_stamp, int oidsize, int bbpversion)
{
	bat bid = 0;
	char buf[4096];
	BATstore *bs;
	int needcommit = 0;

	/* read the BBP.dir and insert the BATs into the BBP */
	while (fgets(buf, sizeof(buf), fp) != NULL) {
		lng batid;
		unsigned short status;
		char headname[129];
		char tailname[129];
		char filename[129];
		unsigned int properties;
		int lastused;
		int nread;
		char *s, *options = NULL;
		char logical[1024];
		lng inserted, deleted, first, count, capacity;
		unsigned short map_head, map_tail, map_hheap, map_theap;
		int Hhashash, Thashash;

		if ((s = strchr(buf, '\r')) != NULL) {
			/* convert \r\n into just \n */
			if (s[1] != '\n')
				GDKfatal("BBPinit: invalid format for BBP.dir");
			*s++ = '\n';
			*s = 0;
		}

		if (sscanf(buf,
			   "%lld %hu %128s %128s %128s %d %u %lld %lld %lld %lld %lld %hu %hu %hu %hu"
			   "%n",
			   &batid, &status, headname, tailname, filename,
			   &lastused, &properties, &inserted, &deleted, &first,
			   &count, &capacity, &map_head, &map_tail, &map_hheap,
			   &map_theap,
			   &nread) < 16)
			GDKfatal("BBPinit: invalid format for BBP.dir%s", buf);

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
		if (batid >= (lng) ATOMIC_GET(BBPsize, BBPsizeLock)) {
			ATOMIC_SET(BBPsize, (ATOMIC_TYPE) (batid + 1), BBPsizeLock);
			if ((bat) ATOMIC_GET(BBPsize, BBPsizeLock) >= BBPlimit)
				BBPextend(0, FALSE);
		}
		if (BBP_desc(bid) != NULL)
			GDKfatal("BBPinit: duplicate entry in BBP.dir.");
		bs = GDKzalloc(sizeof(BATstore));
		if (bs == NULL)
			GDKfatal("BBPinit: cannot allocate memory for BATstore.");
		bs->B.H = &bs->H;
		bs->B.T = &bs->T;
		bs->B.S = &bs->S;
		bs->B.batCacheid = bid;
		bs->BM.H = &bs->T;
		bs->BM.T = &bs->H;
		bs->BM.S = &bs->S;
		bs->BM.batCacheid = -bid;
		BATroles(&bs->B, NULL, NULL);
		bs->S.persistence = PERSISTENT;
		bs->S.copiedtodisk = 1;
		bs->S.restricted = (properties & 0x06) >> 1;
		bs->S.inserted = (BUN) inserted;
		bs->S.deleted = (BUN) deleted;
		bs->S.first = (BUN) first;
		bs->S.count = (BUN) count;
		bs->S.capacity = (BUN) capacity;
		bs->S.map_head = (char) map_head;
		bs->S.map_tail = (char) map_tail;
		bs->S.map_hheap = (char) map_hheap;
		bs->S.map_theap = (char) map_theap;

		nread += heapinit(&bs->H, buf + nread, &Hhashash, "H", oidsize, bbpversion, bid);
		nread += heapinit(&bs->T, buf + nread, &Thashash, "T", oidsize, bbpversion, bid);
		nread += vheapinit(&bs->H, buf + nread, Hhashash, bid);
		nread += vheapinit(&bs->T, buf + nread, Thashash, bid);

		if (bs->S.count > 1) {
			/* fix result of bug in BATappend not clearing
			 * revsorted property */
			if (bs->H.type == TYPE_void && bs->H.seq != oid_nil && bs->H.revsorted) {
				bs->H.revsorted = 0;
				bs->S.descdirty = 1;
				needcommit = 1;
			}
			if (bs->T.type == TYPE_void && bs->T.seq != oid_nil && bs->T.revsorted) {
				bs->T.revsorted = 0;
				bs->S.descdirty = 1;
				needcommit = 1;
			}
		}

		if (buf[nread] != '\n' && buf[nread] != ' ')
			GDKfatal("BBPinit: invalid format for BBP.dir\n%s", buf);
		if (buf[nread] == ' ')
			options = buf + nread + 1;

		BBP_desc(bid) = bs;
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
		if (strcmp(tailname, BBPNONAME) != 0)
			BBP_logical(-bid) = GDKstrdup(tailname);
		else
			BBP_logical(-bid) = GDKstrdup(BBPtmpname(tailname, sizeof(tailname), -bid));
		BBP_physical(bid) = GDKstrdup(filename);
		BBP_options(bid) = NULL;
		if (options)
			BBP_options(bid) = GDKstrdup(options);
		BBP_lastused(bid) = lastused;
		if (lastused > *max_stamp)
			*max_stamp = lastused;
		if (lastused < *min_stamp)
			*min_stamp = lastused;
		BBP_refs(bid) = 0;
		BBP_lrefs(bid) = 1;	/* any BAT we encounter here is persistent, so has a logical reference */
	}
	return needcommit;
}

#ifdef HAVE_HGE
#define SIZEOF_MAX_INT SIZEOF_HGE
#else
#define SIZEOF_MAX_INT SIZEOF_LNG
#endif

static int
BBPheader(FILE *fp, oid *BBPoid, int *OIDsize)
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
	    bbpversion != GDKLIBRARY_64_BIT_INT) {
		GDKfatal("BBPinit: incompatible BBP version: expected 0%o, got 0%o.", GDKLIBRARY, bbpversion);
	}
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	if (bbpversion <= GDKLIBRARY_64_BIT_INT) {
		if (sscanf(buf, "%d %d", &ptrsize, &oidsize) != 2) {
			GDKfatal("BBPinit: BBP.dir has incompatible format: pointer and OID sizes are missing");
		}
		intsize = SIZEOF_LNG;
	} else {
		if (sscanf(buf, "%d %d %d", &ptrsize, &oidsize, &intsize) != 3) {
			GDKfatal("BBPinit: BBP.dir has incompatible format: pointer, OID, and max. integer sizes are missing");
		}
	}
	if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_OID) {
#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
		if (ptrsize != SIZEOF_SIZE_T || oidsize != SIZEOF_INT)
#endif
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected pointer size %d, got %d, expected OID size %d, got %d.",
			 SIZEOF_SIZE_T, ptrsize, SIZEOF_OID, oidsize);
	}
	if (intsize > SIZEOF_MAX_INT) {
		GDKfatal("BBPinit: database created with incompatible server:\n"
			 "expected max. integer size %d, got %d.",
			 SIZEOF_MAX_INT, intsize);
	}
	if (OIDsize)
		*OIDsize = oidsize;
	if (fgets(buf, sizeof(buf), fp) == NULL) {
		GDKfatal("BBPinit: short BBP");
	}
	*BBPoid = OIDread(buf);
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
			return;
		}
	}
	GDKfatal("BBPaddfarm: too many farms\n");
}

gdk_export void BBPresetfarms(void) {
	BBPexit();
	BBPunlock("BBPexit");
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
	int min_stamp = 0x7fffffff, max_stamp = 0;
	bat bid;
	int bbpversion;
	int oidsize;
	oid BBPoid;
	str bbpdirstr = GDKfilepath(0, BATDIR, "BBP", "dir");
	str backupbbpdirstr = GDKfilepath(0, BAKDIR, "BBP", "dir");
	int needcommit;

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&GDKunloadLock, "GDKunloadLock");
	ATOMIC_INIT(stampLock);
	ATOMIC_INIT(BBPsizeLock);
#endif

	if (BBPfarms[0].dirname == NULL)
		BBPaddfarm(".", (1 << PERSISTENT) | (1 << TRANSIENT));

	GDKremovedir(0, DELDIR);

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

	bbpversion = BBPheader(fp, &BBPoid, &oidsize);

	BBPextend(0, FALSE);		/* allocate BBP records */
	ATOMIC_SET(BBPsize, 1, BBPsizeLock);

	needcommit = BBPreadEntries(fp, &min_stamp, &max_stamp, oidsize, bbpversion);
	fclose(fp);

	/* normalize saved LRU stamps */
	if (min_stamp <= max_stamp) {
		for (bid = 1; bid < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); bid++)
			if (BBPvalid(bid))
				BBP_lastused(bid) -= min_stamp;
		BBPsetstamp(max_stamp - min_stamp);
	}

	BBPinithash(0);
	BBP_notrim = 0;

	OIDbase(BBPoid);

	/* will call BBPrecover if needed */
	if (BBPprepare(FALSE) != GDK_SUCCEED)
		GDKfatal("BBPinit: cannot properly prepare process %s. Please check whether your disk is full or write-protected", BAKDIR);

	/* cleanup any leftovers (must be done after BBPrecover) */
	BBPdiskscan(GDKfilepath(0, NULL, BATDIR, NULL));

#if SIZEOF_SIZE_T == 8 && SIZEOF_OID == 8
	if (oidsize == SIZEOF_INT)
		fixoidheap();
#else
	(void) oidsize;
#endif
	if (bbpversion < GDKLIBRARY || needcommit)
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

	BBPlock("BBPexit");	/* stop all threads ever touching more descriptors */

	/* free all memory (just for leak-checking in Purify) */
	do {
		skipped = 0;
		for (i = 0; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
			if (BBPvalid(i)) {
				BAT *b = BBP_cache(i);

				if (b) {
					if (b->batSharecnt > 0) {
						skipped = 1;
						continue;
					}
					/* NIELS ?? Why reduce share count, it's done in VIEWdestroy !!
					if (isVIEW(b)) {
						bat hp = VIEWhparent(b), tp = VIEWtparent(b);
						bat vhp = VIEWvhparent(b), vtp = VIEWvtparent(b);
						if (hp) {
							BBP_cache(hp)->batSharecnt--;
							--BBP_lrefs(hp);
						}
						if (tp) {
							BBP_cache(tp)->batSharecnt--;
							--BBP_lrefs(tp);
						}
						if (vhp) {
							BBP_cache(vhp)->batSharecnt--;
							--BBP_lrefs(vhp);
						}
						if (vtp) {
							BBP_cache(vtp)->batSharecnt--;
							--BBP_lrefs(vtp);
						}
					}*/
					if (isVIEW(b))
						VIEWdestroy(b);
					else
						BATfree(b);
				}
				BBPuncacheit(i, TRUE);
				if (BBP_logical(i) != BBP_bak(i))
					GDKfree(BBP_bak(i));
				BBP_bak(i) = NULL;
				GDKfree(BBP_logical(i));
				BBP_logical(i) = NULL;
				GDKfree(BBP_logical(-i));
				BBP_logical(-i) = NULL;
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
heap_entry(FILE *fp, COLrec *col)
{
	return fprintf(fp, " %s %d %d %d " BUNFMT " " BUNFMT " " BUNFMT " "
		       BUNFMT " " OIDFMT " " OIDFMT " " SZFMT " " SZFMT " %d",
		       col->type >= 0 ? BATatoms[col->type].name : ATOMunknown_name(col->type),
		       col->width,
		       col->varsized | (col->vheap ? col->vheap->hashash << 1 : 0),
		       (unsigned short) col->sorted |
			   ((unsigned short) col->revsorted << 7) |
			   (((unsigned short) col->key & 0x01) << 8) |
			   ((unsigned short) col->dense << 9) |
			   ((unsigned short) col->nonil << 10) |
			   ((unsigned short) col->nil << 11),
		       col->nokey[0],
		       col->nokey[1],
		       col->nosorted,
		       col->norevsorted,
		       col->seq,
		       col->align,
		       col->heap.free,
		       col->heap.size,
		       (int) col->heap.newstorage);
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
new_bbpentry(FILE *fp, bat i)
{
#ifndef NDEBUG
	assert(i > 0);
	assert(i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock));
	assert(BBP_desc(i));
	assert(BBP_desc(i)->B.batCacheid == i);
	assert(BBP_desc(i)->S.role == PERSISTENT);
	assert(0 <= BBP_desc(i)->H.heap.farmid && BBP_desc(i)->H.heap.farmid < MAXFARMS);
	assert(BBPfarms[BBP_desc(i)->H.heap.farmid].roles & (1 << PERSISTENT));
	assert(0 <= BBP_desc(i)->T.heap.farmid && BBP_desc(i)->T.heap.farmid < MAXFARMS);
	assert(BBPfarms[BBP_desc(i)->T.heap.farmid].roles & (1 << PERSISTENT));
	if (BBP_desc(i)->H.vheap) {
		assert(0 <= BBP_desc(i)->H.vheap->farmid && BBP_desc(i)->H.vheap->farmid < MAXFARMS);
		assert(BBPfarms[BBP_desc(i)->H.vheap->farmid].roles & (1 << PERSISTENT));
	}
	if (BBP_desc(i)->T.vheap) {
		assert(0 <= BBP_desc(i)->T.vheap->farmid && BBP_desc(i)->T.vheap->farmid < MAXFARMS);
		assert(BBPfarms[BBP_desc(i)->T.vheap->farmid].roles & (1 << PERSISTENT));
	}
#endif

	if (fprintf(fp, SSZFMT " %d %s %s %s %d %d " BUNFMT " " BUNFMT " "
		    BUNFMT " " BUNFMT " " BUNFMT " %d %d %d %d", /* BAT info */
		    (ssize_t) i,
		    BBP_status(i) & BBPPERSISTENT,
		    BBP_logical(i),
		    BBP_logical(-i) ? BBP_logical(-i) : BBPNONAME,
		    BBP_physical(i),
		    BBP_lastused(i),
		    BBP_desc(i)->S.restricted << 1,
		    BBP_desc(i)->S.inserted,
		    BBP_desc(i)->S.deleted,
		    BBP_desc(i)->S.first,
		    BBP_desc(i)->S.count,
		    BBP_desc(i)->S.capacity,
		    (unsigned char) BBP_desc(i)->S.map_head,
		    (unsigned char) BBP_desc(i)->S.map_tail,
		    (unsigned char) BBP_desc(i)->S.map_hheap,
		    (unsigned char) BBP_desc(i)->S.map_theap) < 0 ||
	    heap_entry(fp, &BBP_desc(i)->H) < 0 ||
	    heap_entry(fp, &BBP_desc(i)->T) < 0 ||
	    vheap_entry(fp, BBP_desc(i)->H.vheap) < 0 ||
	    vheap_entry(fp, BBP_desc(i)->T.vheap) < 0 ||
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
	if (fprintf(f, "BBP.dir, GDKversion %d\n%d %d %d\n",
		    GDKLIBRARY, SIZEOF_SIZE_T, SIZEOF_OID, SIZEOF_MAX_INT) < 0 ||
	    OIDwrite(f) < 0 ||
	    fprintf(f, " BBPsize=%d\n", n) < 0 ||
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
	char *p;
	int n;

	assert(subcommit != NULL);

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
	if ((p = strstr(buf, "BBPsize")) != NULL)
		sscanf(p, "BBPsize=%d", &n);
	if (n < (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
		n = (bat) ATOMIC_GET(BBPsize, BBPsizeLock);

	if (GDKdebug & (IOMASK | THRDMASK))
		fprintf(stderr, "#BBPdir: writing BBP.dir (%d bats).\n", n);
	IODEBUG {
		fprintf(stderr, "#BBPdir start oid=");
		OIDwrite(stderr);
		fprintf(stderr, "\n");
	}

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
				if (new_bbpentry(nbbpf, i) != GDK_SUCCEED) {
					goto bailout;
				}
				IODEBUG new_bbpentry(stderr, i);
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
			IODEBUG fprintf(stderr, "%s", buf);
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
	IODEBUG {
		fprintf(stderr, "#BBPdir start oid=");
		OIDwrite(stderr);
		fprintf(stderr, "\n");
	}
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
			if (new_bbpentry(fp, i) != GDK_SUCCEED) {
				goto bailout;
			}
			IODEBUG new_bbpentry(stderr, i);
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
			"# %d[%s,%s]: nme=['%s','%s'] refs=%d lrefs=%d "
			"status=%d count=" BUNFMT " "
			"Hheap=[" SZFMT "," SZFMT "] "
			"Hvheap=[" SZFMT "," SZFMT "] "
			"Hhash=[" SZFMT "," SZFMT "] "
			"Theap=[" SZFMT "," SZFMT "] "
			"Tvheap=[" SZFMT "," SZFMT "] "
			"Thash=[" SZFMT "," SZFMT "]\n",
			i,
			ATOMname(b->H->type),
			ATOMname(b->T->type),
			BBP_logical(i) ? BBP_logical(i) : "<NULL>",
			BBP_logical(-i) ? BBP_logical(-i) : "<NULL>",
			BBP_refs(i),
			BBP_lrefs(i),
			BBP_status(i),
			b->batCount,
			HEAPmemsize(&b->H->heap),
			HEAPvmsize(&b->H->heap),
			HEAPmemsize(b->H->vheap),
			HEAPvmsize(b->H->vheap),
			b->H->hash && b->H->hash != (Hash *) -1 && b->H->hash != (Hash *) 1 ? HEAPmemsize(b->H->hash->heap) : 0,
			b->H->hash && b->H->hash != (Hash *) -1 && b->H->hash != (Hash *) 1 ? HEAPvmsize(b->H->hash->heap) : 0,
			HEAPmemsize(&b->T->heap),
			HEAPvmsize(&b->T->heap),
			HEAPmemsize(b->T->vheap),
			HEAPvmsize(b->T->vheap),
			b->T->hash && b->T->hash != (Hash *) -1 && b->T->hash != (Hash *) 1 ? HEAPmemsize(b->T->hash->heap) : 0,
			b->T->hash && b->T->hash != (Hash *) -1 && b->T->hash != (Hash *) 1 ? HEAPvmsize(b->T->hash->heap) : 0);
		if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
			cmem += HEAPmemsize(&b->H->heap);
			cvm += HEAPvmsize(&b->H->heap);
			nc++;
		} else {
			mem += HEAPmemsize(&b->H->heap);
			vm += HEAPvmsize(&b->H->heap);
			n++;
		}
		if (b->H->vheap) {
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(b->H->vheap);
				cvm += HEAPvmsize(b->H->vheap);
			} else {
				mem += HEAPmemsize(b->H->vheap);
				vm += HEAPvmsize(b->H->vheap);
			}
		}
		if (b->H->hash && b->H->hash != (Hash *) -1 && b->H->hash != (Hash *) 1) {
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(b->H->hash->heap);
				cvm += HEAPvmsize(b->H->hash->heap);
			} else {
				mem += HEAPmemsize(b->H->hash->heap);
				vm += HEAPvmsize(b->H->hash->heap);
			}
		}
		if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
			cmem += HEAPmemsize(&b->T->heap);
			cvm += HEAPvmsize(&b->T->heap);
		} else {
			mem += HEAPmemsize(&b->T->heap);
			vm += HEAPvmsize(&b->T->heap);
		}
		if (b->T->vheap) {
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(b->T->vheap);
				cvm += HEAPvmsize(b->T->vheap);
			} else {
				mem += HEAPmemsize(b->T->vheap);
				vm += HEAPvmsize(b->T->vheap);
			}
		}
		if (b->T->hash && b->T->hash != (Hash *) -1 && b->T->hash != (Hash *) 1) {
			if (BBP_logical(i) && BBP_logical(i)[0] == '.') {
				cmem += HEAPmemsize(b->T->hash->heap);
				cvm += HEAPvmsize(b->T->hash->heap);
			} else {
				mem += HEAPmemsize(b->T->hash->heap);
				vm += HEAPvmsize(b->T->hash->heap);
			}
		}
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
		/* for tmp_X and tmpr_X BATs, we already know X */
		const char *s;

		if (abs(i) >= (bat) ATOMIC_GET(BBPsize, BBPsizeLock) || (s = BBP_logical(i)) == NULL || strcmp(s, nme)) {
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

BATstore *
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
		if (bid < 0 && BBP_logical(bid) == NULL)
			bid = -bid;
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
		strcpy(buf, BBP_physical(abs(bid)));
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

int
BBPcurstamp(void)
{
	return ATOMIC_GET(BBP_curstamp, BBP_curstampLock) & 0x7fffffff;
}

/* There are BBP_THREADMASK+1 (64) free lists, and ours (idx) is
 * empty.  Here we find a longish free list (at least 20 entries), and
 * if we can find one, we take one entry from that list.  If no long
 * enough list can be found, we create a new entry by either just
 * increasing BBPsize (up to BBPlimit) or extending the BBP (which
 * increases BBPlimit).  Every time this function is called we start
 * searching in a following free list (variable "last"). */
static void
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
			BBPextend(idx, TRUE);
		} else {
			BBP_free(idx) = (bat) ATOMIC_GET(BBPsize, BBPsizeLock) - 1;
		}
	}
	last = (last + 1) & BBP_THREADMASK;
}

bat
BBPinsert(BATstore *bs)
{
	MT_Id pid = MT_getpid();
	int lock = locked_by ? pid != locked_by : 1;
	const char *s;
	long_str dirname;
	bat i;
	int idx = threadmask(pid);

	assert(bs->B.H != NULL);
	assert(bs->B.T != NULL);
	assert(bs->B.H == bs->BM.T);
	assert(bs->B.T == bs->BM.H);

	/* critical section: get a new BBP entry */
	if (lock) {
		MT_lock_set(&GDKtrimLock(idx));
		MT_lock_set(&GDKcacheLock(idx));
	}

	/* find an empty slot */
	if (BBP_free(idx) <= 0) {
		/* we need to extend the BBP */
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
			maybeextend(idx);
		}
		MT_lock_unset(&GDKnameLock);
		if (lock)
			for (i = BBP_THREADMASK; i >= 0; i--)
				if (i != idx)
					MT_lock_unset(&GDKcacheLock(i));
	}
	i = BBP_free(idx);
	assert(i > 0);
	BBP_free(idx) = BBP_next(i);

	if (lock) {
		MT_lock_unset(&GDKcacheLock(idx));
		MT_lock_unset(&GDKtrimLock(idx));
	}
	/* rest of the work outside the lock , as GDKstrdup/GDKmalloc
	 * may trigger a BBPtrim */

	/* fill in basic BBP fields for the new bat */

	bs->S.stamp = ATOMIC_INC(BBP_curstamp, BBP_curstampLock) & 0x7fffffff;
	bs->B.batCacheid = i;
	bs->BM.batCacheid = -i;
	bs->S.tid = MT_getpid();

	BBP_status_set(i, BBPDELETING, "BBPinsert");
	BBP_cache(i) = NULL;
	BBP_desc(i) = NULL;
	BBP_refs(i) = 1;	/* new bats have 1 pin */
	BBP_lrefs(i) = 0;	/* ie. no logical refs */

	if (BBP_bak(i) == NULL) {
		s = BBPtmpname(dirname, 64, i);
		BBP_logical(i) = GDKstrdup(s);
		BBP_bak(i) = BBP_logical(i);
	} else
		BBP_logical(i) = BBP_bak(i);
	s = BBPtmpname(dirname, 64, -i);
	BBP_logical(-i) = GDKstrdup(s);

	/* Keep the physical location around forever */
	if (BBP_physical(i) == NULL) {
		char name[64], *nme;

		BBPgetsubdir(dirname, i);
		nme = BBPphysicalname(name, 64, i);

		BBP_physical(i) = GDKfilepath(NOFARM, dirname, nme, NULL);

		BATDEBUG fprintf(stderr, "#%d = new %s(%s,%s)\n", (int) i, BBPname(i), ATOMname(bs->H.type), ATOMname(bs->T.type));
	}

	return i;
}

void
BBPcacheit(BATstore *bs, int lock)
{
	bat i = bs->B.batCacheid;
	int mode;

	if (lock)
		lock = locked_by ? MT_getpid() != locked_by : 1;

	if (i) {
		assert(i > 0);
	} else {
		i = BBPinsert(bs);	/* bat was not previously entered */
		if (bs->H.vheap)
			bs->H.vheap->parentid = i;
		if (bs->T.vheap)
			bs->T.vheap->parentid = i;
	}
	assert(bs->B.batCacheid > 0);
	assert(bs->BM.batCacheid < 0);
	assert(bs->B.batCacheid == -bs->BM.batCacheid);

	if (lock)
		MT_lock_set(&GDKswapLock(i));
	mode = (BBP_status(i) | BBPLOADED) & ~(BBPLOADING | BBPDELETING);
	BBP_status_set(i, mode, "BBPcacheit");
	BBP_lastused(i) = BBPLASTUSED(BBPstamp() + ((mode == BBPLOADED) ? 150 : 0));
	BBP_desc(i) = bs;

	/* cache it! */
	BBP_cache(i) = &bs->B;
	BBP_cache(-i) = &bs->BM;

	if (lock)
		MT_lock_unset(&GDKswapLock(i));
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
		BATstore *bs = BBP_desc(i);

		if (bs) {
			if (BBP_cache(i)) {
				BATDEBUG fprintf(stderr, "#uncache %d (%s)\n", (int) i, BBPname(i));

				BBP_cache(i) = BBP_cache(-i) = NULL;

				/* clearing bits can be done without the lock */
				BBP_status_off(i, BBPLOADED, "BBPuncacheit");
			}
			if (unloaddesc) {
				BBP_desc(i) = NULL;
				BATdestroy(bs);
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
	if (BBPtmpcheck(BBP_logical(-i)) == 0) {
		MT_lock_set(&GDKnameLock);
		BBP_delete(-i);
		MT_lock_unset(&GDKnameLock);
	}
	if (BBP_logical(i) != BBP_bak(i))
		GDKfree(BBP_logical(i));
	if (BBP_logical(-i) != BBP_bak(-i))
		GDKfree(BBP_logical(-i));
	BBP_status_set(i, 0, "BBPclear");
	BBP_logical(i) = NULL;
	BBP_logical(-i) = NULL;
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
		bbpclear(abs(i), threadmask(pid), lock ? "BBPclear" : NULL);
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

	BBPgetsubdir(dirname, abs(bid));

	if ((tmpid = BBPnamecheck(nme)) && (bid < 0 || tmpid != bid)) {
		return BBPRENAME_ILLEGAL;
	}
	if (strlen(dirname) + strLen(nme) + 1 >= IDLENGTH) {
		return BBPRENAME_LONG;
	}
	idx = threadmask(MT_getpid());
	MT_lock_set(&GDKtrimLock(idx));
	MT_lock_set(&GDKnameLock);
	i = BBP_find(nme, FALSE);
	if (i != 0) {
		MT_lock_unset(&GDKnameLock);
		MT_lock_unset(&GDKtrimLock(idx));
		return BBPRENAME_ALREADY;
	}
	BBP_notrim = MT_getpid();

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
		BBP_status_on(abs(bid), BBPRENAMED, "BBPrename");
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		BBPdirty(1);
	}
	MT_lock_unset(&GDKnameLock);
	BBP_notrim = 0;
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
	bat hp, tp, hvp, tvp;
	BATstore *bs;
	BAT *b;
	int load = 0;

	if (i == bat_nil) {
		/* Stefan: May this happen? Or should we better call
		 * GDKerror(), here? */
		/* GDKerror("BBPincref() called with bat_nil!\n"); */
		return 0;
	}
	if (i < 0)
		i = -i;

	if (!BBPcheck(i, "BBPincref"))
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

	bs = BBP_desc(i);
	if ( bs == 0) {
		/* should not have happened */
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		return 0;
	}

	assert(BBP_refs(i) + BBP_lrefs(i) ||
	       BBP_status(i) & (BBPDELETED | BBPSWAPPED));
	if (logical) {
		/* parent BATs are not relevant for logical refs */
		hp = tp = hvp = tvp = 0;
		refs = ++BBP_lrefs(i);
	} else {
		hp = bs->B.H->heap.parentid;
		tp = bs->B.T->heap.parentid;
		hvp = bs->B.H->vheap == 0 || bs->B.H->vheap->parentid == i ? 0 : bs->B.H->vheap->parentid;
		tvp = bs->B.T->vheap == 0 || bs->B.T->vheap->parentid == i ? 0 : bs->B.T->vheap->parentid;
		refs = ++BBP_refs(i);
		if (refs == 1 && (hp || tp || hvp || tvp)) {
			/* If this is a view, we must load the parent
			 * BATs, but we must do that outside of the
			 * lock.  Set the BBPLOADING flag so that
			 * other threads will wait until we're
			 * done. */
			BBP_status_on(i, BBPLOADING, "BBPincref");
			load = 1;
		}
	}
	if (lock)
		MT_lock_unset(&GDKswapLock(i));

	if (load) {
		/* load the parent BATs and set the heap base pointers
		 * to the correct values */
		assert(!logical);
		if (hp) {
			incref(hp, 0, lock);
			b = getBBPdescriptor(hp, lock);
			bs->B.H->heap.base = b->H->heap.base + (size_t) bs->B.H->heap.base;
			/* if we shared the hash before, share
			 * it again note that if the parent's
			 * hash is destroyed, we also don't
			 * have a hash anymore */
			if (bs->B.H->hash == (Hash *) -1)
				bs->B.H->hash = b->H->hash;
		}
		if (tp) {
			incref(tp, 0, lock);
			b = getBBPdescriptor(tp, lock);
			if (bs->B.H != bs->B.T) {  /* mirror? */
				bs->B.T->heap.base = b->H->heap.base + (size_t) bs->B.T->heap.base;
				/* if we shared the hash before, share
				 * it again note that if the parent's
				 * hash is destroyed, we also don't
				 * have a hash anymore */
				if (bs->B.T->hash == (Hash *) -1)
					bs->B.T->hash = b->H->hash;
			}
		}
		if (hvp) {
			incref(hvp, 0, lock);
			(void) getBBPdescriptor(hvp, lock);
		}
		if (tvp) {
			incref(tvp, 0, lock);
			(void) getBBPdescriptor(tvp, lock);
		}
		/* done loading, release descriptor */
		BBP_status_off(i, BBPLOADING, "BBPincref");
	}
	return refs;
}

int
BBPincref(bat i, int logical)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	return incref(i, logical, lock);
}

void
BBPshare(bat parent)
{
	int lock = locked_by ? MT_getpid() != locked_by : 1;

	if (parent < 0)
		parent = -parent;
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
decref(bat i, int logical, int releaseShare, int lock)
{
	int refs = 0, swap = 0;
	bat hp = 0, tp = 0, hvp = 0, tvp = 0;
	BAT *b;

	assert(i > 0);
	if (lock)
		MT_lock_set(&GDKswapLock(i));
	if (releaseShare) {
		--BBP_desc(i)->S.sharecnt;
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		return refs;
	}

	while (BBP_status(i) & BBPUNLOADING) {
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		BBPspin(i, "BBPdecref", BBPUNLOADING);
		if (lock)
			MT_lock_set(&GDKswapLock(i));
	}

	b = BBP_cache(i);

	/* decrement references by one */
	if (logical) {
		if (BBP_lrefs(i) == 0) {
			GDKerror("BBPdecref: %s does not have logical references.\n", BBPname(i));
			assert(0);
		} else {
			refs = --BBP_lrefs(i);
		}
	} else {
		if (BBP_refs(i) == 0) {
			GDKerror("BBPdecref: %s does not have pointer fixes.\n", BBPname(i));
			assert(0);
		} else {
			assert(b == NULL || b->H->heap.parentid == 0 || BBP_refs(b->H->heap.parentid) > 0);
			assert(b == NULL || b->T->heap.parentid == 0 || BBP_refs(b->T->heap.parentid) > 0);
			assert(b == NULL || b->H->vheap == NULL || b->H->vheap->parentid == 0 || BBP_refs(b->H->vheap->parentid) > 0);
			assert(b == NULL || b->T->vheap == NULL || b->T->vheap->parentid == 0 || BBP_refs(b->T->vheap->parentid) > 0);
			refs = --BBP_refs(i);
			if (b && refs == 0) {
				if ((hp = b->H->heap.parentid) != 0)
					b->H->heap.base = (char *) (b->H->heap.base - BBP_cache(hp)->H->heap.base);
				if ((tp = b->T->heap.parentid) != 0 &&
				    b->H != b->T)
					b->T->heap.base = (char *) (b->T->heap.base - BBP_cache(tp)->H->heap.base);
				/* if a view shared the hash with its
				 * parent, indicate this, but only if
				 * view isn't getting destroyed */
				if (hp && b->H->hash &&
				    b->H->hash == BBP_cache(hp)->H->hash)
					b->H->hash = (Hash *) -1;
				if (tp && b->T->hash &&
				    b->T->hash == BBP_cache(tp)->H->hash)
					b->T->hash = (Hash *) -1;
				hvp = VIEWvhparent(b);
				tvp = VIEWvtparent(b);
			}
		}
	}

	/* we destroy transients asap and unload persistent bats only
	 * if they have been made cold or are not dirty */
	if (BBP_refs(i) > 0 ||
	    (BBP_lrefs(i) > 0 &&
	     BBP_lastused(i) != 0 &&
	     (b == NULL || BATdirty(b) || !(BBP_status(i) & BBPPERSISTENT)))) {
		/* bat cannot be swapped out. renew its last usage
		 * stamp for the BBP LRU policy */
		int sec = BBPLASTUSED(BBPstamp());

		if (sec > BBPLASTUSED(BBP_lastused(i)))
			BBP_lastused(i) = sec;
	} else if (b || (BBP_status(i) & BBPTMP)) {
		/* bat will be unloaded now. set the UNLOADING bit
		 * while locked so no other thread thinks it's
		 * available anymore */
		assert((BBP_status(i) & BBPUNLOADING) == 0);
		BATDEBUG {
			fprintf(stderr, "#BBPdecref set to unloading BAT %d\n", i);
		}
		BBP_status_on(i, BBPUNLOADING, "BBPdecref");
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
				fprintf(stderr, "#BBPdecref unload and free bat %d\n", i);
			}
			BBP_unload_inc(i, "BBPdecref");
			/* free memory of transient */
			if (BBPfree(b, "BBPdecref") != GDK_SUCCEED)
				return -1;	/* indicate failure */
		}
	}
	if (hp)
		decref(abs(hp), FALSE, FALSE, lock);
	if (tp)
		decref(abs(tp), FALSE, FALSE, lock);
	if (hvp)
		decref(abs(hvp), FALSE, FALSE, lock);
	if (tvp)
		decref(abs(tvp), FALSE, FALSE, lock);
	return refs;
}

#undef BBPdecref
int
BBPdecref(bat i, int logical)
{
	if (BBPcheck(i, "BBPdecref") == 0) {
		return -1;
	}
	if (i < 0)
		i = -i;
	return decref(i, logical, FALSE, TRUE);
}

/*
 * M5 often changes the physical ref into a logical reference.  This
 * state change consist of the sequence BBPincref(b,TRUE);BBPunfix(b).
 * A faster solution is given below, because it does not trigger the
 * BBP management actions, such as garbage collecting the bats.
 * [first step, initiate code change]
 */
void
BBPkeepref(bat i)
{
	if (i == bat_nil)
		return;
	if (i < 0)
		i = -i;
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
		decref(i, FALSE, FALSE, lock);
	}
}

static inline void
GDKunshare(bat parent)
{
	if (parent < 0)
		parent = -parent;
	(void) decref(parent, FALSE, TRUE, TRUE);
	(void) decref(parent, TRUE, FALSE, TRUE);
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
	i = abs(b->batCacheid);

	assert(BBP_refs(i) == 1);

	return decref(i, 0, 0, lock) <0;
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
	bat j = abs(i);
	BAT *b = NULL;

	if (!BBPcheck(i, "BBPdescriptor")) {
		return NULL;
	}
	assert(BBP_refs(i));
	if ((b = BBP_cache(i)) == NULL) {

		if (lock)
			MT_lock_set(&GDKswapLock(j));
		while (BBP_status(j) & BBPWAITING) {	/* wait for bat to be loaded by other thread */
			if (lock)
				MT_lock_unset(&GDKswapLock(j));
			MT_sleep_ms(KITTENNAP);
			if (lock)
				MT_lock_set(&GDKswapLock(j));
		}
		if (BBPvalid(j)) {
			b = BBP_cache(i);
			if (b == NULL) {
				load = TRUE;
				BATDEBUG {
					fprintf(stderr, "#BBPdescriptor set to unloading BAT %d\n", j);
				}
				BBP_status_on(j, BBPLOADING, "BBPdescriptor");
			}
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(j));
	}
	if (load) {
		IODEBUG fprintf(stderr, "#load %s\n", BBPname(i));

		b = BATload_intern(j, lock);
		BBPin++;

		/* clearing bits can be done without the lock */
		BBP_status_off(j, BBPLOADING, "BBPdescriptor");
		CHECKDEBUG BATassertProps(b);
		if (i < 0)
			b = BATmirror(b);
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
	bat bid = abs(b->batCacheid);
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
	bat hp = b->H->heap.parentid, tp = b->T->heap.parentid;
	bat vhp = VIEWvhparent(b), vtp = VIEWvtparent(b);

	if (isVIEW(b)) {	/* a physical view */
		VIEWdestroy(b);
	} else {
		/* bats that get destroyed must unfix their atoms */
		int (*hunfix) (const void *) = BATatoms[b->htype].atomUnfix;
		int (*tunfix) (const void *) = BATatoms[b->ttype].atomUnfix;
		BUN p, q;
		BATiter bi = bat_iterator(b);

		assert(b->batSharecnt == 0);
		if (hunfix) {
			DELloop(b, p, q) {
				(*hunfix) (BUNhead(bi, p));
			}
			BATloop(b, p, q) {
				(*hunfix) (BUNhead(bi, p));
			}
		}
		if (tunfix) {
			DELloop(b, p, q) {
				(*tunfix) (BUNtail(bi, p));
			}
			BATloop(b, p, q) {
				(*tunfix) (BUNtail(bi, p));
			}
		}
		BATdelete(b);	/* handles persistent case also (file deletes) */
	}
	BBPclear(b->batCacheid);	/* if destroyed; de-register from BBP */

	/* parent released when completely done with child */
	if (hp)
		GDKunshare(hp);
	if (vhp)
		GDKunshare(vhp);
	if (tp)
		GDKunshare(tp);
	if (vtp)
		GDKunshare(vtp);
}

static gdk_return
BBPfree(BAT *b, const char *calledFrom)
{
	bat bid = abs(b->batCacheid), hp = VIEWhparent(b), tp = VIEWtparent(b), vhp = VIEWvhparent(b), vtp = VIEWvtparent(b);
	gdk_return ret;

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
	if (ret == GDK_SUCCEED && hp)
		GDKunshare(hp);
	if (ret == GDK_SUCCEED && tp)
		GDKunshare(tp);
	if (ret == GDK_SUCCEED && vhp)
		GDKunshare(vhp);
	if (ret == GDK_SUCCEED && vtp)
		GDKunshare(vtp);
	return ret;
}

/*
 * @- Storage trimming
 * BBPtrim unloads the least recently used BATs to free memory
 * resources.  It gets passed targets in bytes of physical memory and
 * logical virtual memory resources to free. Overhead costs are
 * reduced by making just one scan, analyzing the first BBPMAXTRIM
 * bats and keeping the result in a list for later use (the oldest bat
 * now is going to be the oldest bat in the future as well).  This
 * list is sorted on last-used timestamp. BBPtrim keeps unloading BATs
 * till the targets are met or there are no more BATs to unload.
 *
 * In determining whether a BAT will be unloaded, first it has to be
 * BBPswappable, and second its resources occupied must be of the
 * requested type. The algorithm actually makes two passes, in the
 * first only clean bats are unloaded (in order of their stamp).
 *
 * In order to keep this under control with multiple threads all
 * running out of memory at the same time, we make sure that
 * @itemize
 * @item
 * just one thread does a BBPtrim at a time (by having a BBPtrimLock
 * set).
 * @item
 * while decisions are made as to which bats to unload (1) the BBP is
 * scanned, and (2) unload decisions are made. Due to these
 * properties, the search&decide phase of BBPtrim acquires both
 * GDKcacheLock (due to (1)) and all GDKswapLocks (due to (2)). They
 * must be released during the actual unloading.  (as otherwise
 * deadlock occurs => unloading a bat may e.g. kill an accelerator
 * that is a BAT, which in turn requires BBP lock acquisition).
 * @item
 * to avoid further deadlock, the update functions in BBP that hold
 * either GDKcacheLock or a GDKswapLock may never cause a BBPtrim
 * (notice that BBPtrim could theoretically be set off just by
 * allocating a little piece of memory, e.g.  GDKstrdup()). If these
 * routines must alloc memory, they must set the BBP_notrim variable,
 * acquiring the addition GDKtrimLock, in order to prevent such
 * deadlock.
 * @item
 * the BBPtrim is atomic; only releases its locks when all BAT unload
 * work is done. This ensures that if all memory requests that
 * triggered BBPtrim could possible be satisfied by unloading BATs,
 * this will succeed.
 * @end itemize
 *
 * The scan phase was optimized further in order to stop early when it
 * is a priori known that the targets are met (which is the case if
 * the BBPtrim is not due to memory shortage but due to the ndesc
 * quota).  Note that scans may always stop before BBPsize as the
 * BBPMAXTRIM is a fixed number which may be smaller. As such, a
 * mechanism was added to resume a broken off scan at the point where
 * scanning was broken off rather than always starting at BBP[1] (this
 * does more justice to the lower numbered bats and will more quickly
 * find fresh unload candidates).
 *
 * We also refined the swap criterion. If the BBPtrim was initiated
 * due to:
 * - too much descriptors: small bats are unloaded first (from LRU
 *   cold to hot)
 * - too little memory: big bats are unloaded first (from LRU cold to
 *   hot).
 * Unloading-first is enforced by subtracting @math{2^31} from the
 * stamp in the field where the candidates are sorted on.
 *
 * BBPtrim is abandoned when the application has indicated that it
 * does not need it anymore.
 */
#define BBPMAXTRIM 40000
#define BBPSMALLBAT 1000

typedef struct {
	bat bid;		/* bat id */
	int next;		/* next position in list */
	BUN cnt;		/* bat count */
#if SIZEOF_BUN == SIZEOF_INT
	BUN dummy;		/* padding to power-of-two size */
#endif
} bbptrim_t;

static unsigned lastused[BBPMAXTRIM]; /* bat lastused stamp; sort on this field */
static bbptrim_t bbptrim[BBPMAXTRIM];
static int bbptrimfirst = BBPMAXTRIM, bbptrimlast = 0, bbpunloadtail, bbpunload, bbptrimmax = BBPMAXTRIM, bbpscanstart = 1;

static bat
BBPtrim_scan(bat bbppos, bat bbplim)
{
	bbptrimlast = 0;
	bbptrimmax = BBPMAXTRIM;
	MEMDEBUG fprintf(stderr, "#TRIMSCAN: start=%d, limit=%d\n", (int) bbppos, (int) bbplim);

	if (bbppos < (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
		do {
			if (BBPvalid(bbppos)) {
				BAT *b = BBP_cache(bbppos);

				if (BBPtrimmable(b)) {
					/* when unloading for memory,
					 * treat small BATs with a
					 * preference over big ones.
					 * rationale: I/O penalty for
					 * cache miss is relatively
					 * higher for small bats
					 */
					BUN cnt = BATcount(b);
					unsigned swap_first = (cnt >= BBPSMALLBAT);

					/* however, when we are
					 * looking to decrease the
					 * number of descriptors, try
					 * to put the small bats in
					 * front of the load list
					 * instead..
					 */

					/* subtract 2-billion to make
					 * sure the swap_first class
					 * bats are unloaded first */
					lastused[bbptrimlast] = (unsigned) BBPLASTUSED(BBP_lastused(bbppos)) | (swap_first << 31);
					bbptrim[bbptrimlast].bid = bbppos;
					bbptrim[bbptrimlast].cnt = cnt;
					if (++bbptrimlast == bbptrimmax)
						break;
				}
			}
			if (++bbppos == (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
				bbppos = 1;	/* treat BBP as a circular buffer */
		} while (bbppos != bbplim);

	if (bbptrimlast > 0) {
		int i;
		/* sort lastused array as (signed) int */
		GDKqsort(lastused, bbptrim, NULL, bbptrimlast,
			 sizeof(lastused[0]), sizeof(bbptrim[0]), TYPE_int);
		for (i = bbptrimfirst = 0; i < bbptrimlast; i++) {
			MEMDEBUG fprintf(stderr, "#TRIMSCAN: %11d%c %9d=%s\t(#" BUNFMT ")\n", (int) BBPLASTUSED(lastused[i]), lastused[i] & ((unsigned) 1 << 31) ? '*' : ' ', i, BBPname(bbptrim[i].bid), bbptrim[i].cnt);

			bbptrim[i].next = i + 1;
		}
		bbptrim[bbptrimlast - 1].next = BBPMAXTRIM;
	} else {
		bbptrimfirst = BBPMAXTRIM;
	}
	MEMDEBUG fprintf(stderr, "#TRIMSCAN: end at %d (size=%d)\n", bbppos, (int) (bat) ATOMIC_GET(BBPsize, BBPsizeLock));

	return bbppos;
}


/* insert BATs to unload from bbptrim list into bbpunload list;
 * rebuild bbptrimlist only with the useful leftovers */
static size_t
BBPtrim_select(size_t target, int dirty)
{
	int bbptrimtail = BBPMAXTRIM, next = bbptrimfirst;

	MEMDEBUG fprintf(stderr, "#TRIMSELECT: dirty = %d\n", dirty);

	/* make the bbptrim-list empty; we will insert the untouched
	 * elements in it */
	bbptrimfirst = BBPMAXTRIM;

	while (next != BBPMAXTRIM) {
		int cur = next;	/* cur is the entry in the old bbptrimlist we are processing */
		int untouched = BBPLASTUSED(BBP_lastused(bbptrim[cur].bid)) <= (int) BBPLASTUSED(lastused[cur]);
		BAT *b = BBP_cache(bbptrim[cur].bid);

		next = bbptrim[cur].next;	/* do now, because we overwrite bbptrim[cur].next below */

		MEMDEBUG if (b) {
			fprintf(stderr,
				"#TRIMSELECT: candidate=%s BAT*=" PTRFMT "\n",
				BBPname(bbptrim[cur].bid),
				PTRFMTCAST(void *)b);

			fprintf(stderr,
				"#            (cnt=" BUNFMT ", mode=%d, "
				"refs=%d, wait=%d, parent=%d,%d, "
				"lastused=%d,%d,%d)\n",
				bbptrim[cur].cnt,
				(int) b->batPersistence,
				BBP_refs(b->batCacheid),
				(BBP_status(b->batCacheid) & BBPWAITING) != 0,
				VIEWhparent(b),
				VIEWtparent(b),
				BBP_lastused(b->batCacheid),
				(int) BBPLASTUSED(lastused[cur]),
				(int) lastused[cur]);
		}
		/* recheck if conditions encountered by trimscan in
		 * the past still hold */
		if (BBPtrimmable(b) && untouched) {
			size_t memdelta = BATmemsize(b, FALSE) + BATvmsize(b, FALSE);
			size_t memdirty = BATmemsize(b, TRUE) + BATvmsize(b, TRUE);

			if (((b->batPersistence == TRANSIENT &&
			      BBP_lrefs(bbptrim[cur].bid) == 0) || /* needs not be saved when unloaded, OR.. */
			     memdirty <= sizeof(BATstore) || /* the BAT is actually clean, OR.. */
			     dirty) /* we are allowed to cause I/O (second run).. */
			    &&	/* AND ... */
			    target > 0 && memdelta > 0)
				/* there is some reward in terms of
				 * memory requirements */
			{
				/* only then we unload! */
				MEMDEBUG {
					fprintf(stderr,
						"#TRIMSELECT: unload %s [" SZFMT "] bytes [" SZFMT "] dirty\n",
						BBPname(b->batCacheid),
						memdelta,
						memdirty);
				}
				BATDEBUG {
					fprintf(stderr,
						"#BBPtrim_select set to unloading BAT %d\n",
						bbptrim[cur].bid);
				}
				BBP_status_on(bbptrim[cur].bid, BBPUNLOADING, "BBPtrim_select");
				BBP_unload_inc(bbptrim[cur].bid, "BBPtrim_select");
				target = target > memdelta ? target - memdelta : 0;

				/* add to bbpunload list */
				if (bbpunload == BBPMAXTRIM) {
					bbpunload = cur;
				} else {
					bbptrim[bbpunloadtail].next = cur;
				}
				bbptrim[cur].next = BBPMAXTRIM;
				bbpunloadtail = cur;
			} else if (!dirty) {
				/* do not unload now, but keep around;
				 * insert at the end of the new
				 * bbptrim list */
				MEMDEBUG {
					fprintf(stderr,
						"#TRIMSELECT: keep %s [" SZFMT "] bytes [" SZFMT "] dirty target(" SZFMT ")\n",
						BBPname(b->batCacheid),
						memdelta,
						memdirty,
						MAX(0, target));
				}
				if (bbptrimtail == BBPMAXTRIM) {
					bbptrimfirst = cur;
				} else {
					bbptrim[bbptrimtail].next = cur;
				}
				bbptrim[cur].next = BBPMAXTRIM;
				bbptrimtail = cur;
			} else {
				/* bats that even in the second
				 * (dirty) run are not selected,
				 * should be acquitted from the
				 * trimlist until a next scan */
				MEMDEBUG fprintf(stderr, "#TRIMSELECT: delete %s from trimlist (does not match trim needs)\n", BBPname(bbptrim[cur].bid));
			}
		} else {
			/* BAT was touched (or unloaded) since
			 * trimscan => it is discarded from both
			 * lists */
			char buf[80], *bnme = BBP_logical(bbptrim[cur].bid);

			if (bnme == NULL) {
				bnme = BBPtmpname(buf, 64, bbptrim[cur].bid);
			}
			MEMDEBUG fprintf(stderr,
					 "#TRIMSELECT: delete %s from trimlist (has been %s)\n",
					 bnme,
					 b ? "touched since last scan" : "unloaded already");
		}

		if (target == 0) {
			/* we're done; glue the rest of the old
			 * bbptrim list to the new bbptrim list */
			if (bbptrimtail == BBPMAXTRIM) {
				bbptrimfirst = next;
			} else {
				bbptrim[bbptrimtail].next = next;
			}
			break;
		}
	}
	MEMDEBUG fprintf(stderr, "#TRIMSELECT: end\n");
	return target;
}

void
BBPtrim(size_t target)
{
	int i, limit, scan, did_scan = FALSE, done = BBP_THREADMASK;
	int msec = 0, bats_written = 0, bats_unloaded = 0;	/* performance info */
	MT_Id t = MT_getpid();

	PERFDEBUG msec = GDKms();

	if (BBP_notrim == t)
		return;		/* avoid deadlock by one thread going here twice */

	for (i = 0; i <= BBP_THREADMASK; i++)
		MT_lock_set(&GDKtrimLock(i));
	BBP_notrim = t;

	/* recheck targets to see whether the work was already done by
	 * another thread */
	if (target && target != BBPTRIM_ALL) {
		size_t rss2 = MT_getrss() / 2;
		target = GDKvm_cursize();
		if (target > rss2)
			target -= rss2;
		else
			target = 0;
	}
	MEMDEBUG fprintf(stderr,
			 "#BBPTRIM_ENTER: memsize=" SZFMT ",vmsize=" SZFMT "\n",
			 GDKmem_cursize(), GDKvm_cursize());

	MEMDEBUG fprintf(stderr, "#BBPTRIM: target=" SZFMT "\n", target);
	PERFDEBUG fprintf(stderr, "#BBPtrim(mem=%d)\n", target > 0);

	scan = (bbptrimfirst == BBPMAXTRIM);
	if (bbpscanstart >= (bat) ATOMIC_GET(BBPsize, BBPsizeLock))
		bbpscanstart = 1;	/* sometimes, the BBP shrinks! */
	limit = bbpscanstart;

	while (target > 0) {
		/* check for runtime overruling */
		if (GDK_vm_trim == 0)
			break;
		if (done-- < 0)
			break;
		/* acquire the BBP locks */
		for (i = 0; i <= BBP_THREADMASK; i++)
			MT_lock_set(&GDKcacheLock(i));
		for (i = 0; i <= BBP_BATMASK; i++)
			MT_lock_set(&GDKswapLock(i));

		/* gather a list of unload candidate BATs, but try to
		 * avoid scanning by reusing previous leftovers
		 * first */
		if (scan) {
			did_scan = TRUE;
			bbpscanstart = BBPtrim_scan(bbpscanstart, limit);
			scan = (bbpscanstart != limit);
		} else {
			scan = TRUE;
		}

		/* decide which of the candidates to unload using LRU */
		bbpunload = BBPMAXTRIM;
		target = BBPtrim_select(target, FALSE);	/* first try to select only clean BATs */
		if (did_scan && target > 0) {
			target = BBPtrim_select(target, TRUE);	/* if that is not enough, also unload dirty BATs */
		}

		/* release the BBP locks */
		for (i = 0; i <= BBP_BATMASK; i++)
			MT_lock_unset(&GDKswapLock(i));
		for (i = 0; i <= BBP_THREADMASK; i++)
			MT_lock_unset(&GDKcacheLock(i));

		/* do the unload work unlocked */
		MEMDEBUG fprintf(stderr, "#BBPTRIM: %s\n",
				 (bbpunload != BBPMAXTRIM) ? " lastused   batid name" : "no more unload candidates!");

		for (i = bbpunload; i != BBPMAXTRIM; i = bbptrim[i].next) {
			BAT *b = BBP_cache(bbptrim[i].bid);

			if (b == NULL || !(BBP_status(bbptrim[i].bid) & BBPUNLOADING)) {
				IODEBUG fprintf(stderr,
						"BBPtrim: bat(%d) gone\n",
						bbptrim[i].bid);
				continue;
			}
			MEMDEBUG fprintf(stderr, "#BBPTRIM: %8d%c %7d %s\n",
					 (int) BBPLASTUSED(lastused[i]),
					 lastused[i] & ((unsigned) 1 << 31) ? '*' : ' ',
					 (int) bbptrim[i].bid,
					 BBPname(bbptrim[i].bid));

			bats_written += (b->batPersistence != TRANSIENT && BATdirty(b));
			bats_unloaded++;
			BATDEBUG {
				fprintf(stderr,
					"#BBPtrim unloaded and free bat %d\n",
					b->batCacheid);
			}
			BBPfree(b, "BBPtrim");
		}
		/* continue while we can scan for more candiates */
		if (!scan)
			break;
	}
	/* done trimming */
	MEMDEBUG fprintf(stderr, "#BBPTRIM_EXIT: memsize=" SZFMT ",vmsize=" SZFMT "\n", GDKmem_cursize(), GDKvm_cursize());
	PERFDEBUG fprintf(stderr, "#BBPtrim(did_scan=%d, bats_unloaded=%d, bats_written=%d) %d ms\n", did_scan, bats_unloaded, bats_written, GDKms() - msec);

	BBP_notrim = 0;
	for (i = BBP_THREADMASK; i >= 0; i--)
		MT_lock_unset(&GDKtrimLock(i));
}

void
BBPhot(bat i)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(i, "BBPhot")) {
		int lock = locked_by ? MT_getpid() != locked_by : 1;

		if (lock)
			MT_lock_set(&GDKswapLock(i));
		BBP_lastused(i) = BBPLASTUSED(BBPstamp() + 30000);
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
	}
}

void
BBPcold(bat i)
{
	if (i < 0)
		i = -i;
	if (BBPcheck(i, "BBPcold")) {
		MT_Id pid = MT_getpid();
		int idx = threadmask(pid);
		int lock = locked_by ? pid != locked_by : 1;

		MT_lock_set(&GDKtrimLock(idx));
		if (lock)
			MT_lock_set(&GDKswapLock(i));
		/* make very cold and insert on top of trim list */
		BBP_lastused(i) = 0;
		if (BBP_cache(i) && bbptrimlast < bbptrimmax) {
			lastused[--bbptrimmax] = 0;
			bbptrim[bbptrimmax].bid = i;
			bbptrim[bbptrimmax].next = bbptrimfirst;
			bbptrimfirst = bbptrimmax;
		}
		if (lock)
			MT_lock_unset(&GDKswapLock(i));
		MT_lock_unset(&GDKtrimLock(idx));
	}
}

/*
 * BBPquickdesc loads a BAT descriptor without loading the entire BAT,
 * of which the result be used only for a *limited* number of
 * purposes. Specifically, during the global sync/commit, we do not
 * want to load any BATs that are not already loaded, both because
 * this costs performance, and because getting into memory shortage
 * during a commit is extremely dangerous, as the global sync has all
 * the BBPlocks, so no BBPtrim() can be done to free memory when
 * needed. Loading a BAT tends not to be required, since the commit
 * actions mostly involve moving some pointers in the BAT
 * descriptor. However, some column types do require loading the full
 * bat. This is tested by the complexatom() routine. Such columns are
 * those of which the type has a fix/unfix method, or those that have
 * HeapDelete methods. The HeapDelete actions are not always required
 * and therefore the BBPquickdesc is parametrized.
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
	    complexatom(b->htype, delaccess) ||
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
	/* see doc at BATsetaccess()/gdk_bat.mx for an expose on mmap
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
do_backup(const char *srcdir, const char *nme, const char *extbase,
	  Heap *h, int tp, int dirty, bit subcommit)
{
	gdk_return ret = GDK_SUCCEED;

	 /* direct mmap is unprotected (readonly usage, or has WAL
	  * protection); however, if we're backing up for subcommit
	  * and a backup already exists in the main backup directory
	  * (see GDKupgradevarheap), move the file */
	if (subcommit && file_exists(h->farmid, BAKDIR, nme, extbase)) {
		if (file_move(h->farmid, BAKDIR, SUBDIR, nme, extbase) != GDK_SUCCEED)
			return GDK_FAIL;
	} else if (h->storage != STORE_MMAP) {
		/* STORE_PRIV saves into X.new files. Two cases could
		 * happen. The first is when a valid X.new exists
		 * because of an access change or a previous
		 * commit. This X.new should be backed up as
		 * usual. The second case is when X.new doesn't
		 * exist. In that case we could have half written
		 * X.new files (after a crash). To protect against
		 * these we write X.new.kill files in the backup
		 * directory (see heap_move). */
		char ext[16];
		gdk_return mvret = GDK_SUCCEED;

		if (h->filename && h->newstorage == STORE_PRIV)
			snprintf(ext, sizeof(ext), "%s.new", extbase);
		else
			snprintf(ext, sizeof(ext), "%s", extbase);
		if (tp && dirty && !file_exists(h->farmid, BAKDIR, nme, ext)) {
			/* file will be saved (is dirty), move the old
			 * image into backup */
			mvret = heap_move(h, srcdir, subcommit ? SUBDIR : BAKDIR, nme, ext);
		} else if (subcommit && tp &&
			   (dirty || file_exists(h->farmid, BAKDIR, nme, ext))) {
			/* file is clean. move the backup into the
			 * subcommit dir (commit should eliminate
			 * backup) */
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

	if (do_backup(srcdir, nme, "head", &b->H->heap, b->htype,
		      b->batDirty || b->H->heap.dirty, subcommit) != GDK_SUCCEED)
		goto fail;
	if (do_backup(srcdir, nme, "tail", &b->T->heap, b->ttype,
		      b->batDirty || b->T->heap.dirty, subcommit) != GDK_SUCCEED)
		goto fail;
	if (b->H->vheap &&
	    do_backup(srcdir, nme, "hheap", b->H->vheap,
		      b->htype && b->hvarsized,
		      b->batDirty || b->H->vheap->dirty, subcommit) != GDK_SUCCEED)
		goto fail;
	if (b->T->vheap &&
	    do_backup(srcdir, nme, "theap", b->T->vheap,
		      b->ttype && b->tvarsized,
		      b->batDirty || b->T->vheap->dirty, subcommit) != GDK_SUCCEED)
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
	if (OIDdirty()) {
		bbpdirty = BBP_dirty = 1;
	}
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
			(void) GDKremovedir(0, DELDIR);
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
	str bakdirpath = GDKfilepath(farmid, NULL, BAKDIR, NULL);
	str leftdirpath = GDKfilepath(farmid, NULL, LEFTDIR, NULL);

	DIR *dirp = opendir(bakdirpath);
	struct dirent *dent;
	long_str path, dstpath;
	bat i;
	size_t j = strlen(BATDIR);
	gdk_return ret = GDK_SUCCEED;
	int dirseen = FALSE;
	str dstdir;

	if (dirp == NULL) {
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
	str subdirpath = GDKfilepath(0, NULL, SUBDIR, NULL);
	DIR *dirp = opendir(subdirpath);
	struct dirent *dent;
	gdk_return ret = GDK_SUCCEED;

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
			IODEBUG fprintf(stderr, "BBPrecover_subdir: %s%cBBP.dir had disappeared!", SUBDIR, DIR_SEP);
			backup_dir = 0;
		}
	}
	IODEBUG fprintf(stderr, "#BBPrecover_subdir(end) = %d\n", (int) ret);

	if (ret != GDK_SUCCEED)
		GDKerror("BBPrecover_subdir: recovery failed. Please check whether your disk is full or write-protected.\n");
	GDKfree(subdirpath);
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
	BATstore *bs = BBPgetdesc(bid);

	if (bs == NULL)
		BBPclear(bid);
	return &bs->B;
}

static int
BBPdiskscan(const char *parent)
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
		    (strcmp(parent, BATDIR) == 0 ||
		     strncmp(parent, BAKDIR, strlen(BAKDIR)) == 0 ||
		     strncmp(parent, SUBDIR, strlen(SUBDIR)) == 0))
			continue;

		p = strchr(dent->d_name, '.');
		bid = strtol(dent->d_name, NULL, 8);
		ok = p && bid;
		delete = FALSE;

		if (strlen(dent->d_name) >= dstlen) {
			/* found a file with too long a name
			   (i.e. unknown); stop pruning in this
			   subdir */
			IODEBUG fprintf(stderr, "BBPdiskscan: unexpected file %s, leaving %s.\n", dent->d_name, parent);
			break;
		}
		strncpy(dst, dent->d_name, dstlen);
		fullname[sizeof(fullname) - 1] = 0;

		if (p == NULL && BBPdiskscan(fullname) == 0) {
			/* it was a directory */
			continue;
		}

		if (ok == FALSE || !persistent_bat(bid)) {
			delete = TRUE;
		} else if (strstr(p + 1, ".tmp")) {
			delete = 1;	/* throw away any .tmp file */
		} else if (strncmp(p + 1, "head", 4) == 0) {
			BAT *b = getdesc(bid);
			delete = (b == NULL || !b->htype || b->batCopiedtodisk == 0);
		} else if (strncmp(p + 1, "tail", 4) == 0) {
			BAT *b = getdesc(bid);
			delete = (b == NULL || !b->ttype || b->batCopiedtodisk == 0);
		} else if (strncmp(p + 1, "hheap", 5) == 0) {
			BAT *b = getdesc(bid);
			delete = (b == NULL || !b->H->vheap || b->batCopiedtodisk == 0);
		} else if (strncmp(p + 1, "theap", 5) == 0) {
			BAT *b = getdesc(bid);
			delete = (b == NULL || !b->T->vheap || b->batCopiedtodisk == 0);
		} else if (strncmp(p + 1, "hhash", 5) == 0) {
#ifdef PERSISTENTHASH
			BAT *b = getdesc(bid);
			delete = b == NULL;
			if (!delete)
				b->H->hash = (Hash *) 1;
#else
			delete = TRUE;
#endif
		} else if (strncmp(p + 1, "hhash", 5) == 0 ||
			   strncmp(p + 1, "thash", 5) == 0) {
#ifdef PERSISTENTHASH
			BAT *b = getdesc(bid);
			delete = b == NULL;
			if (!delete)
				b->T->hash = (Hash *) 1;
#else
			delete = TRUE;
#endif
		} else if (strncmp(p + 1, "himprints", 9) == 0) {
			BAT *b = getdesc(bid);
			delete = b == NULL;
			if (!delete)
				b->H->imprints = (Imprints *) 1;
		} else if (strncmp(p + 1, "timprints", 9) == 0) {
			BAT *b = getdesc(bid);
			delete = b == NULL;
			if (!delete)
				b->T->imprints = (Imprints *) 1;
		} else if (strncmp(p + 1, "priv", 4) != 0 &&
			   strncmp(p + 1, "new", 3) != 0 &&
			   strncmp(p + 1, "head", 4) != 0 &&
			   strncmp(p + 1, "tail", 4) != 0) {
			ok = FALSE;
		}
		if (!ok) {
			/* found an unknown file; stop pruning in this
			 * subdir */
			IODEBUG fprintf(stderr, "BBPdiskscan: unexpected file %s, leaving %s.\n", dent->d_name, parent);
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

#if 0
void
BBPatom_drop(int atom)
{
	int i;
	const char *nme = ATOMname(atom);
	int unknown = ATOMunknown_add(nme);

	BBPlock("BBPatom_drop");
	for (i = 0; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
		if (BBPvalid(i)) {
			BATstore *b = BBP_desc(i);

			if (!b)
				continue;

			if (b->B.htype == atom)
				b->B.htype = unknown;
			if (b->B.ttype == atom)
				b->B.ttype = unknown;
		}
	}
	BBPunlock("BBPatom_drop");
}

void
BBPatom_load(int atom)
{
	const char *nme;
	int i, unknown;

	BBPlock("BBPatom_load");
	nme = ATOMname(atom);
	unknown = ATOMunknown_find(nme);
	ATOMunknown_del(unknown);
	for (i = 0; i < (bat) ATOMIC_GET(BBPsize, BBPsizeLock); i++) {
		if (BBPvalid(i)) {
			BATstore *b = BBP_desc(i);

			if (!b)
				continue;

			if (b->B.htype == unknown)
				b->B.htype = atom;
			if (b->B.ttype == unknown)
				b->B.ttype = atom;
		}
	}
	BBPunlock("BBPatom_load");
}
#endif

