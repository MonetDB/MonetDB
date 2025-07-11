/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

/* only check whether we exceed gdk_vm_maxsize when allocating heaps */
#define SIZE_CHECK_IN_HEAPS_ONLY 1

#include "gdk_system_private.h"

enum heaptype {
	offheap,
	varheap,
	hashheap,
	orderidxheap,
	strimpheap,
	dataheap
};

enum range_comp_t {
	range_before,		/* search range fully before bat range */
	range_after,		/* search range fully after bat range */
	range_atstart,		/* search range before + inside */
	range_atend,		/* search range inside + after */
	range_contains,		/* search range contains bat range */
	range_inside,		/* search range inside bat range */
};

struct allocator {
	struct allocator *pa;
	size_t size;
	size_t nr;
	char **blks;
	size_t used; 	/* memory used in last block */
	size_t usedmem;	/* used memory */
	void *freelist;	/* list of freed blocks */
	exception_buffer eb;
	char *first_block; /* the special block in blks that also holds our bookkeeping */
	size_t reserved;  /* space in first_block is reserved up to here  */
};

bool ATOMisdescendant(int id, int parentid)
	__attribute__((__visibility__("hidden")));
int ATOMunknown_find(const char *nme)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
const char *ATOMunknown_name(int a)
	__attribute__((__visibility__("hidden")));
void ATOMunknown_clean(void)
	__attribute__((__visibility__("hidden")));
bool BATcheckhash(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return BATcheckmodes(BAT *b, bool persistent)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
BAT *BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role, uint16_t width)
	__attribute__((__visibility__("hidden")));
void BATdelete(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATfree(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, bool subsorted)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
Hash *BAThash_impl(BAT *restrict b, struct canditer *restrict ci, const char *restrict ext)
	__attribute__((__visibility__("hidden")));
void BAThashsave(BAT *b, bool dosync)
	__attribute__((__visibility__("hidden")));
bool BATiscand(BAT *b)
	__attribute__((__visibility__("hidden")));
BAT *BATload_intern(bat bid, bool lock)
	__attribute__((__visibility__("hidden")));
gdk_return BATmaterialize(BAT *b, BUN cap)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return BATsave_iter(BAT *bd, BATiter *bi, BUN size)
	__attribute__((__visibility__("hidden")));
void BATsetdims(BAT *b, uint16_t width)
	__attribute__((__visibility__("hidden")));
void BBPcacheit(BAT *bn, bool lock)
	__attribute__((__visibility__("hidden")));
gdk_return BBPchkfarms(void)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void BBPclear(bat bid)
	__attribute__((__visibility__("hidden")));
void BBPdump(void)		/* never called: for debugging only */
	__attribute__((__cold__));
void BBPexit(void)
	__attribute__((__visibility__("hidden")));
gdk_return BBPinit(bool allow_hge_upgrade, bool no_manager)
	__attribute__((__visibility__("hidden")));
bat BBPallocbat(int tt)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void BBPprintinfo(void)
	__attribute__((__visibility__("hidden")));
int BBPselectfarm(role_t role, int type, enum heaptype hptype)
	__attribute__((__visibility__("hidden")));
gdk_return BBPsync(int cnt, const bat *restrict subcommit, const BUN *restrict sizes, lng logno)
	__attribute__((__visibility__("hidden")))
	__attribute__((__access__(read_only, 2, 1)))
	__attribute__((__access__(read_only, 3, 1)));
BUN binsearch(const oid *restrict indir, oid offset, int type, const void *restrict vals, const char * restrict vars, int width, BUN lo, BUN hi, const void *restrict v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
BUN binsearch_bte(const oid *restrict indir, oid offset, const bte *restrict vals, BUN lo, BUN hi, bte v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
BUN binsearch_sht(const oid *restrict indir, oid offset, const sht *restrict vals, BUN lo, BUN hi, sht v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
BUN binsearch_int(const oid *restrict indir, oid offset, const int *restrict vals, BUN lo, BUN hi, int v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
BUN binsearch_lng(const oid *restrict indir, oid offset, const lng *restrict vals, BUN lo, BUN hi, lng v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
#ifdef HAVE_HGE
BUN binsearch_hge(const oid *restrict indir, oid offset, const hge *restrict vals, BUN lo, BUN hi, hge v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
#endif
BUN binsearch_flt(const oid *restrict indir, oid offset, const flt *restrict vals, BUN lo, BUN hi, flt v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
BUN binsearch_dbl(const oid *restrict indir, oid offset, const dbl *restrict vals, BUN lo, BUN hi, dbl v, int ordering, int last)
	__attribute__((__visibility__("hidden")));
Heap *createOIDXheap(BAT *b, bool stable)
	__attribute__((__visibility__("hidden")));
void doHASHdestroy(BAT *b, Hash *hs)
	__attribute__((__visibility__("hidden")));
void gdk_bbp_reset(void)
	__attribute__((__visibility__("hidden")));
gdk_return GDKextend(const char *fn, size_t size)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKextendf(int fd, size_t size, const char *fn)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
int GDKfdlocate(int farmid, const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")));
FILE *GDKfilelocate(int farmid, const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")))
	__attribute__((__malloc__))
	__attribute__((__malloc__(fclose, 1)));
FILE *GDKfileopen(int farmid, const char *dir, const char *name, const char *extension, const char *mode)
	__attribute__((__visibility__("hidden")))
	__attribute__((__malloc__))
	__attribute__((__malloc__(fclose, 1)));
char *GDKload(int farmid, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
	__attribute__((__visibility__("hidden")));
gdk_return GDKmove(int farmid, const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2, bool report)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void *GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	__attribute__((__visibility__("hidden")));
gdk_return GDKremovedir(int farmid, const char *nme)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKsave(int farmid, const char *nme, const char *ext, void *buf, size_t size, storage_t mode, bool dosync)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKrsort(void *restrict h, void *restrict t, size_t n, size_t hs, size_t ts, bool reverse, bool isuuid)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKssort_rev(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKssort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKtracer_init(const char *dbname, const char *dbtrace)
	__attribute__((__visibility__("hidden")));
gdk_return GDKunlink(int farmid, const char *dir, const char *nme, const char *extension)
	__attribute__((__visibility__("hidden")));
lng getBBPlogno(void)
	__attribute__((__visibility__("hidden")));
BUN HASHappend(BAT *b, BUN i, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHappend_locked(BAT *b, BUN i, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHfree(BAT *b)
	__attribute__((__visibility__("hidden")));
BUN HASHdelete(BATiter *bi, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHdelete_locked(BATiter *bi, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
BUN HASHinsert(BATiter *bi, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHinsert_locked(BATiter *bi, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
__attribute__((__const__))
static inline BUN
HASHmask(BUN cnt)
{
	cnt = cnt * 8 / 7;
	if (cnt < BATTINY)
		cnt = BATTINY;
	return cnt;
}
gdk_return HASHnew(Hash *h, int tpe, BUN size, BUN mask, BUN count, bool bcktonly)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPcopy(Heap *dst, Heap *src, size_t offset)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void HEAPfree(Heap *h, bool remove)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPgrow(Heap **old, size_t size, bool mayshare)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void HEAP_recover(Heap *, const var_t *, BUN)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPsave(Heap *h, const char *nme, const char *ext, bool dosync, BUN free, MT_Lock *lock)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
double joincost(BAT *r, BUN lcount, struct canditer *rci, bool *hash, bool *phash, bool *cand)
	__attribute__((__visibility__("hidden")));
void STRMPincref(Strimps *strimps)
	__attribute__((__visibility__("hidden")));
void STRMPdecref(Strimps *strimps, bool remove)
	__attribute__((__visibility__("hidden")));
void STRMPfree(BAT *b)
	__attribute__((__visibility__("hidden")));
void MT_init_posix(void)
	__attribute__((__visibility__("hidden")));
void *MT_mmap(const char *path, int mode, size_t len)
	__attribute__((__visibility__("hidden")));
void *MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	__attribute__((__visibility__("hidden")));
int MT_msync(void *p, size_t len)
	__attribute__((__visibility__("hidden")));
int MT_munmap(void *p, size_t len)
	__attribute__((__visibility__("hidden")));
void OIDXfree(BAT *b)
	__attribute__((__visibility__("hidden")));
void persistOIDX(BAT *b)
	__attribute__((__visibility__("hidden")));
void PROPdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
void PROPdestroy_nolock(BAT *b)
	__attribute__((__visibility__("hidden")));
void settailname(Heap *restrict tail, const char *restrict physnme, int tt, int width)
	__attribute__((__visibility__("hidden")));
void strCleanHash(Heap *hp, bool rebuild)
	__attribute__((__visibility__("hidden")));
gdk_return strHeap(Heap *d, size_t cap)
	__attribute__((__visibility__("hidden")));
var_t strLocate(Heap *h, const char *v)
	__attribute__((__visibility__("hidden")));
var_t strPut(BAT *b, var_t *dst, const void *v)
	__attribute__((__visibility__("hidden")));
char *strRead(str a, size_t *dstlen, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
ssize_t strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
	__attribute__((__visibility__("hidden")));
gdk_return strWrite(const char *a, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
gdk_return TMcommit(void)
	__attribute__((__visibility__("hidden")));
gdk_return unshare_varsized_heap(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
BAT *virtualize(BAT *bn)
	__attribute__((__visibility__("hidden")));

/* calculate the integer 2 logarithm (i.e. position of highest set
 * bit) of the argument (with a slight twist: 0 gives 0, 1 gives 1,
 * 0x8 to 0xF give 4, etc.) */
__attribute__((__const__))
static inline unsigned
ilog2(BUN x)
{
	if (x == 0)
		return 0;
#ifdef __has_builtin
#if SIZEOF_BUN == 8 && __has_builtin(__builtin_clzll)
	return (unsigned) (64 - __builtin_clzll((unsigned long long) x));
#define BUILTIN_USED
#elif __has_builtin(__builtin_clz)
	return (unsigned) (32 - __builtin_clz((unsigned) x));
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
	unsigned long n;
	if (
#if SIZEOF_BUN == 8
		_BitScanReverse64(&n, (unsigned __int64) x)
#else
		_BitScanReverse(&n, (unsigned long) x)
#endif
		)
		return (unsigned) n + 1;
	else
		return 0;
#else
	unsigned n = 0;
	BUN y;

	/* use a "binary search" method */
#if SIZEOF_BUN == 8
	if ((y = x >> 32) != 0) {
		x = y;
		n += 32;
	}
#endif
	if ((y = x >> 16) != 0) {
		x = y;
		n += 16;
	}
	if ((y = x >> 8) != 0) {
		x = y;
		n += 8;
	}
	if ((y = x >> 4) != 0) {
		x = y;
		n += 4;
	}
	if ((y = x >> 2) != 0) {
		x = y;
		n += 2;
	}
	if ((y = x >> 1) != 0) {
		x = y;
		n += 1;
	}
	return n + (x != 0);
#endif
#endif
#undef BUILTIN_USED
}

/* some macros to help print info about BATs when using ALGODEBUG */
#define ALGOBATFMT	"%s#" BUNFMT "@" OIDFMT "[%s%s]%s%s%s%s%s%s%s%s%s"
#define ALGOBATPAR(b)							\
	BATgetId(b),							\
	BATcount(b),							\
	b->hseqbase,							\
	ATOMname(b->ttype),						\
	b->ttype==TYPE_str?b->twidth==1?"1":b->twidth==2?"2":b->twidth==4?"4":"8":"", \
	!b->batTransient ? "P" : b->theap && b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T", \
	BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && b->tvheap && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "", \
	b->tsorted ? "S" : b->tnosorted ? "!s" : "",			\
	b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "",		\
	b->tkey ? "K" : b->tnokey[1] ? "!k" : "",			\
	b->tnonil ? "N" : "",						\
	b->thash ? "H" : "",						\
	b->torderidx ? "O" : "",					\
	b->tstrimps ? "I" : b->theap && b->theap->parentid && BBP_desc(b->theap->parentid) && BBP_desc(b->theap->parentid)->tstrimps ? "(I)" : ""
/* use ALGOOPTBAT* when BAT is optional (can be NULL) */
#define ALGOOPTBATFMT	"%s%s" BUNFMT "%s" OIDFMT "%s%s%s%s%s%s%s%s%s%s%s%s%s"
#define ALGOOPTBATPAR(b)						\
	b ? BATgetId(b) : "",						\
	b ? "#" : "",							\
	b ? BATcount(b) : 0,						\
	b ? "@" : "",							\
	b ? b->hseqbase : 0,						\
	b ? "[" : "",							\
	b ? ATOMname(b->ttype) : "",					\
	b ? b->ttype==TYPE_str?b->twidth==1?"1":b->twidth==2?"2":b->twidth==4?"4":"8":"" : "", \
	b ? "]" : "",							\
	b ? !b->batTransient ? "P" : b->theap && b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T" : "", \
	b ? BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && b->tvheap && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "" : "", \
	b ? b->tsorted ? "S" : b->tnosorted ? "!s" : "" : "",		\
	b ? b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "" : "",	\
	b ? b->tkey ? "K" : b->tnokey[1] ? "!k" : "" : "",		\
	b && b->tnonil ? "N" : "",					\
	b && b->thash ? "H" : "",					\
	b && b->torderidx ? "O" : "",					\
	b ? b->tstrimps ? "I" : b->theap && b->theap->parentid && BBP_desc(b->theap->parentid) && BBP_desc(b->theap->parentid)->tstrimps ? "(I)" : "" : ""

#ifdef __SANITIZE_THREAD__
#define BBP_BATMASK	31
#else
#define BBP_BATMASK	((1 << (SIZEOF_SIZE_T + 5)) - 1)
#endif

struct PROPrec {
	enum prop_t id;
	ValRecord v;
	struct PROPrec *next;	/* simple chain of properties */
};

typedef uint64_t strimp_masks_t;  /* TODO: make this a sparse matrix */

struct Strimps {
	Heap strimps;
	uint8_t *sizes_base;	/* pointer into strimps heap (pair sizes)  */
	uint8_t *pairs_base;	/* pointer into strimps heap (pairs start)   */
	void *bitstrings_base;	/* pointer into strimps heap (bitstrings
				 * start) bitstrings_base is a pointer
				 * to uint64_t */
	size_t rec_cnt;		/* reconstruction counter: how many
				 * bitstrings were added after header
				 * construction. Currently unused. */
	strimp_masks_t *masks;  /* quick access to masks for
				 * bitstring construction */
};

typedef struct {
	MT_Lock swap;
	MT_Cond cond;
} batlock_t;

typedef char long_str[IDLENGTH];	/* standard GDK static string */

#define MAXFARMS       32

extern struct BBPfarm_t {
	uint32_t roles;		/* bitmask of allowed roles */
	const char *dirname;	/* farm directory */
	FILE *lock_file;
} BBPfarms[MAXFARMS];

extern batlock_t GDKbatLock[BBP_BATMASK + 1];
extern size_t GDK_mmap_minsize_persistent; /* size after which we use memory mapped files for persistent heaps */
extern size_t GDK_mmap_minsize_transient; /* size after which we use memory mapped files for transient heaps */
extern size_t GDK_mmap_pagesize; /* mmap granularity */

#define BATcheck(tst, err)				\
	do {						\
		if ((tst) == NULL) {			\
			GDKerror("BAT required.\n");	\
			return (err);			\
		}					\
	} while (0)
#define ERRORcheck(tst,	msg, err)		\
	do {					\
		if (tst) {			\
			GDKerror(msg);		\
			return (err);		\
		}				\
	} while (0)

#define GDKswapLock(x)  GDKbatLock[(x)&BBP_BATMASK].swap
#define GDKswapCond(x)  GDKbatLock[(x)&BBP_BATMASK].cond

#define HEAPREMOVE	((ATOMIC_BASE_TYPE) 1 << (sizeof(ATOMIC_BASE_TYPE) * 8 - 1))
#define DELAYEDREMOVE	((ATOMIC_BASE_TYPE) 1 << (sizeof(ATOMIC_BASE_TYPE) * 8 - 2))
#define HEAPREFS	(((ATOMIC_BASE_TYPE) 1 << (sizeof(ATOMIC_BASE_TYPE) * 8 - 2)) - 1)

/* when the number of updates to a BAT is less than 1 in this number, we
 * keep the unique_est property */
#define GDK_UNIQUE_ESTIMATE_KEEP_FRACTION	1000
extern BUN gdk_unique_estimate_keep_fraction; /* should become a define once */
/* if the number of unique values is less than 1 in this number, we
 * destroy the hash rather than update it in HASH{append,insert,delete} */
#define HASH_DESTROY_UNIQUES_FRACTION		1000
extern BUN hash_destroy_uniques_fraction;     /* likewise */
/* if the estimated number of unique values is less than 1 in this
 * number, don't build a hash table to do a hashselect */
#define NO_HASH_SELECT_FRACTION			1000
extern dbl no_hash_select_fraction;           /* same here */
/* if the hash chain is longer than this number, we delete the hash
 * rather than maintaining it in HASHdelete */
#define HASH_DESTROY_CHAIN_LENGTH		1000
extern BUN hash_destroy_chain_length;

extern void (*GDKtriggerusr1)(void);

#if !defined(NDEBUG) && !defined(__COVERITY__)
/* see comment in gdk.h */
#ifdef __GNUC__
#define GDKmremap(p, m, oa, os, ns)					\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		void *_oa = (oa);					\
		size_t _os = (os);					\
		size_t *_ns = (ns);					\
		size_t _ons = *_ns;					\
		void *_res = GDKmremap(_path, _mode, _oa, _os, _ns);	\
		TRC_DEBUG(ALLOC,					\
			  "GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p\n", \
			  _path ? _path : "NULL", (unsigned) _mode,	\
			  _oa, _os, _ons, *_ns, _res);			\
		_res;							\
	 })
#else
static inline void *
GDKmremap_debug(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	size_t orig_new_size = *new_size;
	void *res = GDKmremap(path, mode, old_address, old_size, new_size);
	TRC_DEBUG(ALLOC, "GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p\n",
		  path ? path : "NULL", (unsigned) mode,
		  old_address, old_size, orig_new_size, *new_size, res);
	return res;
}
#define GDKmremap(p, m, oa, os, ns)	GDKmremap_debug(p, m, oa, os, ns)

#endif
#endif
