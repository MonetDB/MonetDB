/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _GDK_PRIVATE_H_
#define _GDK_PRIVATE_H_

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

#include "gdk.h"
#include "gdk_system_private.h"

enum range_comp_t {
	range_before,		/* search range fully before bat range */
	range_after,		/* search range fully after bat range */
	range_atstart,		/* search range before + inside */
	range_atend,		/* search range inside + after */
	range_contains,		/* search range contains bat range */
	range_inside,		/* search range inside bat range */
};

bool ATOMisdescendant(int id, int parentid)
	ATTR_VISIBILITY_HIDDEN;
int ATOMunknown_find(const char *nme)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
const char *ATOMunknown_name(int a)
	ATTR_VISIBILITY_HIDDEN;
void ATOMunknown_clean(void)
	ATTR_VISIBILITY_HIDDEN;
bool BATcheckhash(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
bool BATcheckhash_locked(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
BAT *BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role, uint16_t width)
	ATTR_VISIBILITY_HIDDEN;
void BATdelete(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void BATdestroy(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void BATfree(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, bool subsorted)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
Hash *BAThash_impl(BATiter *restrict bi, struct canditer *restrict ci, bool offsets, const char *restrict ext, uint8_t width)
	ATTR_VISIBILITY_HIDDEN;
void BAThashsave(BAT *b, bool dosync)
	ATTR_VISIBILITY_HIDDEN;
bool BATiscand(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
BAT *BATload_intern(bat bid, bool lock)
	ATTR_VISIBILITY_HIDDEN;
gdk_return BATmaterialize(BAT *b, BUN cap)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return BATsave_iter(BAT *bd, BATiter *bi, BUN size)
	ATTR_VISIBILITY_HIDDEN;
void BATsetdims(BAT *b, uint16_t width)
	ATTR_VISIBILITY_HIDDEN;
void BBPcacheit(BAT *bn, bool lock)
	ATTR_VISIBILITY_HIDDEN;
gdk_return BBPchkfarms(void)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
void BBPclear(bat bid)
	ATTR_VISIBILITY_HIDDEN;
void BBPdump(void)		/* never called: for debugging only */
	ATTR_COLD;
void BBPexit(void)
	ATTR_VISIBILITY_HIDDEN;
gdk_return BBPinit(bool allow_hge_upgrade, bool no_manager)
	ATTR_VISIBILITY_HIDDEN;
bat BBPallocbat(int tt)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
void BBPprintinfo(FILE *outf)
	ATTR_NONNULL(1)
	ATTR_VISIBILITY_HIDDEN;
gdk_return BBPsync(int cnt, const bat *restrict subcommit, const BUN *restrict sizes, lng logno)
	ATTR_VISIBILITY_HIDDEN
	ATTR_ACCESS(read_only, 2, 1)
	ATTR_ACCESS(read_only, 3, 1);
BUN binsearch(const oid *restrict indir, int type, const void *restrict vals, const char * restrict vars, int width, BUN lo, BUN hi, const void *restrict v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN binsearch_bte(const oid *restrict indir, const bte *restrict vals, BUN lo, BUN hi, bte v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN binsearch_sht(const oid *restrict indir, const sht *restrict vals, BUN lo, BUN hi, sht v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN binsearch_int(const oid *restrict indir, const int *restrict vals, BUN lo, BUN hi, int v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN binsearch_lng(const oid *restrict indir, const lng *restrict vals, BUN lo, BUN hi, lng v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
#ifdef HAVE_HGE
BUN binsearch_hge(const oid *restrict indir, const hge *restrict vals, BUN lo, BUN hi, hge v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
#endif
BUN binsearch_flt(const oid *restrict indir, const flt *restrict vals, BUN lo, BUN hi, flt v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN binsearch_dbl(const oid *restrict indir, const dbl *restrict vals, BUN lo, BUN hi, dbl v, int ordering, int last)
	ATTR_VISIBILITY_HIDDEN;
BUN countStrings(const Heap *h)
	ATTR_VISIBILITY_HIDDEN;
Heap *createOIDXheap(BAT *b, bool stable)
	ATTR_VISIBILITY_HIDDEN;
void doHASHdestroy(BAT *b, Hash *hs)
	ATTR_VISIBILITY_HIDDEN;
void gdk_bbp_reset(void)
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKextend(const char *fn, size_t size)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKextendf(int fd, size_t size, const char *fn)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
int GDKfdlocate(int farmid, const char *nme, const char *mode, const char *ext)
	ATTR_VISIBILITY_HIDDEN;
FILE *GDKfilelocate(int farmid, const char *nme, const char *mode, const char *ext)
	ATTR_VISIBILITY_HIDDEN
	ATTR_MALLOC
	ATTR_MALLOC_ARGS(fclose, 1);
FILE *GDKfileopen(int farmid, const char *dir, const char *name, const char *extension, const char *mode)
	ATTR_VISIBILITY_HIDDEN
	ATTR_MALLOC
	ATTR_MALLOC_ARGS(fclose, 1);
char *GDKload(int farmid, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
	ATTR_VISIBILITY_HIDDEN
	ATTR_ACCESS(read_write, 5);
gdk_return GDKmove(int farmid, const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2, bool report)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
void *GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKremovedir(int farmid, const char *nme)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKsave(int farmid, const char *nme, const char *ext, void *buf, size_t size, storage_t mode, bool dosync)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKrsort(void *restrict h, void *restrict t, size_t n, size_t hs, size_t ts, bool reverse, bool bigendian, bool isfloat)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKssort_rev(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKssort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKtracer_init(const char *dbname, const char *dbtrace)
	ATTR_VISIBILITY_HIDDEN;
gdk_return GDKunlink(int farmid, const char *dir, const char *nme, const char *extension)
	ATTR_VISIBILITY_HIDDEN;
lng getBBPlogno(void)
	ATTR_VISIBILITY_HIDDEN;
BUN HASHappend(BAT *b, BUN i, const void *v)
	ATTR_VISIBILITY_HIDDEN;
void HASHappend_locked(BATiter *bi, BUN i, const void *v)
	ATTR_VISIBILITY_HIDDEN;
void HASHappend_locked_hashval(BATiter *bi, BUN i, const void *v, BUN hsh)
	ATTR_VISIBILITY_HIDDEN;
void HASHfree(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
BUN HASHdelete(BATiter *bi, BUN p, const void *v)
	ATTR_VISIBILITY_HIDDEN;
void HASHdelete_locked(BATiter *bi, BUN p, const void *v)
	ATTR_VISIBILITY_HIDDEN;
BUN HASHinsert(BATiter *bi, BUN p, const void *v)
	ATTR_VISIBILITY_HIDDEN;
void HASHinsert_locked(BATiter *bi, BUN p, const void *v)
	ATTR_VISIBILITY_HIDDEN;
ATTR_CONST
static inline BUN
HASHmask(BUN cnt)
{
	cnt = cnt * 8 / 7;
	if (cnt < BATTINY)
		cnt = BATTINY;
	return cnt;
}
gdk_return HASHnew(Hash *h, int tpe, BUN size, BUN mask, BUN count, bool bcktonly)
	ATTR_VISIBILITY_HIDDEN;
gdk_return HEAPcopy(Heap *dst, Heap *src, size_t offset)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
void HEAPfree(Heap *h, bool remove)
	ATTR_VISIBILITY_HIDDEN;
gdk_return HEAPgrow(Heap **old, size_t size)
	ATTR_VISIBILITY_HIDDEN;
gdk_return HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
void HEAP_recover(Heap *, const var_t *, BUN)
	ATTR_VISIBILITY_HIDDEN;
gdk_return HEAPsave(Heap *h, const char *nme, const char *ext, bool dosync, BUN free, MT_Lock *lock)
	ATTR_WARN_UNUSED_RESULT
	ATTR_VISIBILITY_HIDDEN;
double joincost(BAT *r, bat lustr, BUN lcount, struct canditer *rci, bool *hash, bool *phash, bool *cand)
	ATTR_VISIBILITY_HIDDEN;
void STRMPincref(Strimps *strimps)
	ATTR_VISIBILITY_HIDDEN;
void STRMPdecref(Strimps *strimps, bool remove)
	ATTR_VISIBILITY_HIDDEN;
void STRMPfree(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void MT_init_posix(void)
	ATTR_VISIBILITY_HIDDEN;
void *MT_mmap(const char *path, int mode, size_t len)
	ATTR_VISIBILITY_HIDDEN;
void *MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	ATTR_VISIBILITY_HIDDEN;
int MT_msync(void *p, size_t len)
	ATTR_VISIBILITY_HIDDEN;
int MT_munmap(void *p, size_t len)
	ATTR_VISIBILITY_HIDDEN;
ssize_t numFromStr(allocator *ma, const char *src, size_t *len, void **dst,
		   int tp, uint8_t scale, uint8_t precision, bool external)
	ATTR_VISIBILITY_HIDDEN;
ssize_t numToStr(allocator *ma, char **dst, size_t *len, const void *src,
		 int tp, uint8_t scale, bool external)
	ATTR_VISIBILITY_HIDDEN;
void OIDXfree(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
#ifdef GDKLIBRARY_USTR
var_t oldstrnilLocate(Heap *h)
	ATTR_VISIBILITY_HIDDEN;
#endif
void persistOIDX(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void PROPdestroy(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void PROPdestroy_nolock(BAT *b)
	ATTR_VISIBILITY_HIDDEN;
void settailname(Heap *restrict tail, const char *restrict physnme, int tt, int width)
	ATTR_VISIBILITY_HIDDEN;
void strCleanHash(Heap *hp, bool rebuild)
	ATTR_VISIBILITY_HIDDEN;
gdk_return strHeap(Heap *d, size_t cap)
	ATTR_VISIBILITY_HIDDEN;
var_t strLocate(Heap *h, const char *v)
	ATTR_VISIBILITY_HIDDEN;
var_t strPut(BAT *b, var_t *dst, const void *v)
	ATTR_VISIBILITY_HIDDEN;
char *strRead(allocator *, str a, size_t *dstlen, stream *s, size_t cnt)
	ATTR_VISIBILITY_HIDDEN;
ssize_t strToStr(allocator *, char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
	ATTR_VISIBILITY_HIDDEN;
gdk_return strWrite(const char *a, stream *s, size_t cnt)
	ATTR_VISIBILITY_HIDDEN;
gdk_return TMcommit(void)
	ATTR_VISIBILITY_HIDDEN;
BAT *virtualize(BAT *bn)
	ATTR_VISIBILITY_HIDDEN;

/* calculate the integer 2 logarithm (i.e. position of highest set
 * bit) of the argument (with a slight twist: 0 gives 0, 1 gives 1,
 * 0x8 to 0xF give 4, etc.) */
ATTR_CONST
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
#define ALGOBATFMT	"%s#" BUNFMT "@" OIDFMT "[%s%s]%s%s%s%s%s%s%s%s%s%s%s"
#define ALGOBATPAR(b)							\
	BATgetId(b),							\
	BATcount(b),							\
	b->hseqbase,							\
	ATOMname(b->ttype),						\
	b->ttype==TYPE_str?b->twidth==1?"1":b->twidth==2?"2":b->twidth==4?"4":"8":"", \
	b->ttype == TYPE_str && b->ustr ? "U" : "",			\
	!b->batTransient ? "P" : b->theap && b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T", \
	BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && b->tvheap && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "", \
	b->tsorted ? "S" : b->tnosorted ? "!s" : "",			\
	b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "",		\
	b->tkey ? "K" : b->tnokey[1] ? "!k" : "",			\
	b->tvkey ? "k" : "",						\
	b->tnonil ? "N" : "",						\
	b->thash ? "H" : "",						\
	b->torderidx ? "O" : "",					\
	b->tstrimps ? "I" : b->theap && b->theap->parentid && BBP_desc(b->theap->parentid) && BBP_desc(b->theap->parentid)->tstrimps ? "(I)" : ""
/* use ALGOOPTBAT* when BAT is optional (can be NULL) */
#define ALGOOPTBATFMT	"%s%s" BUNFMT "%s" OIDFMT "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s"
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
	b && b->ttype == TYPE_str && b->ustr ? "U" : "",		\
	b ? !b->batTransient ? "P" : b->theap && b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T" : "", \
	b ? BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && b->tvheap && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "" : "", \
	b ? b->tsorted ? "S" : b->tnosorted ? "!s" : "" : "",		\
	b ? b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "" : "",	\
	b ? b->tkey ? "K" : b->tnokey[1] ? "!k" : "" : "",		\
	b && b->tvkey ? "k" : "",					       \
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
} BBPfarms[MAXFARMS] ATTR_VISIBILITY_HIDDEN;

extern batlock_t GDKbatLock[BBP_BATMASK + 1] ATTR_VISIBILITY_HIDDEN;
extern size_t GDK_mmap_minsize_persistent ATTR_VISIBILITY_HIDDEN; /* size after which we use memory mapped files for persistent heaps */
extern size_t GDK_mmap_minsize_transient ATTR_VISIBILITY_HIDDEN; /* size after which we use memory mapped files for transient heaps */
extern size_t GDK_mmap_pagesize ATTR_VISIBILITY_HIDDEN; /* mmap granularity */

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
extern BUN gdk_unique_estimate_keep_fraction ATTR_VISIBILITY_HIDDEN; /* should become a define once */
/* if the number of unique values is less than 1 in this number, we
 * destroy the hash rather than update it in HASH{append,insert,delete} */
#define HASH_DESTROY_UNIQUES_FRACTION		1000
extern BUN hash_destroy_uniques_fraction ATTR_VISIBILITY_HIDDEN;     /* likewise */
/* if the estimated number of unique values is less than 1 in this
 * number, don't build a hash table to do a hashselect */
#define NO_HASH_SELECT_FRACTION			1000
extern dbl no_hash_select_fraction ATTR_VISIBILITY_HIDDEN;           /* same here */
/* if the hash chain is longer than this number, we delete the hash
 * rather than maintaining it in HASHdelete */
#define HASH_DESTROY_CHAIN_LENGTH		1000
extern BUN hash_destroy_chain_length ATTR_VISIBILITY_HIDDEN;

extern void (*GDKtriggerusr1)(void);

#if !defined(NDEBUG) && !defined(__COVERITY__) && !defined(_CLANGD)
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

#endif	/* _GDK_PRIVATE_H_ */
