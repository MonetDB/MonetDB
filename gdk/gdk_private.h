/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

/* persist hash heaps for persistent BATs */
#define PERSISTENTHASH 1

/* persist order index heaps for persistent BATs */
#define PERSISTENTIDX 1

#include "gdk_system_private.h"

enum heaptype {
	offheap,
	varheap,
	hashheap,
	imprintsheap,
	orderidxheap
};

gdk_return ATOMheap(int id, Heap *hp, size_t cap)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
bool ATOMisdescendant(int id, int parentid)
	__attribute__((__visibility__("hidden")));
int ATOMunknown_find(const char *nme)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
str ATOMunknown_name(int a)
	__attribute__((__visibility__("hidden")));
void ATOMunknown_clean(void)
	__attribute__((__visibility__("hidden")));
gdk_return BATappend2(BAT *b, BAT *n, BAT *s, bool force, bool mayshare)
	__attribute__((__visibility__("hidden")));
bool BATcheckhash(BAT *b)
	__attribute__((__visibility__("hidden")));
bool BATcheckimprints(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return BATcheckmodes(BAT *b, bool persistent)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
BAT *BATcreatedesc(oid hseq, int tt, bool heapnames, role_t role)
	__attribute__((__visibility__("hidden")));
void BATdelete(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATfree(BAT *b)
	__attribute__((__visibility__("hidden")));
ValPtr BATgetprop_nolock(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
ValPtr BATgetprop_try(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h, bool subsorted)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
Hash *BAThash_impl(BAT *restrict b, struct canditer *restrict ci, const char *restrict ext)
	__attribute__((__visibility__("hidden")));
gdk_return BAThashsave(BAT *b, bool dosync)
	__attribute__((__visibility__("hidden")));
gdk_return BAThashsave(BAT *b, bool dosync)
	__attribute__((__visibility__("hidden")));
void BATinit_idents(BAT *bn)
	__attribute__((__visibility__("hidden")));
bool BATiscand(BAT *b)
	__attribute__((__visibility__("hidden")));
BAT *BATload_intern(bat bid, bool lock)
	__attribute__((__visibility__("hidden")));
gdk_return BATmaterialize(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void BATrmprop(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
void BATrmprop_nolock(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
void BATsetdims(BAT *b)
	__attribute__((__visibility__("hidden")));
ValPtr BATsetprop(BAT *b, enum prop_t idx, int type, const void *v)
	__attribute__((__visibility__("hidden")));
ValPtr BATsetprop_nolock(BAT *b, enum prop_t idx, int type, const void *v)
	__attribute__((__visibility__("hidden")));
gdk_return BBPcacheit(BAT *bn, bool lock)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void BBPdump(void)		/* never called: for debugging only */
	__attribute__((__cold__));
void BBPexit(void)
	__attribute__((__visibility__("hidden")));
BAT *BBPgetdesc(bat i)
	__attribute__((__visibility__("hidden")));
gdk_return BBPinit(void)
	__attribute__((__visibility__("hidden")));
bat BBPinsert(BAT *bn)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
int BBPselectfarm(role_t role, int type, enum heaptype hptype)
	__attribute__((__visibility__("hidden")));
void BBPunshare(bat b)
	__attribute__((__visibility__("hidden")));
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
	__attribute__((__visibility__("hidden")));
FILE *GDKfileopen(int farmid, const char *dir, const char *name, const char *extension, const char *mode)
	__attribute__((__visibility__("hidden")));
char *GDKload(int farmid, const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
	__attribute__((__visibility__("hidden")));
void GDKlog(_In_z_ _Printf_format_string_ FILE * fl, const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)))
	__attribute__((__visibility__("hidden")));
gdk_return GDKmove(int farmid, const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2)
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
gdk_return GDKssort_rev(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return GDKssort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void GDKtracer_init(const char *dbname, const char *dbtrace)
	__attribute__((__visibility__("hidden")));
gdk_return GDKunlink(int farmid, const char *dir, const char *nme, const char *extension)
	__attribute__((__visibility__("hidden")));
void HASHappend(BAT *b, BUN i, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHfree(BAT *b)
	__attribute__((__visibility__("hidden")));
bool HASHgonebad(BAT *b, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHdelete(BAT *b, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHinsert(BAT *b, BUN p, const void *v)
	__attribute__((__visibility__("hidden")));
BUN HASHmask(BUN cnt)
	__attribute__((__const__))
	__attribute__((__visibility__("hidden")));
gdk_return HASHnew(Hash *h, int tpe, BUN size, BUN mask, BUN count, bool bcktonly)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPalloc(Heap *h, size_t nitems, size_t itemsize, size_t itemsizemmap)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPcopy(Heap *dst, Heap *src)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPdelete(Heap *h, const char *o, const char *ext)
	__attribute__((__visibility__("hidden")));
void HEAPfree(Heap *h, bool remove)
	__attribute__((__visibility__("hidden")));
Heap *HEAPgrow(const Heap *old, size_t size)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPload(Heap *h, const char *nme, const char *ext, bool trunc)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void HEAP_recover(Heap *, const var_t *, BUN)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPsave(Heap *h, const char *nme, const char *ext, bool dosync)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPshrink(Heap *h, size_t size)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
int HEAPwarm(Heap *h)
	__attribute__((__visibility__("hidden")));
void IMPSfree(BAT *b)
	__attribute__((__visibility__("hidden")));
int IMPSgetbin(int tpe, bte bits, const char *restrict bins, const void *restrict v)
	__attribute__((__visibility__("hidden")));
#ifndef NDEBUG
void IMPSprint(BAT *b)		/* never called: for debugging only */
	__attribute__((__cold__));
#endif
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
gdk_return rangejoin(BAT *r1, BAT *r2, BAT *l, BAT *rl, BAT *rh, struct canditer *lci, struct canditer *rci, bool li, bool hi, bool anti, bool symmetric, BUN maxsize)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
const char *gettailname(const BAT *b)
	__attribute__((__visibility__("hidden")));
void settailname(Heap *restrict tail, const char *restrict physnme, int tt, int width)
	__attribute__((__visibility__("hidden")));
void strCleanHash(Heap *hp, bool rebuild)
	__attribute__((__visibility__("hidden")));
void strHeap(Heap *d, size_t cap)
	__attribute__((__visibility__("hidden")));
var_t strLocate(Heap *h, const char *v)
	__attribute__((__visibility__("hidden")));
var_t strPut(BAT *b, var_t *dst, const void *v)
	__attribute__((__visibility__("hidden")));
str strRead(str a, size_t *dstlen, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
ssize_t strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
	__attribute__((__visibility__("hidden")));
gdk_return strWrite(const char *a, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
gdk_return unshare_varsized_heap(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void VIEWdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return VIEWreset(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
BAT *virtualize(BAT *bn)
	__attribute__((__visibility__("hidden")));

/* calculate the integer 2 logarithm (i.e. position of highest set
 * bit) of the argument (with a slight twist: 0 gives 0, 1 gives 1,
 * 0x8 to 0xF give 4, etc.) */
static inline unsigned
ilog2(BUN x)
{
	if (x == 0)
		return 0;
#if defined(__GNUC__)
#if SIZEOF_BUN == 8
	return (unsigned) (64 - __builtin_clzll((unsigned long long) x));
#else
	return (unsigned) (32 - __builtin_clz((unsigned) x));
#endif
#elif defined(_MSC_VER)
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
}

/* some macros to help print info about BATs when using ALGODEBUG */
#define ALGOBATFMT	"%s#" BUNFMT "@" OIDFMT "[%s]%s%s%s%s%s%s%s%s%s"
#define ALGOBATPAR(b)							\
	BATgetId(b),							\
	BATcount(b),							\
	b->hseqbase,							\
	ATOMname(b->ttype),						\
	!b->batTransient ? "P" : b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T", \
	BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "", \
	b->tsorted ? "S" : b->tnosorted ? "!s" : "",			\
	b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "",		\
	b->tkey ? "K" : b->tnokey[1] ? "!k" : "",			\
	b->tnonil ? "N" : "",						\
	b->thash ? "H" : "",						\
	b->torderidx ? "O" : "",					\
	b->timprints ? "I" : b->theap->parentid && BBP_cache(b->theap->parentid)->timprints ? "(I)" : ""
/* use ALGOOPTBAT* when BAT is optional (can be NULL) */
#define ALGOOPTBATFMT	"%s%s" BUNFMT "%s" OIDFMT "%s%s%s%s%s%s%s%s%s%s%s%s"
#define ALGOOPTBATPAR(b)						\
	b ? BATgetId(b) : "",						\
	b ? "#" : "",							\
	b ? BATcount(b) : 0,						\
	b ? "@" : "",							\
	b ? b->hseqbase : 0,						\
	b ? "[" : "",							\
	b ? ATOMname(b->ttype) : "",					\
	b ? "]" : "",							\
	b ? !b->batTransient ? "P" : b->theap && b->theap->parentid != b->batCacheid ? "V" : b->tvheap && b->tvheap->parentid != b->batCacheid ? "v" : "T" : "",	\
	b ? BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" : ATOMstorage(b->ttype) == TYPE_str && GDK_ELIMDOUBLES(b->tvheap) ? "E" : "" : "", \
	b ? b->tsorted ? "S" : b->tnosorted ? "!s" : "" : "",		\
	b ? b->trevsorted ? "R" : b->tnorevsorted ? "!r" : "" : "",	\
	b ? b->tkey ? "K" : b->tnokey[1] ? "!k" : "" : "",		\
	b && b->tnonil ? "N" : "",					\
	b && b->thash ? "H" : "",					\
	b && b->torderidx ? "O" : "",					\
	b ? b->timprints ? "I" : b->theap && b->theap->parentid && BBP_cache(b->theap->parentid) && BBP_cache(b->theap->parentid)->timprints ? "(I)" : "" : ""

#define BBP_BATMASK	(128 * SIZEOF_SIZE_T - 1)
#define BBP_THREADMASK	63

struct PROPrec {
	enum prop_t id;
	ValRecord v;
	struct PROPrec *next;	/* simple chain of properties */
};

struct Imprints {
	bte bits;		/* how many bits in imprints */
	Heap imprints;
	void *bins;		/* pointer into imprints heap (bins borders)  */
	BUN *stats;		/* pointer into imprints heap (stats per bin) */
	void *imps;		/* pointer into imprints heap (bit vectors)   */
	void *dict;		/* pointer into imprints heap (dictionary)    */
	BUN impcnt;		/* counter for imprints                       */
	BUN dictcnt;		/* counter for cache dictionary               */
};

typedef struct {
	MT_Lock swap;
} batlock_t;

typedef struct {
	MT_Lock cache;
	MT_Lock trim;
	bat free;
} bbplock_t;

typedef char long_str[IDLENGTH];	/* standard GDK static string */

#define MAXFARMS       32

extern struct BBPfarm_t {
	uint32_t roles;		/* bitmask of allowed roles */
	const char *dirname;	/* farm directory */
	FILE *lock_file;
} BBPfarms[MAXFARMS];

extern batlock_t GDKbatLock[BBP_BATMASK + 1];
extern bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
extern size_t GDK_mmap_minsize_persistent; /* size after which we use memory mapped files for persistent heaps */
extern size_t GDK_mmap_minsize_transient; /* size after which we use memory mapped files for transient heaps */
extern size_t GDK_mmap_pagesize; /* mmap granularity */
extern MT_Lock GDKnameLock;
extern MT_Lock GDKthreadLock;
extern MT_Lock GDKtmLock;

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
#if SIZEOF_SIZE_T == 8
#define threadmask(y)	((int) (mix_lng(y) & BBP_THREADMASK))
#else
#define threadmask(y)	((int) (mix_int(y) & BBP_THREADMASK))
#endif
#define GDKtrimLock(y)	GDKbbpLock[y].trim
#define GDKcacheLock(y)	GDKbbpLock[y].cache
#define BBP_free(y)	GDKbbpLock[y].free

/* extra space in front of strings in string heaps when hashash is set
 * if at least (2*SIZEOF_BUN), also store length (heaps are then
 * incompatible) */
#define EXTRALEN ((SIZEOF_BUN + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1))

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
