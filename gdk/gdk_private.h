/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include "gdk_tracer.h"

enum heaptype {
	offheap,
	varheap,
	hashheap,
	imprintsheap,
	orderidxheap
};

#ifdef GDKLIBRARY_OLDDATE
int cvtdate(int n)
	__attribute__((__visibility__("hidden")));
#endif

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
PROPrec *BATgetprop(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
PROPrec * BATgetprop_nolock(BAT *b, enum prop_t idx)
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
BAT *BATload_intern(bat bid, bool lock)
	__attribute__((__visibility__("hidden")));
gdk_return BATmaterialize(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void BATrmprop(BAT *b, enum prop_t idx)
	__attribute__((__visibility__("hidden")));
void BATsetdims(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATsetprop(BAT *b, enum prop_t idx, int type, const void *v)
	__attribute__((__visibility__("hidden")));
void BATsetprop_nolock(BAT *b, enum prop_t idx, int type, const void *v)
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
gdk_return BUNreplace(BAT *b, oid left, const void *right, bool force)
	__attribute__((__warn_unused_result__))
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
gdk_return GDKmunmap(void *addr, size_t len)
	__attribute__((__warn_unused_result__))
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
gdk_return GDKunlink(int farmid, const char *dir, const char *nme, const char *extension)
	__attribute__((__visibility__("hidden")));
#ifdef NATIVE_WIN32
void GDKwinerror(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)))
	__attribute__((__visibility__("hidden")));
#endif
void HASHfree(BAT *b)
	__attribute__((__visibility__("hidden")));
bool HASHgonebad(BAT *b, const void *v)
	__attribute__((__visibility__("hidden")));
void HASHins(BAT *b, BUN i, const void *v)
	__attribute__((__visibility__("hidden")));
BUN HASHmask(BUN cnt)
	__attribute__((__visibility__("hidden")));
gdk_return HASHnew(Hash *h, int tpe, BUN size, BUN mask, BUN count, bool bcktonly)
	__attribute__((__visibility__("hidden")));
gdk_return HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPcopy(Heap *dst, Heap *src)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
gdk_return HEAPdelete(Heap *h, const char *o, const char *ext)
	__attribute__((__visibility__("hidden")));
void HEAPfree(Heap *h, bool remove)
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
void *MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	__attribute__((__visibility__("hidden")));
int MT_msync(void *p, size_t len)
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
void strCleanHash(Heap *hp, bool rebuild)
	__attribute__((__visibility__("hidden")));
void strHeap(Heap *d, size_t cap)
	__attribute__((__visibility__("hidden")));
var_t strLocate(Heap *h, const char *v)
	__attribute__((__visibility__("hidden")));
var_t strPut(Heap *h, var_t *dst, const char *v)
	__attribute__((__visibility__("hidden")));
str strRead(str a, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
ssize_t strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
	__attribute__((__visibility__("hidden")));
gdk_return strWrite(const char *a, stream *s, size_t cnt)
	__attribute__((__visibility__("hidden")));
gdk_return unshare_string_heap(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
void VIEWdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return VIEWreset(BAT *b)
	__attribute__((__warn_unused_result__))
	__attribute__((__visibility__("hidden")));
BAT *virtualize(BAT *bn)
	__attribute__((__visibility__("hidden")));

/* some macros to help print info about BATs when using ALGODEBUG */
#define ALGOBATFMT	"%s#" BUNFMT "@" OIDFMT "[%s]%s%s%s%s%s%s%s%s%s"
#define ALGOBATPAR(b)	BATgetId(b),					\
			BATcount(b),					\
			b->hseqbase,					\
			ATOMname(b->ttype),				\
			!b->batTransient ? "P" : isVIEW(b) ? "V" : "T", \
			BATtdense(b) ? "D" : b->ttype == TYPE_void && b->tvheap ? "X" :"", \
			b->tsorted ? "S" : "",				\
			b->trevsorted ? "R" : "",			\
			b->tkey ? "K" : "",				\
			b->tnonil ? "N" : "",				\
			b->thash ? "H" : "",				\
			b->torderidx ? "O" : "",			\
			b->timprints ? "I" : b->theap.parentid && BBP_cache(b->theap.parentid)->timprints ? "(I)" : ""
/* use ALGOOPTBAT* when BAT is optional (can be NULL) */
#define ALGOOPTBATFMT	"%s%s" BUNFMT "%s" OIDFMT "%s%s%s%s%s%s%s%s%s%s%s%s"
#define ALGOOPTBATPAR(b)						\
			b ? BATgetId(b) : "",				\
			b ? "#" : "",					\
			b ? BATcount(b) : 0,				\
			b ? "@" : "",					\
			b ? b->hseqbase : 0,				\
			b ? "[" : "",					\
			b ? ATOMname(b->ttype) : "",			\
			b ? "]" : "",					\
			b ? !b->batTransient ? "P" : isVIEW(b) ? "V" : "T" : "", \
			b && BATtdense(b) ? "D" : b && b->ttype == TYPE_void && b->tvheap ? "X" :"", \
			b && b->tsorted ? "S" : "",			\
			b && b->trevsorted ? "R" : "",			\
			b && b->tkey ? "K" : "",			\
			b && b->tnonil ? "N" : "",			\
			b && b->thash ? "H" : "",			\
			b && b->torderidx ? "O" : "",			\
			b ? b->timprints ? "I" : b->theap.parentid && BBP_cache(b->theap.parentid)->timprints ? "(I)" : "" : ""

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
	unsigned int roles;	/* bitmask of allowed roles */
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

#define BATcheck(tst, msg, err)						\
	do {								\
		if ((tst) == NULL) {					\
			if (strchr((msg), ':'))				\
				GDKerror("%s.\n", (msg));		\
			else						\
				GDKerror("%s: BAT required.\n", (msg));	\
			return (err);					\
		}							\
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

#if !defined(NDEBUG) && !defined(STATIC_CODE_ANALYSIS)
/* see comment in gdk.h */
#ifdef __GNUC__
#define GDKmunmap(p, l)						\
	({	void *_ptr = (p);				\
		size_t _len = (l);				\
		gdk_return _res = GDKmunmap(_ptr, _len);	\
		TRC_DEBUG(ALLOC,				\
				"GDKmunmap(%p,%zu) -> %u\n",	\
				_ptr, _len, _res);		\
		_res;						\
	})
#define GDKmremap(p, m, oa, os, ns)					\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		void *_oa = (oa);					\
		size_t _os = (os);					\
		size_t *_ns = (ns);					\
		size_t _ons = *_ns;					\
		void *_res = GDKmremap(_path, _mode, _oa, _os, _ns);	\
			TRC_DEBUG(ALLOC,				\
				"GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p\n", \
				_path ? _path : "NULL", (unsigned) _mode, \
				_oa, _os, _ons, *_ns, _res);		\
		_res;							\
	 })
#else
static inline gdk_return
GDKmunmap_debug(void *ptr, size_t len)
{
	gdk_return res = GDKmunmap(ptr, len);
	TRC_DEBUG(ALLOC, "GDKmunmap(%p,%zu) -> %d\n",
			   	  ptr, len, (int) res);
	return res;
}
#define GDKmunmap(p, l)		GDKmunmap_debug((p), (l))
static inline void *
GDKmremap_debug(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	size_t orig_new_size = *new_size;
	void *res = GDKmremap(path, mode, old_address, old_size, new_size);
		TRC_DEBUG(ALLOC, "GDKmremap(%s,0x%x,%p,%zu,%zu > %zu) -> %p\n",
					  path ? path : "NULL", mode,
					  old_address, old_size, orig_new_size, *new_size, res);
	return res;
}
#define GDKmremap(p, m, oa, os, ns)	GDKmremap_debug(p, m, oa, os, ns)

#endif
#endif
