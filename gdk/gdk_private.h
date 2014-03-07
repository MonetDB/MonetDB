/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/* This file should not be included in any file outside of this directory */

#ifndef LIBGDK
#error this file should not be included outside its source directory
#endif

int ALIGNcommit(BAT *b)
	__attribute__((__visibility__("hidden")));
int ALIGNundo(BAT *b)
	__attribute__((__visibility__("hidden")));
int ATOMheap(int id, Heap *hp, size_t cap)
	__attribute__((__visibility__("hidden")));
int ATOMisdescendant(int id, int parentid)
	__attribute__((__visibility__("hidden")));
int ATOMunknown_add(const char *nme)
	__attribute__((__visibility__("hidden")));
int ATOMunknown_del(int a)
	__attribute__((__visibility__("hidden")));
int ATOMunknown_find(const char *nme)
	__attribute__((__visibility__("hidden")));
str ATOMunknown_name(int a)
	__attribute__((__visibility__("hidden")));
int BATcheckmodes(BAT *b, int persistent)
	__attribute__((__visibility__("hidden")));
BAT *BATclone(BAT *b, BUN capacity)
	__attribute__((__visibility__("hidden")));
BATstore *BATcreatedesc(int ht, int tt, int heapnames)
	__attribute__((__visibility__("hidden")));
void BATdestroy(BATstore *bs)
	__attribute__((__visibility__("hidden")));
int BATfree(BAT *b)
	__attribute__((__visibility__("hidden")));
gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *g, BAT *e, BAT *h, int subsorted)
	__attribute__((__visibility__("hidden")));
BUN BATguess(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATinit_idents(BAT *bn)
	__attribute__((__visibility__("hidden")));
BAT *BATload_intern(bat bid, int lock)
	__attribute__((__visibility__("hidden")));
BAT *BATmaterializet(BAT *b)
	__attribute__((__visibility__("hidden")));
void BATpropagate(BAT *dst, BAT *src, int idx)
	__attribute__((__visibility__("hidden")));
str BATrename(BAT *b, const char *nme)
	__attribute__((__visibility__("hidden")));
void BATsetdims(BAT *b)
	__attribute__((__visibility__("hidden")));
size_t BATvmsize(BAT *b, int dirty)
	__attribute__((__visibility__("hidden")));
void BBPcacheit(BATstore *bs, int lock)
	__attribute__((__visibility__("hidden")));
void BBPdump(void);		/* never called: for debugging only */
void BBPexit(void)
	__attribute__((__visibility__("hidden")));
void BBPinit(void)
	__attribute__((__visibility__("hidden")));
bat BBPinsert(BATstore *bs)
	__attribute__((__visibility__("hidden")));
void BBPtrim(size_t delta)
	__attribute__((__visibility__("hidden")));
void BBPunshare(bat b)
	__attribute__((__visibility__("hidden")));
void GDKclrerr(void)
	__attribute__((__visibility__("hidden")));
int GDKextend(const char *fn, size_t size)
	__attribute__((__visibility__("hidden")));
int GDKextendf(int fd, size_t size)
	__attribute__((__visibility__("hidden")));
int GDKfdlocate(const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")));
FILE *GDKfilelocate(const char *nme, const char *mode, const char *ext)
	__attribute__((__visibility__("hidden")));
char *GDKload(const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode)
	__attribute__((__visibility__("hidden")));
void GDKlog(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)))
	__attribute__((__visibility__("hidden")));
void *GDKmallocmax(size_t size, size_t *maxsize, int emergency)
	__attribute__((__visibility__("hidden")));
int GDKmove(const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2)
	__attribute__((__visibility__("hidden")));
int GDKmunmap(void *addr, size_t len)
	__attribute__((__visibility__("hidden")));
void *GDKreallocmax(void *pold, size_t size, size_t *maxsize, int emergency)
	__attribute__((__visibility__("hidden")));
int GDKremovedir(const char *nme)
	__attribute__((__visibility__("hidden")));
int GDKsave(const char *nme, const char *ext, void *buf, size_t size, storage_t mode)
	__attribute__((__visibility__("hidden")));
int GDKssort_rev(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe)
	__attribute__((__visibility__("hidden")));
int GDKssort(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe)
	__attribute__((__visibility__("hidden")));
int GDKunlink(const char *dir, const char *nme, const char *extension)
	__attribute__((__visibility__("hidden")));
int HASHgonebad(BAT *b, const void *v)
	__attribute__((__visibility__("hidden")));
BUN HASHmask(BUN cnt)
	__attribute__((__visibility__("hidden")));
Hash *HASHnew(Heap *hp, int tpe, BUN size, BUN mask)
	__attribute__((__visibility__("hidden")));
void HASHremove(BAT *b)
	__attribute__((__visibility__("hidden")));
int HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
	__attribute__((__visibility__("hidden")));
int HEAPdelete(Heap *h, const char *o, const char *ext)
	__attribute__((__visibility__("hidden")));
int HEAPload(Heap *h, const char *nme, const char *ext, int trunc)
	__attribute__((__visibility__("hidden")));
int HEAPsave(Heap *h, const char *nme, const char *ext)
	__attribute__((__visibility__("hidden")));
int HEAPshrink(Heap *h, size_t size)
	__attribute__((__visibility__("hidden")));
int HEAPwarm(Heap *h)
	__attribute__((__visibility__("hidden")));
oid MAXoid(BAT *i)
	__attribute__((__visibility__("hidden")));
void MT_global_exit(int status)
	__attribute__((__noreturn__))
	__attribute__((__visibility__("hidden")));
void MT_init_posix(void)
	__attribute__((__visibility__("hidden")));
void *MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
	__attribute__((__visibility__("hidden")));
int MT_msync(void *p, size_t len, int mode)
	__attribute__((__visibility__("hidden")));
int OIDdirty(void)
	__attribute__((__visibility__("hidden")));
int OIDinit(void)
	__attribute__((__visibility__("hidden")));
oid OIDread(str buf)
	__attribute__((__visibility__("hidden")));
int OIDwrite(stream *fp)
	__attribute__((__visibility__("hidden")));
void strCleanHash(Heap *hp, int rebuild)
	__attribute__((__visibility__("hidden")));
int strCmpNoNil(const unsigned char *l, const unsigned char *r)
	__attribute__((__visibility__("hidden")));
int strElimDoubles(Heap *h)
	__attribute__((__visibility__("hidden")));
var_t strLocate(Heap *h, const char *v)
	__attribute__((__visibility__("hidden")));
void VIEWdestroy(BAT *b)
	__attribute__((__visibility__("hidden")));
BAT *VIEWreset(BAT *b)
	__attribute__((__visibility__("hidden")));
int IMPSgetbin(int tpe, bte bits, char *bins, const void *v)
	__attribute__((__visibility__("hidden")));
#ifndef NDEBUG
void IMPSprint(BAT *b)
	__attribute__((__visibility__("hidden")));
#endif

#define BBP_BATMASK	511
#define BBP_THREADMASK	63

typedef struct {
	MT_Lock swap;
	MT_Lock hash;
	MT_Lock imprints;
} batlock_t;

typedef struct {
	MT_Lock alloc;
	MT_Lock trim;
	bat free;
} bbplock_t;

extern int BBP_dirty;	/* BBP table dirty? */
extern batlock_t GDKbatLock[BBP_BATMASK + 1];
extern bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
extern size_t GDK_mmap_minsize;	/* size after which we use memory mapped files */
extern size_t GDK_mmap_pagesize; /* mmap granularity */
extern MT_Lock GDKnameLock;
extern MT_Lock GDKthreadLock;
extern MT_Lock GDKtmLock;
extern MT_Lock MT_system_lock;

#define ATOMappendpriv(t, h) (ATOMstorage(t) != TYPE_str || GDK_ELIMDOUBLES(h))

#define BBPdirty(x)	(BBP_dirty=(x))

#define GDKswapLock(x)  GDKbatLock[(x)&BBP_BATMASK].swap
#define GDKhashLock(x)  GDKbatLock[(x)&BBP_BATMASK].hash
#define GDKimprintsLock(x)  GDKbatLock[(x)&BBP_BATMASK].imprints
#if SIZEOF_SIZE_T == 8
#define threadmask(y)	((int) ((mix_int((unsigned int) y) ^ mix_int((unsigned int) (y >> 32))) & BBP_THREADMASK))
#else
#define threadmask(y)	((int) (mix_int(y) & BBP_THREADMASK))
#endif
#define GDKtrimLock(y)	GDKbbpLock[y].trim
#define GDKcacheLock(y)	GDKbbpLock[y].alloc
#define BBP_free(y)	GDKbbpLock[y].free

#define SORTloop_TYPE(b, p, q, tl, th, TYPE)				\
	if (!BATtordered(b))						\
		GDKerror("SORTloop_" #TYPE ": BAT not sorted.\n");	\
	else for (p = simple_EQ(tl, &TYPE##_nil, TYPE) ? BUNfirst(b) : SORTfndfirst(b, tl), \
		  q = simple_EQ(th, &TYPE##_nil, TYPE) ? BUNfirst(b) : SORTfndlast(b, th); \
		  p < q;						\
		  p++)

#define SORTloop_bte(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, bte)
#define SORTloop_sht(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, sht)
#define SORTloop_int(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, int)
#define SORTloop_lng(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, lng)
#define SORTloop_flt(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, flt)
#define SORTloop_dbl(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, dbl)
#define SORTloop_oid(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, oid)
#define SORTloop_wrd(b, p, q, tl, th)	SORTloop_TYPE(b, p, q, tl, th, wrd)

#define SORTloop_loc(b,p,q,tl,th)					\
	if (!BATtordered(b))						\
		GDKerror("SORTloop_loc: BAT not sorted.\n");		\
	else for (p = atom_EQ(tl, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndfirst(b, tl), \
			  q = atom_EQ(th, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndlast(b, th); \
		  p < q;						\
		  p++)

#define SORTloop_var(b,p,q,tl,th) SORTloop_loc(b,p,q,tl,th)

#define SORTloop_bit(b,p,q,tl,th) SORTloop_bte(b,p,q,tl,th)

#if !defined(NDEBUG) && !defined(__clang_analyzer__)
/* see comment in gdk.h */
#ifdef __GNUC__
#define GDKmallocmax(s,ps,e)						\
	({								\
		size_t _size = (s);					\
		size_t *_psize  = (ps);					\
		void *_res = GDKmallocmax(_size,_psize,e);		\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmallocmax(" SZFMT ",(" SZFMT ")) -> " \
				PTRFMT " %s[%s:%d]\n",			\
				_size, *_psize, PTRFMTCAST _res,	\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#define GDKmunmap(p, l)							\
	({	void *_ptr = (p);					\
		size_t _len = (l);					\
		int _res = GDKmunmap(_ptr, _len);			\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmunmap(" PTRFMT "," SZFMT ") -> %d" \
				" %s[%s:%d]\n",				\
				PTRFMTCAST _ptr, _len, _res,		\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define GDKreallocmax(p,s,ps,e)						\
	({								\
		void *_ptr = (p);					\
		size_t _size = (s);					\
		size_t *_psize  = (ps);					\
		void *_res = GDKreallocmax(_ptr,_size,_psize,e);	\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKreallocmax(" PTRFMT "," SZFMT \
				",(" SZFMT ")) -> " PTRFMT		\
				" %s[%s:%d]\n", PTRFMTCAST _ptr,	\
				_size, *_psize, PTRFMTCAST _res,	\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#else
static inline void *
GDKmallocmax_debug(size_t size, size_t *psize, int emergency,
		   const char *filename, int lineno)
{
	void *res = GDKmallocmax(size, psize, emergency);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmallocmax(" SZFMT ",(" SZFMT ")) -> "
			   PTRFMT " [%s:%d]\n",
			   size, *psize, PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKmallocmax(s, ps, e)	GDKmallocmax_debug((s), (ps), (e), __FILE__, __LINE__)
static inline int
GDKmunmap_debug(void *ptr, size_t len, const char *filename, int lineno)
{
	int res = GDKmunmap(ptr, len);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmunmap(" PTRFMT "," SZFMT ") -> %d [%s:%d]\n",
			   PTRFMTCAST ptr, len, res, filename, lineno);
	return res;
}
#define GDKmunmap(p, l)		GDKmunmap_debug((p), (l), __FILE__, __LINE__)
static inline void *
GDKreallocmax_debug(void *ptr, size_t size, size_t *psize, int emergency,
		    const char *filename, int lineno)
{
	void *res = GDKreallocmax(ptr, size, psize, emergency);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKreallocmax(" PTRFMT "," SZFMT
			   ",(" SZFMT ")) -> " PTRFMT " [%s:%d]\n",
			   PTRFMTCAST ptr, size, *psize, PTRFMTCAST res,
			   filename, lineno);
	return res;
}
#define GDKreallocmax(p, s, ps, e)	GDKreallocmax_debug((p), (s), (ps), (e), __FILE__, __LINE__)
#endif
#endif
