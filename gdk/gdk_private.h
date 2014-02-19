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

/*
 * The different parts of which a BAT consists are physically stored
 * next to each other in the BATstore type.
 */
struct BATstore {
	BAT B;			/* storage for BAT descriptor */
	BAT BM;			/* mirror (reverse) BAT */
	COLrec H;		/* storage for head column */
	COLrec T;		/* storage for tail column */
	BATrec S;		/* the BAT properties */
};

int ALIGNcommit(BAT *b);
int ALIGNundo(BAT *b);
int ATOMheap(int id, Heap *hp, size_t cap);
int ATOMisdescendant(int id, int parentid);
int ATOMunknown_add(const char *nme);
int ATOMunknown_del(int a);
int ATOMunknown_find(const char *nme);
str ATOMunknown_name(int a);
int BATcheckmodes(BAT *b, int persistent);
BAT *BATclone(BAT *b, BUN capacity);
BATstore *BATcreatedesc(int ht, int tt, int heapnames);
void BATdestroy(BATstore *bs);
int BATfree(BAT *b);
gdk_return BATgroup_internal(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *g, BAT *e, BAT *h, int subsorted);
BUN BATguess(BAT *b);
void BATinit_idents(BAT *bn);
BAT *BATload_intern(bat bid, int lock);
BAT *BATmaterializet(BAT *b);
void BATpropagate(BAT *dst, BAT *src, int idx);
str BATrename(BAT *b, const char *nme);
void BATsetdims(BAT *b);
size_t BATvmsize(BAT *b, int dirty);
void BBPcacheit(BATstore *bs, int lock);
void BBPdump(void);		/* never called: for debugging only */
void BBPexit(void);
BATstore *BBPgetdesc(bat i);
void BBPinit(void);
bat BBPinsert(BATstore *bs);
void BBPtrim(size_t delta);
void BBPunshare(bat b);
BUN BUNlocate(BAT *b, const void *left, const void *right);
void GDKclrerr(void);
int GDKextend(const char *fn, size_t size);
int GDKextendf(int fd, size_t size);
int GDKfdlocate(const char *nme, const char *mode, const char *ext);
FILE *GDKfilelocate(const char *nme, const char *mode, const char *ext);
char *GDKload(const char *nme, const char *ext, size_t size, size_t *maxsize, storage_t mode);
void GDKlog(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
void *GDKmallocmax(size_t size, size_t *maxsize, int emergency);
int GDKmove(const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2);
int GDKmunmap(void *addr, size_t len);
void *GDKreallocmax(void *pold, size_t size, size_t *maxsize, int emergency);
int GDKremovedir(const char *nme);
int GDKsave(const char *nme, const char *ext, void *buf, size_t size, storage_t mode);
int GDKssort_rev(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe);
int GDKssort(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe);
int GDKunlink(const char *dir, const char *nme, const char *extension);
int HASHgonebad(BAT *b, const void *v);
BUN HASHmask(BUN cnt);
Hash *HASHnew(Heap *hp, int tpe, BUN size, BUN mask);
void HASHremove(BAT *b);
int HEAPalloc(Heap *h, size_t nitems, size_t itemsize);
int HEAPcopy(Heap *dst, Heap *src);
int HEAPdelete(Heap *h, const char *o, const char *ext);
int HEAPload(Heap *h, const char *nme, const char *ext, int trunc);
int HEAPsave(Heap *h, const char *nme, const char *ext);
int HEAPshrink(Heap *h, size_t size);
int HEAPwarm(Heap *h);
oid MAXoid(BAT *i);
void MT_global_exit(int status)
	__attribute__((__noreturn__));
void MT_init_posix(void);
void *MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size);
int MT_msync(void *p, size_t len, int mode);
int OIDdirty(void);
int OIDinit(void);
oid OIDread(str buf);
int OIDwrite(stream *fp);
void strCleanHash(Heap *hp, int rebuild);
int strCmpNoNil(const unsigned char *l, const unsigned char *r);
int strElimDoubles(Heap *h);
var_t strLocate(Heap *h, const char *v);
void VIEWdestroy(BAT *b);
BAT *VIEWreset(BAT *b);
int IMPSgetbin(int tpe, bte bits, char *bins, const void *v);
#ifndef NDEBUG
void IMPSprint(BAT *b);
#endif

#define BBP_BATMASK	511
#define BBP_THREADMASK	63

struct PROPrec {
	int id;
	ValRecord v;
	struct PROPrec *next;	/* simple chain of properties */
};

struct Imprints {
	bte bits;        /* how many bits in imprints */
	Heap *bins;      /* ranges of bins */
	Heap *imps;      /* heap of imprints */
	BUN impcnt;      /* counter for imprints*/
	Heap *dict;      /* cache dictionary for compressing imprints */
	BUN dictcnt;     /* counter for cache dictionary */
};

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

typedef char long_str[IDLENGTH];	/* standard GDK static string */

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

#ifndef NDEBUG
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
