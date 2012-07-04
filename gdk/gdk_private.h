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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/* This file should not be included in any file outside of this directory */

int ALIGNcommit(BAT *b);
int ALIGNundo(BAT *b);
int ATOMheap(int id, Heap *hp, size_t cap);
int ATOMisdescendant(int id, int parentid);
int ATOMunknown_add(str nme);
int ATOMunknown_del(int a);
int ATOMunknown_find(str nme);
str ATOMunknown_name(int a);
BAT *BAT_select_(BAT *b, const void *tl, const void *th, bit li, bit hi, bit tail, bit anti, bit preserve_order);
BUN BATbuncount(BAT *b);
int BATcheckmodes(BAT *b, int persistent);
BAT *BATclone(BAT *b, BUN capacity);
BAT *BATcol_name(BAT *b, const char *tnme);
BATstore *BATcreatedesc(int ht, int tt, int heapnames);
void BATdestroy(BATstore *bs);
int BATfree(BAT *b);
BUN BATguess(BAT *b);
void BATinit_idents(BAT *bn);
BAT *BATleftmergejoin(BAT *l, BAT *r, BUN estimate);
BAT *BATleftthetajoin(BAT *l, BAT *r, int mode, BUN estimate);
BAT *BATload_intern(bat bid, int lock);
BAT *BATmaterializet(BAT *b);
int BATmultijoin(int argc, BAT *argv[], RowFcn tuple_fcn, ptr tuple_data, ColFcn value_fcn[], ptr value_data[], int orderspec);
BAT *BATnlthetajoin(BAT *l, BAT *r, int mode, BUN estimate);
void BATpropagate(BAT *dst, BAT *src, int idx);
str BATrename(BAT *b, const char *nme);
void BATsetdims(BAT *b);
BAT *BATsorder(BAT *b);
BAT *BATsorder_rev(BAT *b);
size_t BATvmsize(BAT *b, int dirty);
void BBPcacheit(BATstore *bs, int lock);
void BBPdumpcache(void);	/* never called: for debugging only */
void BBPdump(void);		/* never called: for debugging only */
void BBPexit(void);
void BBPinit(void);
int BBPrecover(void);
void BBPreleaselref(bat i);
void BBPtrim(size_t delta);
void BBPunshare(bat b);
void GDKclrerr(void);
FILE *GDKfilelocate(const char *nme, const char *mode, const char *ext);
char *GDKload(const char *nme, const char *ext, size_t size, size_t chunk, storage_t mode);
void GDKlockHome(void);
void GDKlog(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
void *GDKmallocmax(size_t size, size_t *maxsize, int emergency);
int GDKmove(const char *dir1, const char *nme1, const char *ext1, const char *dir2, const char *nme2, const char *ext2);
int GDKmunmap(void *addr, size_t len);
void *GDKreallocmax(void *pold, size_t size, size_t *maxsize, int emergency);
int GDKremovedir(const char *nme);
int GDKsave(const char *nme, const char *ext, void *buf, size_t size, storage_t mode);
int GDKssort_rev(void *h, void *t, void *base, size_t n, int hs, int ts, int tpe);
int GDKssort(void *h, void *t, void *base, size_t n, int hs, int ts, int tpe);
int GDKunlink(const char *dir, const char *nme, const char *extension);
int HASHgonebad(BAT *b, const void *v);
BUN HASHmask(BUN cnt);
Hash *HASHnew(Heap *hp, int tpe, BUN size, BUN mask);
int HEAPalloc(Heap *h, size_t nitems, size_t itemsize);
void HEAPcacheInit(void);
int HEAP_check(Heap *h, HeapRepair *hr);
int HEAPdelete(Heap *h, const char *o, const char *ext);
void HEAP_init(Heap *heap, int tpe);
int HEAPload(Heap *h, const char *nme, const char *ext, int trunc);
int HEAP_mmappable(Heap *heap);
int HEAPsave(Heap *h, const char *nme, const char *ext);
int HEAPwarm(Heap *h);
int intCmp(const int *r, const int *l);
int lngCmp(const lng *r, const lng *l);
void MT_global_exit(int status)
	__attribute__((__noreturn__));
void MT_init_posix(void);
int MT_msync(void *p, size_t off, size_t len, int mode);
void *MT_vmalloc(size_t size, size_t *maxsize);
void MT_vmfree(void *p, size_t size);
void *MT_vmrealloc(void *voidptr, size_t oldsize, size_t newsize, size_t oldmaxsize, size_t *newmaxsize);
int OIDdirty(void);
int OIDinit(void);
oid *oidRead(oid *a, stream *s, size_t cnt);
oid OIDread(str buf);
oid OIDseed(oid seed);
int oidWrite(oid *a, stream *s, size_t cnt);
int OIDwrite(stream *fp);
/* type specific binary search implementations */
BUN SORTfnd_bte(BAT *b, const void *v);
BUN SORTfnd_dbl(BAT *b, const void *v);
BUN SORTfnd_flt(BAT *b, const void *v);
BUN SORTfnd_int(BAT *b, const void *v);
BUN SORTfnd_lng(BAT *b, const void *v);
BUN SORTfnd_loc(BAT *b, const void *v);
BUN SORTfnd_sht(BAT *b, const void *v);
BUN SORTfnd_var(BAT *b, const void *v);
BUN SORTfndfirst_bte(BAT *b, const void *v);
BUN SORTfndfirst_dbl(BAT *b, const void *v);
BUN SORTfndfirst_flt(BAT *b, const void *v);
BUN SORTfndfirst_int(BAT *b, const void *v);
BUN SORTfndfirst_lng(BAT *b, const void *v);
BUN SORTfndfirst_loc(BAT *b, const void *v);
BUN SORTfndfirst_sht(BAT *b, const void *v);
BUN SORTfndfirst_var(BAT *b, const void *v);
BUN SORTfndlast_bte(BAT *b, const void *v);
BUN SORTfndlast_dbl(BAT *b, const void *v);
BUN SORTfndlast_flt(BAT *b, const void *v);
BUN SORTfndlast_int(BAT *b, const void *v);
BUN SORTfndlast_lng(BAT *b, const void *v);
BUN SORTfndlast_loc(BAT *b, const void *v);
BUN SORTfndlast_sht(BAT *b, const void *v);
BUN SORTfndlast_var(BAT *b, const void *v);
void strCleanHash(Heap *hp, int rebuild);
int strCmpNoNil(const unsigned char *l, const unsigned char *r);
int strElimDoubles(Heap *h);
void strHeap(Heap *d, size_t cap);
var_t strLocate(Heap *h, const char *v);
var_t strPut(Heap *b, var_t *off, const char *src);
int VALprint(stream *fd, const ValRecord *res);
void VIEWdestroy(BAT *b);
BAT *VIEWreset(BAT *b);
void VIEWunlink(BAT *b);

#define BBP_BATMASK	511
#define BBP_THREADMASK	63

typedef struct {
	MT_Lock swap;
	MT_Lock hash;
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
extern MT_Lock GDKnameLock;
extern int GDKrecovery;
extern int GDKsilent;	/* should GDK shut up? */
extern MT_Lock GDKthreadLock;
extern MT_Lock GDKtmLock;
extern MT_Cond GDKunloadCond;
extern MT_Lock GDKunloadLock;
extern MT_Lock MT_system_lock;

#define BBPdirty(x)	(BBP_dirty=(x))

#define GDKswapLock(x)  GDKbatLock[(x)&BBP_BATMASK].swap
#define GDKhashLock(x)  GDKbatLock[(x)&BBP_BATMASK].hash
#define GDKtrimLock(y)  GDKbbpLock[(y)&BBP_THREADMASK].trim
#define GDKcacheLock(y) GDKbbpLock[(y)&BBP_THREADMASK].alloc
#define BBP_free(y)	GDKbbpLock[(y)&BBP_THREADMASK].free

#define SORTloop_TYPE(b, p, q, tl, th, TYPE)				\
	if (!BATtordered(b))						\
		GDKerror("SORTloop_" #TYPE ": BAT not sorted.\n");	\
	else for (p = simple_EQ(tl, &TYPE##_nil, TYPE) ? BUNfirst(b) : SORTfndfirst_##TYPE(b, tl), \
		  q = simple_EQ(th, &TYPE##_nil, TYPE) ? BUNfirst(b) : SORTfndlast_##TYPE(b, th); \
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
	else for (p = atom_EQ(tl, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndfirst_loc(b, tl), \
			  q = atom_EQ(th, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndlast_loc(b, th); \
		  p < q;						\
		  p++)

#define SORTloop_var(b,p,q,tl,th)					\
	if (!BATtordered(b))						\
		GDKerror("SORTloop_var: BAT not sorted.\n");		\
	else for (p = atom_EQ(tl, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndfirst_var(b, tl), \
			  q = atom_EQ(th, ATOMnilptr((b)->ttype), (b)->ttype) ? BUNfirst(b) : SORTfndlast_var(b, th); \
		  p < q;						\
		  p++)

/* OIDDEPEND */
#if SIZEOF_OID == SIZEOF_INT
#define SORTfnd_oid(b,v)	SORTfnd_int(b,v)
#define SORTfndfirst_oid(b,v)	SORTfndfirst_int(b,v)
#define SORTfndlast_oid(b,v)	SORTfndlast_int(b,v)
#else
#define SORTfnd_oid(b,v)	SORTfnd_lng(b,v)
#define SORTfndfirst_oid(b,v)	SORTfndfirst_lng(b,v)
#define SORTfndlast_oid(b,v)	SORTfndlast_lng(b,v)
#endif
#if SIZEOF_WRD == SIZEOF_INT
#define SORTfnd_wrd(b,v)	SORTfnd_int(b,v)
#define SORTfndfirst_wrd(b,v)	SORTfndfirst_int(b,v)
#define SORTfndlast_wrd(b,v)	SORTfndlast_int(b,v)
#else
#define SORTfnd_wrd(b,v)	SORTfnd_lng(b,v)
#define SORTfndfirst_wrd(b,v)	SORTfndfirst_lng(b,v)
#define SORTfndlast_wrd(b,v)	SORTfndlast_lng(b,v)
#endif
#define SORTloop_bit(b,p,q,tl,th) SORTloop_bte(b,p,q,tl,th)
