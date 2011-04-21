/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/* This file should not be included in any file outside of this directory */

int HEAPload(Heap *h, const char *nme, const char *ext, int trunc);
int HEAPsave(Heap *h, const char *nme, const char *ext);
int HEAPwarm(Heap *h);
int HEAPdelete(Heap *h, const char *o, const char *ext);
void HEAPcacheInit(void);
int HEAP_check(Heap *h, HeapRepair *hr);
void HEAP_init(Heap *heap, int tpe);
int HEAP_mmappable(Heap *heap);
BAT *BATclone(BAT *b, BUN capacity);
int BATfree(BAT *b);
void BATdestroy(BATstore *bs);
void BATsetdims(BAT *b);
BUN BATbuncount(BAT *b);
BUN BATguess(BAT *b);
int BATcheckmodes(BAT *b, int persistent);
BAT *BATload_intern(bat bid, int lock);
size_t BATvmsize(BAT *b, int dirty);
int GDKfdlocate(const char *nme, const char *mode, const char *ext);
FILE *GDKfilelocate(const char *nme, const char *mode, const char *ext);
char *GDKload(const char *nme, const char *ext, size_t size, size_t chunk, int mode);
int GDKsave(const char *nme, const char *ext, void *buf, size_t size, int mode);
int GDKunlink(const char *dir, const char *nme, const char *extension);
int GDKssort(void *h, void *t, void *base, size_t n, int hs, int ts, int tpe);
int GDKssort_rev(void *h, void *t, void *base, size_t n, int hs, int ts, int tpe);
void BBPtrim(size_t memdelta, size_t vmdelta);
int ATOMisdescendant(int id, int parentid);
int ATOMheap(int id, Heap *hp, size_t cap);
int OIDinit(void);
oid OIDrand(void);
oid OIDseed(oid seed);
oid OIDread(str buf);
int OIDwrite(stream *fp);
int OIDdirty(void);
BUN SORTfndfirst(BAT *b, ptr v);
BUN SORTfndlast(BAT *b, ptr v);
int GDKmunmap(void *addr, size_t len);
void *GDKmallocmax(size_t size, size_t *maxsize, int emergency);
void *GDKreallocmax(void *pold, size_t size, size_t *maxsize, int emergency);
void *GDKvmrealloc(void *pold, size_t oldsize, size_t newsize, size_t oldmax, size_t *maxsize, int emergency);
void GDKvmfree(void *blk, size_t size, size_t maxsize);
void GDKaddbuf(const char *msg);
void GDKclrerr(void);
BAT *VIEWreset(BAT *b);
BAT *BATmaterializet(BAT *b);
void VIEWdestroy(BAT *b);
void VIEWunlink(BAT *b);
int ALIGNundo(BAT *b);
int ALIGNcommit(BAT *b);
void BATpropagate(BAT *dst, BAT *src, int idx);
int BATmultijoin(int argc, BAT *argv[], RowFcn tuple_fcn, ptr tuple_data, ColFcn value_fcn[], ptr value_data[], int orderspec);
int voidFromStr(str src, int *len, void **dst);
int voidToStr(str *dst, int *len, void *src);
void *voidRead(void *a, stream *s, size_t cnt);
int voidWrite(void *a, stream *s, size_t cnt);
lng *lngRead(lng *a, stream *s, size_t cnt);
int lngWrite(lng *a, stream *s, size_t cnt);
int lngCmp(lng *r, lng *l);
BUN lngHash(lng *l);
int *intRead(int *a, stream *s, size_t cnt);
int intWrite(int *a, stream *s, size_t cnt);
int intCmp(int *r, int *l);
BUN intHash(int *i);
bat *batRead(bat *a, stream *s, size_t cnt);
int batWrite(bat *a, stream *s, size_t cnt);
ptr *ptrRead(ptr *a, stream *s, size_t cnt);
int ptrWrite(ptr *a, stream *s, size_t cnt);
bit *bitRead(bit *a, stream *s, size_t cnt);
int bitWrite(bit *a, stream *s, size_t cnt);
oid *oidRead(oid *a, stream *s, size_t cnt);
int oidWrite(oid *a, stream *s, size_t cnt);
sht *shtRead(sht *a, stream *s, size_t cnt);
int shtWrite(sht *a, stream *s, size_t cnt);
int shtCmp(sht *r, sht *l);
BUN shtHash(sht *s);
bte *bteRead(bte *a, stream *s, size_t cnt);
int bteWrite(bte *a, stream *s, size_t cnt);
int bteCmp(bte *r, bte *l);
BUN bteHash(bte *c);
chr *chrRead(chr *a, stream *s, size_t cnt);
int chrWrite(chr *a, stream *s, size_t cnt);
int chrCmp(chr *r, chr *l);
BUN chrHash(chr *c);
flt *fltRead(flt *a, stream *s, size_t cnt);
int fltWrite(flt *a, stream *s, size_t cnt);
int fltCmp(flt *r, flt *l);
dbl *dblRead(dbl *a, stream *s, size_t cnt);
int dblWrite(dbl *a, stream *s, size_t cnt);
int dblCmp(dbl *r, dbl *l);
str strRead(str a, stream *s, size_t cnt);
int strWrite(str a, stream *s, size_t cnt);
void strCleanHash(Heap *hp, int rebuild);
var_t strPut(Heap *b, var_t *off, const char *src);
void strHeap(Heap *d, size_t cap);
int strCmp(const char *l, const char *r);
int strElimDoubles(Heap *h);
var_t strLocate(Heap *h, const char *v);
int strCmpNoNil(const unsigned char *l, const unsigned char *r);
int ATOMunknown_add(str nme);
int ATOMunknown_del(int a);
int ATOMunknown_find(str nme);
str ATOMunknown_name(int a);
void BBPinit(void);
void BBPexit(void);
void BBPuncacheit(bat bid);
BATstore *BBPrecycle(int ht, int tt, size_t cap);
wrd BBPrecycle_minsize(wrd);
int BBPrecover(void);
void BBPunshare(bat b);
void BBPextend(dbl factor, int buildhash);
void BBPatom_drop(int atom);
void BBPatom_load(int atom);
int BBPbackup(BAT *b, bit subcommit);
int MT_alloc_register(void *p, size_t size, char mode);
int MT_alloc_print(void);
int MT_alloc_table(void);
void MT_init_posix(int alloc_map);
int MT_madvise(void *p, size_t len, int advice);
int MT_mmap_trim(size_t lim, void *err);
void MT_mmap_inform(void *p, size_t len, int preload, int pattern, int writable);
void *MT_vmalloc(size_t size, size_t *maxsize);
void MT_vmfree(void *p, size_t size);
void *MT_vmrealloc(void *voidptr, size_t oldsize, size_t newsize, size_t oldmaxsize, size_t *newmaxsize);
Hash *HASHnew(Heap *hp, int tpe, BUN size, BUN mask);
BUN HASHmask(BUN cnt);
BAT *HASHprint(BAT *b);
int HASHgonebad(BAT *b, ptr v);
int MT_alive(int pid);	/* OS independent way to check if some process is still alive. */
#ifdef HAVE_PTHREAD_SIGMASK
void MT_thread_sigmask(sigset_t *new_mask, sigset_t *orig_mask);
#endif
void GDKlog(const char *format, ...);
void GDKunlockHome(void);
int GDKgetHome(void);

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

extern int GDKsilent;	/* should GDK shut up? */
extern int BBP_dirty;	/* BBP table dirty? */
extern ptr GDK_mem_start;		/* sbrk(0) at start of the program */
extern size_t GDK_mmap_minsize;	/* size after which we use tempfile VM rather than malloc/anonymous VM */
extern batlock_t GDKbatLock[BBP_BATMASK + 1];
extern bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
extern MT_Lock GDKnameLock;
extern MT_Lock GDKthreadLock;
extern MT_Lock GDKunloadLock;
extern MT_Cond GDKunloadCond;
extern MT_Lock GDKtmLock;
extern int GDKnrofthreads;

#define BBPdirty(x)	(BBP_dirty=(x))

#define GDKswapLock(x)  GDKbatLock[(x&BBP_BATMASK)].swap
#define GDKhashLock(x)  GDKbatLock[(x&BBP_BATMASK)].hash
#define GDKtrimLock(y)  GDKbbpLock[(y&BBP_THREADMASK)].trim
#define GDKcacheLock(y) GDKbbpLock[(y&BBP_THREADMASK)].alloc
#define BBP_free(y)	GDKbbpLock[(y&BBP_THREADMASK)].free
