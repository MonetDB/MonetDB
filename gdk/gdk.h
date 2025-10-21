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

/*
 * The name GDK comes from the original name: Goblin Database Kernel.
 * This kernel was originally written as a library by Martin L. Kersten,
 * Peter Boncz, and Niels Nes, and subsequently heavily modified by
 * Sjoerd Mullender.
 * For the old documentation on the library, dig down in the archives:
 * you can find the old comments in the repository from which this file
 * came.
 */

#ifndef _GDK_H_
#define _GDK_H_

/* standard includes upon which all configure tests depend */
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#include <ctype.h>		/* isspace etc. */

#ifdef HAVE_SYS_FILE_H
# include <sys/file.h>
#endif

#ifdef HAVE_DIRENT_H
# include <dirent.h>
#endif

#include <limits.h>		/* for *_MIN and *_MAX */
#include <float.h>		/* for FLT_MAX and DBL_MAX */

#ifdef WIN32
#ifndef LIBGDK
#define gdk_export extern __declspec(dllimport)
#else
#define gdk_export extern __declspec(dllexport)
#endif
#else
#define gdk_export extern
#endif

/* Only ever compare with GDK_SUCCEED, never with GDK_FAIL, and do not
 * use as a Boolean. */
typedef enum { GDK_FAIL, GDK_SUCCEED } gdk_return;

gdk_export _Noreturn void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));

typedef struct allocator allocator;
typedef struct allocator_state allocator_state;
#include "gdk_system.h"
#include "gdk_posix.h"
#include "stream.h"
#include "mstring.h"

#undef MIN
#undef MAX
#define MAX(A,B)	((A)<(B)?(B):(A))
#define MIN(A,B)	((A)>(B)?(B):(A))

/* defines from ctype with casts that allow passing char values */
#define GDKisspace(c)	isspace((unsigned char) (c))
#define GDKisalnum(c)	isalnum((unsigned char) (c))
#define GDKisdigit(c)	isdigit((unsigned char) (c))
#define GDKisxdigit(c)	isxdigit((unsigned char) (c))

#define BATDIR		"bat"
#define TEMPDIR_NAME	"TEMP_DATA"

#define DELDIR		BATDIR DIR_SEP_STR "DELETE_ME"
#define BAKDIR		BATDIR DIR_SEP_STR "BACKUP"
#define SUBDIR		BAKDIR DIR_SEP_STR "SUBCOMMIT" /* note K, not T */
#define LEFTDIR		BATDIR DIR_SEP_STR "LEFTOVERS"
#define TEMPDIR		BATDIR DIR_SEP_STR TEMPDIR_NAME

/*
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of the following debug options.
*/

#define THRDMASK	(1U)
#define CHECKMASK	(1U<<1)
#define CHECKDEBUG	if (ATOMIC_GET(&GDKdebug) & CHECKMASK)
#define IOMASK		(1U<<4)
#define BATMASK		(1U<<5)
#define PARMASK		(1U<<7)
#define TESTINGMASK	(1U<<8)
#define TMMASK		(1U<<9)
#define TEMMASK		(1U<<10)
#define PERFMASK	(1U<<12)
#define DELTAMASK	(1U<<13)
#define LOADMASK	(1U<<14)
#define PUSHCANDMASK	(1U<<15)	/* used in opt_pushselect.c */
#define TAILCHKMASK	(1U<<16)	/* check .tail file size during commit */
#define ACCELMASK	(1U<<20)
#define ALGOMASK	(1U<<21)

#define NOSYNCMASK	(1U<<24)

#define DEADBEEFMASK	(1U<<25)
#define DEADBEEFCHK	if (!(ATOMIC_GET(&GDKdebug) & DEADBEEFMASK))

#define ALLOCMASK	(1U<<26)

#define HEAPMASK	(1U<<28)

#define FORCEMITOMASK	(1U<<29)
#define FORCEMITODEBUG	if (ATOMIC_GET(&GDKdebug) & FORCEMITOMASK)

#ifndef TRUE
#define TRUE		true
#define FALSE		false
#endif

#define BATMARGIN	1.2	/* extra free margin for new heaps */
#define BATTINY_BITS	8
#define BATTINY		((BUN)1<<BATTINY_BITS)	/* minimum allocation buncnt for a BAT */

enum {
	TYPE_void = 0,
	TYPE_msk,		/* bit mask */
	TYPE_bit,		/* TRUE, FALSE, or nil */
	TYPE_bte,
	TYPE_sht,
	TYPE_int,
	TYPE_oid,
	TYPE_ptr,		/* C pointer! */
	TYPE_flt,
	TYPE_dbl,
	TYPE_lng,
#ifdef HAVE_HGE
	TYPE_hge,
#endif
	TYPE_date,
	TYPE_daytime,
	TYPE_timestamp,
	TYPE_uuid,
	TYPE_inet4,
	TYPE_inet6,
	TYPE_str,
	TYPE_blob,
	TYPE_any = 255,		/* limit types to <255! */
};

typedef bool msk;
typedef int8_t bit;
typedef int8_t bte;
typedef int16_t sht;
/* typedef int64_t lng; -- defined in gdk_system.h */
typedef uint64_t ulng;
typedef union {
	uint32_t align;		/* force alignment, only used for equality */
	uint8_t quad[4] __attribute__((__nonstring__));
} inet4;
typedef union {
#ifdef HAVE_HGE
	hge align;		/* force alignment, only used for equality */
#else
	lng align[2];		/* force alignment, not otherwise used */
#endif
	uint8_t hex[16];
} inet6;

#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#define OIDFMT		"%zu"

typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

#define UUID_SIZE	16	/* size of a UUID */
#define UUID_STRLEN	36	/* length of string representation */

typedef union {
#ifdef HAVE_HGE
	hge h;			/* force alignment, only used for equality */
#else
	lng l[2];		/* force alignment, not otherwise used */
#endif
	uint8_t u[UUID_SIZE] __attribute__((__nonstring__));
} uuid;

typedef struct {
	size_t nitems;
	uint8_t data[] __attribute__((__nonstring__));
} blob;
gdk_export size_t blobsize(size_t nitems) __attribute__((__const__));

#define SIZEOF_LNG		8
#define LL_CONSTANT(val)	INT64_C(val)
#define LLFMT			"%" PRId64
#define ULLFMT			"%" PRIu64
#define LLSCN			"%" SCNd64
#define ULLSCN			"%" SCNu64

typedef oid var_t;		/* type used for heap index of var-sized BAT */
#define SIZEOF_VAR_T	SIZEOF_OID
#define VARFMT		OIDFMT

#if SIZEOF_VAR_T == SIZEOF_INT
#define VAR_MAX		((var_t) INT_MAX)
#else
#define VAR_MAX		((var_t) INT64_MAX)
#endif

typedef oid BUN;		/* BUN position */
#define SIZEOF_BUN	SIZEOF_OID
#define BUNFMT		OIDFMT
/* alternatively:
typedef size_t BUN;
#define SIZEOF_BUN	SIZEOF_SIZE_T
#define BUNFMT		"%zu"
*/
#if SIZEOF_BUN == SIZEOF_INT
#define BUN_NONE ((BUN) INT_MAX)
#else
#define BUN_NONE ((BUN) INT64_MAX)
#endif
#define BUN_MAX (BUN_NONE - 1)	/* maximum allowed size of a BAT */

#define ATOMextern(t)	(ATOMstorage(t) >= TYPE_str)

typedef enum {
	PERSISTENT = 0,
	TRANSIENT,
	SYSTRANS,
} role_t;

/* Heap storage modes */
typedef enum {
	STORE_INVALID = 0,	/* invalid value, used to indicate error */
	STORE_MEM,		/* load into GDKmalloced memory */
	STORE_MMAP,		/* mmap() into virtual memory */
	STORE_PRIV,		/* BAT copy of copy-on-write mmap */
	STORE_CMEM,		/* load into malloc (not GDKmalloc) memory*/
	STORE_NOWN,		/* memory not owned by the BAT */
	STORE_MMAPABS,		/* mmap() into virtual memory from an
				 * absolute path (not part of dbfarm) */
} storage_t;

typedef struct {
	size_t free;		/* index where free area starts. */
	size_t size;		/* size of the heap (bytes) */
	char *base;		/* base pointer in memory. */
#if SIZEOF_VOID_P == 4
	char filename[32];	/* file containing image of the heap */
#else
	char filename[40];	/* file containing image of the heap */
#endif

	ATOMIC_TYPE refs;	/* reference count for this heap */
	bte farmid;		/* id of farm where heap is located */
	bool cleanhash;		/* string heaps must clean hash */
	bool dirty;		/* specific heap dirty marker */
	bool remove;		/* remove storage file when freeing */
	bool wasempty;		/* heap was empty when last saved/created */
	bool hasfile;		/* .filename exists on disk */
	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
	bat parentid;		/* cache id of VIEW parent bat */
} Heap;

typedef struct Hash Hash;
typedef struct Strimps Strimps;

#ifdef HAVE_RTREE
typedef struct RTree RTree;
#endif

typedef struct {
	union {			/* storage is first in the record */
		int ival;
		oid oval;
		sht shval;
		bte btval;
		msk mval;
		flt fval;
		ptr pval;
		bat bval;
		str sval;
		dbl dval;
		lng lval;
#ifdef HAVE_HGE
		hge hval;
#endif
		uuid uval;
		inet4 ip4val;
		inet6 ip6val;
	} val;
	size_t len;
	short vtype;
	bool allocated;
	bool bat;
} *ValPtr, ValRecord;

/* interface definitions */
gdk_export void *VALconvert(allocator *ma, int typ, ValPtr t);
gdk_export char *VALformat(allocator *ma, const ValRecord *res)
	__attribute__((__warn_unused_result__));
gdk_export void VALempty(ValPtr v)
	__attribute__((__access__(write_only, 1)));
gdk_export void VALclear(ValPtr v);
gdk_export ValPtr VALset(ValPtr v, int t, void *p);
gdk_export void *VALget(ValPtr v);
gdk_export int VALcmp(const ValRecord *p, const ValRecord *q);
gdk_export bool VALisnil(const ValRecord *v);

typedef struct PROPrec PROPrec;

#define ORDERIDXOFF		3

/* assert that atom width is power of 2, i.e., width == 1<<shift */
#define assert_shift_width(shift,width) assert(((shift) == 0 && (width) == 0) || ((unsigned)1<<(shift)) == (unsigned)(width))

#define GDKLIBRARY_HASHASH	061044U /* first in Jul2021: hashash bit in string heaps */
#define GDKLIBRARY_HSIZE	061045U /* first in Jan2022: heap "size" values */
#define GDKLIBRARY_JSON 	061046U /* first in Sep2022: json storage changes*/
#define GDKLIBRARY_STATUS	061047U /* first in Dec2023: no status/filename columns */
#define GDKLIBRARY		061050U /* first in Aug2024 */

/* The batRestricted field indicates whether a BAT is readonly.
 * we have modes: BAT_WRITE  = all permitted
 *                BAT_APPEND = append-only
 *                BAT_READ   = read-only
 * VIEW bats are always mapped read-only.
 */
typedef enum {
	BAT_WRITE,		  /* all kinds of access allowed */
	BAT_READ,		  /* only read-access allowed */
	BAT_APPEND,		  /* only reads and appends allowed */
} restrict_t;

/* theaplock: this lock should be held when reading or writing any of
 * the fields that are saved in the BBP.dir file (plus any, if any, that
 * share bitfields with any of the fields), i.e. hseqbase,
 * batRestricted, batTransient, batCount, and the theap properties tkey,
 * tseqbase, tsorted, trevsorted, twidth, tshift, tnonil, tnil, tnokey,
 * tnosorted, tnorevsorted, tminpos, tmaxpos, and tunique_est, also when
 * BBP_logical(bid) is changed, and also when reading or writing any of
 * the following fields: theap, tvheap, batInserted, batCapacity.  There
 * is no need for the lock if the bat cannot possibly be modified
 * concurrently, e.g. when it is new and not yet returned to the
 * interpreter or during system initialization.
 * If multiple bats need to be locked at the same time by the same
 * thread, first lock the view, then the view's parent(s). */
typedef struct BAT {
	/* static bat properties */
	oid hseqbase;		/* head seq base */
	MT_Id creator_tid;	/* which thread created it */
	bat batCacheid;		/* index into BBP */
	role_t batRole;		/* role of the bat */

	/* dynamic bat properties */
	restrict_t batRestricted:2; /* access privileges */
	bool batTransient:1;	/* should the BAT persist on disk? */
	bool batCopiedtodisk:1;	/* once written */
	uint16_t selcnt;	/* how often used in equi select without hash */
	uint16_t unused;	/* value=0 for now (sneakily used by mat.c) */

	/* delta status administration */
	BUN batInserted;	/* start of inserted elements */
	BUN batCount;		/* tuple count */
	BUN batCapacity;	/* tuple capacity */

	/* dynamic column properties */
	uint16_t twidth;	/* byte-width of the atom array */
	int8_t ttype;		/* type id. */
	uint8_t tshift;		/* log2 of bun width */
	/* see also comment near BATassertProps() for more information
	 * about the properties */
	bool tkey:1;		/* no duplicate values present */
	bool tnonil:1;		/* there are no nils in the column */
	bool tnil:1;		/* there is a nil in the column */
	bool tsorted:1;		/* column is sorted in ascending order */
	bool trevsorted:1;	/* column is sorted in descending order */
	bool tascii:1;		/* string column is fully ASCII (7 bit) */
	BUN tnokey[2];		/* positions that prove key==FALSE */
	BUN tnosorted;		/* position that proves sorted==FALSE */
	BUN tnorevsorted;	/* position that proves revsorted==FALSE */
	BUN tminpos, tmaxpos;	/* location of min/max value */
	double tunique_est;	/* estimated number of unique values */
	oid tseqbase;		/* start of dense sequence */

	Heap *theap;		/* space for the column. */
	BUN tbaseoff;		/* offset in heap->base (in whole items) */
	Heap *tvheap;		/* space for the varsized data. */
	Hash *thash;		/* hash table */
#ifdef HAVE_RTREE
	RTree *trtree;		/* rtree geometric index */
#endif
	Heap *torderidx;	/* order oid index */
	Strimps *tstrimps;	/* string imprint index  */
	PROPrec *tprops;	/* list of dynamic properties stored in the bat descriptor */

	MT_Lock theaplock;	/* lock protecting heap reference changes */
	MT_RWLock thashlock;	/* lock specifically for hash management */
	MT_Lock batIdxLock;	/* lock to manipulate other indexes/properties */
	Heap *oldtail;		/* old tail heap, to be destroyed after commit */
} BAT;

/* some access functions for the bitmask type */
static inline void
mskSet(BAT *b, BUN p)
{
	((uint32_t *) b->theap->base)[p / 32] |= 1U << (p % 32);
}

static inline void
mskClr(BAT *b, BUN p)
{
	((uint32_t *) b->theap->base)[p / 32] &= ~(1U << (p % 32));
}

static inline void
mskSetVal(BAT *b, BUN p, msk v)
{
	if (v)
		mskSet(b, p);
	else
		mskClr(b, p);
}

static inline msk
mskGetVal(BAT *b, BUN p)
{
	return ((uint32_t *) b->theap->base)[p / 32] & (1U << (p % 32));
}

gdk_export gdk_return HEAPextend(Heap *h, size_t size, bool mayshare)
	__attribute__((__warn_unused_result__));
gdk_export size_t HEAPvmsize(Heap *h);
gdk_export size_t HEAPmemsize(Heap *h);
gdk_export void HEAPdecref(Heap *h, bool remove);
gdk_export void HEAPincref(Heap *h);

#define VIEWtparent(x)	((x)->theap == NULL || (x)->theap->parentid == (x)->batCacheid ? 0 : (x)->theap->parentid)
#define VIEWvtparent(x)	((x)->tvheap == NULL || (x)->tvheap->parentid == (x)->batCacheid ? 0 : (x)->tvheap->parentid)

#define isVIEW(x)	(VIEWtparent(x) != 0 || VIEWvtparent(x) != 0)

typedef struct {
	char *logical;		/* logical name (may point at bak) */
	char bak[16];		/* logical name backup (tmp_%o) */
	BAT descr;		/* the BAT descriptor */
	char *options;		/* A string list of options */
#if SIZEOF_VOID_P == 4
	char physical[20];	/* dir + basename for storage */
#else
	char physical[24];	/* dir + basename for storage */
#endif
	bat next;		/* next BBP slot in linked list */
	int refs;		/* in-memory references on which the loaded status of a BAT relies */
	int lrefs;		/* logical references on which the existence of a BAT relies */
	ATOMIC_TYPE status;	/* status mask used for spin locking */
	MT_Id pid;		/* creator of this bat while "private" */
} BBPrec;

gdk_export bat BBPlimit;
#if SIZEOF_VOID_P == 4
#define N_BBPINIT	1000
#define BBPINITLOG	11
#else
#define N_BBPINIT	10000
#define BBPINITLOG	14
#endif
#define BBPINIT		(1 << BBPINITLOG)
/* absolute maximum number of BATs is N_BBPINIT * BBPINIT
 * this also gives the longest possible "physical" name and "bak" name
 * of a BAT: the "bak" name is "tmp_%o", so at most 14 + \0 bytes on 64
 * bit architecture and 11 + \0 on 32 bit architecture; the physical
 * name is a bit more complicated, but the longest possible name is 22 +
 * \0 bytes (16 + \0 on 32 bits), the longest possible extension adds
 * another 17 bytes (.thsh(grp|uni)(l|b)%08x) */
gdk_export BBPrec *BBP[N_BBPINIT];

/* fast defines without checks; internal use only  */
#define BBP_record(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)]
#define BBP_logical(i)	BBP_record(i).logical
#define BBP_bak(i)	BBP_record(i).bak
#define BBP_next(i)	BBP_record(i).next
#define BBP_physical(i)	BBP_record(i).physical
#define BBP_options(i)	BBP_record(i).options
#define BBP_desc(i)	(&BBP_record(i).descr)
#define BBP_refs(i)	BBP_record(i).refs
#define BBP_lrefs(i)	BBP_record(i).lrefs
#define BBP_status(i)	((unsigned) ATOMIC_GET(&BBP_record(i).status))
#define BBP_pid(i)	BBP_record(i).pid
#define BATgetId(b)	BBP_logical((b)->batCacheid)
#define BBPvalid(i)	(BBP_logical(i) != NULL)

#define BBPRENAME_ALREADY	(-1)
#define BBPRENAME_ILLEGAL	(-2)
#define BBPRENAME_LONG		(-3)
#define BBPRENAME_MEMORY	(-4)

gdk_export void BBPlock(void);
gdk_export void BBPunlock(void);
gdk_export void BBPtmlock(void);
gdk_export void BBPtmunlock(void);

gdk_export BAT *BBPquickdesc(bat b);

/* BAT iterator, also protects use of BAT heaps with reference counts.
 *
 * A BAT iterator has to be used with caution, but it does have to be
 * used in many place.
 *
 * An iterator is initialized by assigning it the result of a call to
 * either bat_iterator or bat_iterator_nolock.  The former must be
 * accompanied by a call to bat_iterator_end to release resources.
 *
 * bat_iterator should be used for BATs that could possibly be modified
 * in another thread while we're reading the contents of the BAT.
 * Alternatively, but only for very quick access, the theaplock can be
 * taken, the data read, and the lock released.  For longer duration
 * accesses, it is better to use the iterator, even without the BUNt*
 * macros, since the theaplock is only held very briefly.
 *
 * Note, bat_iterator must only be used for read-only access.
 *
 * If BATs are to be modified, higher level code must assure that no
 * other thread is going to modify the same BAT at the same time.  A
 * to-be-modified BAT should not use bat_iterator.  It can use
 * bat_iterator_nolock, but be aware that this creates a copy of the
 * heap pointer(s) (i.e. theap and tvheap) and if the heaps get
 * extended, the pointers in the BAT structure may be modified, but that
 * does not modify the pointers in the iterator.  This means that after
 * operations that may grow a heap, the iterator should be
 * reinitialized.
 *
 * The BAT iterator provides a number of fields that can (and often
 * should) be used to access information about the BAT.  For string
 * BATs, if a parallel threads adds values, the offset heap (theap) may
 * get replaced by one that is wider.  This involves changing the twidth
 * and tshift values in the BAT structure.  These changed values should
 * not be used to access the data in the iterator.  Instead, use the
 * width and shift values in the iterator itself.
 */
typedef struct BATiter {
	BAT *b;
	Heap *h;
	void *base;
	Heap *vh;
	BUN count;
	BUN baseoff;
	oid tseq;
	BUN hfree, vhfree;
	BUN nokey[2];
	BUN nosorted, norevsorted;
	BUN minpos, maxpos;
	double unique_est;
	uint16_t width;
	uint8_t shift;
	int8_t type;
	bool key:1,
		nonil:1,
		nil:1,
		sorted:1,
		revsorted:1,
		hdirty:1,
		vhdirty:1,
		copiedtodisk:1,
		transient:1,
		ascii:1;
	restrict_t restricted:2;
#ifndef NDEBUG
	bool locked:1;
#endif
	union {
		oid tvid;
		bool tmsk;
	};
} BATiter;

static inline BATiter
bat_iterator_nolock(BAT *b)
{
	/* does not get matched by bat_iterator_end */
	if (b) {
		const bool isview = VIEWtparent(b) != 0;
		return (BATiter) {
			.b = b,
			.h = b->theap,
			.base = b->theap->base ? b->theap->base + (b->tbaseoff << b->tshift) : NULL,
			.baseoff = b->tbaseoff,
			.vh = b->tvheap,
			.count = b->batCount,
			.width = b->twidth,
			.shift = b->tshift,
			.type = b->ttype,
			.tseq = b->tseqbase,
			/* don't use b->theap->free in case b is a slice */
			.hfree = b->ttype ?
				  b->ttype == TYPE_msk ?
				   (((size_t) b->batCount + 31) / 32) * 4 :
				  (size_t) b->batCount << b->tshift :
				 0,
			.vhfree = b->tvheap ? b->tvheap->free : 0,
			.nokey[0] = b->tnokey[0],
			.nokey[1] = b->tnokey[1],
			.nosorted = b->tnosorted,
			.norevsorted = b->tnorevsorted,
			.minpos = isview ? BUN_NONE : b->tminpos,
			.maxpos = isview ? BUN_NONE : b->tmaxpos,
			.unique_est = b->tunique_est,
			.key = b->tkey,
			.nonil = b->tnonil,
			.nil = b->tnil,
			.sorted = b->tsorted,
			.revsorted = b->trevsorted,
			.ascii = b->tascii,
			/* only look at heap dirty flag if we own it */
			.hdirty = b->theap->parentid == b->batCacheid && b->theap->dirty,
			/* also, if there is no vheap, it's not dirty */
			.vhdirty = b->tvheap && b->tvheap->parentid == b->batCacheid && b->tvheap->dirty,
			.copiedtodisk = b->batCopiedtodisk,
			.transient = b->batTransient,
			.restricted = b->batRestricted,
#ifndef NDEBUG
			.locked = false,
#endif
		};
	}
	return (BATiter) {0};
}

static inline void
bat_iterator_incref(BATiter *bi)
{
#ifndef NDEBUG
	bi->locked = true;
#endif
	HEAPincref(bi->h);
	if (bi->vh)
		HEAPincref(bi->vh);
}

static inline BATiter
bat_iterator(BAT *b)
{
	/* needs matching bat_iterator_end */
	BATiter bi;
	if (b) {
		BAT *pb = NULL, *pvb = NULL;
		/* for a view, always first lock the view and then the
		 * parent(s)
		 * note that a varsized bat can have two different
		 * parents and that the parent for the tail can itself
		 * have a parent for its vheap (which would have to be
		 * our own vheap parent), so lock the vheap after the
		 * tail */
		MT_lock_set(&b->theaplock);
		if (b->theap->parentid != b->batCacheid) {
			pb = BBP_desc(b->theap->parentid);
			MT_lock_set(&pb->theaplock);
		}
		if (b->tvheap &&
		    b->tvheap->parentid != b->batCacheid &&
		    b->tvheap->parentid != b->theap->parentid) {
			pvb = BBP_desc(b->tvheap->parentid);
			MT_lock_set(&pvb->theaplock);
		}
		bi = bat_iterator_nolock(b);
		bat_iterator_incref(&bi);
		if (pvb)
			MT_lock_unset(&pvb->theaplock);
		if (pb)
			MT_lock_unset(&pb->theaplock);
		MT_lock_unset(&b->theaplock);
	} else {
		bi = (BATiter) {
			.b = NULL,
#ifndef NDEBUG
			.locked = true,
#endif
		};
	}
	return bi;
}

/* return a copy of a BATiter instance; needs to be released with
 * bat_iterator_end */
static inline BATiter
bat_iterator_copy(BATiter *bip)
{
	assert(bip);
	assert(bip->locked);
	if (bip->h)
		HEAPincref(bip->h);
	if (bip->vh)
		HEAPincref(bip->vh);
	return *bip;
}

static inline void
bat_iterator_end(BATiter *bip)
{
	/* matches bat_iterator */
	assert(bip);
	assert(bip->locked);
	if (bip->h)
		HEAPdecref(bip->h, false);
	if (bip->vh)
		HEAPdecref(bip->vh, false);
	*bip = (BATiter) {0};
}

gdk_export gdk_return HEAP_initialize(
	Heap *heap,		/* nbytes -- Initial size of the heap. */
	size_t nbytes,		/* alignment -- for objects on the heap. */
	size_t nprivate,	/* nprivate -- Size of private space */
	int alignment		/* alignment restriction for allocated chunks */
	);

gdk_export var_t HEAP_malloc(BAT *b, size_t nbytes);
gdk_export void HEAP_free(Heap *heap, var_t block);

gdk_export BAT *COLnew(oid hseq, int tltype, BUN capacity, role_t role)
	__attribute__((__warn_unused_result__));
gdk_export BAT *COLnew2(oid hseq, int tt, BUN cap, role_t role, uint16_t width)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATdense(oid hseq, oid tseq, BUN cnt)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATextend(BAT *b, BUN newcap)
	__attribute__((__warn_unused_result__));

/* internal */
gdk_export uint8_t ATOMelmshift(int sz)
	__attribute__((__const__));
gdk_export gdk_return ATOMheap(int id, Heap *hp, size_t cap)
	__attribute__((__warn_unused_result__));
gdk_export const char *BATtailname(const BAT *b);

gdk_export gdk_return GDKupgradevarheap(BAT *b, var_t v, BUN cap, BUN ncopy)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNappend(BAT *b, const void *right, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNappendmulti(BAT *b, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATappend(BAT *b, BAT *n, BAT *s, bool force)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BUNreplace(BAT *b, oid left, const void *right, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNreplacemulti(BAT *b, const oid *positions, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNreplacemultiincr(BAT *b, oid position, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BUNdelete(BAT *b, oid o)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATdel(BAT *b, BAT *d)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATreplace(BAT *b, BAT *p, BAT *n, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATupdate(BAT *b, BAT *p, BAT *n, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATupdatepos(BAT *b, const oid *positions, BAT *n, bool autoincr, bool force)
	__attribute__((__warn_unused_result__));

/* Functions to perform a binary search on a sorted BAT.
 * See gdk_search.c for details. */
gdk_export BUN SORTfnd(BAT *b, const void *v);
gdk_export BUN SORTfndfirst(BAT *b, const void *v);
gdk_export BUN SORTfndlast(BAT *b, const void *v);

gdk_export BUN ORDERfnd(BAT *b, Heap *oidxh, const void *v);
gdk_export BUN ORDERfndfirst(BAT *b, Heap *oidxh, const void *v);
gdk_export BUN ORDERfndlast(BAT *b, Heap *oidxh, const void *v);

gdk_export BUN BUNfnd(BAT *b, const void *right);

#define BUNfndVOID(b, v)						\
	(((is_oid_nil(*(const oid*)(v)) ^ is_oid_nil((b)->tseqbase)) |	\
		(*(const oid*)(v) < (b)->tseqbase) |			\
		(*(const oid*)(v) >= (b)->tseqbase + (b)->batCount)) ?	\
	 BUN_NONE :							\
	 (BUN) (*(const oid*)(v) - (b)->tseqbase))

#define BATttype(b)	(BATtdense(b) ? TYPE_oid : (b)->ttype)

#define tailsize(b,p)	((b)->ttype ?				\
			 (ATOMstorage((b)->ttype) == TYPE_msk ?	\
			  (((size_t) (p) + 31) / 32) * 4 :	\
			  ((size_t) (p)) << (b)->tshift) :	\
			 0)

#define Tloc(b,p)	((void *)((b)->theap->base+(((size_t)(p)+(b)->tbaseoff)<<(b)->tshift)))

typedef var_t stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_VAR_T
#define GDK_VARALIGN SIZEOF_STRIDX_T

#define BUNtvaroff(bi,p) VarHeapVal((bi).base, (p), (bi).width)

#define BUNtmsk(bi,p)	Tmsk(&(bi), (p))
#define BUNtloc(bi,p)	(assert((bi).type != TYPE_msk), ((void *) ((char *) (bi).base + ((p) << (bi).shift))))
#define BUNtpos(bi,p)	Tpos(&(bi),p)
#define BUNtvar(bi,p)	(assert((bi).type && (bi).vh), (void *) ((bi).vh->base+BUNtvaroff(bi,p)))
#define BUNtail(bi,p)	((bi).type?(bi).vh?BUNtvar(bi,p):(bi).type==TYPE_msk?BUNtmsk(bi,p):BUNtloc(bi,p):BUNtpos(bi,p))

#define BATcount(b)	((b)->batCount)

#include "gdk_atoms.h"

#include "gdk_cand.h"

gdk_export BUN BATcount_no_nil(BAT *b, BAT *s);
gdk_export void BATsetcapacity(BAT *b, BUN cnt);
gdk_export void BATsetcount(BAT *b, BUN cnt);
gdk_export BUN BATgrows(BAT *b);
gdk_export gdk_return BATkey(BAT *b, bool onoff);
gdk_export gdk_return BATmode(BAT *b, bool transient);
gdk_export void BAThseqbase(BAT *b, oid o);
gdk_export void BATtseqbase(BAT *b, oid o);

gdk_export BAT *BATsetaccess(BAT *b, restrict_t mode)
	__attribute__((__warn_unused_result__));
gdk_export restrict_t BATgetaccess(BAT *b);


#define BATdirty(b)	(!(b)->batCopiedtodisk ||			\
			 (b)->theap->dirty ||				\
			 ((b)->tvheap != NULL && (b)->tvheap->dirty))
#define BATdirtybi(bi)	(!(bi).copiedtodisk || (bi).hdirty || (bi).vhdirty)

#define BATcapacity(b)	(b)->batCapacity

gdk_export gdk_return BATclear(BAT *b, bool force);
gdk_export BAT *COLcopy(BAT *b, int tt, bool writable, role_t role);
gdk_export BAT *COLcopy2(BAT *b, int tt, bool writable, bool mayshare, role_t role);

gdk_export gdk_return BATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATsave(BAT *b)
	__attribute__((__warn_unused_result__));

#define NOFARM (-1) /* indicate to GDKfilepath to create relative path */
#define MAXPATH	1024		/* maximum supported file path */

gdk_export gdk_return GDKfilepath(char *buf, size_t bufsize, int farmid, const char *dir, const char *nme, const char *ext)
	__attribute__((__access__(write_only, 1, 2)));
gdk_export bool GDKinmemory(int farmid);
gdk_export bool GDKembedded(void);
gdk_export gdk_return GDKcreatedir(const char *nme);

gdk_export void OIDXdestroy(BAT *b);

gdk_export gdk_return BATprintcolumns(allocator *ma, stream *s, int argc, BAT *argv[]);
gdk_export gdk_return BATprint(allocator *ma, stream *s, BAT *b);

gdk_export bool BATordered(BAT *b);
gdk_export bool BATordered_rev(BAT *b);
gdk_export gdk_return BATsort(BAT **sorted, BAT **order, BAT **groups, BAT *b, BAT *o, BAT *g, bool reverse, bool nilslast, bool stable)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));


gdk_export void GDKqsort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe, bool reverse, bool nilslast);

/* BAT is dense (i.e., BATtvoid() is true and tseqbase is not NIL) */
#define BATtdense(b)	(!is_oid_nil((b)->tseqbase) &&			\
			 ((b)->tvheap == NULL || (b)->tvheap->free == 0))
#define BATtdensebi(bi)	(!is_oid_nil((bi)->tseq) &&			\
			 ((bi)->vh == NULL || (bi)->vhfree == 0))
/* BATtvoid: BAT can be (or actually is) represented by TYPE_void */
#define BATtvoid(b)	(BATtdense(b) || (b)->ttype==TYPE_void)
#define BATtkey(b)	((b)->tkey || BATtdense(b))

/* set some properties that are trivial to deduce; called with theaplock
 * held */
static inline void
BATsettrivprop(BAT *b)
{
	assert(!is_oid_nil(b->hseqbase));
	assert(is_oid_nil(b->tseqbase) || ATOMtype(b->ttype) == TYPE_oid);
	if (b->ttype == TYPE_void) {
		if (is_oid_nil(b->tseqbase)) {
			b->tnonil = b->batCount == 0;
			b->tnil = !b->tnonil;
			b->trevsorted = true;
			b->tkey = b->batCount <= 1;
			b->tunique_est = b->batCount == 0 ? 0.0 : 1.0;
		} else {
			b->tnonil = true;
			b->tnil = false;
			b->tkey = true;
			b->trevsorted = b->batCount <= 1;
			b->tunique_est = (double) b->batCount;
		}
		b->tsorted = true;
	} else if (b->batCount <= 1) {
		b->tnosorted = b->tnorevsorted = 0;
		b->tnokey[0] = b->tnokey[1] = 0;
		b->tunique_est = (double) b->batCount;
		b->tkey = true;
		if (ATOMlinear(b->ttype)) {
			b->tsorted = true;
			b->trevsorted = true;
			if (b->batCount == 0) {
				b->tminpos = BUN_NONE;
				b->tmaxpos = BUN_NONE;
				b->tnonil = true;
				b->tnil = false;
				if (b->ttype == TYPE_oid) {
					b->tseqbase = 0;
				}
			} else if (b->ttype == TYPE_oid) {
				oid sqbs = ((const oid *) b->theap->base)[b->tbaseoff];
				if (is_oid_nil(sqbs)) {
					b->tnonil = false;
					b->tnil = true;
					b->tminpos = BUN_NONE;
					b->tmaxpos = BUN_NONE;
				} else {
					b->tnonil = true;
					b->tnil = false;
					b->tminpos = 0;
					b->tmaxpos = 0;
				}
				b->tseqbase = sqbs;
			} else if (b->tvheap
				   ? ATOMeq(b->ttype,
					    b->tvheap->base + VarHeapVal(Tloc(b, 0), 0, b->twidth),
					    ATOMnilptr(b->ttype))
				   : ATOMeq(b->ttype, Tloc(b, 0),
					    ATOMnilptr(b->ttype))) {
				/* the only value is NIL */
				b->tminpos = BUN_NONE;
				b->tmaxpos = BUN_NONE;
			} else {
				/* the only value is both min and max */
				b->tminpos = 0;
				b->tmaxpos = 0;
			}
		} else {
			b->tsorted = false;
			b->trevsorted = false;
			b->tminpos = BUN_NONE;
			b->tmaxpos = BUN_NONE;
		}
	} else if (b->batCount == 2 && ATOMlinear(b->ttype)) {
		int c;
		if (b->tvheap)
			c = ATOMcmp(b->ttype,
				    b->tvheap->base + VarHeapVal(Tloc(b, 0), 0, b->twidth),
				    b->tvheap->base + VarHeapVal(Tloc(b, 0), 1, b->twidth));
		else
			c = ATOMcmp(b->ttype, Tloc(b, 0), Tloc(b, 1));
		b->tsorted = c <= 0;
		b->tnosorted = !b->tsorted;
		b->trevsorted = c >= 0;
		b->tnorevsorted = !b->trevsorted;
		b->tkey = c != 0;
		b->tnokey[0] = 0;
		b->tnokey[1] = !b->tkey;
		b->tunique_est = (double) (1 + b->tkey);
	} else {
		if (!ATOMlinear(b->ttype)) {
			b->tsorted = false;
			b->trevsorted = false;
			b->tminpos = BUN_NONE;
			b->tmaxpos = BUN_NONE;
		}
		if (b->tkey)
			b->tunique_est = (double) b->batCount;
	}
}

static inline void
BATnegateprops(BAT *b)
{
	/* disable all properties here */
	b->tnonil = false;
	b->tnil = false;
	if (b->ttype) {
		b->tsorted = false;
		b->trevsorted = false;
		b->tnosorted = 0;
		b->tnorevsorted = 0;
	}
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	b->tmaxpos = b->tminpos = BUN_NONE;
}

#define GDKMAXERRLEN	10240
#define GDKERROR	"!ERROR: "
#define GDKFATAL	"!FATAL: "

/* Data Distilleries uses ICU for internationalization of some MonetDB error messages */

#include "gdk_tracer.h"

gdk_export gdk_return GDKtracer_fill_comp_info(BAT *id, BAT *component, BAT *log_level);

#define GDKerror(...)		TRC_ERROR(GDK, __VA_ARGS__)
#define GDKsyserr(errno, ...)						\
	GDKtracer_log(__FILE__, __func__, __LINE__, TRC_NAME(M_ERROR),	\
		      TRC_NAME(GDK), GDKstrerror(errno, (char[64]){0}, 64), \
		      __VA_ARGS__)
#define GDKsyserror(...)	GDKsyserr(errno, __VA_ARGS__)

gdk_export void GDKclrerr(void);


/* tfastins* family: update a value at a particular location in the bat
 * bunfastapp* family: append a value to the bat
 * *_nocheck: do not check whether the capacity is large enough
 * * (without _nocheck): check bat capacity and possibly extend
 *
 * This means, for tfastins* it is the caller's responsibility to set
 * the batCount and theap->free values correctly (e.g. by calling
 * BATsetcount(), and for *_nocheck to make sure there is enough space
 * allocated in the theap (tvheap for variable-sized types is still
 * extended if needed, making that these functions can fail).
 */
__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheckVAR(BAT *b, BUN p, const void *v)
{
	var_t d;
	gdk_return rc;
	assert(b->tbaseoff == 0);
	assert(b->theap->parentid == b->batCacheid);
	MT_lock_set(&b->theaplock);
	rc = ATOMputVAR(b, &d, v);
	MT_lock_unset(&b->theaplock);
	if (rc != GDK_SUCCEED)
		return rc;
	if (b->twidth < SIZEOF_VAR_T &&
	    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 << b->tshift))) {
		/* doesn't fit in current heap, upgrade it */
		rc = GDKupgradevarheap(b, d, 0, MAX(p, b->batCount));
		if (rc != GDK_SUCCEED)
			return rc;
	}
	switch (b->twidth) {
	case 1:
		((uint8_t *) b->theap->base)[p] = (uint8_t) (d - GDK_VAROFFSET);
		break;
	case 2:
		((uint16_t *) b->theap->base)[p] = (uint16_t) (d - GDK_VAROFFSET);
		break;
	case 4:
		((uint32_t *) b->theap->base)[p] = (uint32_t) d;
		break;
#if SIZEOF_VAR_T == 8
	case 8:
		((uint64_t *) b->theap->base)[p] = (uint64_t) d;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheckFIX(BAT *b, BUN p, const void *v)
{
	return ATOMputFIX(b->ttype, Tloc(b, p), v);
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheck(BAT *b, BUN p, const void *v)
{
	assert(b->theap->parentid == b->batCacheid);
	assert(b->tbaseoff == 0);
	if (b->ttype == TYPE_void) {
		;
	} else if (ATOMstorage(b->ttype) == TYPE_msk) {
		mskSetVal(b, p, * (msk *) v);
	} else if (b->tvheap) {
		return tfastins_nocheckVAR(b, p, v);
	} else {
		return tfastins_nocheckFIX(b, p, v);
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins(BAT *b, BUN p, const void *v)
{
	if (p >= BATcapacity(b)) {
		if (p >= BUN_MAX) {
			GDKerror("tfastins: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		BUN sz = BATgrows(b);
		if (sz <= p)
			sz = p + BATTINY;
		gdk_return rc = BATextend(b, sz);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return tfastins_nocheck(b, p, v);
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp_nocheck(BAT *b, const void *v)
{
	BUN p = b->batCount;
	if (ATOMstorage(b->ttype) == TYPE_msk && p % 32 == 0)
		((uint32_t *) b->theap->base)[p / 32] = 0;
	gdk_return rc = tfastins_nocheck(b, p, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			if (p % 32 == 0)
				b->theap->free += 4;
		} else
			b->theap->free += b->twidth;
	}
	return rc;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp(BAT *b, const void *v)
{
	BUN p = b->batCount;
	if (ATOMstorage(b->ttype) == TYPE_msk && p % 32 == 0)
		((uint32_t *) b->theap->base)[p / 32] = 0;
	gdk_return rc = tfastins(b, p, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			if (p % 32 == 0)
				b->theap->free += 4;
		} else
			b->theap->free += b->twidth;
	}
	return rc;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastappOID(BAT *b, oid o)
{
	BUN p = b->batCount;
	if (p >= BATcapacity(b)) {
		if (p >= BUN_MAX) {
			GDKerror("tfastins: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		gdk_return rc = BATextend(b, BATgrows(b));
		if (rc != GDK_SUCCEED)
			return rc;
	}
	((oid *) b->theap->base)[b->batCount++] = o;
	b->theap->free += sizeof(oid);
	return GDK_SUCCEED;
}

#define bunfastappTYPE(TYPE, b, v)					\
	(BATcount(b) >= BATcapacity(b) &&				\
	 ((BATcount(b) == BUN_MAX &&					\
	   (GDKerror("bunfastapp: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX), \
	    true)) ||							\
	  BATextend((b), BATgrows(b)) != GDK_SUCCEED) ?			\
	 GDK_FAIL :							\
	 (assert((b)->theap->parentid == (b)->batCacheid),		\
	  (b)->theap->free += sizeof(TYPE),				\
	  ((TYPE *) (b)->theap->base)[(b)->batCount++] = * (const TYPE *) (v), \
	  GDK_SUCCEED))

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp_nocheckVAR(BAT *b, const void *v)
{
	gdk_return rc;
	rc = tfastins_nocheckVAR(b, b->batCount, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		b->theap->free += b->twidth;
	}
	return rc;
}

/* Strimps exported functions */
gdk_export gdk_return STRMPcreate(BAT *b, BAT *s);
gdk_export BAT *STRMPfilter(BAT *b, BAT *s, const char *q, const bool keep_nils);
gdk_export void STRMPdestroy(BAT *b);
gdk_export bool BAThasstrimps(BAT *b);
gdk_export gdk_return BATsetstrimps(BAT *b);

/* Rtree structure functions */
#ifdef HAVE_RTREE
gdk_export bool RTREEexists(BAT *b);
gdk_export bool RTREEexists_bid(bat bid);
gdk_export gdk_return BATrtree(BAT *wkb, BAT* mbr);
/* inMBR is really a struct mbr * from geom module, but that is not
 * available here */
gdk_export BUN* RTREEsearch(BAT *b, const void *inMBR, int result_limit);
#endif

gdk_export void RTREEdestroy(BAT *b);
gdk_export void RTREEfree(BAT *b);

/* The ordered index structure */

gdk_export gdk_return BATorderidx(BAT *b, bool stable);
gdk_export gdk_return GDKmergeidx(BAT *b, BAT**a, int n_ar);
gdk_export bool BATcheckorderidx(BAT *b);

#define DELTAdirty(b)	((b)->batInserted < BATcount(b))

#include "gdk_hash.h"
#include "gdk_bbp.h"
#include "gdk_utils.h"

/* functions defined in gdk_bat.c */
gdk_export gdk_return void_inplace(BAT *b, oid id, const void *val, bool force)
	__attribute__((__warn_unused_result__));

#ifdef NATIVE_WIN32
#ifdef _MSC_VER
#define fileno _fileno
#endif
#define fdopen _fdopen
#define putenv _putenv
#endif

/* Return a pointer to the value contained in V.  Also see VALget
 * which returns a void *. */
__attribute__((__pure__))
static inline const void *
VALptr(const ValRecord *v)
{
	switch (ATOMstorage(v->vtype)) {
	case TYPE_void: return (const void *) &v->val.oval;
	case TYPE_msk: return (const void *) &v->val.mval;
	case TYPE_bte: return (const void *) &v->val.btval;
	case TYPE_sht: return (const void *) &v->val.shval;
	case TYPE_int: return (const void *) &v->val.ival;
	case TYPE_flt: return (const void *) &v->val.fval;
	case TYPE_dbl: return (const void *) &v->val.dval;
	case TYPE_lng: return (const void *) &v->val.lval;
#ifdef HAVE_HGE
	case TYPE_hge: return (const void *) &v->val.hval;
#endif
	case TYPE_uuid: return (const void *) &v->val.uval;
	case TYPE_inet4: return (const void *) &v->val.ip4val;
	case TYPE_inet6: return (const void *) &v->val.ip6val;
	case TYPE_ptr: return (const void *) &v->val.pval;
	case TYPE_str: return (const void *) v->val.sval;
	default:       return (const void *) v->val.pval;
	}
}

#define THREADS		1024	/* maximum value for gdk_nr_threads */

gdk_export stream *GDKstdout;
gdk_export stream *GDKstdin;

#define GDKerrbuf	(GDKgetbuf())

static inline bat
BBPcheck(bat x)
{
	if (!is_bat_nil(x)) {
		assert(x > 0);

		if (x < 0 || x >= getBBPsize() || BBP_logical(x) == NULL) {
			TRC_DEBUG(CHECK, "range error %d\n", (int) x);
		} else {
			assert(BBP_pid(x) == 0 || BBP_pid(x) == MT_getpid());
			return x;
		}
	}
	return 0;
}

gdk_export BAT *BATdescriptor(bat i);

static inline void *
Tpos(BATiter *bi, BUN p)
{
	assert(bi->base == NULL);
	if (bi->vh) {
		oid o;
		assert(!is_oid_nil(bi->tseq));
		if (((ccand_t *) bi->vh)->type == CAND_NEGOID) {
			BUN nexc = (bi->vhfree - sizeof(ccand_t)) / SIZEOF_OID;
			o = bi->tseq + p;
			if (nexc > 0) {
				const oid *exc = (const oid *) (bi->vh->base + sizeof(ccand_t));
				if (o >= exc[0]) {
					if (o + nexc > exc[nexc - 1]) {
						o += nexc;
					} else {
						BUN lo = 0;
						BUN hi = nexc - 1;
						while (hi - lo > 1) {
							BUN mid = (hi + lo) / 2;
							if (exc[mid] - mid > o)
								hi = mid;
							else
								lo = mid;
						}
						o += hi;
					}
				}
			}
		} else {
			const uint32_t *msk = (const uint32_t *) (bi->vh->base + sizeof(ccand_t));
			BUN nmsk = (bi->vhfree - sizeof(ccand_t)) / sizeof(uint32_t);
			o = 0;
			for (BUN i = 0; i < nmsk; i++) {
				uint32_t m = candmask_pop(msk[i]);
				if (o + m > p) {
					m = msk[i];
					for (i = 0; i < 32; i++) {
						if (m & (1U << i) && ++o == p)
							break;
					}
					break;
				}
				o += m;
			}
		}
		bi->tvid = o;
	} else if (is_oid_nil(bi->tseq)) {
		bi->tvid = oid_nil;
	} else {
		bi->tvid = bi->tseq + p;
	}
	return (void *) &bi->tvid;
}

__attribute__((__pure__))
static inline bool
Tmskval(const BATiter *bi, BUN p)
{
	assert(ATOMstorage(bi->type) == TYPE_msk);
	return ((const uint32_t *) bi->base)[p / 32] & (1U << (p % 32));
}

static inline void *
Tmsk(BATiter *bi, BUN p)
{
	bi->tmsk = Tmskval(bi, p);
	return &bi->tmsk;
}

/* return the oid value at BUN position p from the (v)oid bat b
 * works with any TYPE_void or TYPE_oid bat */
__attribute__((__pure__))
static inline oid
BUNtoid(BAT *b, BUN p)
{
	assert(ATOMtype(b->ttype) == TYPE_oid);
	/* BATcount is the number of valid entries, so with
	 * exceptions, the last value can well be larger than
	 * b->tseqbase + BATcount(b) */
	assert(p < BATcount(b));
	assert(b->ttype == TYPE_void || b->tvheap == NULL);
	if (is_oid_nil(b->tseqbase)) {
		if (b->ttype == TYPE_void)
			return oid_nil;
		MT_lock_set(&b->theaplock);
		oid o = ((const oid *) b->theap->base)[p + b->tbaseoff];
		MT_lock_unset(&b->theaplock);
		return o;
	}
	if (b->ttype == TYPE_oid || b->tvheap == NULL) {
		return b->tseqbase + p;
	}
	/* b->tvheap != NULL, so we know there will be no parallel
	 * modifications (so no locking) */
	BATiter bi = bat_iterator_nolock(b);
	return * (oid *) Tpos(&bi, p);
}

gdk_export gdk_return TMsubcommit_list(bat *restrict subcommit, BUN *restrict sizes, int cnt, lng logno)
	__attribute__((__warn_unused_result__));

gdk_export void BATcommit(BAT *b, BUN size);

gdk_export int ALIGNsynced(BAT *b1, BAT *b2);

gdk_export void BATassertProps(BAT *b);

gdk_export BAT *VIEWcreate(oid seq, BAT *b, BUN l, BUN h);
gdk_export void VIEWbounds(BAT *b, BAT *view, BUN l, BUN h);

#define ALIGNapp(x, f, e)						\
	do {								\
		if (!(f)) {						\
			MT_lock_set(&(x)->theaplock);			\
			if ((x)->batRestricted == BAT_READ ||		\
			   ((ATOMIC_GET(&(x)->theap->refs) & HEAPREFS) > 1)) { \
				GDKerror("access denied to %s, aborting.\n", BATgetId(x)); \
				MT_lock_unset(&(x)->theaplock);		\
				return (e);				\
			}						\
			MT_lock_unset(&(x)->theaplock);			\
		}							\
	} while (false)

#define BATloop(r, p, q)				\
	for (q = BATcount(r), p = 0; p < q; p++)

enum prop_t {
	GDK_MIN_BOUND, /* MINimum allowed value for range partitions [min, max> */
	GDK_MAX_BOUND, /* MAXimum of the range partitions [min, max>, ie. excluding this max value */
	GDK_NOT_NULL,  /* bat bound to be not null */
	/* CURRENTLY_NO_PROPERTIES_DEFINED, */
};

gdk_export ValPtr BATgetprop(BAT *b, enum prop_t idx);
gdk_export ValPtr BATgetprop_nolock(BAT *b, enum prop_t idx);
gdk_export void BATrmprop(BAT *b, enum prop_t idx);
gdk_export void BATrmprop_nolock(BAT *b, enum prop_t idx);
gdk_export ValPtr BATsetprop(BAT *b, enum prop_t idx, int type, const void *v);
gdk_export ValPtr BATsetprop_nolock(BAT *b, enum prop_t idx, int type, const void *v);

#define JOIN_EQ		0
#define JOIN_LT		(-1)
#define JOIN_LE		(-2)
#define JOIN_GT		1
#define JOIN_GE		2
#define JOIN_BAND	3
#define JOIN_NE		(-3)

gdk_export BAT *BATselect(BAT *b, BAT *s, const void *tl, const void *th, bool li, bool hi, bool anti, bool nil_matches);
gdk_export BAT *BATthetaselect(BAT *b, BAT *s, const void *val, const char *op);

gdk_export BAT *BATconstant(oid hseq, int tt, const void *val, BUN cnt, role_t role);
gdk_export gdk_return BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BAToutercross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATmarkjoin(BAT **r1p, BAT **r2p, BAT **r3p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool match_one, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int op, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATintersect(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate);
gdk_export BAT *BATdiff(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool not_in, BUN estimate);
gdk_export gdk_return BATjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BUN BATguess_uniques(BAT *b, struct canditer *ci);
gdk_export gdk_return BATbandjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, const void *c1, const void *c2, bool li, bool hi, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATrangejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *rl, BAT *rh, BAT *sl, BAT *sr, bool li, bool hi, bool anti, bool symmetric, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATproject(BAT *restrict l, BAT *restrict r);
gdk_export BAT *BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2);
gdk_export BAT *BATprojectchain(BAT **bats);

gdk_export BAT *BATslice(BAT *b, BUN low, BUN high);

gdk_export BAT *BATunique(BAT *b, BAT *s);

gdk_export gdk_return BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *cands, BAT *grps, BUN n, bool asc, bool nilslast, bool distinct)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATgroupedfirstn(BUN n, BAT *s, BAT *g, int nbats, BAT **bats, bool *asc, bool *nilslast)
	__attribute__((__warn_unused_result__));


gdk_export gdk_return GDKtoupper(char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 1)))
	__attribute__((__access__(read_write, 2)));
gdk_export gdk_return GDKtolower(char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 1)))
	__attribute__((__access__(read_write, 2)));
gdk_export gdk_return GDKcasefold(char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 1)))
	__attribute__((__access__(read_write, 2)));
gdk_export int GDKstrncasecmp(const char *str1, const char *str2, size_t l1, size_t l2);
gdk_export int GDKstrcasecmp(const char *s1, const char *s2);
gdk_export char *GDKstrcasestr(const char *haystack, const char *needle);
gdk_export BAT *BATtoupper(BAT *b, BAT *s);
gdk_export BAT *BATtolower(BAT *b, BAT *s);
gdk_export BAT *BATcasefold(BAT *b, BAT *s);
gdk_export gdk_return GDKasciify(char **restrict buf, size_t *restrict buflen, const char *restrict s);
gdk_export BAT *BATasciify(BAT *b, BAT *s);

gdk_export BAT *BATsample(BAT *b, BUN n);
gdk_export BAT *BATsample_with_seed(BAT *b, BUN n, uint64_t seed);

#define MAXPARAMS	32

#define CHECK_QRY_TIMEOUT_SHIFT	14
#define CHECK_QRY_TIMEOUT_STEP	(1 << CHECK_QRY_TIMEOUT_SHIFT)
#define CHECK_QRY_TIMEOUT_MASK	(CHECK_QRY_TIMEOUT_STEP - 1)

#define TIMEOUT_MSG "Timeout was reached!"
#define INTERRUPT_MSG "Query interrupted!"
#define DISCONNECT_MSG "Client is disconnected!"
#define EXITING_MSG "Server is exiting!"

#define QRY_TIMEOUT (-1)	/* query timed out */
#define QRY_INTERRUPT (-2)	/* client indicated interrupt */
#define QRY_DISCONNECT (-3)	/* client disconnected */

static const char *
TIMEOUT_MESSAGE(const QryCtx *qc)
{
	if (GDKexiting())
		return EXITING_MSG;
	if (qc) {
		switch (qc->endtime) {
		case QRY_TIMEOUT:
			return TIMEOUT_MSG;
		case QRY_INTERRUPT:
			return INTERRUPT_MSG;
		case QRY_DISCONNECT:
			return DISCONNECT_MSG;
		default:
			MT_UNREACHABLE();
		}
	}
	return NULL;
}

static inline void
TIMEOUT_ERROR(const QryCtx *qc, const char *file, const char *func, int lineno)
{
	const char *e = TIMEOUT_MESSAGE(qc);
	if (e) {
		GDKtracer_log(file, func, lineno, TRC_NAME(M_ERROR),
			      TRC_NAME(GDK), NULL, "%s\n", e);
	}
}

#define TIMEOUT_HANDLER(rtpe, qc)					\
	do {								\
		TIMEOUT_ERROR(qc, __FILE__, __func__, __LINE__);	\
		return rtpe;						\
	} while(0)

static inline bool
TIMEOUT_TEST(QryCtx *qc)
{
	if (qc == NULL)
		return false;
	if (qc->endtime < 0)
		return true;
	if (qc->endtime && GDKusec() > qc->endtime) {
		qc->endtime = QRY_TIMEOUT;
		return true;
	}
	switch (bstream_getoob(qc->bs)) {
	case -1:
		qc->endtime = QRY_DISCONNECT;
		return true;
	case 0:
		return false;
	default:
		qc->endtime = QRY_INTERRUPT;
		return true;
	}
}

#define GOTO_LABEL_TIMEOUT_HANDLER(label, qc)				\
	do {								\
		TIMEOUT_ERROR(qc, __FILE__, __func__, __LINE__);	\
		goto label;						\
	} while(0)

#define GDK_CHECK_TIMEOUT_BODY(qc, callback)		\
	do {						\
		if (GDKexiting() || TIMEOUT_TEST(qc)) {	\
			callback;			\
		}					\
	} while (0)

#define GDK_CHECK_TIMEOUT(qc, counter, callback)		\
	do {							\
		if (counter > CHECK_QRY_TIMEOUT_STEP) {		\
			GDK_CHECK_TIMEOUT_BODY(qc, callback);	\
			counter = 0;				\
		} else {					\
			counter++;				\
		}						\
	} while (0)

/* here are some useful constructs to iterate a number of times (the
 * REPEATS argument--only evaluated once) and checking for a timeout
 * every once in a while; the QC->endtime value is a variable of type lng
 * which is either 0 or the GDKusec() compatible time after which the
 * loop should terminate; check for this condition after the loop using
 * the TIMEOUT_CHECK macro; in order to break out of any of these loops,
 * use TIMEOUT_LOOP_BREAK since plain break won't do it; it is perfectly
 * ok to use continue inside the body */

/* use IDX as a loop variable (already declared), initializing it to 0
 * and incrementing it on each iteration */
#define TIMEOUT_LOOP_IDX(IDX, REPEATS, QC)				\
	for (BUN REPS = (IDX = 0, (REPEATS)); REPS > 0; REPS = 0) /* "loops" at most once */ \
		for (BUN CTR1 = 0, END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
			if (CTR1 > 0 && TIMEOUT_TEST(QC)) {		\
				break;					\
			} else						\
				for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++, IDX++)

/* declare and use IDX as a loop variable, initializing it to 0 and
 * incrementing it on each iteration */
#define TIMEOUT_LOOP_IDX_DECL(IDX, REPEATS, QC)				\
	for (BUN IDX = 0, REPS = (REPEATS); REPS > 0; REPS = 0) /* "loops" at most once */ \
		for (BUN CTR1 = 0, END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
			if (CTR1 > 0 && TIMEOUT_TEST(QC)) {		\
				break;					\
			} else						\
				for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++, IDX++)

/* there is no user-visible loop variable */
#define TIMEOUT_LOOP(REPEATS, QC)					\
	for (BUN CTR1 = 0, REPS = (REPEATS), END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
		if (CTR1 > 0 && TIMEOUT_TEST(QC)) {			\
			break;						\
		} else							\
			for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++)

/* break out of the loop (cannot use do/while trick here) */
#define TIMEOUT_LOOP_BREAK			\
	{					\
		END1 = END2 = 0;		\
		continue;			\
	}

/* check whether a timeout occurred, and execute the CALLBACK argument
 * if it did */
#define TIMEOUT_CHECK(QC, CALLBACK)					\
	do {								\
		if (GDKexiting() || ((QC) && (QC)->endtime < 0))	\
			CALLBACK;					\
	} while (0)

typedef gdk_return gdk_callback_func(int argc, void *argv[]);

gdk_export gdk_return gdk_add_callback(const char *name, gdk_callback_func *f,
				       int argc, void *argv[], int interval);
gdk_export gdk_return gdk_remove_callback(const char *, gdk_callback_func *f);

gdk_export void GDKusr1triggerCB(void (*func)(void));

#define SQLSTATE(sqlstate)	#sqlstate "!"
#define MAL_MALLOC_FAIL	"Could not allocate memory"

#include <setjmp.h>

typedef struct exception_buffer {
#ifdef HAVE_SIGLONGJMP
	sigjmp_buf state;
#else
	jmp_buf state;
#endif
	int code;
	const char *msg;
	int enabled;
} exception_buffer;

gdk_export exception_buffer *eb_init(exception_buffer *eb)
	__attribute__((__access__(write_only, 1)));

/* != 0 on when we return to the savepoint */
#ifdef HAVE_SIGLONGJMP
#define eb_savepoint(eb) ((eb)->enabled = 1, sigsetjmp((eb)->state, 0))
#else
#define eb_savepoint(eb) ((eb)->enabled = 1, setjmp((eb)->state))
#endif
gdk_export _Noreturn void eb_error(exception_buffer *eb, const char *msg, int val);


#include "gdk_calc.h"

gdk_export ValPtr VALcopy(allocator *va, ValPtr dst, const ValRecord *src)
	__attribute__((__access__(write_only, 1)));
gdk_export ValPtr VALinit(allocator *va, ValPtr d, int tpe, const void *s)
	__attribute__((__access__(write_only, 1)));

gdk_export allocator *create_allocator(allocator *pa, const char *, bool use_lock);
gdk_export allocator *ma_get_parent(const allocator *sa);
gdk_export bool ma_tmp_active(const allocator *sa);
gdk_export allocator *ma_reset(allocator *sa);
gdk_export void *ma_alloc(allocator *sa,  size_t sz);
gdk_export void *ma_zalloc(allocator *sa,  size_t sz);
gdk_export void *ma_realloc(allocator *sa,  void *ptr, size_t sz, size_t osz);
gdk_export void ma_destroy(allocator *sa);
gdk_export char *ma_strndup(allocator *sa, const char *s, size_t l);
gdk_export char *ma_strdup(allocator *sa, const char *s);
gdk_export char *ma_strconcat(allocator *sa, const char *s1, const char *s2);
gdk_export size_t ma_size(allocator *sa);
gdk_export const char* ma_name(allocator *sa);
gdk_export allocator_state* ma_open(allocator *sa);  /* open new frame of tempory allocations */
gdk_export void ma_close(allocator *sa); /* close temporary frame, reset to initial state */
gdk_export void ma_close_to(allocator *sa, allocator_state *); /* close temporary frame, reset to old state */
gdk_export void ma_free(allocator *sa, void *);
gdk_export exception_buffer *ma_get_eb(allocator *sa)
       __attribute__((__pure__));

gdk_export void ma_set_ta(allocator *sa,  allocator *ta);
gdk_export allocator *ma_get_ta(allocator *sa);

#define MA_NEW( sa, type )				((type*)ma_alloc( sa, sizeof(type)))
#define MA_ZNEW( sa, type )				((type*)ma_zalloc( sa, sizeof(type)))
#define MA_NEW_ARRAY( sa, type, size )			(type*)ma_alloc( sa, ((size)*sizeof(type)))
#define MA_ZNEW_ARRAY( sa, type, size )			(type*)ma_zalloc( sa, ((size)*sizeof(type)))
#define MA_RENEW_ARRAY( sa, type, ptr, sz, osz )	(type*)ma_realloc( sa, ptr, ((sz)*sizeof(type)), ((osz)*sizeof(type)))
#define MA_STRDUP( sa, s)				ma_strdup(sa, s)
#define MA_STRNDUP( sa, s, l)				ma_strndup(sa, s, l)


#if !defined(NDEBUG) && !defined(__COVERITY__) && defined(__GNUC__)
#define ma_alloc(sa, sz)					\
	({							\
		allocator *_sa = (sa);				\
		size_t _sz = (sz);				\
		void *_res = ma_alloc(_sa, _sz);		\
		TRC_DEBUG(ALLOC,				\
			  "ma_alloc(%p,%zu) -> %p\n",		\
			  _sa, _sz, _res);			\
		_res;						\
	})
#define ma_zalloc(sa, sz)					\
	({							\
		allocator *_sa = (sa);				\
		size_t _sz = (sz);				\
		void *_res = ma_zalloc(_sa, _sz);		\
		TRC_DEBUG(ALLOC,				\
			  "ma_zalloc(%p,%zu) -> %p\n",		\
			  _sa, _sz, _res);			\
		_res;						\
	})
#define ma_realloc(sa, ptr, sz, osz)					\
	({								\
		allocator *_sa = (sa);					\
		void *_ptr = (ptr);					\
		size_t _sz = (sz);					\
		size_t _osz = (osz);					\
		void *_res = ma_realloc(_sa, _ptr, _sz, _osz);		\
		TRC_DEBUG(ALLOC,					\
			  "ma_realloc(%p,%p,%zu,%zu) -> %p\n",		\
			  _sa, _ptr, _sz, _osz, _res);			\
		_res;							\
	})
#define ma_strdup(sa, s)						\
	({								\
		allocator *_sa = (sa);					\
		const char *_s = (s);					\
		char *_res = ma_strdup(_sa, _s);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_strdup(%p,len=%zu) -> %p\n",		\
			  _sa, strlen(_s), _res);			\
		_res;							\
	})
#define ma_strndup(sa, s, l)						\
	({								\
		allocator *_sa = (sa);					\
		const char *_s = (s);					\
		size_t _l = (l);					\
		char *_res = ma_strndup(_sa, _s, _l);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_strndup(%p,len=%zu) -> %p\n",		\
			  _sa, _l, _res);				\
		_res;							\
	})
#endif

#endif /* _GDK_H_ */
