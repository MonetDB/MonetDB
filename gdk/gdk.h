/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @t The Goblin Database Kernel
 * @v Version 3.05
 * @a Martin L. Kersten, Peter Boncz, Niels Nes, Sjoerd Mullender
 *
 * @+ The Inner Core
 * The innermost library of the MonetDB database system is formed by
 * the library called GDK, an abbreviation of Goblin Database Kernel.
 * Its development was originally rooted in the design of a pure
 * active-object-oriented programming language, before development
 * was shifted towards a re-usable database kernel engine.
 *
 * GDK is a C library that provides ACID properties on a DSM model
 * @tex
 * [@cite{Copeland85}]
 * @end tex
 * , using main-memory
 * database algorithms
 * @tex
 * [@cite{Garcia-Molina92}]
 * @end tex
 *  built on virtual-memory
 * OS primitives and multi-threaded parallelism.
 * Its implementation has undergone various changes over its decade
 * of development, many of which were driven by external needs to
 * obtain a robust and fast database system.
 *
 * The coding scheme explored in GDK has also laid a foundation to
 * communicate over time experiences and to provide (hopefully)
 * helpful advice near to the place where the code-reader needs it.
 * Of course, over such a long time the documentation diverges from
 * reality. Especially in areas where the environment of this package
 * is being described.
 * Consider such deviations as historic landmarks, e.g. crystallization
 * of brave ideas and mistakes rectified at a later stage.
 *
 * @+ Short Outline
 * The facilities provided in this implementation are:
 * @itemize
 * @item
 * GDK or Goblin Database Kernel routines for session management
 * @item
 *  BAT routines that define the primitive operations on the
 * database tables (BATs).
 * @item
 *  BBP routines to manage the BAT Buffer Pool (BBP).
 * @item
 *  ATOM routines to manipulate primitive types, define new types
 * using an ADT interface.
 * @item
 *  HEAP routines for manipulating heaps: linear spaces of memory
 * that are GDK's vehicle of mass storage (on which BATs are built).
 * @item
 *  DELTA routines to access inserted/deleted elements within a
 * transaction.
 * @item
 *  HASH routines for manipulating GDK's built-in linear-chained
 * hash tables, for accelerating lookup searches on BATs.
 * @item
 *  TM routines that provide basic transaction management primitives.
 * @item
 *  TRG routines that provided active database support. [DEPRECATED]
 * @item
 *  ALIGN routines that implement BAT alignment management.
 * @end itemize
 *
 * The Binary Association Table (BAT) is the lowest level of storage
 * considered in the Goblin runtime system
 * @tex
 * [@cite{Goblin}]
 * @end tex
 * .  A BAT is a
 * self-descriptive main-memory structure that represents the
 * @strong{binary relationship} between two atomic types.  The
 * association can be defined over:
 * @table @code
 * @item void:
 *  virtual-OIDs: a densely ascending column of OIDs (takes zero-storage).
 * @item bit:
 *  Booleans, implemented as one byte values.
 * @item bte:
 *  Tiny (1-byte) integers (8-bit @strong{integer}s).
 * @item sht:
 *  Short integers (16-bit @strong{integer}s).
 * @item int:
 *  This is the C @strong{int} type (32-bit).
 * @item oid:
 *  Unique @strong{long int} values uses as object identifier. Highest
 *	    bit cleared always.  Thus, oids-s are 31-bit numbers on
 *	    32-bit systems, and 63-bit numbers on 64-bit systems.
 * @item ptr:
 * Memory pointer values. DEPRECATED.  Can only be stored in transient
 * BATs.
 * @item flt:
 *  The IEEE @strong{float} type.
 * @item dbl:
 *  The IEEE @strong{double} type.
 * @item lng:
 *  Longs: the C @strong{long long} type (64-bit integers).
 * @item hge:
 *  "huge" integers: the GCC @strong{__int128} type (128-bit integers).
 * @item str:
 *  UTF-8 strings (Unicode). A zero-terminated byte sequence.
 * @item bat:
 *  Bat descriptor. This allows for recursive administered tables, but
 *  severely complicates transaction management. Therefore, they CAN
 *  ONLY BE STORED IN TRANSIENT BATs.
 * @end table
 *
 * This model can be used as a back-end model underlying other -higher
 * level- models, in order to achieve @strong{better performance} and
 * @strong{data independence} in one go. The relational model and the
 * object-oriented model can be mapped on BATs by vertically splitting
 * every table (or class) for each attribute. Each such a column is
 * then stored in a BAT with type @strong{bat[oid,attribute]}, where
 * the unique object identifiers link tuples in the different BATs.
 * Relationship attributes in the object-oriented model hence are
 * mapped to @strong{bat[oid,oid]} tables, being equivalent to the
 * concept of @emph{join indexes} @tex [@cite{Valduriez87}] @end tex .
 *
 * The set of built-in types can be extended with user-defined types
 * through an ADT interface.  They are linked with the kernel to
 * obtain an enhanced library, or they are dynamically loaded upon
 * request.
 *
 * Types can be derived from other types. They represent something
 * different than that from which they are derived, but their internal
 * storage management is equal. This feature facilitates the work of
 * extension programmers, by enabling reuse of implementation code,
 * but is also used to keep the GDK code portable from 32-bits to
 * 64-bits machines: the @strong{oid} and @strong{ptr} types are
 * derived from @strong{int} on 32-bits machines, but is derived from
 * @strong{lng} on 64 bits machines. This requires changes in only two
 * lines of code each.
 *
 * To accelerate lookup and search in BATs, GDK supports one built-in
 * search accelerator: hash tables. We choose an implementation
 * efficient for main-memory: bucket chained hash
 * @tex
 * [@cite{LehCar86,Analyti92}]
 * @end tex
 * . Alternatively, when the table is sorted, it will resort to
 * merge-scan operations or binary lookups.
 *
 * BATs are built on the concept of heaps, which are large pieces of
 * main memory. They can also consist of virtual memory, in case the
 * working set exceeds main-memory. In this case, GDK supports
 * operations that cluster the heaps of a BAT, in order to improve
 * performance of its main-memory.
 *
 *
 * @- Rationale
 * The rationale for choosing a BAT as the building block for both
 * relational and object-oriented system is based on the following
 * observations:
 *
 * @itemize
 * @item -
 * Given the fact that CPU speed and main-memory increase in current
 * workstation hardware for the last years has been exceeding IO
 * access speed increase, traditional disk-page oriented algorithms do
 * no longer take best advantage of hardware, in most database
 * operations.
 *
 * Instead of having a disk-block oriented kernel with a large memory
 * cache, we choose to build a main-memory kernel, that only under
 * large data volumes slowly degrades to IO-bound performance,
 * comparable to traditional systems
 * @tex
 * [@cite{boncz95,boncz96}]
 * @end tex
 * .
 *
 * @item -
 * Traditional (disk-based) relational systems move too much data
 * around to save on (main-memory) join operations.
 *
 * The fully decomposed store (DSM
 * @tex
 * [@cite{Copeland85})]
 * @end tex
 * assures that only those attributes of a relation that are needed,
 * will have to be accessed.
 *
 * @item -
 * The data management issues for a binary association is much
 * easier to deal with than traditional @emph{struct}-based approaches
 * encountered in relational systems.
 *
 * @item -
 * Object-oriented systems often maintain a double cache, one with the
 * disk-based representation and a C pointer-based main-memory
 * structure.  This causes expensive conversions and replicated
 * storage management.  GDK does not do such `pointer swizzling'. It
 * used virtual-memory (@strong{mmap()}) and buffer management advice
 * (@strong{madvise()}) OS primitives to cache only once. Tables take
 * the same form in memory as on disk, making the use of this
 * technique transparent
 * @tex
 * [@cite{oo7}]
 * @end tex
 * .
 * @end itemize
 *
 * A RDBMS or OODBMS based on BATs strongly depends on our ability to
 * efficiently support tuples and to handle small joins, respectively.
 *
 * The remainder of this document describes the Goblin Database kernel
 * implementation at greater detail. It is organized as follows:
 * @table @code
 * @item @strong{GDK Interface}:
 *
 * It describes the global interface with which GDK sessions can be
 * started and ended, and environment variables used.
 *
 * @item @strong{Binary Association Tables}:
 *
 * As already mentioned, these are the primary data structure of GDK.
 * This chapter describes the kernel operations for creation,
 * destruction and basic manipulation of BATs and BUNs (i.e. tuples:
 * Binary UNits).
 *
 * @item @strong{BAT Buffer Pool:}
 *
 * All BATs are registered in the BAT Buffer Pool. This directory is
 * used to guide swapping in and out of BATs. Here we find routines
 * that guide this swapping process.
 *
 * @item @strong{GDK Extensibility:}
 *
 * Atoms can be defined using a unified ADT interface.  There is also
 * an interface to extend the GDK library with dynamically linked
 * object code.
 *
 * @item @strong{GDK Utilities:}
 *
 * Memory allocation and error handling primitives are
 * provided. Layers built on top of GDK should use them, for proper
 * system monitoring.  Thread management is also included here.
 *
 * @item @strong{Transaction Management:}
 *
 * For the time being, we just provide BAT-grained concurrency and
 * global transactions. Work is needed here.
 *
 * @item @strong{BAT Alignment:}
 * Due to the mapping of multi-ary datamodels onto the BAT model, we
 * expect many correspondences among BATs, e.g.
 * @emph{bat(oid,attr1),..  bat(oid,attrN)} vertical
 * decompositions. Frequent activities will be to jump from one
 * attribute to the other (`bunhopping'). If the head columns are
 * equal lists in two BATs, merge or even array lookups can be used
 * instead of hash lookups. The alignment interface makes these
 * relations explicitly manageable.
 *
 * In GDK, complex data models are mapped with DSM on binary tables.
 * Usually, one decomposes @emph{N}-ary relations into @emph{N} BATs
 * with an @strong{oid} in the head column, and the attribute in the
 * tail column.  There may well be groups of tables that have the same
 * sets of @strong{oid}s, equally ordered. The alignment interface is
 * intended to make this explicit.  Implementations can use this
 * interface to detect this situation, and use cheaper algorithms
 * (like merge-join, or even array lookup) instead.
 *
 * @item @strong{BAT Iterators:}
 *
 * Iterators are C macros that generally encapsulate a complex
 * for-loop.  They would be the equivalent of cursors in the SQL
 * model. The macro interface (instead of a function call interface)
 * is chosen to achieve speed when iterating main-memory tables.
 *
 * @item @strong{Common BAT Operations:}
 *
 * These are much used operations on BATs, such as aggregate functions
 * and relational operators. They are implemented in terms of BAT- and
 * BUN-manipulation GDK primitives.
 * @end table
 *
 * @+ Interface Files
 * In this section we summarize the user interface to the GDK library.
 * It consist of a header file (gdk.h) and an object library
 * (gdklib.a), which implements the required functionality. The header
 * file must be included in any program that uses the library. The
 * library must be linked with such a program.
 *
 * @- Database Context
 *
 * The MonetDB environment settings are collected in a configuration
 * file. Amongst others it contains the location of the database
 * directory.  First, the database directory is closed for other
 * servers running at the same time.  Second, performance enhancements
 * may take effect, such as locking the code into memory (if the OS
 * permits) and preloading the data dictionary.  An error at this
 * stage normally lead to an abort.
 */

#ifndef _GDK_H_
#define _GDK_H_

/* standard includes upon which all configure tests depend */
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#include <stddef.h>
#include <string.h>
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

#define THRDMASK	(1)
#define CHECKMASK	(1<<1)
#define CHECKDEBUG	if (GDKdebug & CHECKMASK)
#define PROPMASK	(1<<3)
#define PROPDEBUG	if (GDKdebug & PROPMASK)
#define IOMASK		(1<<4)
#define BATMASK		(1<<5)
#define PARMASK		(1<<7)
#define TMMASK		(1<<9)
#define TEMMASK		(1<<10)
#define PERFMASK	(1<<12)
#define DELTAMASK	(1<<13)
#define LOADMASK	(1<<14)
#define ACCELMASK	(1<<20)
#define ALGOMASK	(1<<21)

#define NOSYNCMASK	(1<<24)

#define DEADBEEFMASK	(1<<25)
#define DEADBEEFCHK	if (!(GDKdebug & DEADBEEFMASK))

#define ALLOCMASK	(1<<26)

/* M5, only; cf.,
 * monetdb5/mal/mal.h
 */
#define OPTMASK		(1<<27)

#define HEAPMASK	(1<<28)

#define FORCEMITOMASK	(1<<29)
#define FORCEMITODEBUG	if (GDKdebug & FORCEMITOMASK)

/*
 * @- GDK session handling
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab GDKinit (char *db, char *dbpath, int allocmap)
 * @item int
 * @tab GDKexit (int status)
 * @end multitable
 *
 * The session is bracketed by GDKinit and GDKexit. Initialization
 * involves setting up the administration for database access, such as
 * memory allocation for the database buffer pool.  During the exit
 * phase any pending transaction is aborted and the database is freed
 * for access by other users.  A zero is returned upon encountering an
 * erroneous situation.
 *
 * @- Definitions
 * The interface definitions for the application programs are shown
 * below.  The global variables should not be modified directly.
 */
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
	TYPE_bat,		/* BAT id: index in BBPcache */
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
	TYPE_str,
	TYPE_any = 255,		/* limit types to <255! */
};

typedef bool msk;
typedef int8_t bit;
typedef int8_t bte;
typedef int16_t sht;
typedef int64_t lng;
typedef uint64_t ulng;

#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#define OIDFMT		"%zu"

typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

#ifdef HAVE_UUID_UUID_H
#include <uuid/uuid.h>
#endif

#ifdef HAVE_UUID
#define UUID_SIZE	((int) sizeof(uuid_t)) /* size of a UUID */
#else
#define UUID_SIZE	16	/* size of a UUID */
#endif
#define UUID_STRLEN	36	/* length of string representation */

typedef union {
#ifdef HAVE_HGE
	hge h;			/* force alignment, not otherwise used */
#else
	lng l[2];		/* force alignment, not otherwise used */
#endif
#ifdef HAVE_UUID
	uuid_t u;
#else
	uint8_t u[UUID_SIZE];
#endif
} uuid;

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

/*
 * @- Checking and Error definitions:
 */
typedef enum { GDK_FAIL, GDK_SUCCEED } gdk_return;

#define ATOMextern(t)	(ATOMstorage(t) >= TYPE_str)

typedef enum {
	PERSISTENT = 0,
	TRANSIENT,
} role_t;

/* Heap storage modes */
typedef enum {
	STORE_MEM     = 0,	/* load into GDKmalloced memory */
	STORE_MMAP    = 1,	/* mmap() into virtual memory */
	STORE_PRIV    = 2,	/* BAT copy of copy-on-write mmap */
	STORE_CMEM    = 3,	/* load into malloc (not GDKmalloc) memory*/
	STORE_NOWN    = 4,	/* memory not owned by the BAT */
	STORE_MMAPABS = 5,	/* mmap() into virtual memory from an
				 * absolute path (not part of dbfarm) */
	STORE_INVALID		/* invalid value, used to indicate error */
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
	bool hashash:1,		/* the string heap contains hash values */
		cleanhash:1,	/* string heaps must clean hash */
		dirty:1,	/* specific heap dirty marker */
		remove:1;	/* remove storage file when freeing */
	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
	bat parentid;		/* cache id of VIEW parent bat */
} Heap;

typedef struct Hash Hash;
typedef struct Imprints Imprints;

/*
 * @+ Binary Association Tables
 * Having gone to the previous preliminary definitions, we will now
 * introduce the structure of Binary Association Tables (BATs) in
 * detail. They are the basic storage unit on which GDK is modeled.
 *
 * The BAT holds an unlimited number of binary associations, called
 * BUNs (@strong{Binary UNits}).  The two attributes of a BUN are
 * called @strong{head} (left) and @strong{tail} (right) in the
 * remainder of this document.
 *
 *  @c image{http://monetdb.cwi.nl/projects/monetdb-mk/imgs/bat1,,,,feps}
 *
 * The above figure shows what a BAT looks like. It consists of two
 * columns, called head and tail, such that we have always binary
 * tuples (BUNs). The overlooking structure is the @strong{BAT
 * record}.  It points to a heap structure called the @strong{BUN
 * heap}.  This heap contains the atomic values inside the two
 * columns. If they are fixed-sized atoms, these atoms reside directly
 * in the BUN heap. If they are variable-sized atoms (such as string
 * or polygon), however, the columns has an extra heap for storing
 * those (such @strong{variable-sized atom heaps} are then referred to
 * as @strong{Head Heap}s and @strong{Tail Heap}s). The BUN heap then
 * contains integer byte-offsets (fixed-sized, of course) into a head-
 * or tail-heap.
 *
 * The BUN heap contains a contiguous range of BUNs. It starts after
 * the @strong{first} pointer, and finishes at the end in the
 * @strong{free} area of the BUN. All BUNs after the @strong{inserted}
 * pointer have been added in the last transaction (and will be
 * deleted on a transaction abort). All BUNs between the
 * @strong{deleted} pointer and the @strong{first} have been deleted
 * in this transaction (and will be reinserted at a transaction
 * abort).
 *
 * The location of a certain BUN in a BAT may change between
 * successive library routine invocations.  Therefore, one should
 * avoid keeping references into the BAT storage area for long
 * periods.
 *
 * Passing values between the library routines and the enclosing C
 * program is primarily through value pointers of type ptr. Pointers
 * into the BAT storage area should only be used for retrieval. Direct
 * updates of data stored in a BAT is forbidden. The user should
 * adhere to the interface conventions to guarantee the integrity
 * rules and to maintain the (hidden) auxiliary search structures.
 *
 * @- GDK variant record type
 * When manipulating values, MonetDB puts them into value records.
 * The built-in types have a direct entry in the union. Others should
 * be represented as a pointer of memory in pval or as a string, which
 * is basically the same. In such cases the len field indicates the
 * size of this piece of memory.
 */
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
	} val;
	size_t len;
	int vtype;
} *ValPtr, ValRecord;

/* interface definitions */
gdk_export void *VALconvert(int typ, ValPtr t);
gdk_export char *VALformat(const ValRecord *res);
gdk_export ValPtr VALcopy(ValPtr dst, const ValRecord *src);
gdk_export ValPtr VALinit(ValPtr d, int tpe, const void *s);
gdk_export void VALempty(ValPtr v);
gdk_export void VALclear(ValPtr v);
gdk_export ValPtr VALset(ValPtr v, int t, void *p);
gdk_export void *VALget(ValPtr v);
gdk_export int VALcmp(const ValRecord *p, const ValRecord *q);
gdk_export bool VALisnil(const ValRecord *v);

/*
 * @- The BAT record
 * The elements of the BAT structure are introduced in the remainder.
 * Instead of using the underlying types hidden beneath it, one should
 * use a @emph{BAT} type that is supposed to look like this:
 * @verbatim
 * typedef struct {
 *           // static BAT properties
 *           bat    batCacheid;       // bat id: index in BBPcache
 *           bool   batTransient;     // persistence mode
 *           bool   batCopiedtodisk;  // BAT is saved on disk?
 *           // dynamic BAT properties
 *           int    batHeat;          // heat of BAT in the BBP
 *           bool   batDirtydesc;     // BAT descriptor specific dirty flag
 *           Heap*  batBuns;          // Heap where the buns are stored
 *           // DELTA status
 *           BUN    batInserted;      // first inserted BUN
 *           BUN    batCount;         // Tuple count
 *           // Tail properties
 *           int    ttype;            // Tail type number
 *           str    tident;           // name for tail column
 *           bool   tkey;             // tail values are unique
 *           bool   tnonil;           // tail has no nils
 *           bool   tsorted;          // are tail values currently ordered?
 *           bool   tvarsized;        // for speed: tail type is varsized?
 *           // Tail storage
 *           int    tloc;             // byte-offset in BUN for tail elements
 *           Heap   *theap;           // heap for varsized tail values
 *           Hash   *thash;           // linear chained hash table on tail
 *           Imprints *timprints;     // column imprints index on tail
 *           orderidx torderidx;      // order oid index on tail
 *  } BAT;
 * @end verbatim
 *
 * The internal structure of the @strong{BAT} record is in fact much
 * more complex, but GDK programmers should refrain of making use of
 * that.
 *
 * Since we don't want to pay cost to keep both views in line with
 * each other under BAT updates, we work with shared pieces of memory
 * between the two views. An update to one will thus automatically
 * update the other.  In the same line, we allow @strong{synchronized
 * BATs} (BATs with identical head columns, and marked as such in the
 * @strong{BAT Alignment} interface) now to be clustered horizontally.
 *
 *  @c image{http://monetdb.cwi.nl/projects/monetdb-mk/imgs/bat2,,,,feps}
 */

typedef struct PROPrec PROPrec;

/* see also comment near BATassertProps() for more information about
 * the properties */
typedef struct {
	str id;			/* label for column */

	uint16_t width;		/* byte-width of the atom array */
	int8_t type;		/* type id. */
	uint8_t shift;		/* log2 of bun width */
	bool varsized:1,	/* varsized/void (true) or fixedsized (false) */
		key:1,		/* no duplicate values present */
		nonil:1,	/* there are no nils in the column */
		nil:1,		/* there is a nil in the column */
		sorted:1,	/* column is sorted in ascending order */
		revsorted:1;	/* column is sorted in descending order */
	BUN nokey[2];		/* positions that prove key==FALSE */
	BUN nosorted;		/* position that proves sorted==FALSE */
	BUN norevsorted;	/* position that proves revsorted==FALSE */
	oid seq;		/* start of dense sequence */

	Heap *heap;		/* space for the column. */
	BUN baseoff;		/* offset in heap->base (in whole items) */
	Heap *vheap;		/* space for the varsized data. */
	Hash *hash;		/* hash table */
	Imprints *imprints;	/* column imprints index */
	Heap *orderidx;		/* order oid index */

	PROPrec *props;		/* list of dynamic properties stored in the bat descriptor */
} COLrec;

#define ORDERIDXOFF		3

/* assert that atom width is power of 2, i.e., width == 1<<shift */
#define assert_shift_width(shift,width) assert(((shift) == 0 && (width) == 0) || ((unsigned)1<<(shift)) == (unsigned)(width))

#define GDKLIBRARY_MINMAX_POS	061042U /* first in Nov2019: no min/max position; no BBPinfo value */
#define GDKLIBRARY_TAILN	061043U /* first after Oct2020: str offset heaps names don't take width into account */
/* if the version number is updated, also fix snapshot_bats() in bat_logger.c */
#define GDKLIBRARY		061044U /* first after Oct2020 */

typedef struct BAT {
	/* static bat properties */
	bat batCacheid;		/* index into BBP */
	oid hseqbase;		/* head seq base */

	/* dynamic bat properties */
	MT_Id creator_tid;	/* which thread created it */
	bool
	 batCopiedtodisk:1,	/* once written */
	 batDirtyflushed:1,	/* was dirty before commit started? */
	 batDirtydesc:1,	/* bat descriptor dirty marker */
	 batTransient:1;	/* should the BAT persist on disk? */
	uint8_t	/* adjacent bit fields are packed together (if they fit) */
	 batRestricted:2;	/* access privileges */
	role_t batRole;		/* role of the bat */
	uint16_t unused; 	/* value=0 for now (sneakily used by mat.c) */
	int batSharecnt;	/* incoming view count */

	/* delta status administration */
	BUN batInserted;	/* start of inserted elements */
	BUN batCount;		/* tuple count */
	BUN batCapacity;	/* tuple capacity */

	/* dynamic column properties */
	COLrec T;		/* column info */
	MT_Lock theaplock;	/* lock protecting heap reference changes */
	MT_RWLock thashlock;	/* lock specifically for hash management */
	MT_Lock batIdxLock;	/* lock to manipulate other indexes/properties */
} BAT;

typedef struct BATiter {
	BAT *b;
	union {
		oid tvid;
		bool tmsk;
	};
} BATiter;

/* macros to hide complexity of the BAT structure */
#define ttype		T.type
#define tkey		T.key
#define tvarsized	T.varsized
#define tseqbase	T.seq
#define tsorted		T.sorted
#define trevsorted	T.revsorted
#define tident		T.id
#define torderidx	T.orderidx
#define twidth		T.width
#define tshift		T.shift
#define tnonil		T.nonil
#define tnil		T.nil
#define tnokey		T.nokey
#define tnosorted	T.nosorted
#define tnorevsorted	T.norevsorted
#define theap		T.heap
#define tbaseoff	T.baseoff
#define tvheap		T.vheap
#define thash		T.hash
#define timprints	T.imprints
#define tprops		T.props


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

/*
 * @- Heap Management
 * Heaps are the low-level entities of mass storage in
 * BATs. Currently, they can either be stored on disk, loaded into
 * memory, or memory mapped.
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab
 *  HEAPalloc (Heap *h, size_t nitems, size_t itemsize);
 * @item int
 * @tab
 *  HEAPfree (Heap *h, bool remove);
 * @item int
 * @tab
 *  HEAPextend (Heap *h, size_t size, bool mayshare);
 * @item int
 * @tab
 *  HEAPload (Heap *h, str nme,ext, bool trunc);
 * @item int
 * @tab
 *  HEAPsave (Heap *h, str nme,ext, bool dosync);
 * @item int
 * @tab
 *  HEAPcopy (Heap *dst,*src);
 * @item int
 * @tab
 *  HEAPdelete (Heap *dst, str o, str ext);
 * @item int
 * @tab
 *  HEAPwarm (Heap *h);
 * @end multitable
 *
 *
 * These routines should be used to alloc free or extend heaps; they
 * isolate you from the different ways heaps can be accessed.
 */
gdk_export gdk_return HEAPextend(Heap *h, size_t size, bool mayshare)
	__attribute__((__warn_unused_result__));
gdk_export size_t HEAPvmsize(Heap *h);
gdk_export size_t HEAPmemsize(Heap *h);
gdk_export void HEAPdecref(Heap *h, bool remove);
gdk_export void HEAPincref(Heap *h);

/*
 * @- Internal HEAP Chunk Management
 * Heaps are used in BATs to store data for variable-size atoms.  The
 * implementor must manage malloc()/free() functionality for atoms in
 * this heap. A standard implementation is provided here.
 *
 * @table @code
 * @item void
 * HEAP_initialize  (Heap* h, size_t nbytes, size_t nprivate, int align )
 * @item void
 * HEAP_destroy     (Heap* h)
 * @item var_t
 * HEAP_malloc      (Heap* heap, size_t nbytes)
 * @item void
 * HEAP_free        (Heap *heap, var_t block)
 * @item int
 * HEAP_private     (Heap* h)
 * @item void
 * HEAP_printstatus (Heap* h)
 * @end table
 *
 * The heap space starts with a private space that is left untouched
 * by the normal chunk allocation.  You can use this private space
 * e.g. to store the root of an rtree HEAP_malloc allocates a chunk of
 * memory on the heap, and returns an index to it.  HEAP_free frees a
 * previously allocated chunk HEAP_private returns an integer index to
 * private space.
 */

gdk_export void HEAP_initialize(
	Heap *heap,		/* nbytes -- Initial size of the heap. */
	size_t nbytes,		/* alignment -- for objects on the heap. */
	size_t nprivate,	/* nprivate -- Size of private space */
	int alignment		/* alignment restriction for allocated chunks */
	);

gdk_export var_t HEAP_malloc(BAT *b, size_t nbytes);
gdk_export void HEAP_free(Heap *heap, var_t block);

/*
 * @- BAT construction
 * @multitable @columnfractions 0.08 0.7
 * @item @code{BAT* }
 * @tab COLnew (oid headseq, int tailtype, BUN cap, role_t role)
 * @item @code{BAT* }
 * @tab BATextend (BAT *b, BUN newcap)
 * @end multitable
 *
 * A temporary BAT is instantiated using COLnew with the type aliases
 * of the required binary association. The aliases include the
 * built-in types, such as TYPE_int....TYPE_ptr, and the atomic types
 * introduced by the user. The initial capacity to be accommodated
 * within a BAT is indicated by cap.  Their extend is automatically
 * incremented upon storage overflow.  Failure to create the BAT
 * results in a NULL pointer.
 *
 * The routine BATclone creates an empty BAT storage area with the
 * properties inherited from its argument.
 */
gdk_export BAT *COLnew(oid hseq, int tltype, BUN capacity, role_t role)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATdense(oid hseq, oid tseq, BUN cnt)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATextend(BAT *b, BUN newcap)
	__attribute__((__warn_unused_result__));

/* internal */
gdk_export uint8_t ATOMelmshift(int sz)
	__attribute__((__const__));

gdk_export gdk_return GDKupgradevarheap(BAT *b, var_t v, BUN cap, bool copyall)
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

/* Functions to perform a binary search on a sorted BAT.
 * See gdk_search.c for details. */
gdk_export BUN SORTfnd(BAT *b, const void *v);
gdk_export BUN SORTfndfirst(BAT *b, const void *v);
gdk_export BUN SORTfndlast(BAT *b, const void *v);

gdk_export BUN ORDERfnd(BAT *b, const void *v);
gdk_export BUN ORDERfndfirst(BAT *b, const void *v);
gdk_export BUN ORDERfndlast(BAT *b, const void *v);

gdk_export BUN BUNfnd(BAT *b, const void *right);

#define BUNfndVOID(b, v)						\
	(((is_oid_nil(*(const oid*)(v)) ^ is_oid_nil((b)->tseqbase)) |	\
		(*(const oid*)(v) < (b)->tseqbase) |			\
		(*(const oid*)(v) >= (b)->tseqbase + (b)->batCount)) ?	\
	 BUN_NONE :							\
	 (BUN) (*(const oid*)(v) - (b)->tseqbase))

#define BATttype(b)	(BATtdense(b) ? TYPE_oid : (b)->ttype)
#define Tbase(b)	((b)->tvheap->base)

#define Tsize(b)	((b)->twidth)

#define tailsize(b,p)	((b)->ttype ?				\
			 (ATOMstorage((b)->ttype) == TYPE_msk ?	\
			  (((size_t) (p) + 31) / 32) * 4 :	\
			  ((size_t) (p)) << (b)->tshift) :	\
			 0)

#define Tloc(b,p)	((void *)((b)->theap->base+(((size_t)(p)+(b)->tbaseoff)<<(b)->tshift)))

typedef var_t stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_VAR_T
#define GDK_VARALIGN SIZEOF_STRIDX_T

#define BUNtvaroff(bi,p) VarHeapVal(Tloc((bi).b, 0), (p), (bi).b->twidth)

#define BUNtloc(bi,p)	(ATOMstorage((bi).b->ttype) == TYPE_msk ? Tmsk(&(bi), p) : Tloc((bi).b,p))
#define BUNtpos(bi,p)	Tpos(&(bi),p)
#define BUNtvar(bi,p)	(assert((bi).b->ttype && (bi).b->tvarsized), (void *) (Tbase((bi).b)+BUNtvaroff(bi,p)))
#define BUNtail(bi,p)	((bi).b->ttype?(bi).b->tvarsized?BUNtvar(bi,p):BUNtloc(bi,p):BUNtpos(bi,p))

#define BUNlast(b)	(assert((b)->batCount <= BUN_MAX), (b)->batCount)

#define BATcount(b)	((b)->batCount)

#include "gdk_atoms.h"

#include "gdk_cand.h"

/* return the oid value at BUN position p from the (v)oid bat b
 * works with any TYPE_void or TYPE_oid bat */
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
			return b->tseqbase;
		return ((const oid *) b->theap->base)[p + b->tbaseoff];
	}
	oid o = b->tseqbase + p;
	if (b->ttype == TYPE_oid || b->tvheap == NULL) {
		return o;
	}
	assert(!mask_cand(b));
	/* exceptions only allowed on transient BATs */
	assert(b->batRole == TRANSIENT);
	/* make sure exception area is a reasonable size */
	assert(ccand_free(b) % SIZEOF_OID == 0);
	BUN nexc = (BUN) (ccand_free(b) / SIZEOF_OID);
	if (nexc == 0) {
		/* no exceptions (why the vheap?) */
		return o;
	}
	const oid *exc = (oid *) ccand_first(b);
	if (o < exc[0])
		return o;
	if (o + nexc > exc[nexc - 1])
		return o + nexc;
	BUN lo = 0, hi = nexc - 1;
	while (hi - lo > 1) {
		BUN mid = (hi + lo) / 2;
		if (exc[mid] - mid > o)
			hi = mid;
		else
			lo = mid;
	}
	return o + hi;
}

#define bat_iterator(_b)	((BATiter) {.b = (_b), .tvid = 0})

/*
 * @- BAT properties
 * @multitable @columnfractions 0.08 0.7
 * @item BUN
 * @tab BATcount (BAT *b)
 * @item void
 * @tab BATsetcapacity (BAT *b, BUN cnt)
 * @item void
 * @tab BATsetcount (BAT *b, BUN cnt)
 * @item BAT *
 * @tab BATkey (BAT *b, bool onoff)
 * @item BAT *
 * @tab BATmode (BAT *b, bool transient)
 * @item BAT *
 * @tab BATsetaccess (BAT *b, restrict_t mode)
 * @item int
 * @tab BATdirty (BAT *b)
 * @item restrict_t
 * @tab BATgetaccess (BAT *b)
 * @end multitable
 *
 * The function BATcount returns the number of associations stored in
 * the BAT.
 *
 * The BAT is given a new logical name using BBPrename.
 *
 * The integrity properties to be maintained for the BAT are
 * controlled separately.  A key property indicates that duplicates in
 * the association dimension are not permitted.
 *
 * The persistency indicator tells the retention period of BATs.  The
 * system support two modes: PERSISTENT and TRANSIENT.
 * The PERSISTENT BATs are automatically saved upon session boundary
 * or transaction commit.  TRANSIENT BATs are removed upon transaction
 * boundary.  All BATs are initially TRANSIENT unless their mode is
 * changed using the routine BATmode.
 *
 * The BAT properties may be changed at any time using BATkey
 * and BATmode.
 *
 * Valid BAT access properties can be set with BATsetaccess and
 * BATgetaccess: BAT_READ, BAT_APPEND, and BAT_WRITE.  BATs can be
 * designated to be read-only. In this case some memory optimizations
 * may be made (slice and fragment bats can point to stable subsets of
 * a parent bat).  A special mode is append-only. It is then allowed
 * to insert BUNs at the end of the BAT, but not to modify anything
 * that already was in there.
 */
gdk_export BUN BATcount_no_nil(BAT *b, BAT *s);
gdk_export void BATsetcapacity(BAT *b, BUN cnt);
gdk_export void BATsetcount(BAT *b, BUN cnt);
gdk_export BUN BATgrows(BAT *b);
gdk_export gdk_return BATkey(BAT *b, bool onoff);
gdk_export gdk_return BATmode(BAT *b, bool transient);
gdk_export gdk_return BATroles(BAT *b, const char *tnme);
gdk_export void BAThseqbase(BAT *b, oid o);
gdk_export void BATtseqbase(BAT *b, oid o);

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

gdk_export gdk_return BATsetaccess(BAT *b, restrict_t mode);
gdk_export restrict_t BATgetaccess(BAT *b);


#define BATdirty(b)	(!(b)->batCopiedtodisk ||			\
			 (b)->batDirtydesc ||				\
			 (b)->theap->dirty ||				\
			 ((b)->tvheap != NULL && (b)->tvheap->dirty))
#define BATdirtydata(b)	(!(b)->batCopiedtodisk ||			\
			 (b)->theap->dirty ||				\
			 ((b)->tvheap != NULL && (b)->tvheap->dirty))

#define BATcapacity(b)	(b)->batCapacity
/*
 * @- BAT manipulation
 * @multitable @columnfractions 0.08 0.7
 * @item BAT *
 * @tab BATclear (BAT *b, bool force)
 * @item BAT *
 * @tab COLcopy (BAT *b, int tt, bool writeable, role_t role)
 * @end multitable
 *
 * The routine BATclear removes the binary associations, leading to an
 * empty, but (re-)initialized BAT. Its properties are retained.  A
 * temporary copy is obtained with Colcopy. The new BAT has an unique
 * name.
 */
gdk_export gdk_return BATclear(BAT *b, bool force);
gdk_export BAT *COLcopy(BAT *b, int tt, bool writable, role_t role);

gdk_export gdk_return BATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h)
	__attribute__((__warn_unused_result__));

/*
 * @- BAT Input/Output
 * @multitable @columnfractions 0.08 0.7
 * @item BAT *
 * @tab BATload (str name)
 * @item BAT *
 * @tab BATsave (BAT *b)
 * @item int
 * @tab BATdelete (BAT *b)
 * @end multitable
 *
 * A BAT created by COLnew is considered temporary until one calls the
 * routine BATsave or BATmode.  This routine reserves disk space and
 * checks for name clashes in the BAT directory. It also makes the BAT
 * persistent. The empty BAT is initially marked as ordered on both
 * columns.
 *
 * Failure to read or write the BAT results in a NULL, otherwise it
 * returns the BAT pointer.
 *
 * @- Heap Storage Modes
 * The discriminative storage modes are memory-mapped, compressed, or
 * loaded in memory.  As can be seen in the bat record, each BAT has
 * one BUN-heap (@emph{bn}), and possibly two heaps (@emph{hh} and
 * @emph{th}) for variable-sized atoms.
 */

gdk_export gdk_return BATsave(BAT *b)
	__attribute__((__warn_unused_result__));
gdk_export void BATmsync(BAT *b);

#define NOFARM (-1) /* indicate to GDKfilepath to create relative path */

gdk_export char *GDKfilepath(int farmid, const char *dir, const char *nme, const char *ext);
gdk_export bool GDKinmemory(int farmid);
gdk_export bool GDKembedded(void);
gdk_export gdk_return GDKcreatedir(const char *nme);

gdk_export void OIDXdestroy(BAT *b);

/*
 * @- Printing
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab BATprintcolumns (stream *f, int argc, BAT *b[]);
 * @end multitable
 *
 * The functions to convert BATs into ASCII. They are primarily meant for ease of
 * debugging and to a lesser extent for output processing.  Printing a
 * BAT is done essentially by looping through its components, printing
 * each association.
 *
 */
gdk_export gdk_return BATprintcolumns(stream *s, int argc, BAT *argv[]);
gdk_export gdk_return BATprint(stream *s, BAT *b);

/*
 * @- BAT clustering
 * @multitable @columnfractions 0.08 0.7
 * @item bool
 * @tab BATordered (BAT *b)
 * @end multitable
 *
 * When working in a main-memory situation, clustering of data on
 * disk-pages is not important. Whenever mmap()-ed data is used
 * intensively, reducing the number of page faults is a hot issue.
 *
 * The above functions rearrange data in MonetDB heaps (used for
 * storing BUNs var-sized atoms, or accelerators). Applying these
 * clusterings will allow that MonetDB's main-memory oriented
 * algorithms work efficiently also in a disk-oriented context.
 *
 * BATordered starts a check on the tail values to see if they are
 * ordered. The result is returned and stored in the tsorted field of
 * the BAT.
 */
gdk_export bool BATkeyed(BAT *b);
gdk_export bool BATordered(BAT *b);
gdk_export bool BATordered_rev(BAT *b);
gdk_export gdk_return BATsort(BAT **sorted, BAT **order, BAT **groups, BAT *b, BAT *o, BAT *g, bool reverse, bool nilslast, bool stable)
	__attribute__((__warn_unused_result__));


gdk_export void GDKqsort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe, bool reverse, bool nilslast);

#define BATtordered(b)	((b)->tsorted)
#define BATtrevordered(b) ((b)->trevsorted)
/* BAT is dense (i.e., BATtvoid() is true and tseqbase is not NIL) */
#define BATtdense(b)	(!is_oid_nil((b)->tseqbase) &&			\
			 ((b)->tvheap == NULL || (b)->tvheap->free == 0))
/* BATtvoid: BAT can be (or actually is) represented by TYPE_void */
#define BATtvoid(b)	(BATtdense(b) || (b)->ttype==TYPE_void)
#define BATtkey(b)	((b)->tkey || BATtdense(b))

/* set some properties that are trivial to deduce */
static inline void
BATsettrivprop(BAT *b)
{
	assert(!is_oid_nil(b->hseqbase));
	assert(is_oid_nil(b->tseqbase) || ATOMtype(b->ttype) == TYPE_oid);
	if (!b->batDirtydesc)
		return;
	if (b->ttype == TYPE_void) {
		if (is_oid_nil(b->tseqbase)) {
			b->tnonil = b->batCount == 0;
			b->tnil = !b->tnonil;
			b->trevsorted = true;
			b->tkey = b->batCount <= 1;
		} else {
			b->tnonil = true;
			b->tnil = false;
			b->tkey = true;
			b->trevsorted = b->batCount <= 1;
		}
		b->tsorted = true;
	} else if (b->batCount <= 1) {
		if (ATOMlinear(b->ttype)) {
			b->tsorted = true;
			b->trevsorted = true;
		}
		b->tkey = true;
		if (b->batCount == 0) {
			b->tnonil = true;
			b->tnil = false;
			if (b->ttype == TYPE_oid) {
				b->tseqbase = 0;
			}
		} else if (b->ttype == TYPE_oid) {
			/* b->batCount == 1 */
			oid sqbs = ((const oid *) b->theap->base)[b->tbaseoff];
			if (is_oid_nil(sqbs)) {
				b->tnonil = false;
				b->tnil = true;
			} else {
				b->tnonil = true;
				b->tnil = false;
			}
			b->tseqbase = sqbs;
		}
	} else if (b->batCount == 2 && ATOMlinear(b->ttype)) {
		int c;
		if (b->tvarsized)
			c = ATOMcmp(b->ttype,
				    Tbase(b) + VarHeapVal(Tloc(b, 0), 0, b->twidth),
				    Tbase(b) + VarHeapVal(Tloc(b, 0), 1, b->twidth));
		else
			c = ATOMcmp(b->ttype, Tloc(b, 0), Tloc(b, 1));
		b->tsorted = c <= 0;
		b->tnosorted = !b->tsorted;
		b->trevsorted = c >= 0;
		b->tnorevsorted = !b->trevsorted;
		b->tkey = c != 0;
		b->tnokey[0] = 0;
		b->tnokey[1] = !b->tkey;
	} else if (!ATOMlinear(b->ttype)) {
		b->tsorted = false;
		b->trevsorted = false;
	}
}

/*
 * @+ BAT Buffer Pool
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab BBPfix (bat bi)
 * @item int
 * @tab BBPunfix (bat bi)
 * @item int
 * @tab BBPretain (bat bi)
 * @item int
 * @tab BBPrelease (bat bi)
 * @item str
 * @tab BBPname (bat bi)
 * @item bat
 * @tab BBPindex  (str nme)
 * @item BAT*
 * @tab BATdescriptor (bat bi)
 * @end multitable
 *
 * The BAT Buffer Pool module contains the code to manage the storage
 * location of BATs.
 *
 * The remaining BBP tables contain status information to load, swap
 * and migrate the BATs. The core table is BBPcache which contains a
 * pointer to the BAT descriptor with its heaps.  A zero entry means
 * that the file resides on disk. Otherwise it has been read or mapped
 * into memory.
 *
 * BATs loaded into memory are retained in a BAT buffer pool.  They
 * retain their position within the cache during their life cycle,
 * which make indexing BATs a stable operation.
 *
 * The BBPindex routine checks if a BAT with a certain name is
 * registered in the buffer pools. If so, it returns its BAT id.  The
 * BATdescriptor routine has a BAT id parameter, and returns a pointer
 * to the corresponding BAT record (after incrementing the reference
 * count). The BAT will be loaded into memory, if necessary.
 *
 * The structure of the BBP file obeys the tuple format for GDK.
 *
 * The status and BAT persistency information is encoded in the status
 * field.
 */
typedef struct {
	BAT *cache;		/* if loaded: BAT* handle */
	char *logical;		/* logical name (may point at bak) */
	char bak[16];		/* logical name backup (tmp_%o) */
	BAT *desc;		/* the BAT descriptor */
	char *options;		/* A string list of options */
#if SIZEOF_VOID_P == 4
	char physical[20];	/* dir + basename for storage */
#else
	char physical[24];	/* dir + basename for storage */
#endif
	bat next;		/* next BBP slot in linked list */
	int refs;		/* in-memory references on which the loaded status of a BAT relies */
	int lrefs;		/* logical references on which the existence of a BAT relies */
	volatile unsigned status; /* status mask used for spin locking */
	/* MT_Id pid;           non-zero thread-id if this BAT is private */
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
 * of a BAT: the "bak" name is "tmp_%o", so at most 14 + \0 bytes on
 * 64 bit architecture and 11 + \0 on 32 bit architecture; the
 * physical name is a bit more complicated, but the longest possible
 * name is 22 + \0 bytes (16 + \0 on 32 bits) */
gdk_export BBPrec *BBP[N_BBPINIT];

/* fast defines without checks; internal use only  */
#define BBP_record(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)]
#define BBP_cache(i)	BBP_record(i).cache
#define BBP_logical(i)	BBP_record(i).logical
#define BBP_bak(i)	BBP_record(i).bak
#define BBP_next(i)	BBP_record(i).next
#define BBP_physical(i)	BBP_record(i).physical
#define BBP_options(i)	BBP_record(i).options
#define BBP_desc(i)	BBP_record(i).desc
#define BBP_refs(i)	BBP_record(i).refs
#define BBP_lrefs(i)	BBP_record(i).lrefs
#define BBP_status(i)	BBP_record(i).status
#define BBP_pid(i)	BBP_record(i).pid
#define BATgetId(b)	BBP_logical((b)->batCacheid)
#define BBPvalid(i)	(BBP_logical(i) != NULL && *BBP_logical(i) != '.')

/* macros that nicely check parameters */
#define BBPstatus(i)	(BBPcheck((i),"BBPstatus")?BBP_status(i):0)
#define BBPrefs(i)	(BBPcheck((i),"BBPrefs")?BBP_refs(i):-1)
#define BBPcache(i)	(BBPcheck((i),"BBPcache")?BBP_cache(i):(BAT*) NULL)
#define BBPname(i)	(BBPcheck((i), "BBPname") ? BBP_logical(i) : "")

#define BBPRENAME_ALREADY	(-1)
#define BBPRENAME_ILLEGAL	(-2)
#define BBPRENAME_LONG		(-3)
#define BBPRENAME_MEMORY	(-4)

gdk_export void BBPlock(void);

gdk_export void BBPunlock(void);

gdk_export BAT *BBPquickdesc(bat b, bool delaccess);

/*
 * @- GDK error handling
 *  @multitable @columnfractions 0.08 0.7
 * @item str
 * @tab
 *  GDKmessage
 * @item bit
 * @tab
 *  GDKfatal(str msg)
 * @item int
 * @tab
 *  GDKwarning(str msg)
 * @item int
 * @tab
 *  GDKerror (str msg)
 * @item int
 * @tab
 *  GDKgoterrors ()
 * @item int
 * @tab
 *  GDKsyserror (str msg)
 * @item str
 * @tab
 *  GDKerrbuf
 *  @item
 * @tab GDKsetbuf (str buf)
 * @end multitable
 *
 * The error handling mechanism is not sophisticated yet. Experience
 * should show if this mechanism is sufficient.  Most routines return
 * a pointer with zero to indicate an error.
 *
 * The error messages are also copied to standard output.  The last
 * error message is kept around in a global variable.
 *
 * Error messages can also be collected in a user-provided buffer,
 * instead of being echoed to a stream. This is a thread-specific
 * issue; you want to decide on the error mechanism on a
 * thread-specific basis.  This effect is established with
 * GDKsetbuf. The memory (de)allocation of this buffer, that must at
 * least be 1024 chars long, is entirely by the user. A pointer to
 * this buffer is kept in the pseudo-variable GDKerrbuf. Normally,
 * this is a NULL pointer.
 */
#define GDKMAXERRLEN	10240
#define GDKWARNING	"!WARNING: "
#define GDKERROR	"!ERROR: "
#define GDKMESSAGE	"!OS: "
#define GDKFATAL	"!FATAL: "

/* Data Distilleries uses ICU for internationalization of some MonetDB error messages */

#include "gdk_tracer.h"

#define GDKerror(format, ...)					\
	GDKtracer_log(__FILE__, __func__, __LINE__, M_ERROR,	\
		      GDK, NULL, format, ##__VA_ARGS__)
#define GDKsyserr(errno, format, ...)					\
	GDKtracer_log(__FILE__, __func__, __LINE__, M_CRITICAL,		\
		      GDK, GDKstrerror(errno, (char[64]){0}, 64),	\
		      format, ##__VA_ARGS__)
#define GDKsyserror(format, ...)	GDKsyserr(errno, format, ##__VA_ARGS__)

gdk_export _Noreturn void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
	/*
gdk_export void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
	*/
gdk_export void GDKclrerr(void);

/*
 * @- BUN manipulation
 * @multitable @columnfractions 0.08 0.7
 * @item BAT*
 * @tab BATappend (BAT *b, BAT *n, BAT *s, bool force)
 * @item BAT*
 * @tab BUNappend (BAT *b, ptr right, bool force)
 * @item BAT*
 * @tab BUNreplace (BAT *b, oid left, ptr right, bool force)
 * @item int
 * @tab BUNfnd (BAT *b, ptr tail)
 * @item BUN
 * @tab BUNlocate (BAT *b, ptr head, ptr tail)
 * @item ptr
 * @tab BUNtail (BAT *b, BUN p)
 * @end multitable
 *
 * The BATs contain a number of fixed-sized slots to store the binary
 * associations.  These slots are called BUNs or BAT units. A BUN
 * variable is a pointer into the storage area of the BAT, but it has
 * limited validity. After a BAT modification, previously obtained
 * BUNs may no longer reside at the same location.
 *
 * The association list does not contain holes.  This density permits
 * users to quickly access successive elements without the need to
 * test the items for validity. Moreover, it simplifies transport to
 * disk and other systems. The negative effect is that the user should
 * be aware of the evolving nature of the sequence, which may require
 * copying the BAT first.
 *
 * The update operations come in two flavors: BUNappend and
 * BUNreplace.  The batch version of BUNappend is BATappend.
 *
 * The routine BUNfnd provides fast access to a single BUN providing a
 * value for the tail of the binary association.
 *
 * The routine BUNtail returns a pointer to the second value in an
 * association.  To guard against side effects on the BAT, one should
 * normally copy this value into a scratch variable for further
 * processing.
 *
 * Behind the interface we use several macros to access the BUN fixed
 * part and the variable part. The BUN operators always require a BAT
 * pointer and BUN identifier.
 * @itemize
 * @item
 * BATttype(b) finds out the type of a BAT.
 * @item
 * BUNlast(b) returns the BUN pointer directly after the last BUN
 * in the BAT.
 * @end itemize
 */
/* NOTE: `p' is evaluated after a possible upgrade of the heap */
static inline gdk_return __attribute__((__warn_unused_result__))
Tputvalue(BAT *b, BUN p, const void *v, bool copyall)
{
	assert(b->tbaseoff == 0);
	if (b->tvarsized && b->ttype) {
		var_t d;
		gdk_return rc;

		rc = ATOMputVAR(b, &d, v);
		if (rc != GDK_SUCCEED)
			return rc;
		if (b->twidth < SIZEOF_VAR_T &&
		    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 * b->twidth))) {
			/* doesn't fit in current heap, upgrade it */
			rc = GDKupgradevarheap(b, d, 0, copyall);
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
		}
	} else if (b->ttype == TYPE_msk) {
		mskSetVal(b, p, * (msk *) v);
	} else {
		return ATOMputFIX(b->ttype, Tloc(b, p), v);
	}
	return GDK_SUCCEED;
}

static inline gdk_return __attribute__((__warn_unused_result__))
tfastins_nocheck(BAT *b, BUN p, const void *v, int s)
{
	assert(b->theap->parentid == b->batCacheid);
	if (ATOMstorage(b->ttype) == TYPE_msk) {
		if (p % 32 == 0) {
			((uint32_t *) b->theap->base)[b->theap->free / 4] = 0;
			b->theap->free += 4;
		}
	} else
		b->theap->free += s;
	return Tputvalue(b, p, v, false);
}

static inline gdk_return __attribute__((__warn_unused_result__))
bunfastapp_nocheck(BAT *b, BUN p, const void *v, int ts)
{
	gdk_return rc;
	rc = tfastins_nocheck(b, p, v, ts);
	if (rc == GDK_SUCCEED)
		b->batCount++;
	return rc;
}

static inline gdk_return __attribute__((__warn_unused_result__))
bunfastapp(BAT *b, const void *v)
{
	if (BATcount(b) >= BATcapacity(b)) {
		if (BATcount(b) == BUN_MAX) {
			GDKerror("bunfastapp: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		gdk_return rc = BATextend(b, BATgrows(b));
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return bunfastapp_nocheck(b, b->batCount, v, Tsize(b));
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

static inline gdk_return __attribute__((__warn_unused_result__))
tfastins_nocheckVAR(BAT *b, BUN p, const void *v, int s)
{
	var_t d;
	gdk_return rc;
	assert(b->tbaseoff == 0);
	assert(b->theap->parentid == b->batCacheid);
	b->theap->free += s;
	if ((rc = ATOMputVAR(b, &d, v)) != GDK_SUCCEED)
		return rc;
	if (b->twidth < SIZEOF_VAR_T &&
	    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 * b->twidth))) {
		/* doesn't fit in current heap, upgrade it */
		rc = GDKupgradevarheap(b, d, 0, false);
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
	}
	return GDK_SUCCEED;
}

static inline gdk_return __attribute__((__warn_unused_result__))
bunfastapp_nocheckVAR(BAT *b, BUN p, const void *v, int ts)
{
	gdk_return rc;
	rc = tfastins_nocheckVAR(b, p, v, ts);
	if (rc == GDK_SUCCEED)
		b->batCount++;
	return rc;
}

static inline gdk_return __attribute__((__warn_unused_result__))
bunfastappVAR(BAT *b, const void *v)
{
	if (BATcount(b) >= BATcapacity(b)) {
		if (BATcount(b) == BUN_MAX) {
			GDKerror("too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		gdk_return rc = BATextend(b, BATgrows(b));
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return bunfastapp_nocheckVAR(b, b->batCount, v, Tsize(b));
}

/*
 * @- Column Imprints Functions
 *
 * @multitable @columnfractions 0.08 0.7
 * @item BAT*
 * @tab
 *  BATimprints (BAT *b)
 * @end multitable
 *
 * The column imprints index structure.
 *
 */

gdk_export gdk_return BATimprints(BAT *b);
gdk_export void IMPSdestroy(BAT *b);
gdk_export lng IMPSimprintsize(BAT *b);

/* The ordered index structure */

gdk_export gdk_return BATorderidx(BAT *b, bool stable);
gdk_export gdk_return GDKmergeidx(BAT *b, BAT**a, int n_ar);
gdk_export bool BATcheckorderidx(BAT *b);

#include "gdk_delta.h"
#include "gdk_hash.h"
#include "gdk_bbp.h"
#include "gdk_utils.h"

/* functions defined in gdk_bat.c */
gdk_export gdk_return void_inplace(BAT *b, oid id, const void *val, bool force)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATattach(int tt, const char *heapfile, role_t role);

#ifdef NATIVE_WIN32
#ifdef _MSC_VER
#define fileno _fileno
#endif
#define fdopen _fdopen
#define putenv _putenv
#endif

/* Return a pointer to the value contained in V.  Also see VALget
 * which returns a void *. */
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
	case TYPE_ptr: return (const void *) &v->val.pval;
	case TYPE_str: return (const void *) v->val.sval;
	default:       return (const void *) v->val.pval;
	}
}

/*
 * The kernel maintains a central table of all active threads.  They
 * are indexed by their tid. The structure contains information on the
 * input/output file descriptors, which should be set before a
 * database operation is started. It ensures that output is delivered
 * to the proper client.
 *
 * The Thread structure should be ideally made directly accessible to
 * each thread. This speeds up access to tid and file descriptors.
 */
#define THREADS	1024
#define THREADDATA	3

typedef struct threadStruct {
	int tid;		/* logical ID by MonetDB; val == index
				 * into this array + 1 (0 is
				 * invalid) */
	ATOMIC_TYPE pid;	/* thread id, 0 = unallocated */
	char name[MT_NAME_LEN];
	void *data[THREADDATA];
	uintptr_t sp;
} *Thread;


gdk_export int THRgettid(void);
gdk_export Thread THRget(int tid);
gdk_export MT_Id THRcreate(void (*f) (void *), void *arg, enum MT_thr_detach d, const char *name);
gdk_export void THRdel(Thread t);
gdk_export void THRsetdata(int, void *);
gdk_export void *THRgetdata(int);
gdk_export int THRhighwater(void);

gdk_export void *THRdata[THREADDATA];

#define GDKstdout	((stream*)THRdata[0])
#define GDKstdin	((stream*)THRdata[1])

#define GDKerrbuf	((char*)THRgetdata(2))
#define GDKsetbuf(x)	THRsetdata(2,(void *)(x))

#define THRget_errbuf(t)	((char*)t->data[2])
#define THRset_errbuf(t,b)	(t->data[2] = b)

static inline bat
BBPcheck(bat x, const char *y)
{
	if (!is_bat_nil(x)) {
		assert(x > 0);

		if (x < 0 || x >= getBBPsize() || BBP_logical(x) == NULL) {
			TRC_DEBUG(CHECK_, "%s: range error %d\n", y, (int) x);
		} else {
			return x;
		}
	}
	return 0;
}

static inline BAT *
BATdescriptor(bat i)
{
	BAT *b = NULL;

	if (BBPcheck(i, "BATdescriptor")) {
		if (BBPfix(i) <= 0)
			return NULL;
		b = BBP_cache(i);
		if (b == NULL)
			b = BBPdescriptor(i);
	}
	return b;
}

static inline void *
Tpos(BATiter *bi, BUN p)
{
	bi->tvid = BUNtoid(bi->b, p);
	return (void*)&bi->tvid;
}

static inline void *
Tmsk(BATiter *bi, BUN p)
{
	bi->tmsk = mskGetVal(bi->b, p);
	return &bi->tmsk;
}

/*
 * @+ Transaction Management
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab
 *  TMcommit ()
 * @item int
 * @tab
 *  TMabort ()
 * @item int
 * @tab
 *  TMsubcommit ()
 * @end multitable
 *
 * MonetDB by default offers a global transaction environment.  The
 * global transaction involves all activities on all persistent BATs
 * by all threads.  Each global transaction ends with either TMabort
 * or TMcommit, and immediately starts a new transaction.  TMcommit
 * implements atomic commit to disk on the collection of all
 * persistent BATs. For all persistent BATs, the global commit also
 * flushes the delta status for these BATs (see
 * BATcommit/BATabort). This allows to perform TMabort quickly in
 * memory (without re-reading all disk images from disk).  The
 * collection of which BATs is persistent is also part of the global
 * transaction state. All BATs that where persistent at the last
 * commit, but were made transient since then, are made persistent
 * again by TMabort.  In other words, BATs that are deleted, are only
 * physically deleted at TMcommit time. Until that time, rollback
 * (TMabort) is possible.
 *
 * Use of TMabort is currently NOT RECOMMENDED due to two bugs:
 *
 * @itemize
 * @item
 * TMabort after a failed %TMcommit@ does not bring us back to the
 * previous committed state; but to the state at the failed TMcommit.
 * @item
 * At runtime, TMabort does not undo BAT name changes, whereas a cold
 * MonetDB restart does.
 * @end itemize
 *
 * In effect, the problems with TMabort reduce the functionality of
 * the global transaction mechanism to consistent checkpointing at
 * each TMcommit. For many applications, consistent checkpointingis
 * enough.
 *
 * Extension modules exist that provide fine grained locking (lock
 * module) and Write Ahead Logging (sqlserver).  Applications that
 * need more fine-grained transactions, should build this on top of
 * these extension primitives.
 *
 * TMsubcommit is intended to quickly add or remove BATs from the
 * persistent set. In both cases, rollback is not necessary, such that
 * the commit protocol can be accelerated. It comes down to writing a
 * new BBP.dir.
 *
 * Its parameter is a BAT-of-BATs (in the tail); the persistence
 * status of that BAT is committed. We assume here that the calling
 * thread has exclusive access to these bats.  An error is reported if
 * you try to partially commit an already committed persistent BAT (it
 * needs the rollback mechanism).
 */
gdk_export gdk_return TMcommit(void);
gdk_export void TMabort(void);
gdk_export gdk_return TMsubcommit(BAT *bl);
gdk_export gdk_return TMsubcommit_list(bat *restrict subcommit, BUN *restrict sizes, int cnt, lng logno, lng transid);

/*
 * @- Delta Management
 *  @multitable @columnfractions 0.08 0.6
 * @item BAT *
 * @tab BATcommit (BAT *b)
 * @item BAT *
 * @tab BATfakeCommit (BAT *b)
 * @item BAT *
 * @tab BATundo (BAT *b)
 * @end multitable
 *
 * The BAT keeps track of updates with respect to a 'previous state'.
 * Do not confuse 'previous state' with 'stable' or
 * 'commited-on-disk', because these concepts are not always the
 * same. In particular, they diverge when BATcommit, BATfakecommit,
 * and BATundo are called explictly, bypassing the normal global
 * TMcommit protocol (some applications need that flexibility).
 *
 * BATcommit make the current BAT state the new 'stable state'.  This
 * happens inside the global TMcommit on all persistent BATs previous
 * to writing all bats to persistent storage using a BBPsync.
 *
 * EXPERT USE ONLY: The routine BATfakeCommit updates the delta
 * information on BATs and clears the dirty bit. This avoids any
 * copying to disk.  Expert usage only, as it bypasses the global
 * commit protocol, and changes may be lost after quitting or crashing
 * MonetDB.
 *
 * BATabort undo-s all changes since the previous state. The global
 * TMabort achieves a rollback to the previously committed state by
 * doing BATabort on all persistent bats.
 *
 * BUG: after a failed TMcommit, TMabort does not do anything because
 * TMcommit does the BATcommits @emph{before} attempting to sync to
 * disk instead of @sc{after} doing this.
 */
gdk_export void BATcommit(BAT *b, BUN size);
gdk_export void BATfakeCommit(BAT *b);
gdk_export void BATundo(BAT *b);

/*
 * @+ BAT Alignment and BAT views
 * @multitable @columnfractions 0.08 0.7
 * @item int
 * @tab ALIGNsynced (BAT* b1, BAT* b2)
 * @item int
 * @tab ALIGNsync   (BAT *b1, BAT *b2)
 * @item int
 * @tab ALIGNrelated (BAT *b1, BAT *b2)
 *
 * @item BAT*
 * @tab VIEWcreate   (oid seq, BAT *b)
 * @item int
 * @tab isVIEW   (BAT *b)
 * @item bat
 * @tab VIEWhparent   (BAT *b)
 * @item bat
 * @tab VIEWtparent   (BAT *b)
 * @item BAT*
 * @tab VIEWreset    (BAT *b)
 * @end multitable
 *
 * Alignments of two columns of a BAT means that the system knows
 * whether these two columns are exactly equal. Relatedness of two
 * BATs means that one pair of columns (either head or tail) of both
 * BATs is aligned. The first property is checked by ALIGNsynced, the
 * latter by ALIGNrelated.
 *
 * All algebraic BAT commands propagate the properties - including
 * alignment properly on their results.
 *
 * VIEW BATs are BATs that lend their storage from a parent BAT.  They
 * are just a descriptor that points to the data in this parent BAT. A
 * view is created with VIEWcreate. The cache id of the parent (if
 * any) is returned by VIEWtparent (otherwise it returns 0).
 *
 * VIEW bats are read-only!!
 *
 * VIEWreset creates a normal BAT with the same contents as its view
 * parameter (it converts void columns with seqbase!=nil to
 * materialized oid columns).
 */
gdk_export int ALIGNsynced(BAT *b1, BAT *b2);

gdk_export void BATassertProps(BAT *b);

gdk_export BAT *VIEWcreate(oid seq, BAT *b);
gdk_export void VIEWbounds(BAT *b, BAT *view, BUN l, BUN h);

#define ALIGNapp(x, f, e)						\
	do {								\
		if (!(f) && ((x)->batRestricted == BAT_READ ||		\
			     (x)->batSharecnt > 0)) {			\
			GDKerror("access denied to %s, aborting.\n",	\
				 BATgetId(x));				\
			return (e);					\
		}							\
	} while (false)

/* the parentid in a VIEW is correct for the normal view. We must
 * correct for the reversed view.
 */
#define isVIEW(x)							\
	(assert((x)->batCacheid > 0),					\
	 (((x)->theap && (x)->theap->parentid != (x)->batCacheid) ||	\
	  ((x)->tvheap && (x)->tvheap->parentid != (x)->batCacheid)))

#define VIEWtparent(x)	((x)->theap == NULL || (x)->theap->parentid == (x)->batCacheid ? 0 : (x)->theap->parentid)
#define VIEWvtparent(x)	((x)->tvheap == NULL || (x)->tvheap->parentid == (x)->batCacheid ? 0 : (x)->tvheap->parentid)

/*
 * @+ BAT Iterators
 *  @multitable @columnfractions 0.15 0.7
 * @item BATloop
 * @tab
 *  (BAT *b; BUN p, BUN q)
 * @item BATloopDEL
 * @tab
 *  (BAT *b; BUN p; BUN q; int dummy)
 * @item HASHloop
 * @tab
 *  (BAT *b; Hash *h, size_t dummy; ptr value)
 * @item HASHloop_bte
 * @tab
 *  (BAT *b; Hash *h, size_t idx; bte *value, BUN w)
 * @item HASHloop_sht
 * @tab
 *  (BAT *b; Hash *h, size_t idx; sht *value, BUN w)
 * @item HASHloop_int
 * @tab
 *  (BAT *b; Hash *h, size_t idx; int *value, BUN w)
 * @item HASHloop_flt
 * @tab
 *  (BAT *b; Hash *h, size_t idx; flt *value, BUN w)
 * @item HASHloop_lng
 * @tab
 *  (BAT *b; Hash *h, size_t idx; lng *value, BUN w)
 * @item HASHloop_hge
 * @tab
 *  (BAT *b; Hash *h, size_t idx; hge *value, BUN w)
 * @item HASHloop_dbl
 * @tab
 *  (BAT *b; Hash *h, size_t idx; dbl *value, BUN w)
 * @item  HASHloop_str
 * @tab
 *  (BAT *b; Hash *h, size_t idx; str value, BUN w)
 * @item HASHlooploc
 * @tab
 *  (BAT *b; Hash *h, size_t idx; ptr value, BUN w)
 * @item HASHloopvar
 * @tab
 *  (BAT *b; Hash *h, size_t idx; ptr value, BUN w)
 * @end multitable
 *
 * The @emph{BATloop()} looks like a function call, but is actually a
 * macro.
 *
 * @- simple sequential scan
 * The first parameter is a BAT, the p and q are BUN pointers, where p
 * is the iteration variable.
 */
#define BATloop(r, p, q)			\
	for (q = BUNlast(r), p = 0; p < q; p++)

/*
 * @+ Common BAT Operations
 * Much used, but not necessarily kernel-operations on BATs.
 *
 * For each BAT we maintain its dimensions as separately accessible
 * properties. They can be used to improve query processing at higher
 * levels.
 */
enum prop_t {
	GDK_MIN_VALUE = 3,	/* smallest non-nil value in BAT */
	GDK_MIN_POS,		/* BUN position of smallest value  */
	GDK_MAX_VALUE,		/* largest non-nil value in BAT */
	GDK_MAX_POS,		/* BUN position of largest value  */
	GDK_HASH_BUCKETS,	/* last used hash bucket size */
	GDK_NUNIQUE,		/* number of unique values */
	GDK_UNIQUE_ESTIMATE,	/* estimate of number of distinct values */
};
gdk_export ValPtr BATgetprop(BAT *b, enum prop_t idx);

/*
 * @- BAT relational operators
 *
 * The full-materialization policy intermediate results in MonetDB
 * means that a join can produce an arbitrarily large result and choke
 * the system. The Data Distilleries tool therefore first computes the
 * join result size before the actual join (better waste time than
 * crash the server). To exploit that perfect result size knowledge,
 * an result-size estimate parameter was added to all equi-join
 * implementations.  TODO: add this for
 * semijoin/select/unique/diff/intersect
 *
 * @- modes for thethajoin
 */
#define JOIN_EQ		0
#define JOIN_LT		(-1)
#define JOIN_LE		(-2)
#define JOIN_GT		1
#define JOIN_GE		2
#define JOIN_BAND	3
#define JOIN_NE		(-3)

gdk_export BAT *BATselect(BAT *b, BAT *s, const void *tl, const void *th, bool li, bool hi, bool anti);
gdk_export BAT *BATthetaselect(BAT *b, BAT *s, const void *val, const char *op);

gdk_export BAT *BATconstant(oid hseq, int tt, const void *val, BUN cnt, role_t role);
gdk_export gdk_return BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool match_one, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int op, bool nil_matches, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATintersect(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate);
gdk_export BAT *BATdiff(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool not_in, BUN estimate);
gdk_export gdk_return BATjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export BUN BATguess_uniques(BAT *b, struct canditer *ci);
gdk_export gdk_return BATbandjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, const void *c1, const void *c2, bool li, bool hi, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATrangejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *rl, BAT *rh, BAT *sl, BAT *sr, bool li, bool hi, bool anti, bool symmetric, BUN estimate)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATproject(BAT *restrict l, BAT *restrict r);
gdk_export BAT *BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2);
gdk_export BAT *BATprojectchain(BAT **bats);

gdk_export BAT *BATslice(BAT *b, BUN low, BUN high);

gdk_export BAT *BATunique(BAT *b, BAT *s);

gdk_export BAT *BATmergecand(BAT *a, BAT *b);
gdk_export BAT *BATintersectcand(BAT *a, BAT *b);
gdk_export BAT *BATdiffcand(BAT *a, BAT *b);

gdk_export gdk_return BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *cands, BAT *grps, BUN n, bool asc, bool nilslast, bool distinct)
	__attribute__((__warn_unused_result__));

#include "gdk_calc.h"

/*
 * @- BAT sample operators
 *
 * @multitable @columnfractions 0.08 0.7
 * @item BAT *
 * @tab BATsample (BAT *b, n)
 * @end multitable
 *
 * The routine BATsample returns a random sample on n BUNs of a BAT.
 *
 */
gdk_export BAT *BATsample(BAT *b, BUN n);
gdk_export BAT *BATsample_with_seed(BAT *b, BUN n, uint64_t seed);

/*
 *
 */
#define MAXPARAMS	32

#endif /* _GDK_H_ */
