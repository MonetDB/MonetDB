/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @t The Goblin Database Kernel
 * @v Version 3.05
 * @a Martin L. Kersten, Peter Boncz, Niels Nes
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
#include <stdio.h>
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#ifdef STDC_HEADERS
# include <stdlib.h>
# include <stddef.h>
#else
# ifdef HAVE_STDLIB_H
#  include <stdlib.h>
# endif
#endif
#ifdef HAVE_STRING_H
# if !defined(STDC_HEADERS) && defined(HAVE_MEMORY_H)
#  include <memory.h>
# endif
# include <string.h>
#endif
#ifdef HAVE_STRINGS_H
# include <strings.h>
#endif
#ifdef HAVE_INTTYPES_H
# include <inttypes.h>
#else
# ifdef HAVE_STDINT_H
#  include <stdint.h>
# endif
#endif
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#include <ctype.h>		/* isspace etc. */

#ifdef HAVE_SYS_FILE_H
# include <sys/file.h>
#endif
#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>		/* MAXPATHLEN */
#endif

#ifdef HAVE_DIRENT_H
# include <dirent.h>
#else
# define dirent direct
# ifdef HAVE_SYS_NDIR_H
#  include <sys/ndir.h>
# endif
# ifdef HAVE_SYS_DIR_H
#  include <sys/dir.h>
# endif
# ifdef HAVE_NDIR_H
#  include <ndir.h>
# endif
#endif

#include <limits.h>		/* for *_MIN and *_MAX */
#include <float.h>		/* for FLT_MAX and DBL_MAX */
#ifndef LLONG_MAX
#ifdef LONGLONG_MAX
#define LLONG_MAX LONGLONG_MAX
#define LLONG_MIN LONGLONG_MIN
#else
#define LLONG_MAX LL_CONSTANT(9223372036854775807)
#define LLONG_MIN (-LL_CONSTANT(9223372036854775807) - LL_CONSTANT(1))
#endif
#endif

#include "gdk_system.h"
#include "gdk_posix.h"
#include <stream.h>

#undef MIN
#undef MAX
#define MAX(A,B)	((A)<(B)?(B):(A))
#define MIN(A,B)	((A)>(B)?(B):(A))

/* defines from ctype with casts that allow passing char values */
#define GDKisspace(c)	isspace((int) (unsigned char) (c))
#define GDKisalnum(c)	isalnum((int) (unsigned char) (c))
#define GDKisdigit(c)	(((unsigned char) (c)) >= '0' && ((unsigned char) (c)) <= '9')

#ifndef NATIVE_WIN32
#define BATDIR		"bat"
#define DELDIR		"bat/DELETE_ME"
#define BAKDIR		"bat/BACKUP"
#define SUBDIR		"bat/BACKUP/SUBCOMMIT"
#define LEFTDIR		"bat/LEFTOVERS"
#else
#define BATDIR		"bat"
#define DELDIR		"bat\\DELETE_ME"
#define BAKDIR		"bat\\BACKUP"
#define SUBDIR		"bat\\BACKUP\\SUBCOMMIT"
#define LEFTDIR		"bat\\LEFTOVERS"
#endif

#ifdef MAXPATHLEN
#define PATHLENGTH	MAXPATHLEN
#else
#define PATHLENGTH	1024	/* maximum file pathname length */
#endif

/*
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of the following debug options.
*/

#define THRDMASK	(1)
#define CHECKMASK	(1<<1)
#define CHECKDEBUG	if (GDKdebug & CHECKMASK)
#define MEMMASK		(1<<2)
#define MEMDEBUG	if (GDKdebug & MEMMASK)
#define PROPMASK	(1<<3)
#define PROPDEBUG	if (GDKdebug & PROPMASK)
#define IOMASK		(1<<4)
#define IODEBUG		if (GDKdebug & IOMASK)
#define BATMASK		(1<<5)
#define BATDEBUG	if (GDKdebug & BATMASK)
/* PARSEMASK not used anymore
#define PARSEMASK	(1<<6)
#define PARSEDEBUG	if (GDKdebug & PARSEMASK)
*/
#define PARMASK		(1<<7)
#define PARDEBUG	if (GDKdebug & PARMASK)
#define HEADLESSMASK	(1<<8)
#define HEADLESSDEBUG	if (GDKdebug & HEADLESSMASK)
#define TMMASK		(1<<9)
#define TMDEBUG		if (GDKdebug & TMMASK)
#define TEMMASK		(1<<10)
#define TEMDEBUG	if (GDKdebug & TEMMASK)
/* DLMASK not used anymore
#define DLMASK		(1<<11)
#define DLDEBUG		if (GDKdebug & DLMASK)
*/
#define PERFMASK	(1<<12)
#define PERFDEBUG	if (GDKdebug & PERFMASK)
#define DELTAMASK	(1<<13)
#define DELTADEBUG	if (GDKdebug & DELTAMASK)
#define LOADMASK	(1<<14)
#define LOADDEBUG	if (GDKdebug & LOADMASK)
/* YACCMASK not used anymore
#define YACCMASK	(1<<15)
#define YACCDEBUG	if (GDKdebug & YACCMASK)
*/
/*
#define ?tcpip?		if (GDKdebug&(1<<16))
#define ?monet_multiplex?	if (GDKdebug&(1<<17))
#define ?ddbench?	if (GDKdebug&(1<<18))
#define ?ddbench?	if (GDKdebug&(1<<19))
#define ?ddbench?	if (GDKdebug&(1<<20))
*/
#define ALGOMASK	(1<<21)
#define ALGODEBUG	if (GDKdebug & ALGOMASK)
#define ESTIMASK	(1<<22)
#define ESTIDEBUG	if (GDKdebug & ESTIMASK)
/* XPROPMASK not used anymore
#define XPROPMASK	(1<<23)
#define XPROPDEBUG	if (GDKdebug & XPROPMASK)
*/

/* JOINPROPMASK not used anymore
#define JOINPROPMASK	(1<<24)
#define JOINPROPCHK	if (!(GDKdebug & JOINPROPMASK))
*/
#define DEADBEEFMASK	(1<<25)
#define DEADBEEFCHK	if (!(GDKdebug & DEADBEEFMASK))

#define ALLOCMASK	(1<<26)
#define ALLOCDEBUG	if (GDKdebug & ALLOCMASK)

/* M5, only; cf.,
 * monetdb5/mal/mal.h
 */
#define OPTMASK		(1<<27)
#define OPTDEBUG	if (GDKdebug & OPTMASK)

#define HEAPMASK	(1<<28)
#define HEAPDEBUG	if (GDKdebug & HEAPMASK)

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

#define IDLENGTH	64	/* maximum BAT id length */
#define BATMARGIN	1.2	/* extra free margin for new heaps */
#define BATTINY_BITS	8
#define BATTINY		((BUN)1<<BATTINY_BITS)	/* minimum allocation buncnt for a BAT */

#define TYPE_void	0
#define TYPE_bit	1
#define TYPE_bte	2
#define TYPE_sht	3
#define TYPE_bat	4	/* BAT id: index in BBPcache */
#define TYPE_int	5
#define TYPE_oid	6
#define TYPE_ptr	7	/* C pointer! */
#define TYPE_flt	8
#define TYPE_dbl	9
#define TYPE_lng	10
#ifdef HAVE_HGE
#define TYPE_hge	11
#define TYPE_str	12
#else
#define TYPE_str	11
#endif
#define TYPE_any	255	/* limit types to <255! */

typedef signed char bit;
typedef signed char bte;
typedef short sht;

#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#define OIDFMT		SZFMT

typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

#if SIZEOF_INT==8
#	define LL_CONSTANT(val)	(val)
#elif SIZEOF_LONG==8
#	define LL_CONSTANT(val)	(val##L)
#elif defined(HAVE_LONG_LONG)
#	define LL_CONSTANT(val)	(val##LL)
#elif defined(HAVE___INT64)
#	define LL_CONSTANT(val)	(val##i64)
#endif

typedef oid var_t;		/* type used for heap index of var-sized BAT */
#define SIZEOF_VAR_T	SIZEOF_OID
#define VARFMT		OIDFMT

#if SIZEOF_VAR_T == SIZEOF_INT
#define VAR_MAX		((var_t) INT_MAX)
#else
#define VAR_MAX		((var_t) LLONG_MAX)
#endif

typedef oid BUN;		/* BUN position */
#define SIZEOF_BUN	SIZEOF_OID
#define BUNFMT		OIDFMT
/* alternatively:
typedef size_t BUN;
#define SIZEOF_BUN	SIZEOF_SIZE_T
#define BUNFMT		SZFMT
*/
#if SIZEOF_BUN == SIZEOF_INT
#define BUN_NONE ((BUN) INT_MAX)
#else
#define BUN_NONE ((BUN) LLONG_MAX)
#endif
#define BUN_MAX (BUN_NONE - 1)	/* maximum allowed size of a BAT */

#define BUN2 2
#define BUN4 4
#if SIZEOF_BUN > 4
#define BUN8 8
#endif
typedef uint16_t BUN2type;
typedef uint32_t BUN4type;
#if SIZEOF_BUN > 4
typedef uint64_t BUN8type;
#endif
#define BUN2_NONE ((BUN2type) 0xFFFF)
#define BUN4_NONE ((BUN4type) 0xFFFFFFFF)
#if SIZEOF_BUN > 4
#define BUN8_NONE ((BUN8type) LL_CONSTANT(0xFFFFFFFFFFFFFFFF))
#endif


/*
 * @- Checking and Error definitions:
 */
typedef enum { GDK_FAIL, GDK_SUCCEED } gdk_return;

#define ATOMextern(t)	(ATOMstorage(t) >= TYPE_str)

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
	str filename;		/* file containing image of the heap */

	unsigned int copied:1,	/* a copy of an existing map. */
		hashash:1,	/* the string heap contains hash values */
		forcemap:1,	/* force STORE_MMAP even if heap exists */
		cleanhash:1;	/* string heaps must clean hash */
	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
	bte dirty;		/* specific heap dirty marker */
	bte farmid;		/* id of farm where heap is located */
	bat parentid;		/* cache id of VIEW parent bat */
} Heap;

typedef struct {
	int type;		/* type of index entity */
	int width;		/* width of hash entries */
	BUN nil;		/* nil representation */
	BUN lim;		/* collision list size */
	BUN mask;		/* number of hash buckets-1 (power of 2) */
	void *Hash;		/* hash table */
	void *Link;		/* collision list */
	Heap *heap;		/* heap where the hash is stored */
} Hash;

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
		flt fval;
		ptr pval;
		bat bval;
		str sval;
		dbl dval;
		lng lval;
#ifdef HAVE_HGE
		hge hval;
#endif
	} val;
	size_t len;
	int vtype;
} *ValPtr, ValRecord;

/* interface definitions */
gdk_export ptr VALconvert(int typ, ValPtr t);
gdk_export ssize_t VALformat(char **buf, const ValRecord *res);
gdk_export ValPtr VALcopy(ValPtr dst, const ValRecord *src);
gdk_export ValPtr VALinit(ValPtr d, int tpe, const void *s);
gdk_export void VALempty(ValPtr v);
gdk_export void VALclear(ValPtr v);
gdk_export ValPtr VALset(ValPtr v, int t, ptr p);
gdk_export void *VALget(ValPtr v);
gdk_export int VALcmp(const ValRecord *p, const ValRecord *q);
gdk_export int VALisnil(const ValRecord *v);

/*
 * @- The BAT record
 * The elements of the BAT structure are introduced in the remainder.
 * Instead of using the underlying types hidden beneath it, one should
 * use a @emph{BAT} type that is supposed to look like this:
 * @verbatim
 * typedef struct {
 *           // static BAT properties
 *           bat    batCacheid;       // bat id: index in BBPcache
 *           int    batPersistence;   // persistence mode
 *           bit    batCopiedtodisk;  // BAT is saved on disk?
 *           // dynamic BAT properties
 *           int    batHeat;          // heat of BAT in the BBP
 *           sht    batDirty;         // BAT modified after last commit?
 *           bit    batDirtydesc;     // BAT descriptor specific dirty flag
 *           Heap*  batBuns;          // Heap where the buns are stored
 *           // DELTA status
 *           BUN    batInserted;      // first inserted BUN
 *           BUN    batCount;         // Tuple count
 *           // Tail properties
 *           int    ttype;            // Tail type number
 *           str    tident;           // name for tail column
 *           bit    tkey;             // tail values are unique
 *           bit    tunique;          // tail values must be kept unique
 *           bit    tnonil;           // tail has no nils
 *           bit    tsorted;          // are tail values currently ordered?
 *           bit    tvarsized;        // for speed: tail type is varsized?
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

typedef struct {
	/* dynamic bat properties */
	MT_Id tid;		/* which thread created it */
	unsigned int
	 copiedtodisk:1,	/* once written */
	 dirty:2,		/* dirty wrt disk? */
	 dirtyflushed:1,	/* was dirty before commit started? */
	 descdirty:1,		/* bat descriptor dirty marker */
	 restricted:2,		/* access privileges */
	 persistence:1,		/* should the BAT persist on disk? */
	 role:8,		/* role of the bat */
	 unused:15;		/* value=0 for now */
	int sharecnt;		/* incoming view count */

	/* delta status administration */
	BUN inserted;		/* start of inserted elements */
	BUN count;		/* tuple count */
	BUN capacity;		/* tuple capacity */
} BATrec;

typedef struct PROPrec PROPrec;

/* see also comment near BATassertProps() for more information about
 * the properties */
typedef struct {
	str id;			/* label for head/tail column */

	unsigned short width;	/* byte-width of the atom array */
	bte type;		/* type id. */
	bte shift;		/* log2 of bunwidth */
	unsigned int
	 varsized:1,		/* varsized (1) or fixedsized (0) */
	 key:1,			/* no duplicate values present */
	 unique:1,		/* no duplicate values allowed */
	 dense:1,		/* OID only: only consecutive values */
	 nonil:1,		/* there are no nils in the column */
	 nil:1,			/* there is a nil in the column */
	 sorted:1,		/* column is sorted in ascending order */
	 revsorted:1;		/* column is sorted in descending order */
	BUN nokey[2];		/* positions that prove key==FALSE */
	BUN nosorted;		/* position that proves sorted==FALSE */
	BUN norevsorted;	/* position that proves revsorted==FALSE */
	BUN nodense;		/* position that proves dense==FALSE */
	oid seq;		/* start of dense head sequence */

	Heap heap;		/* space for the column. */
	Heap *vheap;		/* space for the varsized data. */
	Hash *hash;		/* hash table */
	Imprints *imprints;	/* column imprints index */
	Heap *orderidx;		/* order oid index */

	PROPrec *props;		/* list of dynamic properties stored in the bat descriptor */
} COLrec;

#define ORDERIDXOFF		3

/* assert that atom width is power of 2, i.e., width == 1<<shift */
#define assert_shift_width(shift,width) assert(((shift) == 0 && (width) == 0) || ((unsigned)1<<(shift)) == (unsigned)(width))

#define GDKLIBRARY_SORTEDPOS	061030	/* version where we can't trust no(rev)sorted */
#define GDKLIBRARY_OLDWKB	061031	/* old geom WKB format */
#define GDKLIBRARY_INSERTED	061032	/* inserted and deleted in BBP.dir */
#define GDKLIBRARY_HEADED	061033	/* head properties are stored */
#define GDKLIBRARY_NOKEY	061034	/* nokey values can't be trusted */
#define GDKLIBRARY_BADEMPTY	061035	/* possibility of duplicate empty str */
#define GDKLIBRARY_TALIGN	061036	/* talign field in BBP.dir */
#define GDKLIBRARY		061037

typedef struct BAT {
	/* static bat properties */
	bat batCacheid;		/* index into BBP */
	oid hseqbase;		/* head seq base */

	/* dynamic column properties */
	COLrec T;		/* column info */

	BATrec S;		/* the BAT properties */
} BAT;

typedef struct BATiter {
	BAT *b;
	oid tvid;
} BATiter;

/* macros to hide complexity of the BAT structure */
#define batPersistence	S.persistence
#define batCopiedtodisk	S.copiedtodisk
#define batDirty	S.dirty
#define batConvert	S.convert
#define batDirtyflushed	S.dirtyflushed
#define batDirtydesc	S.descdirty
#define batInserted	S.inserted
#define batCount	S.count
#define batCapacity	S.capacity
#define batSharecnt	S.sharecnt
#define batRestricted	S.restricted
#define batRole		S.role
#define creator_tid	S.tid
#define ttype		T.type
#define tkey		T.key
#define tunique		T.unique
#define tvarsized	T.varsized
#define tseqbase	T.seq
#define tsorted		T.sorted
#define trevsorted	T.revsorted
#define tdense		T.dense
#define tident		T.id
#define torderidx	T.orderidx
#define twidth		T.width
#define tshift		T.shift
#define tnonil		T.nonil
#define tnil		T.nil
#define tnokey		T.nokey
#define tnosorted	T.nosorted
#define tnorevsorted	T.norevsorted
#define tnodense	T.nodense
#define theap		T.heap
#define tvheap		T.vheap
#define thash		T.hash
#define timprints	T.imprints
#define tprops		T.props



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
 *  HEAPfree (Heap *h, int remove);
 * @item int
 * @tab
 *  HEAPextend (Heap *h, size_t size, int mayshare);
 * @item int
 * @tab
 *  HEAPload (Heap *h, str nme,ext, int trunc);
 * @item int
 * @tab
 *  HEAPsave (Heap *h, str nme,ext);
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
gdk_export gdk_return HEAPextend(Heap *h, size_t size, int mayshare)
	__attribute__ ((__warn_unused_result__));
gdk_export size_t HEAPvmsize(Heap *h);
gdk_export size_t HEAPmemsize(Heap *h);

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

gdk_export var_t HEAP_malloc(Heap *heap, size_t nbytes);
gdk_export void HEAP_free(Heap *heap, var_t block);

/*
 * @- BAT construction
 * @multitable @columnfractions 0.08 0.7
 * @item @code{BAT* }
 * @tab COLnew (oid headseq, int tailtype, BUN cap, int role)
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
#define BATDELETE	(-9999)

gdk_export BAT *COLnew(oid hseq, int tltype, BUN capacity, int role)
	__attribute__((warn_unused_result));
gdk_export BAT *BATdense(oid hseq, oid tseq, BUN cnt)
	__attribute__((warn_unused_result));
gdk_export gdk_return BATextend(BAT *b, BUN newcap)
	__attribute__ ((__warn_unused_result__));

/* internal */
gdk_export bte ATOMelmshift(int sz);

/*
 * @- BUN manipulation
 * @multitable @columnfractions 0.08 0.7
 * @item BAT*
 * @tab BATappend (BAT *b, BAT *n, BAT *s, bit force)
 * @item BAT*
 * @tab BUNappend (BAT *b, ptr right, bit force)
 * @item BAT*
 * @tab BUNreplace (BAT *b, oid left, ptr right, bit force)
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
 * BAThtype(b) and  BATttype(b) find out the head and tail type of a BAT.
 * @item
 * BUNlast(b) returns the BUN pointer directly after the last BUN
 * in the BAT.
 * @end itemize
 */
/* NOTE: `p' is evaluated after a possible upgrade of the heap */
#if SIZEOF_VAR_T == 8
#define Tputvalue(b, p, v, copyall)					\
	do {								\
		if ((b)->tvarsized && (b)->ttype) {			\
			var_t _d;					\
			ptr _ptr;					\
			ATOMputVAR((b)->ttype, (b)->tvheap, &_d, v);	\
			if ((b)->twidth < SIZEOF_VAR_T &&		\
			    ((b)->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 * (b)->twidth))) { \
				/* doesn't fit in current heap, upgrade it */ \
				if (GDKupgradevarheap((b), _d, (copyall), (b)->batRestricted == BAT_READ) != GDK_SUCCEED) \
					goto bunins_failed;		\
			}						\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				* (unsigned char *) _ptr = (unsigned char) (_d - GDK_VAROFFSET); \
				break;					\
			case 2:						\
				* (unsigned short *) _ptr = (unsigned short) (_d - GDK_VAROFFSET); \
				break;					\
			case 4:						\
				* (unsigned int *) _ptr = (unsigned int) _d; \
				break;					\
			case 8:						\
				* (var_t *) _ptr = _d;			\
				break;					\
			}						\
		} else {						\
			ATOMputFIX((b)->ttype, (p), v);			\
		}							\
	} while (0)
#define Treplacevalue(b, p, v)						\
	do {								\
		if ((b)->tvarsized && (b)->ttype) {			\
			var_t _d;					\
			ptr _ptr;					\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				_d = (var_t) * (unsigned char *) _ptr + GDK_VAROFFSET; \
				break;					\
			case 2:						\
				_d = (var_t) * (unsigned short *) _ptr + GDK_VAROFFSET; \
				break;					\
			case 4:						\
				_d = (var_t) * (unsigned int *) _ptr;	\
				break;					\
			case 8:						\
				_d = * (var_t *) _ptr;			\
				break;					\
			}						\
			ATOMreplaceVAR((b)->ttype, (b)->tvheap, &_d, v); \
			if ((b)->twidth < SIZEOF_VAR_T &&		\
			    ((b)->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 * (b)->twidth))) { \
				/* doesn't fit in current heap, upgrade it */ \
				if (GDKupgradevarheap((b), _d, 0, (b)->batRestricted == BAT_READ) != GDK_SUCCEED) \
					goto bunins_failed;		\
			}						\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				* (unsigned char *) _ptr = (unsigned char) (_d - GDK_VAROFFSET); \
				break;					\
			case 2:						\
				* (unsigned short *) _ptr = (unsigned short) (_d - GDK_VAROFFSET); \
				break;					\
			case 4:						\
				* (unsigned int *) _ptr = (unsigned int) _d; \
				break;					\
			case 8:						\
				* (var_t *) _ptr = _d;			\
				break;					\
			}						\
		} else {						\
			ATOMreplaceFIX((b)->ttype, (p), v);		\
		}							\
	} while (0)
#else
#define Tputvalue(b, p, v, copyall)					\
	do {								\
		if ((b)->tvarsized && (b)->ttype) {			\
			var_t _d;					\
			ptr _ptr;					\
			ATOMputVAR((b)->ttype, (b)->tvheap, &_d, v);	\
			if ((b)->twidth < SIZEOF_VAR_T &&		\
			    ((b)->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 * (b)->twidth))) { \
				/* doesn't fit in current heap, upgrade it */ \
				if (GDKupgradevarheap((b), _d, (copyall), (b)->batRestricted == BAT_READ) != GDK_SUCCEED) \
					goto bunins_failed;		\
			}						\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				* (unsigned char *) _ptr = (unsigned char) (_d - GDK_VAROFFSET); \
				break;					\
			case 2:						\
				* (unsigned short *) _ptr = (unsigned short) (_d - GDK_VAROFFSET); \
				break;					\
			case 4:						\
				* (var_t *) _ptr = _d;			\
				break;					\
			}						\
		} else {						\
			ATOMputFIX((b)->ttype, (p), v);			\
		}							\
	} while (0)
#define Treplacevalue(b, p, v)						\
	do {								\
		if ((b)->tvarsized && (b)->ttype) {			\
			var_t _d;					\
			ptr _ptr;					\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				_d = (var_t) * (unsigned char *) _ptr + GDK_VAROFFSET; \
				break;					\
			case 2:						\
				_d = (var_t) * (unsigned short *) _ptr + GDK_VAROFFSET; \
				break;					\
			case 4:						\
				_d = * (var_t *) _ptr;			\
				break;					\
			}						\
			ATOMreplaceVAR((b)->ttype, (b)->tvheap, &_d, v); \
			if ((b)->twidth < SIZEOF_VAR_T &&		\
			    ((b)->twidth <= 2 ? _d - GDK_VAROFFSET : _d) >= ((size_t) 1 << (8 * (b)->twidth))) { \
				/* doesn't fit in current heap, upgrade it */ \
				if (GDKupgradevarheap((b), _d, 0, (b)->batRestricted == BAT_READ) != GDK_SUCCEED) \
					goto bunins_failed;		\
			}						\
			_ptr = (p);					\
			switch ((b)->twidth) {				\
			case 1:						\
				* (unsigned char *) _ptr = (unsigned char) (_d - GDK_VAROFFSET); \
				break;					\
			case 2:						\
				* (unsigned short *) _ptr = (unsigned short) (_d - GDK_VAROFFSET); \
				break;					\
			case 4:						\
				* (var_t *) _ptr = _d;			\
				break;					\
			}						\
		} else {						\
			ATOMreplaceFIX((b)->ttype, (p), v);		\
		}							\
	} while (0)
#endif
#define tfastins_nocheck(b, p, v, s)			\
	do {						\
		(b)->theap.free += (s);			\
		(b)->theap.dirty |= (s) != 0;		\
		Tputvalue((b), Tloc((b), (p)), (v), 0);	\
	} while (0)

#define bunfastapp_nocheck(b, p, t, ts)		\
	do {					\
		tfastins_nocheck(b, p, t, ts);	\
		(b)->batCount++;		\
	} while (0)

#define bunfastapp_nocheck_inc(b, p, t)			\
	do {						\
		bunfastapp_nocheck(b, p, t, Tsize(b));	\
		p++;					\
	} while (0)

#define bunfastapp(b, t)						\
	do {								\
		BUN _p = BUNlast(b);					\
		if (_p >= BATcapacity(b)) {				\
			if (_p == BUN_MAX || BATcount(b) == BUN_MAX) {	\
				GDKerror("bunfastapp: too many elements to accomodate (" BUNFMT ")\n", BUN_MAX); \
				goto bunins_failed;			\
			}						\
			if (BATextend((b), BATgrows(b)) != GDK_SUCCEED)	\
				goto bunins_failed;			\
		}							\
		bunfastapp_nocheck(b, _p, t, Tsize(b));			\
	} while (0)

gdk_export gdk_return GDKupgradevarheap(BAT *b, var_t v, int copyall, int mayshare)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BUNappend(BAT *b, const void *right, bit force)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATappend(BAT *b, BAT *n, BAT *s, bit force)
	__attribute__ ((__warn_unused_result__));

gdk_export gdk_return BUNdelete(BAT *b, oid o)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATdel(BAT *b, BAT *d)
	__attribute__ ((__warn_unused_result__));

gdk_export gdk_return BUNinplace(BAT *b, BUN p, const void *right, bit force)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATreplace(BAT *b, BAT *p, BAT *n, bit force)
	__attribute__ ((__warn_unused_result__));

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
	(((*(const oid*)(v) == oid_nil) ^ ((b)->tseqbase == oid_nil)) | \
		(*(const oid*)(v) < (b)->tseqbase) |			\
		(*(const oid*)(v) >= (b)->tseqbase + (b)->batCount) ?	\
	 BUN_NONE :							\
	 (BUN) (*(const oid*)(v) - (b)->tseqbase))

#define BATttype(b)	((b)->ttype == TYPE_void && (b)->tseqbase != oid_nil ? \
			 TYPE_oid : (b)->ttype)
#define Tbase(b)	((b)->tvheap->base)

#define Tsize(b)	((b)->twidth)

#define tailsize(b,p)	((b)->ttype?((size_t)(p))<<(b)->tshift:0)

#define Tloc(b,p)	((b)->theap.base+(((size_t)(p))<<(b)->tshift))

typedef var_t stridx_t;
#define SIZEOF_STRIDX_T SIZEOF_VAR_T
#define GDK_VARALIGN SIZEOF_STRIDX_T

#if SIZEOF_VAR_T == 8
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((unsigned char *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 2 ? (var_t) ((unsigned short *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 4 ? (var_t) ((unsigned int *) (b))[p] :			\
	 ((var_t *) (b))[p])
#else
#define VarHeapValRaw(b,p,w)						\
	((w) == 1 ? (var_t) ((unsigned char *) (b))[p] + GDK_VAROFFSET : \
	 (w) == 2 ? (var_t) ((unsigned short *) (b))[p] + GDK_VAROFFSET : \
	 ((var_t *) (b))[p])
#endif
#define VarHeapVal(b,p,w) ((size_t) VarHeapValRaw(b,p,w))
#define BUNtvaroff(bi,p) VarHeapVal((bi).b->theap.base, (p), (bi).b->twidth)

#define BUNtloc(bi,p)	Tloc((bi).b,p)
#define BUNtpos(bi,p)	Tpos(&(bi),p)
#define BUNtvar(bi,p)	(assert((bi).b->ttype && (bi).b->tvarsized), Tbase((bi).b)+BUNtvaroff(bi,p))
#define BUNtail(bi,p)	((bi).b->ttype?(bi).b->tvarsized?BUNtvar(bi,p):BUNtloc(bi,p):BUNtpos(bi,p))

static inline BATiter
bat_iterator(BAT *b)
{
	BATiter bi;

	bi.b = b;
	bi.tvid = 0;
	return bi;
}

#define BUNlast(b)	(assert((b)->batCount <= BUN_MAX), (b)->batCount)

#define BATcount(b)	((b)->batCount)

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
 * @tab BATkey (BAT *b, int onoff)
 * @item BAT *
 * @tab BATmode (BAT *b, int mode)
 * @item BAT *
 * @tab BATsetaccess (BAT *b, int mode)
 * @item int
 * @tab BATdirty (BAT *b)
 * @item int
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
 * system support three modes: PERSISTENT and TRANSIENT.
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
gdk_export BUN BATcount_no_nil(BAT *b);
gdk_export void BATsetcapacity(BAT *b, BUN cnt);
gdk_export void BATsetcount(BAT *b, BUN cnt);
gdk_export BUN BATgrows(BAT *b);
gdk_export gdk_return BATkey(BAT *b, int onoff);
gdk_export gdk_return BATmode(BAT *b, int onoff);
gdk_export void BATroles(BAT *b, const char *tnme);
gdk_export void BAThseqbase(BAT *b, oid o);
gdk_export void BATtseqbase(BAT *b, oid o);
gdk_export gdk_return BATsetaccess(BAT *b, int mode);
gdk_export int BATgetaccess(BAT *b);


#define BATdirty(b)	((b)->batCopiedtodisk == 0 || (b)->batDirty ||	\
			 (b)->batDirtydesc ||				\
			 (b)->theap.dirty ||				\
			 ((b)->tvheap?(b)->tvheap->dirty:0))

#define PERSISTENT		0
#define TRANSIENT		1
#define LOG_DIR			2
#define SHARED_LOG_DIR	3

#define BAT_WRITE		0	/* all kinds of access allowed */
#define BAT_READ		1	/* only read-access allowed */
#define BAT_APPEND		2	/* only reads and appends allowed */

#define BATcapacity(b)	(b)->batCapacity
/*
 * @- BAT manipulation
 * @multitable @columnfractions 0.08 0.7
 * @item BAT *
 * @tab BATclear (BAT *b, int force)
 * @item BAT *
 * @tab COLcopy (BAT *b, int tt, int writeable, int role)
 * @end multitable
 *
 * The routine BATclear removes the binary associations, leading to an
 * empty, but (re-)initialized BAT. Its properties are retained.  A
 * temporary copy is obtained with Colcopy. The new BAT has an unique
 * name.
 */
gdk_export gdk_return BATclear(BAT *b, int force);
gdk_export BAT *COLcopy(BAT *b, int tt, int writeable, int role);

gdk_export gdk_return BATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h)
	__attribute__ ((__warn_unused_result__));

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

gdk_export void BATmsync(BAT *b);

gdk_export size_t BATmemsize(BAT *b, int dirty);

#define NOFARM (-1) /* indicate to GDKfilepath to create relative path */

gdk_export char *GDKfilepath(int farmid, const char *dir, const char *nme, const char *ext);
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
gdk_export gdk_return BATprint(BAT *b);

/*
 * @- BAT clustering
 * @multitable @columnfractions 0.08 0.7
 * @item int
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
gdk_export int BATkeyed(BAT *b);
gdk_export int BATordered(BAT *b);
gdk_export int BATordered_rev(BAT *b);
gdk_export gdk_return BATsort(BAT **sorted, BAT **order, BAT **groups, BAT *b, BAT *o, BAT *g, int reverse, int stable)
	__attribute__ ((__warn_unused_result__));


gdk_export void GDKqsort(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe);
gdk_export void GDKqsort_rev(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe);

#define BATtordered(b)	((b)->ttype == TYPE_void || (b)->tsorted)
#define BATtrevordered(b) (((b)->ttype == TYPE_void && (b)->tseqbase == oid_nil) || (b)->trevsorted)
#define BATtdense(b)	(BATtvoid(b) && (b)->tseqbase != oid_nil)
#define BATtvoid(b)	(((b)->tdense && (b)->tsorted) || (b)->ttype==TYPE_void)
#define BATtkey(b)	(b->tkey != FALSE || BATtdense(b))

/* set some properties that are trivial to deduce */
#define BATsettrivprop(b)						\
	do {								\
		assert((b)->hseqbase != oid_nil);			\
		(b)->batDirtydesc = 1;	/* likely already set */	\
		/* the other head properties should already be correct */ \
		if ((b)->ttype == TYPE_void) {				\
			if ((b)->tseqbase == oid_nil) {			\
				(b)->tnonil = (b)->batCount == 0;	\
				(b)->tnil = !(b)->tnonil;		\
				(b)->trevsorted = 1;			\
				(b)->tkey = (b)->batCount <= 1;		\
				(b)->tdense = 0;			\
			} else {					\
				(b)->tdense = 1;			\
				(b)->tnonil = 1;			\
				(b)->tnil = 0;				\
				(b)->tkey = 1;				\
				(b)->trevsorted = (b)->batCount <= 1;	\
			}						\
			(b)->tsorted = 1;				\
		} else if ((b)->batCount <= 1) {			\
			if (ATOMlinear((b)->ttype)) {			\
				(b)->tsorted = 1;			\
				(b)->trevsorted = 1;			\
			}						\
			(b)->tkey = 1;					\
			if ((b)->batCount == 0) {			\
				(b)->tnonil = 1;			\
				(b)->tnil = 0;				\
				if ((b)->ttype == TYPE_oid) {		\
					(b)->tdense = 1;		\
					(b)->tseqbase = 0;		\
				}					\
			} else if ((b)->ttype == TYPE_oid) {		\
				/* b->batCount == 1 */			\
				oid sqbs;				\
				if ((sqbs = ((oid *) (b)->theap.base)[0]) == oid_nil) { \
					(b)->tdense = 0;		\
					(b)->tnonil = 0;		\
					(b)->tnil = 1;			\
				} else {				\
					(b)->tdense = 1;		\
					(b)->tnonil = 1;		\
					(b)->tnil = 0;			\
				}					\
				(b)->tseqbase = sqbs;			\
			}						\
		}							\
		if (!ATOMlinear((b)->ttype)) {				\
			(b)->tsorted = 0;				\
			(b)->trevsorted = 0;				\
		}							\
	} while (0)

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
 * @item bat
 * @tab BBPcacheid (BAT *b)
 * @end multitable
 *
 * The BAT Buffer Pool module contains the code to manage the storage
 * location of BATs. It uses two tables BBPlogical and BBphysical to
 * relate the BAT name with its corresponding file system name.  This
 * information is retained in an ASCII file within the database home
 * directory for ease of inspection. It is loaded upon restart of the
 * server and saved upon transaction commit (if necessary).
 *
 * The remaining BBP tables contain status information to load, swap
 * and migrate the BATs. The core table is BBPcache which contains a
 * pointer to the BAT descriptor with its heaps.  A zero entry means
 * that the file resides on disk. Otherwise it has been read or mapped
 * into memory.
 *
 * BATs loaded into memory are retained in a BAT buffer pool.  They
 * retain their position within the cache during their life cycle,
 * which make indexing BATs a stable operation.  Their descriptor can
 * be obtained using BBPcacheid.
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
	str logical;		/* logical name */
	str bak;		/* logical name backup */
	bat next;		/* next BBP slot in linked list */
	BAT *desc;		/* the BAT descriptor */
	str physical;		/* dir + basename for storage */
	str options;		/* A string list of options */
	int refs;		/* in-memory references on which the loaded status of a BAT relies */
	int lrefs;		/* logical references on which the existence of a BAT relies */
	volatile int status;	/* status mask used for spin locking */
	/* MT_Id pid;           non-zero thread-id if this BAT is private */
} BBPrec;

gdk_export bat BBPlimit;
#define N_BBPINIT	1000
#if SIZEOF_VOID_P == 4
#define BBPINITLOG	11
#else
#define BBPINITLOG	14
#endif
#define BBPINIT		(1 << BBPINITLOG)
/* absolute maximum number of BATs is N_BBPINIT * BBPINIT */
gdk_export BBPrec *BBP[N_BBPINIT];

/* fast defines without checks; internal use only  */
#define BBP_cache(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].cache
#define BBP_logical(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].logical
#define BBP_bak(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].bak
#define BBP_next(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].next
#define BBP_physical(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].physical
#define BBP_options(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].options
#define BBP_desc(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].desc
#define BBP_refs(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].refs
#define BBP_lrefs(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].lrefs
#define BBP_status(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].status
#define BBP_pid(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)].pid

/* macros that nicely check parameters */
#define BBPcacheid(b)	((b)->batCacheid)
#define BBPstatus(i)	(BBPcheck((i),"BBPstatus")?BBP_status(i):-1)
#define BBPrefs(i)	(BBPcheck((i),"BBPrefs")?BBP_refs(i):-1)
#define BBPcache(i)	(BBPcheck((i),"BBPcache")?BBP_cache(i):(BAT*) NULL)
#define BBPname(i)						\
	(BBPcheck((i), "BBPname") ?				\
	 BBP[(i) >> BBPINITLOG][(i) & (BBPINIT - 1)].logical :	\
	 "")
#define BBPvalid(i)	(BBP_logical(i) != NULL && *BBP_logical(i) != '.')
#define BATgetId(b)	BBPname((b)->batCacheid)

#define BBPRENAME_ALREADY	(-1)
#define BBPRENAME_ILLEGAL	(-2)
#define BBPRENAME_LONG		(-3)

gdk_export void BBPlock(void);

gdk_export void BBPunlock(void);

gdk_export str BBPlogical(bat b, str buf);
gdk_export str BBPphysical(bat b, str buf);
gdk_export BAT *BBPquickdesc(bat b, int delaccess);

/*
 * @+ GDK Extensibility
 * GDK can be extended with new atoms, search accelerators and storage
 * modes.
 *
 * @- Atomic Type Descriptors
 * The atomic types over which the binary associations are maintained
 * are described by an atom descriptor.
 *  @multitable @columnfractions 0.08 0.7
 * @item void
 * @tab ATOMallocate    (str   nme);
 * @item int
 * @tab ATOMindex       (char *nme);
 * @item int
 * @tab ATOMdump        ();
 * @item void
 * @tab ATOMdelete      (int id);
 * @item str
 * @tab ATOMname        (int id);
 * @item int
 * @tab ATOMsize        (int id);
  * @item int
 * @tab ATOMvarsized    (int id);
 * @item ptr
 * @tab ATOMnilptr      (int id);
 * @item ssize_t
 * @tab ATOMfromstr     (int id, str s, size_t* len, ptr* v_dst);
 * @item ssize_t
 * @tab ATOMtostr       (int id, str s, size_t* len, ptr* v_dst);
 * @item hash_t
 * @tab ATOMhash        (int id, ptr val, in mask);
 * @item int
 * @tab ATOMcmp         (int id, ptr val_1, ptr val_2);
 * @item int
 * @tab ATOMfix         (int id, ptr v);
 * @item int
 * @tab ATOMunfix       (int id, ptr v);
 * @item int
 * @tab ATOMheap        (int id, Heap *hp, size_t cap);
 * @item int
 * @tab ATOMput         (int id, Heap *hp, BUN pos_dst, ptr val_src);
 * @item int
 * @tab ATOMdel         (int id, Heap *hp, BUN v_src);
 * @item size_t
 * @tab ATOMlen         (int id, ptr val);
 * @item ptr
 * @tab ATOMnil         (int id);
 * @item ssize_t
 * @tab ATOMformat      (int id, ptr val, char** buf);
 * @item int
 * @tab ATOMprint       (int id, ptr val, stream *fd);
 * @item ptr
 * @tab ATOMdup         (int id, ptr val );
 * @end multitable
 *
 * @- Atom Definition
 * User defined atomic types can be added to a running system with the
 * following interface:.
 *
 * @itemize
 * @item @emph{ATOMallocate()} registers a new atom definition if
 * there is no atom registered yet under that name.
 *
 * @item @emph{ATOMdelete()} unregisters an atom definition.
 *
 * @item @emph{ATOMindex()} looks up the atom descriptor with a certain name.
 * @end itemize
 *
 * @- Atom Manipulation
 *
 * @itemize
 * @item The @emph{ATOMname()} operation retrieves the name of an atom
 * using its id.
 *
 * @item The @emph{ATOMsize()} operation returns the atoms fixed size.
 *
 * @item The @emph{ATOMnilptr()} operation returns a pointer to the
 * nil-value of an atom. We usually take one dedicated value halfway
 * down the negative extreme of the atom range (if such a concept
 * fits), as the nil value.
 *
 * @item The @emph{ATOMnil()} operation returns a copy of the nil
 * value, allocated with GDKmalloc().
 *
 * @item The @emph{ATOMheap()} operation creates a new var-sized atom
 * heap in 'hp' with capacity 'cap'.
 *
 * @item The @emph{ATOMhash()} computes a hash index for a
 * value. `val' is a direct pointer to the atom value. Its return
 * value should be an hash_t between 0 and 'mask'.
 *
 * @item The @emph{ATOMcmp()} operation computes two atomic
 * values. Its parameters are pointers to atomic values.
 *
 * @item The @emph{ATOMlen()} operation computes the byte length for a
 * value.  `val' is a direct pointer to the atom value. Its return
 * value should be an integer between 0 and 'mask'.
 *
 * @item The @emph{ATOMdel()} operation deletes a var-sized atom from
 * its heap `hp'.  The integer byte-index of this value in the heap is
 * pointed to by `val_src'.
 *
 * @item The @emph{ATOMput()} operation inserts an atom `src_val' in a
 * BUN at `dst_pos'. This involves copying the fixed sized part in the
 * BUN. In case of a var-sized atom, this fixed sized part is an
 * integer byte-index into a heap of var-sized atoms. The atom is then
 * also copied into that heap `hp'.
 *
 * @item The @emph{ATOMfix()} and @emph{ATOMunfix()} operations do
 * bookkeeping on the number of references that a GDK application
 * maintains to the atom.  In MonetDB, we use this to count the number
 * of references directly, or through BATs that have columns of these
 * atoms. The only operator for which this is currently relevant is
 * BAT. The operators return the POST reference count to the
 * atom. BATs with fixable atoms may not be stored persistently.
 *
 * @item The @emph{ATOMfromstr()} parses an atom value from string
 * `s'. The memory allocation policy is the same as in
 * @emph{ATOMget()}. The return value is the number of parsed
 * characters or -1 on failure.  Also in case of failure, the output
 * parameter buf is a valid pointer or NULL.
 *
 * @item The @emph{ATOMprint()} prints an ASCII description of the
 * atom value pointed to by `val' on file descriptor `fd'. The return
 * value is the number of parsed characters.
 *
 * @item The @emph{ATOMformat()} is similar to @emph{ATOMprint()}. It
 * prints an atom on a newly allocated string. It must later be freed
 * with @strong{GDKfree}.  The number of characters written is
 * returned. This is minimally the size of the allocated buffer.
 *
 * @item The @emph{ATOMdup()} makes a copy of the given atom. The
 * storage needed for this is allocated and should be removed by the
 * user.
 * @end itemize
 *
 * These wrapper functions correspond closely to the interface
 * functions one has to provide for a user-defined atom. They
 * basically (with exception of @emph{ATOMput()}, @emph{ATOMprint()}
 * and @emph{ATOMformat()}) just have the atom id parameter prepended
 * to them.
 */
typedef struct {
	/* simple attributes */
	char name[IDLENGTH];
	short storage;		/* stored as another type? */
	short linear;		/* atom can be ordered linearly */
	unsigned short size;	/* fixed size of atom */

	/* automatically generated fields */
	const void *atomNull;	/* global nil value */

	/* generic (fixed + varsized atom) ADT functions */
	ssize_t (*atomFromStr) (const char *src, size_t *len, ptr *dst);
	ssize_t (*atomToStr) (str *dst, size_t *len, const void *src);
	void *(*atomRead) (void *dst, stream *s, size_t cnt);
	gdk_return (*atomWrite) (const void *src, stream *s, size_t cnt);
	int (*atomCmp) (const void *v1, const void *v2);
	BUN (*atomHash) (const void *v);
	/* optional functions */
	int (*atomFix) (const void *atom);
	int (*atomUnfix) (const void *atom);

	/* varsized atom-only ADT functions */
	var_t (*atomPut) (Heap *, var_t *off, const void *src);
	void (*atomDel) (Heap *, var_t *atom);
	size_t (*atomLen) (const void *atom);
	void (*atomHeap) (Heap *, size_t);
} atomDesc;

gdk_export atomDesc BATatoms[];
gdk_export int GDKatomcnt;

gdk_export int ATOMallocate(const char *nme);
gdk_export int ATOMindex(const char *nme);

gdk_export str ATOMname(int id);
gdk_export size_t ATOMlen(int id, const void *v);
gdk_export ptr ATOMnil(int id);
gdk_export int ATOMcmp(int id, const void *v_1, const void *v_2);
gdk_export int ATOMprint(int id, const void *val, stream *fd);
gdk_export ssize_t ATOMformat(int id, const void *val, char **buf);

gdk_export ptr ATOMdup(int id, const void *val);

/*
 * @- Built-in Accelerator Functions
 *
 * @multitable @columnfractions 0.08 0.7
 * @item BAT*
 * @tab
 *  BAThash (BAT *b, BUN masksize)
 * @end multitable
 *
 * The current BAT implementation supports three search accelerators:
 * hashing, imprints, and oid ordered index.
 *
 * The routine BAThash makes sure that a hash accelerator on the tail of the
 * BAT exists. GDK_FAIL is returned upon failure to create the supportive
 * structures.
 */
gdk_export gdk_return BAThash(BAT *b, BUN masksize);

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

gdk_export gdk_return BATorderidx(BAT *b, int stable);
gdk_export gdk_return GDKmergeidx(BAT *b, BAT**a, int n_ar);

/*
 * @- Multilevel Storage Modes
 *
 * We should bring in the compressed mode as the first, maybe
 * built-in, mode. We could then add for instance HTTP remote storage,
 * SQL storage, and READONLY (cd-rom) storage.
 *
 * @+ GDK Utilities
 * Interfaces for memory management, error handling, thread management
 * and system information.
 *
 * @- GDK memory management
 * @multitable @columnfractions 0.08 0.8
 * @item void*
 * @tab GDKmalloc (size_t size)
 * @item void*
 * @tab GDKzalloc (size_t size)
 * @item void*
 * @tab GDKmallocmax (size_t size, size_t *maxsize, int emergency)
 * @item void*
 * @tab GDKrealloc (void* pold, size_t size)
 * @item void*
 * @tab GDKreallocmax (void* pold, size_t size, size_t *maxsize, int emergency)
 * @item void
 * @tab GDKfree (void* blk)
 * @item str
 * @tab GDKstrdup (str s)
 * @item str
 * @tab GDKstrndup (str s, size_t n)
 * @end multitable
 *
 * These utilities are primarily used to maintain control over
 * critical interfaces to the C library.  Moreover, the statistic
 * routines help in identifying performance and bottlenecks in the
 * current implementation.
 *
 * Compiled with -DMEMLEAKS the GDK memory management log their
 * activities, and are checked on inconsistent frees and memory leaks.
 */

/* we prefer to use vm_alloc routines on size > GDKmmap */
gdk_export void *GDKmmap(const char *path, int mode, size_t len);

gdk_export size_t GDK_mem_maxsize;	/* max allowed size of committed memory */
gdk_export size_t GDK_vm_maxsize;	/* max allowed size of reserved vm */
gdk_export int	GDK_vm_trim;		/* allow trimming */

gdk_export size_t GDKmem_cursize(void);	/* RAM/swapmem that MonetDB has claimed from OS */
gdk_export size_t GDKvm_cursize(void);	/* current MonetDB VM address space usage */

gdk_export void *GDKmalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__ ((__warn_unused_result__));
gdk_export void *GDKzalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__ ((__warn_unused_result__));
gdk_export void *GDKrealloc(void *pold, size_t size)
	__attribute__ ((__warn_unused_result__));
gdk_export void GDKfree(void *blk);
gdk_export str GDKstrdup(const char *s)
	__attribute__ ((__warn_unused_result__));
gdk_export str GDKstrndup(const char *s, size_t n)
	__attribute__ ((__warn_unused_result__));

#if !defined(NDEBUG) && !defined(STATIC_CODE_ANALYSIS)
/* In debugging mode, replace GDKmalloc and other functions with a
 * version that optionally prints calling information.
 *
 * We have two versions of this code: one using a GNU C extension, and
 * one using traditional C.  The GNU C version also prints the name of
 * the calling function.
 */
#ifdef __GNUC__
#define GDKmalloc(s)							\
	({								\
		size_t _size = (s);					\
		void *_res = GDKmalloc(_size);				\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmalloc(" SZFMT ") -> " PTRFMT	\
				" %s[%s:%d]\n",				\
				_size, PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define GDKzalloc(s)							\
	({								\
		size_t _size = (s);					\
		void *_res = GDKzalloc(_size);				\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKzalloc(" SZFMT ") -> " PTRFMT	\
				" %s[%s:%d]\n",				\
				_size, PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define GDKrealloc(p, s)						\
	({								\
		void *_ptr = (p);					\
		size_t _size = (s);					\
		void *_res = GDKrealloc(_ptr, _size);			\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKrealloc(" PTRFMT "," SZFMT ") -> " PTRFMT \
				" %s[%s:%d]\n",				\
				PTRFMTCAST _ptr, _size, PTRFMTCAST _res, \
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#define GDKfree(p)							\
	({								\
		void *_ptr = (p);					\
		ALLOCDEBUG if (_ptr)					\
			fprintf(stderr,					\
				"#GDKfree(" PTRFMT ")"			\
				" %s[%s:%d]\n",				\
				PTRFMTCAST _ptr,			\
				__func__, __FILE__, __LINE__);		\
		GDKfree(_ptr);						\
	})
#define GDKstrdup(s)							\
	({								\
		const char *_str = (s);					\
		void *_res = GDKstrdup(_str);				\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKstrdup(len=" SZFMT ") -> " PTRFMT	\
				" %s[%s:%d]\n",				\
				strlen(_str),				\
				PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define GDKstrndup(s, n)						\
	({								\
		const char *_str = (s);					\
		size_t _n = (n);					\
		void *_res = GDKstrndup(_str, _n);			\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKstrndup(len=" SZFMT ") -> " PTRFMT	\
				" %s[%s:%d]\n",				\
				_n,					\
				PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define GDKmmap(p, m, l)						\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		size_t _len = (l);					\
		void *_res = GDKmmap(_path, _mode, _len);		\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#GDKmmap(%s,0x%x," SZFMT ") -> " PTRFMT \
				" %s[%s:%d]\n",				\
				_path ? _path : "NULL", _mode, _len,	\
				PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#define malloc(s)							\
	({								\
		size_t _size = (s);					\
		void *_res = malloc(_size);				\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#malloc(" SZFMT ") -> " PTRFMT		\
				" %s[%s:%d]\n",				\
				_size, PTRFMTCAST _res,			\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define calloc(n, s)							\
	({								\
		size_t _nmemb = (n);					\
		size_t _size = (s);					\
		void *_res = calloc(_nmemb,_size);			\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#calloc(" SZFMT "," SZFMT ") -> " PTRFMT \
				" %s[%s:%d]\n",				\
				_nmemb, _size, PTRFMTCAST _res,		\
				__func__, __FILE__, __LINE__);		\
		_res;							\
	})
#define realloc(p, s)							\
	({								\
		void *_ptr = (p);					\
		size_t _size = (s);					\
		void *_res = realloc(_ptr, _size);			\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#realloc(" PTRFMT "," SZFMT ") -> " PTRFMT \
				" %s[%s:%d]\n",				\
				PTRFMTCAST _ptr, _size, PTRFMTCAST _res, \
				__func__, __FILE__, __LINE__);		\
		_res;							\
	 })
#define free(p)								\
	({								\
		void *_ptr = (p);					\
		ALLOCDEBUG						\
			fprintf(stderr,					\
				"#free(" PTRFMT ")"			\
				" %s[%s:%d]\n",				\
				PTRFMTCAST _ptr,			\
				__func__, __FILE__, __LINE__);		\
		free(_ptr);						\
	})
#else
static inline void *
GDKmalloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = GDKmalloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmalloc(" SZFMT ") -> " PTRFMT " [%s:%d]\n",
			   size, PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKmalloc(s)	GDKmalloc_debug((s), __FILE__, __LINE__)
static inline void *
GDKzalloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = GDKzalloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKzalloc(" SZFMT ") -> " PTRFMT " [%s:%d]\n",
			   size, PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKzalloc(s)	GDKzalloc_debug((s), __FILE__, __LINE__)
static inline void *
GDKrealloc_debug(void *ptr, size_t size, const char *filename, int lineno)
{
	void *res = GDKrealloc(ptr, size);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKrealloc(" PTRFMT "," SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   PTRFMTCAST ptr, size, PTRFMTCAST res,
			   filename, lineno);
	return res;
}
#define GDKrealloc(p, s)	GDKrealloc_debug((p), (s), __FILE__, __LINE__)
static inline void
GDKfree_debug(void *ptr, const char *filename, int lineno)
{
	ALLOCDEBUG fprintf(stderr, "#GDKfree(" PTRFMT ") [%s:%d]\n",
			   PTRFMTCAST ptr, filename, lineno);
	GDKfree(ptr);
}
#define GDKfree(p)	GDKfree_debug((p), __FILE__, __LINE__)
static inline char *
GDKstrdup_debug(const char *str, const char *filename, int lineno)
{
	void *res = GDKstrdup(str);
	ALLOCDEBUG fprintf(stderr, "#GDKstrdup(len=" SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   strlen(str), PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKstrdup(s)	GDKstrdup_debug((s), __FILE__, __LINE__)
static inline char *
GDKstrndup_debug(const char *str, size_t n, const char *filename, int lineno)
{
	void *res = GDKstrndup(str, n);
	ALLOCDEBUG fprintf(stderr, "#GDKstrndup(len=" SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   n, PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKstrndup(s, n)	GDKstrndup_debug((s), (n), __FILE__, __LINE__)
static inline void *
GDKmmap_debug(const char *path, int mode, size_t len, const char *filename, int lineno)
{
	void *res = GDKmmap(path, mode, len);
	ALLOCDEBUG fprintf(stderr,
			   "#GDKmmap(%s,0x%x," SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   path ? path : "NULL", mode, len,
			   PTRFMTCAST res, filename, lineno);
	return res;
}
#define GDKmmap(p, m, l)	GDKmmap_debug((p), (m), (l), __FILE__, __LINE__)
static inline void *
malloc_debug(size_t size, const char *filename, int lineno)
{
	void *res = malloc(size);
	ALLOCDEBUG fprintf(stderr,
			   "#malloc(" SZFMT ") -> " PTRFMT " [%s:%d]\n",
			   size, PTRFMTCAST res, filename, lineno);
	return res;
}
#define malloc(s)	malloc_debug((s), __FILE__, __LINE__)
static inline void *
calloc_debug(size_t nmemb, size_t size, const char *filename, int lineno)
{
	void *res = calloc(nmemb, size);
	ALLOCDEBUG fprintf(stderr,
			   "#calloc(" SZFMT "," SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   nmemb, size, PTRFMTCAST res, filename, lineno);
	return res;
}
#define calloc(n, s)	calloc_debug((n), (s), __FILE__, __LINE__)
static inline void *
realloc_debug(void *ptr, size_t size, const char *filename, int lineno)
{
	void *res = realloc(ptr, size);
	ALLOCDEBUG fprintf(stderr,
			   "#realloc(" PTRFMT "," SZFMT ") -> "
			   PTRFMT " [%s:%d]\n",
			   PTRFMTCAST ptr, size, PTRFMTCAST res,
			   filename, lineno);
	return res;
}
#define realloc(p, s)	realloc_debug((p), (s), __FILE__, __LINE__)
static inline void
free_debug(void *ptr, const char *filename, int lineno)
{
	ALLOCDEBUG fprintf(stderr, "#free(" PTRFMT ") [%s:%d]\n",
			   PTRFMTCAST ptr, filename, lineno);
	free(ptr);
}
#define free(p)	free_debug((p), __FILE__, __LINE__)
#endif
#endif

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

gdk_export void GDKerror(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
gdk_export void GDKsyserror(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
#ifndef HAVE_EMBEDDED
__declspec(noreturn) gdk_export void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)))
	__attribute__((__noreturn__));
#else
gdk_export void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
#endif
gdk_export void GDKclrerr(void);

#include "gdk_delta.h"
#include "gdk_hash.h"
#include "gdk_atoms.h"
#include "gdk_bbp.h"
#include "gdk_utils.h"

/* functions defined in gdk_bat.c */
gdk_export BUN void_replace_bat(BAT *b, BAT *p, BAT *u, bit force)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return void_inplace(BAT *b, oid id, const void *val, bit force)
	__attribute__ ((__warn_unused_result__));
gdk_export BAT *BATattach(int tt, const char *heapfile, int role);

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
	case TYPE_bte: return (const void *) &v->val.btval;
	case TYPE_sht: return (const void *) &v->val.shval;
	case TYPE_int: return (const void *) &v->val.ival;
	case TYPE_flt: return (const void *) &v->val.fval;
	case TYPE_dbl: return (const void *) &v->val.dval;
	case TYPE_lng: return (const void *) &v->val.lval;
#ifdef HAVE_HGE
	case TYPE_hge: return (const void *) &v->val.hval;
#endif
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
#define THREADDATA	16

typedef struct threadStruct {
	int tid;		/* logical ID by MonetDB; val == index into this array + 1 (0 is invalid) */
	MT_Id pid;		/* physical thread id (pointer-sized) from the OS thread library */
	str name;
	ptr data[THREADDATA];
	size_t sp;
} ThreadRec, *Thread;


gdk_export ThreadRec GDKthreads[THREADS];

gdk_export int THRgettid(void);
gdk_export Thread THRget(int tid);
gdk_export Thread THRnew(const char *name);
gdk_export void THRdel(Thread t);
gdk_export void THRsetdata(int, ptr);
gdk_export void *THRgetdata(int);
gdk_export int THRhighwater(void);
gdk_export int THRprintf(stream *s, _In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));

gdk_export void *THRdata[THREADDATA];

#define GDKstdout	((stream*)THRdata[0])
#define GDKstdin	((stream*)THRdata[1])

#define GDKout		((stream*)THRgetdata(0))
#define GDKin		((stream*)THRgetdata(1))
#define GDKerrbuf	((char*)THRgetdata(2))
#define GDKsetbuf(x)	THRsetdata(2,(ptr)(x))
#define GDKerr		GDKout

#define THRget_errbuf(t)	((char*)t->data[2])
#define THRset_errbuf(t,b)	(t->data[2] = b)

#ifndef GDK_NOLINK

static inline bat
BBPcheck(bat x, const char *y)
{
	if (x && x != bat_nil) {
		assert(x > 0);

		if (x < 0 || x >= getBBPsize() || BBP_logical(x) == NULL) {
			CHECKDEBUG fprintf(stderr,"#%s: range error %d\n", y, (int) x);
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
		BBPfix(i);
		b = BBP_cache(i);
		if (b == NULL)
			b = BBPdescriptor(i);
	}
	return b;
}

static inline char *
Tpos(BATiter *bi, BUN p)
{
	bi->tvid = bi->b->tseqbase;
	if (bi->tvid != oid_nil)
		bi->tvid += p;
	return (char*)&bi->tvid;
}

#endif

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
gdk_export gdk_return TMsubcommit_list(bat *subcommit, int cnt);

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
gdk_export void BATcommit(BAT *b);
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
 * @item int
 * @tab ALIGNsetT    ((BAT *dst, BAT *src)
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
 * @item BAT*
 * @tab BATmaterialize  (BAT *b)
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
 *
 * The BATmaterialize materializes a VIEW (TODO) or void bat inplace.
 * This is useful as materialization is usually needed for updates.
 */
gdk_export int ALIGNsynced(BAT *b1, BAT *b2);

gdk_export void BATassertProps(BAT *b);

#define BATPROPS_QUICK  0	/* only derive easy (non-resource consuming) properties */
#define BATPROPS_ALL	1	/* derive all possible properties; no matter what cost (key=hash) */
#define BATPROPS_CHECK  3	/* BATPROPS_ALL, but start from scratch and report illegally set properties */

gdk_export BAT *VIEWcreate(oid seq, BAT *b);
gdk_export BAT *VIEWcreate_(oid seq, BAT *b, int stable);
gdk_export void VIEWbounds(BAT *b, BAT *view, BUN l, BUN h);

/* low level functions */
gdk_export void ALIGNsetH(BAT *b1, BAT *b2);
gdk_export void ALIGNsetT(BAT *b1, BAT *b2);

#define ALIGNinp(x,y,f,e)	do {if (!(f)) VIEWchk(x,y,BAT_READ|BAT_APPEND,e); } while (0)
#define ALIGNapp(x,y,f,e)	do {if (!(f)) VIEWchk(x,y,BAT_READ,e); } while (0)

#define BAThrestricted(b) ((b)->batRestricted)
#define BATtrestricted(b) (VIEWtparent(b) ? BBP_cache(VIEWtparent(b))->batRestricted : (b)->batRestricted)

/* The batRestricted field indicates whether a BAT is readonly.
 * we have modes: BAT_WRITE  = all permitted
 *                BAT_APPEND = append-only
 *                BAT_READ   = read-only
 * VIEW bats are always mapped read-only.
 */
#define	VIEWchk(x,y,z,e)						\
	do {								\
		if ((((x)->batRestricted & (z)) != 0) | ((x)->batSharecnt > 0)) { \
			GDKerror("%s: access denied to %s, aborting.\n", \
				 (y), BATgetId(x));			\
			return (e);					\
		}							\
	} while (0)

/* the parentid in a VIEW is correct for the normal view. We must
 * correct for the reversed view.
 */
#define isVIEW(x)							\
	(assert((x)->batCacheid > 0),					\
	 ((x)->theap.parentid ||					\
	  ((x)->tvheap && (x)->tvheap->parentid != (x)->batCacheid)))

#define VIEWtparent(x)	((x)->theap.parentid)
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
 * @- hash-table supported loop over BUNs
 * The first parameter `b' is a BAT, the second (`h') should point to
 * `b->thash', and `v' a pointer to an atomic value (corresponding
 * to the head column of `b'). The 'hb' is an integer index, pointing
 * out the `hb'-th BUN.
 */
#define GDK_STREQ(l,r) (*(char*) (l) == *(char*) (r) && !strcmp(l,r))

#define HASHloop(bi, h, hb, v)					\
	for (hb = HASHget(h, HASHprobe((h), v));		\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h,hb))				\
		if (ATOMcmp(h->type, v, BUNtail(bi, hb)) == 0)
#define HASHloop_str_hv(bi, h, hb, v)				\
	for (hb = HASHget((h),((BUN *) (v))[-1]&(h)->mask);	\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h,hb))				\
		if (GDK_STREQ(v, BUNtvar(bi, hb)))
#define HASHloop_str(bi, h, hb, v)			\
	for (hb = HASHget((h),strHash(v)&(h)->mask);	\
	     hb != HASHnil(h);				\
	     hb = HASHgetlink(h,hb))			\
		if (GDK_STREQ(v, BUNtvar(bi, hb)))

/*
 * HASHloops come in various flavors, from the general HASHloop, as
 * above, to specialized versions (for speed) where the type is known
 * (e.g. HASHloop_int), or the fact that the atom is fixed-sized
 * (HASHlooploc) or variable-sized (HASHloopvar).
 */
#define HASHlooploc(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h,hb))				\
		if (ATOMcmp(h->type, v, BUNtloc(bi, hb)) == 0)
#define HASHloopvar(bi, h, hb, v)				\
	for (hb = HASHget(h,HASHprobe(h, v));			\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h,hb))				\
		if (ATOMcmp(h->type, v, BUNtvar(bi, hb)) == 0)

#define HASHloop_TYPE(bi, h, hb, v, TYPE)			\
	for (hb = HASHget(h, hash_##TYPE(h, v));		\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h,hb))				\
		if (simple_EQ(v, BUNtloc(bi, hb), TYPE))

#define HASHloop_bte(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, bte)
#define HASHloop_sht(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, sht)
#define HASHloop_int(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, int)
#define HASHloop_lng(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, lng)
#ifdef HAVE_HGE
#define HASHloop_hge(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, hge)
#endif
#define HASHloop_flt(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, flt)
#define HASHloop_dbl(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, dbl)

/*
 * @+ Common BAT Operations
 * Much used, but not necessarily kernel-operations on BATs.
 *
 * For each BAT we maintain its dimensions as separately accessible
 * properties. They can be used to improve query processing at higher
 * levels.
 */

#define GDK_MIN_VALUE 3
#define GDK_MAX_VALUE 4

gdk_export void PROPdestroy(PROPrec *p);
gdk_export PROPrec *BATgetprop(BAT *b, int idx);
gdk_export void BATsetprop(BAT *b, int idx, int type, void *v);

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

gdk_export BAT *BATselect(BAT *b, BAT *s, const void *tl, const void *th, int li, int hi, int anti);
gdk_export BAT *BATthetaselect(BAT *b, BAT *s, const void *val, const char *op);

gdk_export BAT *BATconstant(oid hseq, int tt, const void *val, BUN cnt, int role);
gdk_export gdk_return BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr)
	__attribute__ ((__warn_unused_result__));

gdk_export gdk_return BATleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int op, int nil_matches, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export BAT *BATdiff(BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, BUN estimate);
gdk_export gdk_return BATjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int nil_matches, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATbandjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, const void *c1, const void *c2, int li, int hi, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return BATrangejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *rl, BAT *rh, BAT *sl, BAT *sr, int li, int hi, BUN estimate)
	__attribute__ ((__warn_unused_result__));
gdk_export BAT *BATproject(BAT *l, BAT *r);
gdk_export BAT *BATprojectchain(BAT **bats);

gdk_export BAT *BATslice(BAT *b, BUN low, BUN high);

gdk_export BAT *BATunique(BAT *b, BAT *s);

gdk_export BAT *BATmergecand(BAT *a, BAT *b);
gdk_export BAT *BATintersectcand(BAT *a, BAT *b);

gdk_export gdk_return BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *cands, BAT *grps, BUN n, int asc, int distinct)
	__attribute__ ((__warn_unused_result__));

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

/*
 *
 */
#define MAXPARAMS	32

#endif /* _GDK_H_ */
