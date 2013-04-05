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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 *
 * @* Utilities
 * The utility section contains functions to initialize the Monet
 * database system, memory allocation details, and a basic system
 * logging scheme.
 */
#include "monetdb_config.h"

#include "gdk.h"
#include "gdk_private.h"
#include "mutils.h"

static char GDKdbpathStr[PATHLENGTH] = { "dbpath" };

BAT *GDKkey = NULL;
BAT *GDKval = NULL;

#include <signal.h>

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifdef HAVE_PWD_H
# include <pwd.h>
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* prerequisite of sys/sysctl on OpenBSD */
#endif
#ifdef HAVE_SYS_SYSCTL_H
# include <sys/sysctl.h>
#endif

#ifdef NATIVE_WIN32
#define chdir _chdir
#endif

#ifdef NDEBUG
#ifndef NVALGRIND
#define NVALGRIND NDEBUG
#endif
#endif

#if defined(__GNUC__) && defined(HAVE_VALGRIND)
#include <valgrind.h>
#else
#define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed)
#define VALGRIND_FREELIKE_BLOCK(addr, rzB)
#endif

static volatile int GDKstopped = 1;
static void GDKunlockHome(void);
static int GDKgetHome(void);

/*
 * @+ Monet configuration file
 * Parse a possible MonetDB config file (if specified by command line
 * option -c/--config) to extract pre-settings of system variables.
 * Un-recognized parameters are simply skipped, because they may be
 * picked up by other components of the system.  The consequence is
 * that making a typing error in the configuration file may be
 * unnoticed for a long time.  Syntax errors are immediately flagged,
 * though.
 *
 * Since the GDK kernel moves into the database directory, we need to
 * keep the absolute path to the MonetDB config file for top-levels to
 * access its information.
 */

static int
GDKenvironment(str dbpath)
{
	if (dbpath == 0) {
		fprintf(stderr, "!GDKenvironment: database name missing.\n");
		return 0;
	}
	if (strlen(dbpath) >= PATHLENGTH) {
		fprintf(stderr, "!GDKenvironment: database name too long.\n");
		return 0;
	}
	if (!MT_path_absolute(dbpath)) {
		fprintf(stderr, "!GDKenvironment: directory not an absolute path: %s.\n", dbpath);
		return 0;
	}
	strncpy(GDKdbpathStr, dbpath, PATHLENGTH);
	/* make coverity happy: */
	GDKdbpathStr[PATHLENGTH - 1] = 0;
	return 1;
}

char *
GDKgetenv(const char *name)
{
	BUN b = BUNfnd(BATmirror(GDKkey), (ptr) name);

	if (b != BUN_NONE) {
		BATiter GDKenvi = bat_iterator(GDKval);
		return BUNtail(GDKenvi, b);
	}
	return NULL;
}

int
GDKgetenv_isyes(const char *name)
{
	char *val = GDKgetenv(name);

	if (val && strcasecmp(val, "yes") == 0) {
		return 1;
	}
	return 0;
}

int
GDKgetenv_istrue(const char *name)
{
	char *val = GDKgetenv(name);

	if (val && strcasecmp(val, "true") == 0) {
		return 1;
	}
	return 0;
}

int
GDKgetenv_int(const char *name, int def)
{
	char *val = GDKgetenv(name);

	if (val)
		return atoi(val);
	return def;
}

void
GDKsetenv(str name, str value)
{
	BUNappend(GDKkey, name, FALSE);
	BUNappend(GDKval, value, FALSE);
	BATfakeCommit(GDKkey);
	BATfakeCommit(GDKval);
}


/*
 * @+ System logging
 * Per database a log file can be maintained for collection of system
 * management information. Its contents is driven by the upper layers,
 * which encode information such as who logged on and how long the
 * session went on.  The lower layers merely store error information
 * on the file.  It should not be used for crash recovery, because
 * this should be dealt with on a per client basis.
 *
 * A system log can be maintained in the database to keep track of
 * session and crash information. It should regularly be refreshed to
 * avoid disk overflow.
 */
#define GDKLOCK	".gdk_lock"

static FILE *GDKlockFile = 0;

#define GDKLOGOFF	"LOGOFF"
#define GDKFOUNDDEAD	"FOUND	DEAD"
#define GDKLOGON	"LOGON"
#define GDKCRASH	"CRASH"

/*
 * Single-lined comments can now be logged safely, together with
 * process, thread and user ID, and the current time.
 */
void
GDKlog(const char *format, ...)
{
	va_list ap;
	char *p = 0, buf[1024];
	int mustopen = GDKgetHome();
	time_t tm = time(0);

	if (MT_pagesize() == 0)
		return;

	va_start(ap, format);
	vsprintf(buf, format, ap);
	va_end(ap);

	/* remove forbidden characters from message */
	for (p = buf; (p = strchr(p, '\n')) != NULL; *p = ' ')
		;
	for (p = buf; (p = strchr(p, '@')) != NULL; *p = ' ')
		;

	fseek(GDKlockFile, 0, SEEK_END);
#ifndef HAVE_GETUID
#define getuid() 0
#endif
	fprintf(GDKlockFile, "USR=%d PID=%d TIME=%.24s @ %s\n", (int) getuid(), (int) getpid(), ctime(&tm), buf);
	fflush(GDKlockFile);

	if (mustopen)
		GDKunlockHome();
}

/*
 * @+ Interrupt handling
 * The current version simply catches signals and prints a warning.
 * It should be extended to cope with the specifics of the interrupt
 * received.
 */
#if 0				/* these are unused */
static void
BATSIGignore(int nr)
{
	GDKsyserror("! ERROR signal %d caught by thread " SZFMT "\n", nr, (size_t) MT_getpid());
}
#endif

#ifdef WIN32
static void
BATSIGabort(int nr)
{
	GDKexit(3);		/* emulate Windows exit code without pop-up */
}
#endif

#ifndef NATIVE_WIN32
static void
BATSIGinterrupt(int nr)
{
	GDKexit(nr);
}

static int
BATSIGinit(void)
{
/* HACK to pacify compiler */
#if (defined(__INTEL_COMPILER) && (SIZEOF_VOID_P > SIZEOF_INT))
#undef  SIG_IGN			/*((__sighandler_t)1 ) */
#define SIG_IGN   ((__sighandler_t)1L)
#endif

#ifdef SIGPIPE
	(void) signal(SIGPIPE, SIG_IGN);
#endif
#ifdef __SIGRTMIN
	(void) signal(__SIGRTMIN + 1, SIG_IGN);
#endif
#ifdef SIGHUP
	(void) signal(SIGHUP, MT_global_exit);
#endif
#ifdef SIGINT
	(void) signal(SIGINT, BATSIGinterrupt);
#endif
#ifdef SIGTERM
	(void) signal(SIGTERM, BATSIGinterrupt);
#endif
	return 0;
}
#endif /* NATIVE_WIN32 */

/* memory thresholds; these values some "sane" constants only, really
 * set in GDKinit() */
size_t GDK_mmap_minsize = GDK_VM_MAXSIZE;
static size_t GDK_mem_maxsize_max = GDK_VM_MAXSIZE;
size_t GDK_mem_maxsize = GDK_VM_MAXSIZE;
size_t GDK_mem_bigsize = GDK_VM_MAXSIZE;
size_t GDK_vm_maxsize = GDK_VM_MAXSIZE;

int GDK_vm_trim = 1;

#define SEG_SIZE(x,y)   ((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))
#define MAX_BIT         ((int) (sizeof(ssize_t)<<3))

#if defined(GDK_MEM_KEEPHISTO) || defined(GDK_VM_KEEPHISTO)
/* histogram update macro */
#define GDKmallidx(idx, size)				\
	do {						\
		int _mask;				\
							\
		if (size < 128) {			\
			_mask = (1<<6);			\
			idx = 7;			\
		} else {				\
			_mask = (1<<(MAX_BIT-1));	\
			idx = MAX_BIT;			\
		}					\
		while(idx-- > 4) {			\
			if (_mask&size) break;		\
			_mask >>=1;			\
		}					\
	} while (0)
#endif

/* This block is to provide atomic addition and subtraction to select
 * variables.  We use intrinsic functions (recognized and inlined by
 * the compiler) for both the GNU C compiler and Microsoft Visual
 * Studio.  By doing this, we avoid locking overhead.  There is also a
 * fall-back for other compilers. */
#include "gdk_atomic.h"
static volatile ATOMIC_TYPE GDK_mallocedbytes_estimate = 0;
static volatile ATOMIC_TYPE GDK_vm_cursize = 0;
#ifdef GDK_VM_KEEPHISTO
volatile ATOMIC_TYPE GDK_vm_nallocs[MAX_BIT] = { 0 };
#endif
#ifdef GDK_MEM_KEEPHISTO
volatile ATOMIC_TYPE GDK_nmallocs[MAX_BIT] = { 0 };
#endif
#ifdef ATOMIC_LOCK
#ifdef PTHREAD_MUTEX_INITIALIZER
static MT_Lock mbyteslock = PTHREAD_MUTEX_INITIALIZER;
static MT_Lock GDKstoppedLock = PTHREAD_MUTEX_INITIALIZER;
#else
static MT_Lock mbyteslock;
static MT_Lock GDKstoppedLock;
#endif
#endif

size_t _MT_pagesize = 0;	/* variable holding memory size */
size_t _MT_npages = 0;		/* variable holding page size */

void
MT_init(void)
{
#ifdef _MSC_VER
	{
		SYSTEM_INFO sysInfo;

		GetSystemInfo(&sysInfo);
		_MT_pagesize = sysInfo.dwPageSize;
	}
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_PAGESIZE)
	{
		int size;
		size_t len = sizeof(int);
		int mib[2];

		/* Everyone should have permission to make this call,
		 * if we get a failure something is really wrong. */
		mib[0] = CTL_HW;
		mib[1] = HW_PAGESIZE;
		sysctl(mib, 2, &size, &len, NULL, 0);
		_MT_pagesize = size;
	}
#elif defined(HAVE_SYSCONF) && defined(_SC_PAGESIZE)
	_MT_pagesize = (size_t)sysconf(_SC_PAGESIZE);
#endif
	if (_MT_pagesize <= 0)
		_MT_pagesize = 4096;	/* default */

#ifdef _MSC_VER
	{
		MEMORYSTATUSEX memStatEx;

		memStatEx.dwLength = sizeof(memStatEx);
		if (GlobalMemoryStatusEx(&memStatEx))
			_MT_npages = (size_t) (memStatEx.ullTotalPhys / _MT_pagesize);
	}
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_MEMSIZE) && SIZEOF_SIZE_T == SIZEOF_LNG
	/* Darwin, 64-bits */
	{
		uint64_t size = 0;
		size_t len = sizeof(size);
		int mib[2];

		/* Everyone should have permission to make this call,
		 * if we get a failure something is really wrong. */
		mib[0] = CTL_HW;
		mib[1] = HW_MEMSIZE;
		sysctl(mib, 2, &size, &len, NULL, 0);
		_MT_npages = size / _MT_pagesize;
	}
#elif defined(HAVE_SYS_SYSCTL_H) && defined (HW_PHYSMEM64) && SIZEOF_SIZE_T == SIZEOF_LNG
	/* OpenBSD, 64-bits */
	{
		int64_t size = 0;
		size_t len = sizeof(size);
		int mib[2];

		/* Everyone should have permission to make this call,
		 * if we get a failure something is really wrong. */
		mib[0] = CTL_HW;
		mib[1] = HW_PHYSMEM64;
		sysctl(mib, 2, &size, &len, NULL, 0);
		_MT_npages = size / _MT_pagesize;
	}
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_PHYSMEM)
	/* NetBSD, OpenBSD, Darwin, 32-bits; FreeBSD 32 & 64-bits */
	{
# ifdef __FreeBSD__
		unsigned long size = 0;
# else
		int size = 0;
# endif
		size_t len = sizeof(size);
		int mib[2];

		/* Everyone should have permission to make this call,
		 * if we get a failure something is really wrong. */
		mib[0] = CTL_HW;
		mib[1] = HW_PHYSMEM;
		sysctl(mib, 2, &size, &len, NULL, 0);
		_MT_npages = size / _MT_pagesize;
	}
#elif defined(HAVE_SYSCONF) && defined(_SC_PHYS_PAGES)
	_MT_npages = (size_t)sysconf(_SC_PHYS_PAGES);
# if SIZEOF_SIZE_T == SIZEOF_INT
	/* Bug #2935: the value returned here can be more than what can be
	 * addressed on Solaris, so cap the value */
	if (UINT_MAX / _MT_pagesize < _MT_npages)
		_MT_npages = UINT_MAX / _MT_pagesize;
# endif
#else
# error "don't know how to get the amount of physical memory for your OS"
#endif
}

size_t
GDKmem_cursize(void)
{
	/* RAM/swapmem that Monet has claimed from OS */
	size_t heapsize = MT_heapcur() - MT_heapbase;

	return (size_t) SEG_SIZE(heapsize, MT_VMUNITLOG);
}

size_t
GDKmem_inuse(void)
{
	/* RAM/swapmem that Monet is really using now */
	return (size_t) ATOMIC_GET(GDK_mallocedbytes_estimate, mbyteslock, "GDKmem_inuse");
}

size_t
GDKvm_cursize(void)
{
	/* current Monet VM address space usage */
	return (size_t) ATOMIC_GET(GDK_vm_cursize, mbyteslock, "GDKvm_cursize") + GDKmem_inuse();
}

#ifdef GDK_MEM_KEEPHISTO
#define heapinc(_memdelta)						\
	do {								\
		int _idx;						\
									\
		ATOMIC_ADD(GDK_mallocedbytes_estimate, _memdelta, mbyteslock, "heapinc"); \
		GDKmallidx(_idx, _memdelta);				\
		ATOMIC_INC(GDK_nmallocs[_idx], mbyteslock, "heapinc");	\
	} while (0)
#define heapdec(memdelta)						\
	do {								\
		ssize_t _memdelta = (ssize_t) (memdelta);		\
		int _idx;						\
									\
		ATOMIC_SUB(GDK_mallocedbytes_estimate, _memdelta, mbyteslock, "heapdec"); \
		GDKmallidx(_idx, _memdelta);				\
		ATOMIC_DEC(GDK_nmallocs[_idx], mbyteslock, "heapdec");	\
	} while (0)
#else
#define heapinc(_memdelta)						\
	ATOMIC_ADD(GDK_mallocedbytes_estimate, _memdelta, mbyteslock, "heapinc")
#define heapdec(_memdelta)						\
	ATOMIC_SUB(GDK_mallocedbytes_estimate, _memdelta, mbyteslock, "heapdec")
#endif

#ifdef GDK_VM_KEEPHISTO
#define meminc(vmdelta, fcn)						\
	do {								\
		ssize_t _vmdelta = (ssize_t) SEG_SIZE((vmdelta),MT_VMUNITLOG); \
		int _idx;						\
									\
		GDKmallidx(_idx, _vmdelta);				\
		ATOMIC_INC(GDK_vm_nallocs[_idx], mbyteslock, fcn);	\
		ATOMIC_ADD(GDK_vm_cursize, _vmdelta, mbyteslock, fcn);	\
	} while (0)
#define memdec(vmdelta, fcn)						\
	do {								\
		ssize_t _vmdelta = (ssize_t) SEG_SIZE((vmdelta),MT_VMUNITLOG); \
		int _idx;						\
									\
		GDKmallidx(_idx, _vmdelta);				\
		ATOMIC_DEC(GDK_vm_nallocs[_idx], mbyteslock, fcn);	\
		ATOMIC_SUB(GDK_vm_cursize, _vmdelta, mbyteslock, fcn);	\
	} while (0)
#else
#define meminc(vmdelta, fcn)						\
	ATOMIC_ADD(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock, fcn)
#define memdec(vmdelta, fcn)						\
	ATOMIC_SUB(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock, fcn)
#endif

static void
GDKmemdump(void)
{
	struct Mallinfo m = MT_mallinfo();

	MEMDEBUG {
		THRprintf(GDKstdout, "\n#mallinfo.arena = " SSZFMT "\n", (ssize_t) m.arena);
		THRprintf(GDKstdout, "#mallinfo.ordblks = " SSZFMT "\n", (ssize_t) m.ordblks);
		THRprintf(GDKstdout, "#mallinfo.smblks = " SSZFMT "\n", (ssize_t) m.smblks);
		THRprintf(GDKstdout, "#mallinfo.hblkhd = " SSZFMT "\n", (ssize_t) m.hblkhd);
		THRprintf(GDKstdout, "#mallinfo.hblks = " SSZFMT "\n", (ssize_t) m.hblks);
		THRprintf(GDKstdout, "#mallinfo.usmblks = " SSZFMT "\n", (ssize_t) m.usmblks);
		THRprintf(GDKstdout, "#mallinfo.fsmblks = " SSZFMT "\n", (ssize_t) m.fsmblks);
		THRprintf(GDKstdout, "#mallinfo.uordblks = " SSZFMT "\n", (ssize_t) m.uordblks);
		THRprintf(GDKstdout, "#mallinfo.fordblks = " SSZFMT "\n", (ssize_t) m.fordblks);
	}
#ifdef GDK_MEM_KEEPHISTO
	{
		int i;

		THRprintf(GDKstdout, "#memory histogram\n");
		for (i = 3; i < GDK_HISTO_MAX_BIT - 1; i++) {
			size_t j = 1 << i;

			THRprintf(GDKstdout, "# " SZFMT " " SZFMT "\n", j,
				  ATOMIC_GET(GDK_nmallocs[i],
					     mbyteslock, "GDKmemdump"));
		}
	}
#endif
#ifdef GDK_VM_KEEPHISTO
	{
		int i;

		THRprintf(GDKstdout, "\n#virtual memory histogram\n");
		for (i = 12; i < GDK_HISTO_MAX_BIT - 1; i++) {
			size_t j = 1 << i;

			THRprintf(GDKstdout, "# " SZFMT " " SZFMT "\n", j,
				  ATOMIC_GET(GDK_vm_nallocs[i],
					     mbyteslock, "GDKmemdump"));
		}
	}
#endif
}


/*
 * @+ Malloc
 * Malloc normally maps through directly to the OS provided
 * malloc/free/realloc calls. Where possible, we want to use the
 * -lmalloc library on Unix systems, because it allows to influence
 * the memory allocation strategy. This can prevent fragmentation and
 * greatly help enhance performance.
 *
 * The "added-value" of the GDKmalloc/GDKfree/GDKrealloc over the
 * standard OS primitives is that the GDK versions try to do recovery
 * from failure to malloc by initiating a BBPtrim. Also, big requests
 * are redirected to anonymous virtual memory. Finally, additional
 * information on block sizes is kept (helping efficient
 * reallocations) as well as some debugging that guards against
 * duplicate frees.
 *
 * A number of different strategies are available using different
 * switches, however:
 *
 * - zero sized blocks
 *   Normally, GDK gives fatal errors on illegal block sizes.
 *   This can be overridden with  GDK_MEM_NULLALLOWED.
 *
 * - resource tracking
 *   Many malloc interfaces lack a routine that tells the size of a
 *   block by the pointer. We need this information for correct malloc
 *   statistics.
 *
 * - outstanding block histograms
 *   In order to solve the problem, we allocate extra memory in front
 *   of the returned block. With the resource tracking in place, we
 *   keep a total of allocated bytes.  Also, if GDK_MEM_KEEPHISTO is
 *   defined, we keep a histogram of the outstanding blocks on the
 *   log2 of the block size (similarly for virtual.  memory blocks;
 *   define GDK_VM_KEEPHISTO).
 *
 * 64-bits update: Some 64-bit implementations (Linux) of mallinfo is
 * severely broken, as they use int-s for memory sizes!!  This causes
 * corruption of mallinfo stats. As we depend on those, we should keep
 * the malloc arena small. Thus, VM redirection is now quickly
 * applied: for all mallocs > 1MB.
 */
static void
GDKmemfail(str s, size_t len)
{
	int bak = GDKdebug;

	/* bumped your nose against the wall; try to prevent
	 * repetition by adjusting maxsizes
	   if (memtarget < 0.3 * GDKmem_inuse()) {
		   size_t newmax = (size_t) (0.7 * (double) GDKmem_inuse());

		   if (newmax < GDK_mem_maxsize)
		   GDK_mem_maxsize = newmax;
	   }
	   if (vmtarget < 0.3 * GDKvm_cursize()) {
		   size_t newmax = (size_t) (0.7 * (double) GDKvm_cursize());

		   if (newmax < GDK_vm_maxsize)
			   GDK_vm_maxsize = newmax;
	   }
	 */

	THRprintf(GDKstdout, "#%s(" SZFMT ") fails, try to free up space [memory in use=" SZFMT ",virtual memory in use=" SZFMT "]\n", s, len, GDKmem_inuse(), GDKvm_cursize());
	GDKmemdump();
/*	GDKdebug |= MEMMASK;  avoid debugging output */

	BBPtrim(BBPTRIM_ALL);

	GDKdebug = MIN(GDKdebug, bak);
	THRprintf(GDKstdout, "#%s(" SZFMT ") result [mem=" SZFMT ",vm=" SZFMT "]\n", s, len, GDKmem_inuse(), GDKvm_cursize());
	GDKmemdump();
}

/* the blocksize is stored in the ssize_t before it. Negative size <=>
 * VM memory */
#define GDK_MEM_BLKSIZE(p) ((ssize_t*) (p))[-1]
#ifdef __GLIBC__
#define GLIBC_BUG 8
#else
#define GLIBC_BUG 0
#endif

/* we allocate extra space and return a pointer offset by this amount */
#define MALLOC_EXTRA_SPACE	(2 * SIZEOF_VOID_P)

/* allocate 8 bytes extra (so it stays 8-bytes aligned) and put
 * realsize in front */
#define GDKmalloc_prefixsize(s,size)					\
	do {								\
		s = (ssize_t *) malloc(size + MALLOC_EXTRA_SPACE + GLIBC_BUG); \
		if (s != NULL) {					\
			assert((((size_t) s)&7) == 0); /* no MISALIGN */ \
			s = (ssize_t*) ((char*) s + MALLOC_EXTRA_SPACE); \
			s[-1] = (ssize_t) (size + MALLOC_EXTRA_SPACE);	\
		}							\
	} while (0)

/*
 * The emergency flag can be set to force a fatal error if needed.
 * Otherwise, the caller is able to deal with the lack of memory.
 */
void *
GDKmallocmax(size_t size, size_t *maxsize, int emergency)
{
	ssize_t *s = NULL;

	if (size == 0) {
#ifdef GDK_MEM_NULLALLOWED
		return NULL;
#else
		GDKfatal("GDKmallocmax: called with size " SZFMT "", size);
#endif
	}
	size = (size + 7) & ~7;	/* round up to a multiple of eight */
	GDKmalloc_prefixsize(s, size);
	if (s == NULL) {
		GDKmemfail("GDKmalloc", size);
		GDKmalloc_prefixsize(s, size);
		if (s == NULL) {
			if (emergency == 0) {
				GDKerror("GDKmallocmax: failed for " SZFMT " bytes", size);
				return NULL;
			}
			GDKfatal("GDKmallocmax: failed for " SZFMT " bytes", size);
		} else {
			THRprintf(GDKstdout, "#GDKmallocmax: recovery ok. Continuing..\n");
		}
	}
	*maxsize = size;
	heapinc(size + MALLOC_EXTRA_SPACE);
	return (void *) s;
}

void *
GDKmalloc(size_t size)
{
	size_t maxsize = size;
	void *p = GDKmallocmax(size, &maxsize, 0);
	ALLOCDEBUG fprintf(stderr, "#GDKmalloc " SZFMT " " SZFMT " " PTRFMT "\n", size, maxsize, PTRFMTCAST p);
#ifndef NDEBUG
	DEADBEEFCHK if (p)
		memset(p, 0xBD, size);
#endif
	return p;
}

void *
GDKzalloc(size_t size)
{
	size_t maxsize = size;
	void *p = GDKmallocmax(size, &maxsize, 0);
	ALLOCDEBUG fprintf(stderr, "#GDKzalloc " SZFMT " " SZFMT " " PTRFMT "\n", size, maxsize, PTRFMTCAST p);
	if (p)
		memset(p, 0, size);
	return p;
}

static void
GDKfree_(void *blk)
{
	ssize_t size = 0, *s = (ssize_t *) blk;

	if (s == NULL)
		return;

	size = GDK_MEM_BLKSIZE(s);

	/* check against duplicate free */
	assert((size & 2) == 0);

	assert(size != 0);

#ifndef NDEBUG
	/* The check above detects obvious duplicate free's, but fails
	 * in case the "check-bit" is cleared between two free's
	 * (e.g., as the respective memory has been re-allocated and
	 * initialized.
	 * To simplify detection & debugging of duplicate free's, we
	 * now overwrite the to be freed memory, which will trigger a
	 * segfault in case the memory had already been freed and/or
	 * trigger some error in case the memory is accessed after is
	 * has been freed.
	 * To avoid performance penalty in the "production version",
	 * we only do this in debugging/development mode (i.e., when
	 * configured with --enable-assert).
	 * Disable at command line using --debug=33554432
	 */
	DEADBEEFCHK memset(s, 0xDB, size - (MALLOC_EXTRA_SPACE + (size & 1)));	/* 0xDeadBeef */
#endif
	free(((char *) s) - MALLOC_EXTRA_SPACE);
	heapdec(size);
}

void
GDKfree(void *blk)
{
	ALLOCDEBUG fprintf(stderr, "#GDKfree " PTRFMT "\n", PTRFMTCAST blk);
	GDKfree_(blk);
}

ptr
GDKreallocmax(void *blk, size_t size, size_t *maxsize, int emergency)
{
	void *oldblk = blk;
	ssize_t oldsize = 0;
	size_t newsize;

	if (blk == NULL) {
		return GDKmallocmax(size, maxsize, emergency);
	}
	if (size == 0) {
#ifdef GDK_MEM_NULLALLOWED
		GDKfree_(blk);
		*maxsize = 0;
		return NULL;
#else
		GDKfatal("GDKreallocmax: called with size 0");
#endif
	}
	size = (size + 7) & ~7;	/* round up to a multiple of eight */
	oldsize = GDK_MEM_BLKSIZE(blk);

	/* check against duplicate free */
	assert((oldsize & 2) == 0);

	newsize = size + MALLOC_EXTRA_SPACE;

	blk = realloc(((char *) blk) - MALLOC_EXTRA_SPACE,
		      newsize + GLIBC_BUG);
	if (blk == NULL) {
		GDKmemfail("GDKrealloc", newsize);
		blk = realloc(((char *) oldblk) - MALLOC_EXTRA_SPACE,
			      newsize);
		if (blk == NULL) {
			if (emergency == 0) {
				GDKerror("GDKreallocmax: failed for "
					 SZFMT " bytes", newsize);
				return NULL;
			}
			GDKfatal("GDKreallocmax: failed for "
				 SZFMT " bytes", newsize);
		} else {
			THRprintf(GDKstdout, "#GDKremallocmax: "
				  "recovery ok. Continuing..\n");
		}
	}
	/* place MALLOC_EXTRA_SPACE bytes before it */
	assert((((size_t) blk) & 4) == 0);
	blk = ((char *) blk) + MALLOC_EXTRA_SPACE;
	((ssize_t *) blk)[-1] = (ssize_t) newsize;

	/* adapt statistics */
	heapinc(newsize);
	heapdec(oldsize);
	*maxsize = size;
	return blk;
}

ptr
GDKrealloc(void *blk, size_t size)
{
	size_t sz = size;
	void *p;

	p = GDKreallocmax(blk, size, &size, 0);
	ALLOCDEBUG fprintf(stderr, "#GDKrealloc " SZFMT " " SZFMT " " PTRFMT " " PTRFMT "\n", sz, size, PTRFMTCAST blk, PTRFMTCAST p);
	return p;
}


char *
GDKstrdup(const char *s)
{
	int l = strLen(s);
	char *n = (char *) GDKmalloc(l);

	if (n)
		memcpy(n, s, l);
	return n;
}


/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret = MT_mmap(path, mode, len);

	if (ret == (void *) -1L) {
		GDKmemfail("GDKmmap", len);
		ret = MT_mmap(path, mode, len);
		if (ret != (void *) -1L) {
			THRprintf(GDKstdout, "#GDKmmap: recovery ok. Continuing..\n");
		}
	}
	ALLOCDEBUG fprintf(stderr, "#GDKmmap " SZFMT " " PTRFMT "\n", len, PTRFMTCAST ret);
	if (ret != (void *) -1L) {
		/* since mmap directly have content we say it's zero-ed
		 * memory */
		VALGRIND_MALLOCLIKE_BLOCK(ret, len, 0, 1);
		meminc(len, "GDKmmap");
	}
	return (void *) ret;
}

int
GDKmunmap(void *addr, size_t size)
{
	int ret;

	ALLOCDEBUG fprintf(stderr, "#GDKmunmap " SZFMT " " PTRFMT "\n", size, PTRFMTCAST addr);
	ret = MT_munmap(addr, size);
	VALGRIND_FREELIKE_BLOCK(addr, 0);
	if (ret == 0)
		memdec(size, "GDKunmap");
	return ret;
}


/*
 * @+ Session Initialization
 * The interface code to the operating system is highly dependent on
 * the processing environment. It can be filtered away with
 * compile-time flags.  Suicide is necessary due to some system
 * implementation errors.
 *
 * The kernel requires file descriptors for I/O with the user.  They
 * are thread specific and should be obtained by a function.
 *
 * The arguments relevant for the kernel are extracted from the list.
 * Their value is turned into a blanc space.
 */

#define CATNAP		50	/* time to sleep in ms for catnaps */

static MT_Id GDKvmtrim_id;

static void
GDKvmtrim(void *limit)
{
	int highload = 0;
	ssize_t prevmem = 0, prevrss = 0;

	(void) limit;

	do {
		int t;
		size_t rss;
		ssize_t rssdiff, memdiff;
		size_t cursize;

		/* sleep using catnaps so we can exit in a timely fashion */
		for (t = highload ? 500 : 5000; t > 0; t -= CATNAP) {
			MT_sleep_ms(CATNAP);
			if (GDKexiting())
				return;
		}
		rss = MT_getrss();
		rssdiff = (ssize_t) rss - (ssize_t) prevrss;
		cursize = GDKvm_cursize();
		memdiff = (ssize_t) cursize - (ssize_t) prevmem;
		MEMDEBUG THRprintf(GDKstdout, "alloc = " SZFMT " %+zd rss = " SZFMT " %+zd\n", cursize, memdiff, rss, rssdiff);
		prevmem = cursize;
		prevrss = rss;
		if (memdiff >= 0 && rssdiff < -32 * (ssize_t) MT_pagesize()) {
			BBPtrim(rss);
			highload = 1;
		} else {
			highload = 0;
		}
	} while (!GDKexiting());
}

static int THRinit(void);
static void GDKlockHome(void);

int
GDKinit(opt *set, int setlen)
{
	char *dbpath = mo_find_option(set, setlen, "gdk_dbpath");
	char *p;
	opt *n;
	int i, j, nlen = 0;
	char buf[16];

	/* some sanity checks (should also find if symbols are not defined) */
	assert(sizeof(char) == SIZEOF_CHAR);
	assert(sizeof(short) == SIZEOF_SHORT);
	assert(sizeof(int) == SIZEOF_INT);
	assert(sizeof(long) == SIZEOF_LONG);
	assert(sizeof(lng) == SIZEOF_LNG);
	assert(sizeof(oid) == SIZEOF_OID);
	assert(sizeof(void *) == SIZEOF_VOID_P);
	assert(sizeof(wrd) == SIZEOF_WRD);
	assert(sizeof(size_t) == SIZEOF_SIZE_T);
	assert(sizeof(ptrdiff_t) == SIZEOF_PTRDIFF_T);
	assert(SIZEOF_OID == SIZEOF_INT || SIZEOF_OID == SIZEOF_LNG);

#ifndef PTHREAD_MUTEX_INITIALIZER
	MT_lock_init(&MT_system_lock,"GDKinit");
	ATOMIC_INIT(GDKstoppedLock, "GDKinit");
	ATOMIC_INIT(mbyteslock, "mbyteslock");
	MT_lock_init(&GDKnameLock, "GDKnameLock");
	MT_lock_init(&GDKthreadLock, "GDKthreadLock");
	MT_lock_init(&GDKtmLock, "GDKtmLock");
#endif
	for (i = 0; i <= BBP_BATMASK; i++) {
		MT_lock_init(&GDKbatLock[i].swap, "GDKswapLock");
		MT_lock_init(&GDKbatLock[i].hash, "GDKhashLock");
	}
	for (i = 0; i <= BBP_THREADMASK; i++) {
		MT_lock_init(&GDKbbpLock[i].alloc, "GDKcacheLock");
		MT_lock_init(&GDKbbpLock[i].trim, "GDKtrimLock");
		GDKbbpLock[i].free = 0;
	}
	errno = 0;
	if (!GDKenvironment(dbpath))
		return 0;

	if ((p = mo_find_option(set, setlen, "gdk_debug")))
		GDKdebug = strtol(p, NULL, 10);

	if ((p = mo_find_option(set, setlen, "gdk_mem_pagebits")))
		GDK_mem_pagebits = (int) strtol(p, NULL, 10);

	mnstr_init();
	MT_init_posix();
	THRinit();
#ifndef NATIVE_WIN32
	BATSIGinit();
#endif
#ifdef WIN32
	(void) signal(SIGABRT, BATSIGabort);
	_set_abort_behavior(0, _CALL_REPORTFAULT | _WRITE_ABORT_MSG);
	_set_error_mode(_OUT_TO_STDERR);
#endif
	GDKlockHome();

	/* Mserver by default takes 80% of all memory as a default */
	GDK_mem_maxsize = GDK_mem_maxsize_max = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
#ifdef NATIVE_WIN32
	GDK_mmap_minsize = GDK_mem_maxsize_max;
#else
	GDK_mmap_minsize = MIN( 1<<30 , GDK_mem_maxsize_max/6 );
	/*   per op:  2 args + 1 res, each with head & tail  =>  (2+1)*2 = 6  ^ */
#endif
	GDK_mem_bigsize = 1024*1024;
	GDKremovedir(DELDIR);
	BBPinit();

	HEAPcacheInit();

	GDKkey = BATnew(TYPE_void, TYPE_str, 100);
	GDKval = BATnew(TYPE_void, TYPE_str, 100);
	if (GDKkey == NULL)
		GDKfatal("GDKinit: Could not create environment BAT");
	if (GDKval == NULL)
		GDKfatal("GDKinit: Could not create environment BAT");
	BATseqbase(GDKkey,0);
	BATkey(GDKkey, BOUND2BTRUE);
	BATrename(GDKkey, "environment_key");
	BATmode(GDKkey, TRANSIENT);

	BATseqbase(GDKval,0);
	BATkey(GDKval, BOUND2BTRUE);
	BATrename(GDKval, "environment_val");
	BATmode(GDKval, TRANSIENT);

	n = (opt *) malloc(setlen * sizeof(opt));
	for (i = 0; i < setlen; i++) {
		int done = 0;

		for (j = 0; j < nlen; j++) {
			if (strcmp(n[j].name, set[i].name) == 0) {
				if (n[j].kind < set[i].kind) {
					n[j] = set[i];
				}
				done = 1;
				break;
			}
		}
		if (!done) {
			n[nlen] = set[i];
			nlen++;
		}
	}
	for (i = 0; i < nlen; i++)
		GDKsetenv(n[i].name, n[i].value);
	free(n);

	if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, DIR_SEP)) != NULL) {
		GDKsetenv("gdk_dbname", p + 1);
#if DIR_SEP != '/'		/* on Windows look for different separator */
	} else if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, '/')) != NULL) {
		GDKsetenv("gdk_dbname", p + 1);
#endif
	}
	if ((p = GDKgetenv("gdk_mem_maxsize"))) {
		GDK_mem_maxsize = MAX(1 << 26, (size_t) strtoll(p, NULL, 10));
	}
	if ((p = GDKgetenv("gdk_vm_maxsize"))) {
		GDK_vm_maxsize = MAX(1 << 30, (size_t) strtoll(p, NULL, 10));
	}
	if ((p = GDKgetenv("gdk_mem_bigsize"))) {
		/* when allocating >6% of all RAM; do so using
		 * vmalloc() iso malloc() */
		lng max_mem_bigsize = GDK_mem_maxsize_max / 16;

		/* sanity check to avoid memory fragmentation */
		GDK_mem_bigsize = (size_t) MIN(max_mem_bigsize, strtoll(p, NULL, 10));
	}
	if ((p = GDKgetenv("gdk_mmap_minsize"))) {
		GDK_mmap_minsize = MAX(REMAP_PAGE_MAXSIZE, (size_t) strtoll(p, NULL, 10));
	}
	if (GDKgetenv("gdk_mem_pagebits") == NULL) {
		snprintf(buf, sizeof(buf), "%d", GDK_mem_pagebits);
		GDKsetenv("gdk_mem_pagebits", buf);
	}
	if (GDKgetenv("gdk_mem_bigsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mem_bigsize);
		GDKsetenv("gdk_mem_bigsize", buf);
	}
	if (GDKgetenv("monet_pid") == NULL) {
		snprintf(buf, sizeof(buf), "%d", (int) getpid());
		GDKsetenv("monet_pid", buf);
	}

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0)
		GDKnr_threads = MT_check_nr_cores();
#ifdef NATIVE_WIN32
	GDK_mmap_minsize /= (GDKnr_threads ? GDKnr_threads : 1);
#else
	/* WARNING: This unconditionally overwrites above settings, */
	/* incl. setting via MonetDB env. var. "gdk_mmap_minsize" ! */
	GDK_mmap_minsize = MIN( 1<<30 , (GDK_mem_maxsize_max/6) / (GDKnr_threads ? GDKnr_threads : 1) );
	/*    per op:  2 args + 1 res, each with head & tail  =>  (2+1)*2 = 6  ^ */
#endif

	if ((p = mo_find_option(set, setlen, "gdk_vmtrim")) == NULL ||
	    strcasecmp(p, "yes") == 0)
		MT_create_thread(&GDKvmtrim_id, GDKvmtrim, &GDK_mem_maxsize,
				 MT_THR_JOINABLE);

	return 1;
}

int GDKnr_threads = 0;
static int GDKnrofthreads;

int
GDKexiting(void)
{
	return ATOMIC_GET_int(GDKstopped, GDKstoppedLock, "GDKexiting");
}

/* coverity[+kill] */
void
GDKexit(int status)
{
	MT_lock_set(&GDKthreadLock, "GDKexit");
	if (ATOMIC_CAS_int(GDKstopped, 0, 1, GDKstoppedLock, "GDKexit") == 0) {
		if (GDKvmtrim_id)
			MT_join_thread(GDKvmtrim_id);
		GDKnrofthreads = 0;
		MT_lock_unset(&GDKthreadLock, "GDKexit");
		MT_sleep_ms(CATNAP);

		/* Kill all threads except myself */
		if (status == 0) {
			MT_Id pid = MT_getpid();
			Thread t, s;

			for (t = GDKthreads, s = t + THREADS; t < s; t++) {
				if (t->pid) {
					MT_Id victim = t->pid;

					if (t->pid != pid)
						MT_kill_thread(victim);
				}
			}
		}
		(void) GDKgetHome();
#if 0
		/* we can't clean up after killing threads */
		BBPexit();
#endif
		GDKlog(GDKLOGOFF);
		GDKunlockHome();
		MT_global_exit(status);
	}
	MT_lock_unset(&GDKthreadLock, "GDKexit");
}

/*
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 */
int GDKdebug = 0;

batlock_t GDKbatLock[BBP_BATMASK + 1];
bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
#ifdef PTHREAD_MUTEX_INITIALIZER
MT_Lock GDKnameLock = PTHREAD_MUTEX_INITIALIZER;
MT_Lock GDKthreadLock = PTHREAD_MUTEX_INITIALIZER;
MT_Lock GDKtmLock = PTHREAD_MUTEX_INITIALIZER;
#else
MT_Lock GDKnameLock;
MT_Lock GDKthreadLock;
MT_Lock GDKtmLock;
#endif

/*
 * @+ Concurrency control
 * Concurrency control requires actions at several levels of the
 * system.  First, it should be ensured that each database is
 * controlled by a single server process (group). Subsequent attempts
 * should be stopped.  This is regulated through file locking against
 * ".gdk_lock".  Furthermore, the server process is moved to the
 * database directory for improved speed.
 *
 * Before the locks and threads are initiated, we cannot use the
 * normal routines yet. So we have a local fatal here instead of
 * GDKfatal.
 */
static void
GDKlockHome(void)
{
	char *p = 0, buf[1024], host[PATHLENGTH];

	/*
	 * Go there and obtain the global database lock.
	 */
	if (chdir(GDKdbpathStr) < 0) {
		char GDKdirStr[PATHLENGTH];

		/* The DIR_SEP at the end of the path is needed for a
		 * successful call to GDKcreatedir */
		snprintf(GDKdirStr, PATHLENGTH, "%s%c", GDKdbpathStr, DIR_SEP);
		if (!GDKcreatedir(GDKdirStr))
			GDKfatal("GDKlockHome: could not create %s\n", GDKdbpathStr);
		if (chdir(GDKdbpathStr) < 0)
			GDKfatal("GDKlockHome: could not move to %s\n", GDKdbpathStr);
		IODEBUG THRprintf(GDKstdout, "#GDKlockHome: created directory %s\n", GDKdbpathStr);
	}
	if (MT_lockf(GDKLOCK, F_TLOCK, 4, 1) < 0) {
		GDKlockFile = 0;
		GDKfatal("GDKlockHome: Database lock '%s' denied\n", GDKLOCK);
	}
	if ((GDKlockFile = fopen(GDKLOCK, "rb+")) == NULL) {
		GDKfatal("GDKlockHome: Could not open %s\n", GDKLOCK);
	}
	if (fgets(buf, 1024, GDKlockFile) && (p = strchr(buf, ':')))
		*p = 0;
	if (p) {
		sprintf(host, " from '%s'", buf);
	} else {
		IODEBUG THRprintf(GDKstdout, "#GDKlockHome: ignoring empty or invalid %s.\n", GDKLOCK);
		host[0] = 0;
	}
	/*
	 * We have the lock, are the only process currently allowed in
	 * this section.
	 */
	MT_init();
	OIDinit();
	/*
	 * Print the new process list in the global lock file.
	 */
	fseek(GDKlockFile, 0, SEEK_SET);
	if (ftruncate(fileno(GDKlockFile), 0) < 0)
		GDKfatal("GDKlockHome: Could not truncate %s\n", GDKLOCK);
	fflush(GDKlockFile);
	GDKlog(GDKLOGON);
	/*
	 * In shared mode, we allow more parties to join. Release the lock.
	 * No need yet to use GDKstoppedLock: there are no other threads.
	 */
	ATOMIC_SET_int(GDKstopped, 0, GDKstoppedLock, "");
}

static void
GDKunlockHome(void)
{
	if (GDKlockFile) {
		MT_lockf(GDKLOCK, F_ULOCK, 4, 1);
		fclose(GDKlockFile);
		GDKlockFile = 0;
	}
}

/*
 * Really really get the lock. Now!!
 */
static int
GDKgetHome(void)
{
	if (MT_pagesize() == 0 || GDKlockFile)
		return 0;
	while ((GDKlockFile = fopen(GDKLOCK, "r+")) == NULL) {
		GDKerror("GDKgetHome: PANIC on open %s. sleep(1)\n", GDKLOCK);
		MT_sleep_ms(1000);
	}
	if (MT_lockf(GDKLOCK, F_TLOCK, 4, 1) < 0) {
		IODEBUG THRprintf(GDKstdout, "#GDKgetHome: blocking on lock '%s'.\n", GDKLOCK);
		MT_lockf(GDKLOCK, F_LOCK, 4, 1);
	}
	return 1;
}


/*
 * @+ Error handling
  * Errors come in three flavors: warnings, non-fatal and fatal errors.
 * A fatal error leaves a core dump behind after trying to safe the
 * content of the relation.  A non-fatal error returns a message to
 * the user and aborts the current transaction.  Fatal errors are also
 * recorded on the system log for post-mortem analysis.
 * In non-silent mode the errors are immediately sent to output, which
 * makes it hard for upper layers to detect if an error was produced
 * in the process. To facilitate such testing, a global error count is
 * maintained on a thread basis, which can be read out by the function
 * GDKerrorCount(); Furthermore, threads may have set their private
 * error buffer.
 */
static int THRerrorcount[THREADDATA];

/* do the real work for GDKaddbuf below. */
static void
doGDKaddbuf(const char *prefix, const char *message, size_t messagelen, const char *suffix)
{
	char *buf;

	THRerrorcount[THRgettid()]++;
	buf = GDKerrbuf;
	if (buf) {
		char *dst = buf + strlen(buf);
		size_t maxlen = GDKMAXERRLEN - (dst - buf) - 1;

		if (prefix && *prefix && dst < buf + GDKMAXERRLEN) {
			size_t preflen;

			strncpy(dst, prefix, maxlen);
			dst[maxlen] = '\0';
			preflen = strlen(dst);
			maxlen -= preflen;
			dst += preflen;
		}
		if (maxlen > messagelen)
			maxlen = messagelen;
		strncpy(dst, message, maxlen);
		dst += maxlen;
		if (suffix && *suffix && dst < buf + GDKMAXERRLEN) {
			size_t sufflen;

			maxlen = buf + GDKMAXERRLEN - dst - 1;
			strncpy(dst, suffix, maxlen);
			dst[maxlen] = '\0';
			sufflen = strlen(dst);
			maxlen -= sufflen;
			dst += sufflen;
		}
		*dst = '\0';
	} else {
		/* construct format string because the format string
		 * must start with ! */
		char format[32];

		snprintf(format, sizeof(format), "%s%%.*s%s", prefix ? prefix : "", suffix ? suffix : "");
		THRprintf(GDKout, format, (int) messagelen, message);
	}
}

/* print an error or warning message, making sure the message ends in
 * a newline, and also that every line in the message (if there are
 * multiple), starts with an exclamation point.
 * One of the problems complicating this whole issue is that each line
 * should be printed using a single call to THRprintf, and moreover,
 * the format string should start with a "!".  This is because
 * THRprintf adds a "#" to the start of the printed text if the format
 * string doesn't start with "!".
 * Another problem is that we're religious about bounds checking. It
 * would probably also not be quite as bad if we could write in the
 * message buffer.
 */
static void
GDKaddbuf(const char *message)
{
	const char *p, *q;
	char prefix[16];

	if (message == NULL || *message == '\0')	/* empty message, nothing to do */
		return;
	p = message;
	strcpy(prefix, "!");	/* default prefix */
	while (p && *p) {
		if (*p == '!') {
			size_t preflen;

			/* remember last ! prefix (e.g. "!ERROR: ")
			 * for any subsequent lines that start without
			 * ! */
			message = p;
			/* A prefix consists of a ! immediately
			 * followed by some text, followed by a : and
			 * a space.  Anything else results in no
			 * prefix being remembered */
			while (*++p && *p != ':' && *p != '\n' && *p != ' ')
				;
			if (*p == ':' && *++p == ' ') {
				/* found prefix, now remember it */
				preflen = (size_t) (p - message) + 1;
				if (preflen > sizeof(prefix) - 1)
					preflen = sizeof(prefix) - 1;
				strncpy(prefix, message, preflen);
				prefix[preflen] = 0;
			} else {
				/* there is a ! but no proper prefix */
				strcpy(prefix, "!");
				preflen = 1;
			}
			p = message + preflen;
		}

		/* find end of line */
		q = strchr(p, '\n');
		if (q) {
			/* print line including newline */
			q++;
			doGDKaddbuf(prefix, p, (size_t) (q - p), "");
		} else {
			/* no newline at end of buffer: print all the
			 * rest and add a newline */
			doGDKaddbuf(prefix, p, strlen(p), "\n");
			/* we're done since there were no more newlines */
			break;
		}
		p = q;
	}
}

#define GDKERRLEN	(1024+512)

int
GDKerror(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);
	va_list ap;

	if (!strncmp(format, GDKERROR, len)) {
		len = 0;
	} else {
		strcpy(message, GDKERROR);
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

	GDKaddbuf(message);

	return 0;
}

int
GDKsyserror(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);

#ifdef NATIVE_WIN32
	DWORD err = GetLastError();
#else
	int err = errno;
#endif
	va_list ap;

	if (strncmp(format, GDKERROR, len) == 0) {
		len = 0;
	} else {
		strncpy(message, GDKERROR, sizeof(message));
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);
#ifndef NATIVE_WIN32
	if (err > 0 && err < 1024)
#endif
	{
		size_t len1;
		size_t len2;
		size_t len3;
		char *osmsg;
#ifdef NATIVE_WIN32
		char osmsgbuf[256];
		osmsg = osmsgbuf;
		FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, err,
			      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			      (LPTSTR) osmsgbuf, sizeof(osmsgbuf), NULL);
#else
		osmsg = strerror(err);
#endif
		len1 = strlen(message);
		len2 = len1 + strlen(GDKMESSAGE);
		len3 = len2 + strlen(osmsg);

		if (len3 + 2 < sizeof(message)) {
			strcpy(message + len1, GDKMESSAGE);
			strcpy(message + len2, osmsg);
			if (len3 > 0 && message[len3 - 1] != '\n') {
				message[len3] = '\n';
				message[len3 + 1] = 0;
			}
		}
	}
	GDKaddbuf(message);

	errno = 0;
	return err;
}

void
GDKclrerr(void)
{
	char *buf;

	buf = GDKerrbuf;
	if (buf)
		*buf = 0;
}

/* coverity[+kill] */
int
GDKfatal(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKFATAL);
	va_list ap;

	GDKdebug |= IOMASK;
#ifndef NATIVE_WIN32
	BATSIGinit();
#endif
	if (!strncmp(format, GDKFATAL, len)) {
		len = 0;
	} else {
		strcpy(message, GDKFATAL);
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

	fputs(message, stderr);
	fputs("\n", stderr);
	fflush(stderr);

	/*
	 * Real errors should be saved in the lock file for post-crash
	 * inspection.
	 */
	if (GDKexiting()) {
		fflush(stdout);
		MT_exit_thread(1);
		/* exit(1); */
	} else {
		GDKlog("%s", message);
#ifdef COREDUMP
		abort();
#else
		GDKexit(1);
#endif
	}
	return -1;
}


/*
 * @+ Logical Thread management
 *
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 *
 * The first action upon thread creation is to add it to the pool of
 * known threads. This should be done by the thread itself.
 * Subsequently, the thread descriptor can be obtained using THRget.
 * Note that the users should have gained exclusive access already.  A
 * new entry is initialized automatically when not found.  Its file
 * descriptors are the same as for the server and should be
 * subsequently reset.
 */
ThreadRec GDKthreads[THREADS];
void *THRdata[THREADDATA] = { 0 };

Thread
THRget(int tid)
{
	assert(0 < tid && tid <= THREADS);
	return (GDKthreads + tid - 1);
}

static inline size_t
THRsp(void)
{
	int l = 0;
	size_t sp = (size_t) (&l);

	return sp;
}

static Thread
GDK_find_thread(MT_Id pid)
{
	Thread t, s;

	for (t = GDKthreads, s = t + THREADS; t < s; t++) {
		if (t->pid && t->pid == pid) {
			return t;
		}
	}
	return NULL;
}

Thread
THRnew(str name)
{
	int tid = 0;
	Thread t;
	Thread s;
	MT_Id pid = MT_getpid();

	MT_lock_set(&GDKthreadLock, "THRnew");
	s = GDK_find_thread(pid);
	if (s == NULL) {
		for (s = GDKthreads, t = s + THREADS; s < t; s++) {
			if (s->pid == pid) {
				MT_lock_unset(&GDKthreadLock, "THRnew");
				IODEBUG THRprintf(GDKstdout, "#THRnew:duplicate " SZFMT "\n", (size_t) pid);
				return s;
			}
		}
		for (s = GDKthreads, t = s + THREADS; s < t; s++) {
			if (s->pid == 0) {
				break;
			}
		}
		if (s == t) {
			MT_lock_unset(&GDKthreadLock, "THRnew");
			IODEBUG THRprintf(GDKstdout, "#THRnew: too many threads\n");
			return NULL;
		}
		tid = s->tid;
		memset((char *) s, 0, sizeof(*s));
		s->pid = pid;
		s->tid = tid;
		s->data[1] = THRdata[1];
		s->data[0] = THRdata[0];
		s->sp = THRsp();

		PARDEBUG THRprintf(GDKstdout, "#%x " SZFMT " sp = " SZFMT "\n", s->tid, (size_t) pid, s->sp);
		PARDEBUG THRprintf(GDKstdout, "#nrofthreads %d\n", GDKnrofthreads);

		GDKnrofthreads++;
	}
	s->name = name;
	MT_lock_unset(&GDKthreadLock, "THRnew");

	return s;
}

void
THRdel(Thread t)
{
	if (t < GDKthreads || t > GDKthreads + THREADS) {
		GDKfatal("THRdel: illegal call\n");
	}
	MT_lock_set(&GDKthreadLock, "THRdel");
/*	The stream may haven been closed (e.g. in freeClient)  causing an abort
	PARDEBUG THRprintf(GDKstdout, "#pid = " SZFMT ", disconnected, %d left\n", (size_t) t->pid, GDKnrofthreads);
*/

	t->pid = 0;
	GDKnrofthreads--;
	MT_lock_unset(&GDKthreadLock, "THRdel");
}

int
THRhighwater(void)
{
	size_t c;
	Thread s;
	size_t diff;

	s = GDK_find_thread(MT_getpid());
	if (s == NULL)
		return 0;
	c = THRsp();
	diff = (c < s->sp) ? s->sp - c : c - s->sp;
	if (diff > (THREAD_STACK_SIZE - 16 * 1024)) {
		return 1;
	}
	return 0;
}

/*
 * I/O is organized per thread, because users may gain access through
 * the network.  The code below should be improved to gain speed.
 */

static int
THRinit(void)
{
	int i = 0;

	THRdata[0] = (void *) file_wastream(stdout, "stdout");
	THRdata[1] = (void *) file_rastream(stdin, "stdin");
	for (i = 0; i < THREADS; i++) {
		GDKthreads[i].tid = i + 1;
	}
	return 0;
}

void
THRsetdata(int n, ptr val)
{
	Thread s = GDK_find_thread(MT_getpid());

	if (s)
		s->data[n] = val;
}

void *
THRgetdata(int n)
{
	Thread s = GDK_find_thread(MT_getpid());

	return (s ? s->data[n] : THRdata[n]);
}

int
THRgettid(void)
{
	Thread s = GDK_find_thread(MT_getpid());

	return s ? s->tid : 1;
}

static char THRprintbuf[BUFSIZ];

int
THRprintf(stream *s, const char *format, ...)
{
	str bf = THRprintbuf, p = 0;
	size_t bfsz = BUFSIZ;
	int n = 0;
	ptrdiff_t m = 0;
	char c;
	va_list ap;

	if (!s)
		return -1;

	MT_lock_set(&MT_system_lock, "THRprintf");
	if (*format != '!') {
		c = '#';
		if (*format == '#')
			format++;
	} else {
		c = '!';
		format++;
	}

	do {
		p = bf;
		*p++ = c;
		if (GDKdebug & THRDMASK) {
			sprintf(p, "%02d ", THRgettid());
			while (*p)
				p++;
		}
		m = p - bf;
		va_start(ap, format);
		n = vsnprintf(p, bfsz-m, format, ap);
		va_end(ap);
		if (n < 0)
			goto cleanup;
		if ((size_t) n < bfsz - m)
			break;	  /* normal loop exit, usually 1st iteration */
		bfsz = m + n + 1;	/* precisely what is needed */
		if (bf != THRprintbuf)
			free(bf);
		bf = (str) malloc(bfsz);
		assert(bf != NULL);
	} while (1);

	p += n;

	n = 0;
	if (mnstr_write(s, bf, p - bf, 1) != 1)
		n = -1;
      cleanup:
	if (bf != THRprintbuf)
		free(bf);
	MT_lock_unset(&MT_system_lock, "THRprintf");
	return n;
}

static char *_gdk_version_string = VERSION;
/**
 * Returns the GDK version as internally allocated string.  Hence the
 * string does not have to (and should not) be freed.  Do not inline
 * this function or the wrong VERSION will be used.
 */
const char *
GDKversion(void)
{
	return (_gdk_version_string);
}

