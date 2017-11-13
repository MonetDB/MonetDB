/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

BAT *GDKkey = NULL;
BAT *GDKval = NULL;
int GDKdebug = 0;

static char THRprintbuf[BUFSIZ];

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

static volatile ATOMIC_FLAG GDKstopped = ATOMIC_FLAG_INIT;
static void GDKunlockHome(int farmid);

#undef malloc
#undef calloc
#undef realloc
#undef free

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
GDKenvironment(const char *dbpath)
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
	return 1;
}

char *
GDKgetenv(const char *name)
{
	BUN b = BUNfnd(GDKkey, (ptr) name);

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

gdk_return
GDKsetenv(const char *name, const char *value)
{
	if (BUNappend(GDKkey, name, FALSE) != GDK_SUCCEED ||
	    BUNappend(GDKval, value, FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
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

#define GET_GDKLOCK(x) BBPfarms[BBPselectfarm((x), 0, offheap)].lock_file

#define GDKLOGOFF	"LOGOFF"
#define GDKFOUNDDEAD	"FOUND	DEAD"
#define GDKLOGON	"LOGON"
#define GDKCRASH	"CRASH"

/*
 * Single-lined comments can now be logged safely, together with
 * process, thread and user ID, and the current time.
 */
void
GDKlog(FILE *lockFile, const char *format, ...)
{
	va_list ap;
	char *p = 0, buf[1024];
	time_t tm = time(0);
#if defined(HAVE_CTIME_R3) || defined(HAVE_CTIME_R)
	char tbuf[26];
#endif
	char *ctm;

	if (MT_pagesize() == 0 || lockFile == NULL)
		return;

	va_start(ap, format);
	vsprintf(buf, format, ap);
	va_end(ap);

	/* remove forbidden characters from message */
	for (p = buf; (p = strchr(p, '\n')) != NULL; *p = ' ')
		;
	for (p = buf; (p = strchr(p, '@')) != NULL; *p = ' ')
		;

	fseek(lockFile, 0, SEEK_END);
#ifndef HAVE_GETUID
#define getuid() 0
#endif
#ifdef HAVE_CTIME_R3
	ctm = ctime_r(&tm, tbuf, sizeof(tbuf));
#else
#ifdef HAVE_CTIME_R
	ctm = ctime_r(&tm, tbuf);
#else
	ctm = ctime(&tm);
#endif
#endif
	fprintf(lockFile, "USR=%d PID=%d TIME=%.24s @ %s\n", (int) getuid(), (int) getpid(), ctm, buf);
	fflush(lockFile);
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
#define MMAP_MINSIZE_PERSISTENT	((size_t) 1 << 18)
#if SIZEOF_SIZE_T == 4
#define MMAP_MINSIZE_TRANSIENT	((size_t) 1 << 20)
#else
#define MMAP_MINSIZE_TRANSIENT	((size_t) 1 << 32)
#endif
#define MMAP_PAGESIZE		((size_t) 1 << 16)
size_t GDK_mmap_minsize_persistent = MMAP_MINSIZE_PERSISTENT;
size_t GDK_mmap_minsize_transient = MMAP_MINSIZE_TRANSIENT;
size_t GDK_mmap_pagesize = MMAP_PAGESIZE; /* mmap granularity */
size_t GDK_mem_maxsize = GDK_VM_MAXSIZE;
size_t GDK_vm_maxsize = GDK_VM_MAXSIZE;

int GDK_vm_trim = 1;

#define SEG_SIZE(x,y)	((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))

/* This block is to provide atomic addition and subtraction to select
 * variables.  We use intrinsic functions (recognized and inlined by
 * the compiler) for both the GNU C compiler and Microsoft Visual
 * Studio.  By doing this, we avoid locking overhead.  There is also a
 * fall-back for other compilers. */
#include "gdk_atomic.h"
static volatile ATOMIC_TYPE GDK_mallocedbytes_estimate = 0;
#ifndef NDEBUG
static volatile lng GDK_malloc_success_count = -1;
#endif
static volatile ATOMIC_TYPE GDK_vm_cursize = 0;
#ifdef ATOMIC_LOCK
static MT_Lock mbyteslock MT_LOCK_INITIALIZER("mbyteslock");
static MT_Lock GDKstoppedLock MT_LOCK_INITIALIZER("GDKstoppedLock");
#endif

size_t _MT_pagesize = 0;	/* variable holding page size */
size_t _MT_npages = 0;		/* variable holding memory size in pages */

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

#ifdef WIN32
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
		unsigned long size = 0; /* type long required by sysctl() (?) */
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

static void THRinit(void);
static void GDKlockHome(int farmid);

#ifndef STATIC_CODE_ANALYSIS
#ifndef NDEBUG
static MT_Lock mallocsuccesslock MT_LOCK_INITIALIZER("mallocsuccesslock");
#endif
#endif

int
GDKinit(opt *set, int setlen)
{
	char *dbpath = mo_find_option(set, setlen, "gdk_dbpath");
	char *p;
	opt *n;
	int i, nlen = 0;
	int farmid;
	char buf[16];

	/* some sanity checks (should also find if symbols are not defined) */
	assert(sizeof(char) == SIZEOF_CHAR);
	assert(sizeof(short) == SIZEOF_SHORT);
	assert(sizeof(int) == SIZEOF_INT);
	assert(sizeof(long) == SIZEOF_LONG);
	assert(sizeof(lng) == SIZEOF_LNG);
#ifdef HAVE_HGE
	assert(sizeof(hge) == SIZEOF_HGE);
#endif
	assert(sizeof(oid) == SIZEOF_OID);
	assert(sizeof(void *) == SIZEOF_VOID_P);
	assert(sizeof(size_t) == SIZEOF_SIZE_T);
	assert(SIZEOF_OID == SIZEOF_INT || SIZEOF_OID == SIZEOF_LNG);

#ifdef __INTEL_COMPILER
	/* stupid Intel compiler uses a value that cannot be used in an
	 * initializer for NAN, so we have to initialize at run time */
	flt_nil = NAN;
	dbl_nil = NAN;
#endif

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&MT_system_lock,"MT_system_lock");
	ATOMIC_INIT(GDKstoppedLock);
	ATOMIC_INIT(mbyteslock);
	MT_lock_init(&GDKnameLock, "GDKnameLock");
	MT_lock_init(&GDKthreadLock, "GDKthreadLock");
	MT_lock_init(&GDKtmLock, "GDKtmLock");
#ifndef NDEBUG
	MT_lock_init(&mallocsuccesslock, "mallocsuccesslock");
#endif
#endif
	for (i = 0; i <= BBP_BATMASK; i++) {
		MT_lock_init(&GDKbatLock[i].swap, "GDKswapLock");
		MT_lock_init(&GDKbatLock[i].hash, "GDKhashLock");
		MT_lock_init(&GDKbatLock[i].imprints, "GDKimprintsLock");
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

	if (mnstr_init() < 0)
		return 0;
	MT_init_posix();
	THRinit();
#ifndef NATIVE_WIN32
	BATSIGinit();
#endif
#ifdef WIN32
	(void) signal(SIGABRT, BATSIGabort);
#ifndef __MINGW32__ // MinGW does not have these
	_set_abort_behavior(0, _CALL_REPORTFAULT | _WRITE_ABORT_MSG);
	_set_error_mode(_OUT_TO_STDERR);
#endif
#endif
	MT_init();
	BBPdirty(1);

	/* now try to lock the database: go through all farms, and if
	 * we see a new directory, lock it */
	for (farmid = 0; farmid < MAXFARMS; farmid++) {
		if (BBPfarms[farmid].dirname != NULL) {
			int skip = 0;
			int j;
			for (j = 0; j < farmid; j++) {
				if (BBPfarms[j].dirname != NULL &&
				    strcmp(BBPfarms[farmid].dirname, BBPfarms[j].dirname) == 0) {
					skip = 1;
					break;
				}
			}
			if (!skip)
				GDKlockHome(farmid);
		}
	}

	/* Mserver by default takes 80% of all memory as a default */
	GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
	BBPinit();

	if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
		GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
		if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
			GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
	}

	n = (opt *) malloc(setlen * sizeof(opt));
	if (n == NULL)
		GDKfatal("GDKinit: malloc failed\n");
	for (i = 0; i < setlen; i++) {
		int done = 0;
		int j;

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
	/* check some options before creating our first BAT */
	for (i = 0; i < nlen; i++) {
		if (strcmp("gdk_mem_maxsize", n[i].name) == 0) {
			GDK_mem_maxsize = (size_t) strtoll(n[i].value, NULL, 10);
			GDK_mem_maxsize = MAX(1 << 26, GDK_mem_maxsize);
		} else if (strcmp("gdk_vm_maxsize", n[i].name) == 0) {
			GDK_vm_maxsize = (size_t) strtoll(n[i].value, NULL, 10);
			GDK_vm_maxsize = MAX(1 << 30, GDK_vm_maxsize);
			if (GDK_vm_maxsize < GDK_mmap_minsize_persistent / 4)
				GDK_mmap_minsize_persistent = GDK_vm_maxsize / 4;
			if (GDK_vm_maxsize < GDK_mmap_minsize_transient / 4)
				GDK_mmap_minsize_transient = GDK_vm_maxsize / 4;
		} else if (strcmp("gdk_mmap_minsize_persistent", n[i].name) == 0) {
			GDK_mmap_minsize_persistent = (size_t) strtoll(n[i].value, NULL, 10);
		} else if (strcmp("gdk_mmap_minsize_transient", n[i].name) == 0) {
			GDK_mmap_minsize_transient = (size_t) strtoll(n[i].value, NULL, 10);
		} else if (strcmp("gdk_mmap_pagesize", n[i].name) == 0) {
			GDK_mmap_pagesize = (size_t) strtoll(n[i].value, NULL, 10);
			if (GDK_mmap_pagesize < 1 << 12 ||
			    GDK_mmap_pagesize > 1 << 20 ||
			    /* x & (x - 1): turn off rightmost 1 bit;
			     * i.e. if result is zero, x is power of
			     * two */
			    (GDK_mmap_pagesize & (GDK_mmap_pagesize - 1)) != 0)
				GDKfatal("GDKinit: gdk_mmap_pagesize must be power of 2 between 2**12 and 2**20\n");
		}
	}

	GDKkey = COLnew(0, TYPE_str, 100, TRANSIENT);
	GDKval = COLnew(0, TYPE_str, 100, TRANSIENT);
	if (GDKkey == NULL || GDKval == NULL) {
		/* no cleanup necessary before GDKfatal */
		GDKfatal("GDKinit: Could not create environment BAT");
	}
	if (BBPrename(GDKkey->batCacheid, "environment_key") != 0 ||
	    BBPrename(GDKval->batCacheid, "environment_val") != 0)
		GDKfatal("GDKinit: BBPrename failed");

	/* store options into environment BATs */
	for (i = 0; i < nlen; i++)
		if (GDKsetenv(n[i].name, n[i].value) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	free(n);

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0)
		GDKnr_threads = MT_check_nr_cores();

	if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, DIR_SEP)) != NULL) {
		if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
#if DIR_SEP != '/'		/* on Windows look for different separator */
	} else if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, '/')) != NULL) {
		if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
#endif
	}
	if (GDKgetenv("gdk_vm_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_vm_maxsize);
		if (GDKsetenv("gdk_vm_maxsize", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}
	if (GDKgetenv("gdk_mem_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mem_maxsize);
		if (GDKsetenv("gdk_mem_maxsize", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}
	if (GDKgetenv("gdk_mmap_minsize_persistent") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mmap_minsize_persistent);
		if (GDKsetenv("gdk_mmap_minsize_persistent", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}
	if (GDKgetenv("gdk_mmap_minsize_transient") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mmap_minsize_transient);
		if (GDKsetenv("gdk_mmap_minsize_transient", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}
	if (GDKgetenv("gdk_mmap_pagesize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mmap_pagesize);
		if (GDKsetenv("gdk_mmap_pagesize", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}
	if (GDKgetenv("monet_pid") == NULL) {
		snprintf(buf, sizeof(buf), "%d", (int) getpid());
		if (GDKsetenv("monet_pid", buf) != GDK_SUCCEED)
			GDKfatal("GDKinit: GDKsetenv failed");
	}

	return 1;
}

int GDKnr_threads = 0;
static int GDKnrofthreads;

int
GDKexiting(void)
{
	int stopped;
#ifdef ATOMIC_LOCK
	pthread_mutex_lock(&GDKstoppedLock.lock);
#endif
	stopped = GDKstopped != 0;
#ifdef ATOMIC_LOCK
	pthread_mutex_unlock(&GDKstoppedLock.lock);
#endif
	return stopped;
}

static struct serverthread {
	struct serverthread *next;
	MT_Id pid;
} *serverthread;

void
GDKprepareExit(void)
{
	struct serverthread *st;

	if (ATOMIC_TAS(GDKstopped, GDKstoppedLock) != 0)
		return;

	MT_lock_set(&GDKthreadLock);
	for (st = serverthread; st; st = serverthread) {
		MT_lock_unset(&GDKthreadLock);
		MT_join_thread(st->pid);
		MT_lock_set(&GDKthreadLock);
		serverthread = st->next;
		GDKfree(st);
	}
	MT_lock_unset(&GDKthreadLock);
	join_detached_threads();
}

/* Register a thread that should be waited for in GDKreset.  The
 * thread must exit by itself when GDKexiting() returns true. */
void
GDKregister(MT_Id pid)
{
	struct serverthread *st;

	if ((st = GDKmalloc(sizeof(struct serverthread))) == NULL)
		return;
	st->pid = pid;
	MT_lock_set(&GDKthreadLock);
	st->next = serverthread;
	serverthread = st;
	MT_lock_unset(&GDKthreadLock);
}

/* coverity[+kill] */
void
GDKreset(int status, int exit)
{
	MT_Id pid = MT_getpid();
	Thread t, s;
	struct serverthread *st;
	int farmid;
	int i;

	if( GDKkey){
		BBPunfix(GDKkey->batCacheid);
		GDKkey = 0;
	}
	if( GDKval){
		BBPunfix(GDKval->batCacheid);
		GDKval = 0;
	}

	MT_lock_set(&GDKthreadLock);
	for (st = serverthread; st; st = serverthread) {
		MT_lock_unset(&GDKthreadLock);
		MT_join_thread(st->pid);
		MT_lock_set(&GDKthreadLock);
		serverthread = st->next;
		GDKfree(st);
	}
	MT_lock_unset(&GDKthreadLock);
	join_detached_threads();

	if (status == 0) {
		/* they had their chance, now kill them */
		int killed = 0;
		MT_lock_set(&GDKthreadLock);
		for (t = GDKthreads, s = t + THREADS; t < s; t++) {
			if (t->pid) {
				MT_Id victim = t->pid;

				if (t->pid != pid) {
					int e;

					killed = 1;
					e = MT_kill_thread(victim);
					fprintf(stderr, "#GDKexit: killing thread %d\n", e);
					GDKnrofthreads --;
				}
			}
			if (t->name)
				GDKfree(t->name);
		}
		assert(GDKnrofthreads <= 1);
		/* all threads ceased running, now we can clean up */
		if (!killed) {
			/* we can't clean up after killing threads */
			BBPexit();
		}
		GDKlog(GET_GDKLOCK(0), GDKLOGOFF);

		for (farmid = 0; farmid < MAXFARMS; farmid++) {
			if (BBPfarms[farmid].dirname != NULL) {
				int skip = 0;
				int j;
				for (j = 0; j < farmid; j++) {
					if (BBPfarms[j].dirname != NULL &&
					    strcmp(BBPfarms[farmid].dirname, BBPfarms[j].dirname) == 0) {
						skip = 1;
						break;
					}
				}
				if (!skip)
					GDKunlockHome(farmid);
			}
		}

#if !defined(USE_PTHREAD_LOCKS) && !defined(NDEBUG)
		TEMDEBUG GDKlockstatistics(1);
#endif
		GDKdebug = 0;
		GDK_mmap_minsize_persistent = MMAP_MINSIZE_PERSISTENT;
		GDK_mmap_minsize_transient = MMAP_MINSIZE_TRANSIENT;
		GDK_mmap_pagesize = MMAP_PAGESIZE;
		GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
		GDK_vm_maxsize = GDK_VM_MAXSIZE;
		GDKatomcnt = TYPE_str + 1;

		GDK_vm_trim = 1;

		if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
			GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
			if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
				GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
		}

		GDKnr_threads = 0;
		GDKnrofthreads = 0;
		close_stream((stream *) THRdata[0]);
		close_stream((stream *) THRdata[1]);
		for (i = 0; i <= BBP_BATMASK; i++) {
			MT_lock_destroy(&GDKbatLock[i].swap);
			MT_lock_destroy(&GDKbatLock[i].hash);
			MT_lock_destroy(&GDKbatLock[i].imprints);
		}
		for (i = 0; i <= BBP_THREADMASK; i++) {
			MT_lock_destroy(&GDKbbpLock[i].alloc);
			MT_lock_destroy(&GDKbbpLock[i].trim);
			GDKbbpLock[i].free = 0;
		}

		memset((char*) GDKthreads, 0, sizeof(GDKthreads));
		memset((char*) THRdata, 0, sizeof(THRdata));
		memset((char*) THRprintbuf,0, sizeof(THRprintbuf));
		gdk_bbp_reset();
		MT_lock_unset(&GDKthreadLock);
		//gdk_system_reset(); CHECK OUT
	}
	ATOMunknown_clean();
#ifdef NEED_MT_LOCK_INIT
	MT_lock_destroy(&MT_system_lock);
#if defined(USE_PTHREAD_LOCKS) && defined(ATOMIC_LOCK)
	MT_lock_destroy(&GDKstoppedLock);
	MT_lock_destroy(&mbyteslock);
#endif
	MT_lock_destroy(&GDKnameLock);
	MT_lock_destroy(&GDKthreadLock);
	MT_lock_destroy(&GDKtmLock);
#ifndef NDEBUG
	MT_lock_destroy(&mallocsuccesslock);
#endif
#endif
#ifndef HAVE_EMBEDDED
	if (exit) {
		MT_global_exit(status);
	}
#endif
}

void
GDKexit(int status)
{
	if (GET_GDKLOCK(0) == NULL) {
#ifdef HAVE_EMBEDDED
		return;
#endif
		/* no database lock, so no threads, so exit now */
		exit(status);
	}
	GDKprepareExit();
	GDKreset(status, 1);
#ifndef HAVE_EMBEDDED
	MT_exit_thread(-1);
#endif
}

/*
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 */

batlock_t GDKbatLock[BBP_BATMASK + 1];
bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
MT_Lock GDKnameLock MT_LOCK_INITIALIZER("GDKnameLock");
MT_Lock GDKthreadLock MT_LOCK_INITIALIZER("GDKthreadLock");
MT_Lock GDKtmLock MT_LOCK_INITIALIZER("GDKtmLock");

/*
 * @+ Concurrency control
 * Concurrency control requires actions at several levels of the
 * system.  First, it should be ensured that each database is
 * controlled by a single server process (group). Subsequent attempts
 * should be stopped.  This is regulated through file locking against
 * ".gdk_lock".
 *
 * Before the locks and threads are initiated, we cannot use the
 * normal routines yet. So we have a local fatal here instead of
 * GDKfatal.
 */
static void
GDKlockHome(int farmid)
{
	int fd;
	struct stat st;
	char *gdklockpath;
	FILE *GDKlockFile;

	assert(BBPfarms[farmid].dirname != NULL);
	assert(BBPfarms[farmid].lock_file == NULL);

	gdklockpath = GDKfilepath(farmid, NULL, GDKLOCK, NULL);

	/*
	 * Obtain the global database lock.
	 */
	if (stat(BBPfarms[farmid].dirname, &st) < 0 &&
	    GDKcreatedir(gdklockpath) != GDK_SUCCEED) {
		GDKfatal("GDKlockHome: could not create %s\n",
			 BBPfarms[farmid].dirname);
	}
	if ((fd = MT_lockf(gdklockpath, F_TLOCK, 4, 1)) < 0) {
		GDKfatal("GDKlockHome: Database lock '%s' denied\n",
			 gdklockpath);
	}

	/* now we have the lock on the database and are the only
	 * process allowed in this section */

	if ((GDKlockFile = fdopen(fd, "r+")) == NULL) {
		close(fd);
		GDKfatal("GDKlockHome: Could not fdopen %s\n", gdklockpath);
	}
	BBPfarms[farmid].lock_file = GDKlockFile;

	/*
	 * Print the new process list in the global lock file.
	 */
	fseek(GDKlockFile, 0, SEEK_SET);
	if (ftruncate(fileno(GDKlockFile), 0) < 0)
		GDKfatal("GDKlockHome: Could not truncate %s\n", gdklockpath);
	fflush(GDKlockFile);
	GDKlog(GDKlockFile, GDKLOGON);
	GDKfree(gdklockpath);
}


static void
GDKunlockHome(int farmid)
{
	if (BBPfarms[farmid].lock_file) {
		char *gdklockpath = GDKfilepath(farmid, NULL, GDKLOCK, NULL);
		MT_lockf(gdklockpath, F_ULOCK, 4, 1);
		fclose(BBPfarms[farmid].lock_file);
		BBPfarms[farmid].lock_file = NULL;
		GDKfree(gdklockpath);
	}
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

/* do the real work for GDKaddbuf below. */
static void
doGDKaddbuf(const char *prefix, const char *message, size_t messagelen, const char *suffix)
{
	char *buf;

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
		THRprintf(GDKout, "%s%.*s%s", prefix ? prefix : "",
			  (int) messagelen, message, suffix ? suffix : "");
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

void
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
	if (vsnprintf(message + len, sizeof(message) - (len + 2), format, ap) < 0)
		strcpy(message, GDKERROR "an error occurred within GDKerror.\n");
	va_end(ap);

	GDKaddbuf(message);
}

void
GDKsyserror(const char *format, ...)
{
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);

	int err = errno;
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
}

void
GDKclrerr(void)
{
	char *buf;

	buf = GDKerrbuf;
	if (buf)
		*buf = 0;
}

jmp_buf GDKfataljump;
str GDKfatalmsg;
bit GDKfataljumpenable = 0;

/* coverity[+kill] */
void
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

	if (!GDKfataljumpenable) {
		fputs(message, stderr);
		fputs("\n", stderr);
		fflush(stderr);

		/*
		 * Real errors should be saved in the log file for post-crash
		 * inspection.
		 */
		if (GDKexiting()) {
			fflush(stdout);
			MT_exit_thread(1);
			/* exit(1); */
		} else {
			GDKlog(GET_GDKLOCK(0), "%s", message);
	#ifdef COREDUMP
			abort();
	#else
			GDKexit(1);
	#endif
		}
	} else { // in embedded mode, we really don't want to kill our host
		GDKfatalmsg = GDKstrdup(message);
		longjmp(GDKfataljump, 42);
	}
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

#if defined(_MSC_VER) && _MSC_VER >= 1900
#pragma warning(disable : 4172)
#endif
static inline size_t
THRsp(void)
{
	int l = 0;
	uintptr_t sp = (uintptr_t) (&l);

	return (size_t) sp;
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
THRnew(const char *name)
{
	int tid = 0;
	Thread t;
	Thread s;
	MT_Id pid = MT_getpid();

	MT_lock_set(&GDKthreadLock);
	s = GDK_find_thread(pid);
	if (s == NULL) {
		for (s = GDKthreads, t = s + THREADS; s < t; s++) {
			if (s->pid == pid) {
				MT_lock_unset(&GDKthreadLock);
				IODEBUG fprintf(stderr, "#THRnew:duplicate " SZFMT "\n", (size_t) pid);
				return s;
			}
		}
		for (s = GDKthreads, t = s + THREADS; s < t; s++) {
			if (s->pid == 0) {
				break;
			}
		}
		if (s == t) {
			MT_lock_unset(&GDKthreadLock);
			IODEBUG fprintf(stderr, "#THRnew: too many threads\n");
			GDKerror("THRnew: too many threads\n");
			return NULL;
		}
		tid = s->tid;
		memset(s, 0, sizeof(*s));
		s->pid = pid;
		s->tid = tid;
		s->data[1] = THRdata[1];
		s->data[0] = THRdata[0];
		s->sp = THRsp();

		PARDEBUG fprintf(stderr, "#%x " SZFMT " sp = " SZFMT "\n", s->tid, (size_t) pid, s->sp);
		PARDEBUG fprintf(stderr, "#nrofthreads %d\n", GDKnrofthreads);

		GDKnrofthreads++;
		s->name = GDKstrdup(name);
	}
	MT_lock_unset(&GDKthreadLock);

	return s;
}

void
THRdel(Thread t)
{
	if (t < GDKthreads || t > GDKthreads + THREADS) {
		GDKfatal("THRdel: illegal call\n");
	}
	MT_lock_set(&GDKthreadLock);
	PARDEBUG fprintf(stderr, "#pid = " SZFMT ", disconnected, %d left\n", (size_t) t->pid, GDKnrofthreads);

	GDKfree(t->name);
	t->name = NULL;
	t->pid = 0;
	GDKnrofthreads--;
	MT_lock_unset(&GDKthreadLock);
}

int
THRhighwater(void)
{
	size_t c;
	Thread s;
	size_t diff;
	int rc = 0;

	MT_lock_set(&GDKthreadLock);
	s = GDK_find_thread(MT_getpid());
	if (s != NULL) {
		c = THRsp();
		diff = c < s->sp ? s->sp - c : c - s->sp;
		if (diff > THREAD_STACK_SIZE - 80 * 1024)
			rc = 1;
	}
	MT_lock_unset(&GDKthreadLock);
	return rc;
}

/*
 * I/O is organized per thread, because users may gain access through
 * the network.  The code below should be improved to gain speed.
 */

static void
THRinit(void)
{
	int i = 0;

	THRdata[0] = (void *) file_wastream(stdout, "stdout");
	THRdata[1] = (void *) file_rastream(stdin, "stdin");
	for (i = 0; i < THREADS; i++) {
		GDKthreads[i].tid = i + 1;
	}
}

void
THRsetdata(int n, ptr val)
{
	Thread s;

	MT_lock_set(&GDKthreadLock);
	s = GDK_find_thread(MT_getpid());
	if (s) {
		assert(val == NULL || s->data[n] == NULL);
		s->data[n] = val;
	}
	MT_lock_unset(&GDKthreadLock);
}

void *
THRgetdata(int n)
{
	Thread s;
	void *d;

	MT_lock_set(&GDKthreadLock);
	s = GDK_find_thread(MT_getpid());
	d = s ? s->data[n] : THRdata[n];
	MT_lock_unset(&GDKthreadLock);
	return d;
}

int
THRgettid(void)
{
	Thread s;
	int t;

	MT_lock_set(&GDKthreadLock);
	s = GDK_find_thread(MT_getpid());
	t = s ? s->tid : 1;
	MT_lock_unset(&GDKthreadLock);
	return t;
}

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

	MT_lock_set(&MT_system_lock);
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
		if (bf == NULL)
			return -1;
	} while (1);

	p += n;

	n = 0;
	if (mnstr_write(s, bf, p - bf, 1) != 1)
		n = -1;
      cleanup:
	if (bf != THRprintbuf)
		free(bf);
	MT_lock_unset(&MT_system_lock);
	return n;
}

static const char *_gdk_version_string = VERSION;
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

/**
 * Extracts the last directory from a path string, if possible.
 * Stores the parent directory (path) in last_dir_parent and
 * the last directory (name) without a leading separators in last_dir.
 * Returns GDK_SUCCEED for success, GDK_FAIL on failure.
 */
gdk_return
GDKextractParentAndLastDirFromPath(const char *path, char *last_dir_parent, char *last_dir)
{
	char *last_dir_with_sep;
	ptrdiff_t last_dirsep_index;

	if (path == NULL || *path == 0) {
		GDKerror("GDKextractParentAndLastDirFromPath: no path\n");
		return GDK_FAIL;
	}

	last_dir_with_sep = strrchr(path, DIR_SEP);
	if (last_dir_with_sep == NULL) {
		/* it wasn't a path, can't work with that */
		GDKerror("GDKextractParentAndLastDirFromPath: no separator\n");
		return GDK_FAIL;
	}
	last_dirsep_index = last_dir_with_sep - path;
	/* split the dir string into absolute parent dir path and
	 * (relative) log dir name */
	strncpy(last_dir, last_dir_with_sep + 1, strlen(path));
	strncpy(last_dir_parent, path, last_dirsep_index);
	last_dir_parent[last_dirsep_index] = 0;

	return GDK_SUCCEED;
}

size_t
GDKmem_cursize(void)
{
	/* RAM/swapmem that Monet is really using now */
	return (size_t) ATOMIC_GET(GDK_mallocedbytes_estimate, mbyteslock);
}

size_t
GDKvm_cursize(void)
{
	/* current Monet VM address space usage */
	return (size_t) ATOMIC_GET(GDK_vm_cursize, mbyteslock) + GDKmem_cursize();
}

#define heapinc(_memdelta)						\
	(void) ATOMIC_ADD(GDK_mallocedbytes_estimate, _memdelta, mbyteslock)
#define heapdec(_memdelta)						\
	(void) ATOMIC_SUB(GDK_mallocedbytes_estimate, _memdelta, mbyteslock)

#define meminc(vmdelta)							\
	(void) ATOMIC_ADD(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock)
#define memdec(vmdelta)							\
	(void) ATOMIC_SUB(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock)

#ifndef STATIC_CODE_ANALYSIS

static void
GDKmemfail(const char *s, size_t len)
{
	/* bumped your nose against the wall; try to prevent
	 * repetition by adjusting maxsizes
	   if (memtarget < 0.3 * GDKmem_cursize()) {
		   size_t newmax = (size_t) (0.7 * (double) GDKmem_cursize());

		   if (newmax < GDK_mem_maxsize)
		   GDK_mem_maxsize = newmax;
	   }
	   if (vmtarget < 0.3 * GDKvm_cursize()) {
		   size_t newmax = (size_t) (0.7 * (double) GDKvm_cursize());

		   if (newmax < GDK_vm_maxsize)
			   GDK_vm_maxsize = newmax;
	   }
	 */

	fprintf(stderr, "#%s(" SZFMT ") fails, try to free up space [memory in use=" SZFMT ",virtual memory in use=" SZFMT "]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
}

/* Memory allocation
 *
 * The functions GDKmalloc, GDKzalloc, GDKrealloc, GDKstrdup, and
 * GDKfree are used throughout to allocate and free memory.  These
 * functions are almost directly mapped onto the system
 * malloc/realloc/free functions, but they give us some extra
 * debugging hooks.
 *
 * When allocating memory, we allocate a bit more than was asked for.
 * The extra space is added onto the front of the memory area that is
 * returned, and in debug builds also some at the end.  The area in
 * front is used to store the actual size of the allocated area.  The
 * most important use is to be able to keep statistics on how much
 * memory is being used.  In debug builds, the size is also used to
 * make sure that we don't write outside of the allocated arena.  This
 * is also where the extra space at the end comes in.
 */

/* we allocate extra space and return a pointer offset by this amount */
#define MALLOC_EXTRA_SPACE	(2 * SIZEOF_VOID_P)

#ifdef NDEBUG
#define DEBUG_SPACE	0
#else
#define DEBUG_SPACE	16
#endif

static void *
GDKmalloc_internal(size_t size)
{
	void *s;
	size_t nsize;

	assert(size != 0);
#ifndef NDEBUG
	/* fail malloc for testing purposes depending on set limit */
	if (GDK_malloc_success_count > 0) {
		MT_lock_set(&mallocsuccesslock);
		if (GDK_malloc_success_count > 0)
			GDK_malloc_success_count--;
		MT_lock_unset(&mallocsuccesslock);
	}
	if (GDK_malloc_success_count == 0) {
		return NULL;
	}
#endif
	if (GDKvm_cursize() + size >= GDK_vm_maxsize) {
		GDKerror("allocating too much memory\n");
		return NULL;
	}

	/* pad to multiple of eight bytes and add some extra space to
	 * write real size in front; when debugging, also allocate
	 * extra space for check bytes */
	nsize = (size + 7) & ~7;
	if ((s = malloc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE)) == NULL) {
		GDKmemfail("GDKmalloc", size);
		GDKerror("GDKmalloc_internal: failed for " SZFMT " bytes", size);
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);

	heapinc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);

	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#ifndef NDEBUG
	/* just before that, write how much was asked of us */
	((size_t *) s)[-2] = size;
	/* write pattern to help find out-of-bounds writes */
	memset((char *) s + size, '\xBD', nsize + DEBUG_SPACE - size);
#endif
	return s;
}

#undef GDKmalloc
void *
GDKmalloc(size_t size)
{
	void *s;

	if ((s = GDKmalloc_internal(size)) == NULL)
		return NULL;
#ifndef NDEBUG
	/* write a pattern to help make sure all data is properly
	 * initialized by the caller */
	DEADBEEFCHK memset(s, '\xBD', size);
#endif
	return s;
}

#undef GDKzalloc
void *
GDKzalloc(size_t size)
{
	void *s;

	if ((s = GDKmalloc_internal(size)) == NULL)
		return NULL;
	memset(s, 0, size);
	return s;
}

#undef GDKstrdup
char *
GDKstrdup(const char *s)
{
	size_t size;
	char *p;

	if (s == NULL)
		return NULL;
	size = strlen(s) + 1;

	if ((p = GDKmalloc_internal(size)) == NULL)
		return NULL;
	memcpy(p, s, size);	/* including terminating NULL byte */
	return p;
}

#undef GDKstrndup
char *
GDKstrndup(const char *s, size_t size)
{
	char *p;

	if (s == NULL)
		return NULL;
	if ((p = GDKmalloc_internal(size + 1)) == NULL)
		return NULL;
	if (size > 0)
		memcpy(p, s, size);
	p[size] = '\0';		/* make sure it's NULL terminated */
	return p;
}

#undef GDKfree
void
GDKfree(void *s)
{
	size_t asize;

	if (s == NULL)
		return;

	asize = ((size_t *) s)[-1]; /* how much allocated last */

#ifndef NDEBUG
	assert((asize & 2) == 0);   /* check against duplicate free */
	/* check for out-of-bounds writes */
	{
		size_t i = ((size_t *) s)[-2]; /* how much asked for last */
		for (; i < asize - MALLOC_EXTRA_SPACE; i++)
			assert(((char *) s)[i] == '\xBD');
	}
	((size_t *) s)[-1] |= 2; /* indicate area is freed */
#endif

#ifndef NDEBUG
	/* overwrite memory that is to be freed with a pattern that
	 * will help us recognize access to already freed memory in
	 * the debugger */
	DEADBEEFCHK memset(s, '\xDB', asize - MALLOC_EXTRA_SPACE);
#endif

	free((char *) s - MALLOC_EXTRA_SPACE);
	heapdec((ssize_t) asize);
}

#undef GDKrealloc
void *
GDKrealloc(void *s, size_t size)
{
	size_t nsize, asize;
#ifndef NDEBUG
	size_t osize;
	size_t *os;
#endif

	assert(size != 0);

	if (s == NULL)
		return GDKmalloc(size);

	nsize = (size + 7) & ~7;
	asize = ((size_t *) s)[-1]; /* how much allocated last */

	if (nsize > asize &&
	    GDKvm_cursize() + nsize - asize >= GDK_vm_maxsize) {
		GDKerror("allocating too much memory\n");
		return NULL;
	}
#ifndef NDEBUG
	assert((asize & 2) == 0);   /* check against duplicate free */
	/* check for out-of-bounds writes */
	osize = ((size_t *) s)[-2]; /* how much asked for last */
	{
		size_t i;
		for (i = osize; i < asize - MALLOC_EXTRA_SPACE; i++)
			assert(((char *) s)[i] == '\xBD');
	}
	/* if shrinking, write debug pattern into to-be-freed memory */
	DEADBEEFCHK if (size < osize)
		memset((char *) s + size, '\xDB', osize - size);
	os = s;
	os[-1] |= 2;		/* indicate area is freed */
#endif
	s = realloc((char *) s - MALLOC_EXTRA_SPACE,
		    nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	if (s == NULL) {
#ifndef NDEBUG
		os[-1] &= ~2;	/* not freed after all */
#endif
		GDKmemfail("GDKrealloc", size);
		GDKerror("GDKrealloc: failed for " SZFMT " bytes", size);
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);
	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#ifndef NDEBUG
	/* just before that, write how much was asked of us */
	((size_t *) s)[-2] = size;
	/* if growing, initialize new memory with debug pattern */
	DEADBEEFCHK if (size > osize)
 		memset((char *) s + osize, '\xBD', size - osize);
	/* write pattern to help find out-of-bounds writes */
	memset((char *) s + size, '\xBD', nsize + DEBUG_SPACE - size);
#endif

	heapinc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	heapdec((ssize_t) asize);

	return s;
}

#else

#define GDKmemfail(s, len)	/* nothing */

void *
GDKmalloc(size_t size)
{
	void *p = malloc(size);
	if (p == NULL)
		GDKerror("GDKmalloc: failed for " SZFMT " bytes", size);
	return p;
}

void
GDKfree(void *ptr)
{
	if (ptr)
		free(ptr);
}

void *
GDKzalloc(size_t size)
{
	void *ptr = calloc(size, 1);
	if (ptr == NULL)
		GDKerror("GDKzalloc: failed for " SZFMT " bytes", size);
	return ptr;
}

void *
GDKrealloc(void *ptr, size_t size)
{
	void *p = realloc(ptr, size);
	if (p == NULL)
		GDKerror("GDKrealloc: failed for " SZFMT " bytes", size);
	return p;
}

char *
GDKstrdup(const char *s)
{
	char *p = strdup(s);
	if (p == NULL)
		GDKerror("GDKstrdup failed for %s\n", s);
	return p;
}

char *
GDKstrndup(const char *s, size_t size)
{
	char *p = malloc(size + 1);
	if (p == NULL) {
		GDKerror("GDKstrdup failed for %s\n", s);
		return NULL;
	}
	memcpy(p, s, size);
	p[size] = 0;
	return p;
}

#endif	/* STATIC_CODE_ANALYSIS */

void
GDKsetmallocsuccesscount(lng count)
{
	(void) count;
#ifndef NDEBUG
	GDK_malloc_success_count = count;
#endif
}

/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
#undef GDKmmap
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret;

	if (GDKvm_cursize() + len >= GDK_vm_maxsize) {
		GDKerror("allocating too much virtual address space\n");
		return NULL;
	}
	ret = MT_mmap(path, mode, len);
	if (ret == NULL) {
		GDKmemfail("GDKmmap", len);
	}
	if (ret != NULL) {
		meminc(len);
	}
	return ret;
}

#undef GDKmunmap
gdk_return
GDKmunmap(void *addr, size_t size)
{
	int ret;

	ret = MT_munmap(addr, size);
	if (ret == 0)
		memdec(size);
	return ret == 0 ? GDK_SUCCEED : GDK_FAIL;
}

#undef GDKmremap
void *
GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *ret;

	if (*new_size > old_size &&
	    GDKvm_cursize() + *new_size - old_size >= GDK_vm_maxsize) {
		GDKerror("allocating too much virtual address space\n");
		return NULL;
	}
	ret = MT_mremap(path, mode, old_address, old_size, new_size);
	if (ret == NULL) {
		GDKmemfail("GDKmremap", *new_size);
	}
	if (ret != NULL) {
		memdec(old_size);
		meminc(*new_size);
	}
	return ret;
}
