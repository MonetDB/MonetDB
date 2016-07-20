/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
static void GDKunlockHome(void);

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
	strncpy(GDKdbpathStr, dbpath, PATHLENGTH);
	/* make coverity happy: */
	GDKdbpathStr[PATHLENGTH - 1] = 0;
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
	time_t tm = time(0);
#if defined(HAVE_CTIME_R3) || defined(HAVE_CTIME_R)
	char tbuf[26];
#endif
	char *ctm;

	if (MT_pagesize() == 0 || GDKlockFile == NULL)
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
#ifdef HAVE_CTIME_R3
	ctm = ctime_r(&tm, tbuf, sizeof(tbuf));
#else
#ifdef HAVE_CTIME_R
	ctm = ctime_r(&tm, tbuf);
#else
	ctm = ctime(&tm);
#endif
#endif
	fprintf(GDKlockFile, "USR=%d PID=%d TIME=%.24s @ %s\n", (int) getuid(), (int) getpid(), ctm, buf);
	fflush(GDKlockFile);
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
size_t GDK_mmap_minsize = (size_t) 1 << 18;
size_t GDK_mmap_pagesize = (size_t) 1 << 16; /* mmap granularity */
size_t GDK_mem_maxsize = GDK_VM_MAXSIZE;
size_t GDK_vm_maxsize = GDK_VM_MAXSIZE;

int GDK_vm_trim = 1;

#define SEG_SIZE(x,y)	((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))
#define MAX_BIT		((int) (sizeof(ssize_t)<<3))

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
#ifndef NDEBUG
static volatile lng GDK_mallocedbytes_limit = -1;
#endif
static volatile ATOMIC_TYPE GDK_vm_cursize = 0;
#ifdef GDK_VM_KEEPHISTO
volatile ATOMIC_TYPE GDK_vm_nallocs[MAX_BIT] = { 0 };
#endif
#ifdef GDK_MEM_KEEPHISTO
volatile ATOMIC_TYPE GDK_nmallocs[MAX_BIT] = { 0 };
#endif
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
		MEMDEBUG fprintf(stderr, "alloc = " SZFMT " %+zd rss = " SZFMT " %+zd\n", cursize, memdiff, rss, rssdiff);
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

static void THRinit(void);
static void GDKlockHome(void);

int
GDKinit(opt *set, int setlen)
{
	char *dbpath = mo_find_option(set, setlen, "gdk_dbpath");
	char *p;
	opt *n;
	int i, nlen = 0;
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
	assert(sizeof(ptrdiff_t) == SIZEOF_PTRDIFF_T);
	assert(SIZEOF_OID == SIZEOF_INT || SIZEOF_OID == SIZEOF_LNG);

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&MT_system_lock,"MT_system_lock");
	ATOMIC_INIT(GDKstoppedLock);
	ATOMIC_INIT(mbyteslock);
	MT_lock_init(&GDKnameLock, "GDKnameLock");
	MT_lock_init(&GDKthreadLock, "GDKthreadLock");
	MT_lock_init(&GDKtmLock, "GDKtmLock");
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
	/* now try to lock the database */
	GDKlockHome();

	/* Mserver by default takes 80% of all memory as a default */
	GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
	BBPinit();

	n = (opt *) malloc(setlen * sizeof(opt));
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
		} else if (strcmp("gdk_mmap_minsize", n[i].name) == 0) {
			GDK_mmap_minsize = (size_t) strtoll(n[i].value, NULL, 10);
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
	BATrename(GDKkey, "environment_key");
	BATrename(GDKval, "environment_val");

	/* store options into environment BATs */
	for (i = 0; i < nlen; i++)
		GDKsetenv(n[i].name, n[i].value);
	free(n);

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0)
		GDKnr_threads = MT_check_nr_cores();

	if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, DIR_SEP)) != NULL) {
		GDKsetenv("gdk_dbname", p + 1);
#if DIR_SEP != '/'		/* on Windows look for different separator */
	} else if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
	    (p = strrchr(p, '/')) != NULL) {
		GDKsetenv("gdk_dbname", p + 1);
#endif
	}
	if (GDKgetenv("gdk_vm_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_vm_maxsize);
		GDKsetenv("gdk_vm_maxsize", buf);
	}
	if (GDKgetenv("gdk_mem_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mem_maxsize);
		GDKsetenv("gdk_mem_maxsize", buf);
	}
	if (GDKgetenv("gdk_mmap_minsize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mmap_minsize);
		GDKsetenv("gdk_mmap_minsize", buf);
	}
	if (GDKgetenv("gdk_mmap_pagesize") == NULL) {
		snprintf(buf, sizeof(buf), SZFMT, GDK_mmap_pagesize);
		GDKsetenv("gdk_mmap_pagesize", buf);
	}
	if (GDKgetenv("monet_pid") == NULL) {
		snprintf(buf, sizeof(buf), "%d", (int) getpid());
		GDKsetenv("monet_pid", buf);
	}

	/* only start vmtrim thread when explicitly asked to do so or
	 * when on a 32 bit architecture and not told to not start
	 * it;
	 * see also mo_builtin_settings() in common/options/monet_options.c
	 */
	p = mo_find_option(set, setlen, "gdk_vmtrim");
	if (
#if SIZEOF_VOID_P == 4
	    /* 32 bit architecture */
	    p == NULL ||	/* default is yes */
#else
	    /* 64 bit architecture */
	    p != NULL &&	/* default is no */
#endif
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
	if (GDKvmtrim_id)
		MT_join_thread(GDKvmtrim_id);

	MT_lock_set(&GDKthreadLock);
	for (st = serverthread; st; st = serverthread) {
		MT_lock_unset(&GDKthreadLock);
		MT_join_thread(st->pid);
		MT_lock_set(&GDKthreadLock);
		serverthread = st->next;
		GDKfree(st);
	}
	MT_lock_unset(&GDKthreadLock);
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
GDKreset(int status)
{
	MT_Id pid = MT_getpid();
	Thread t, s;
	struct serverthread *st;

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
		GDKlog(GDKLOGOFF);
		GDKunlockHome();
#if !defined(USE_PTHREAD_LOCKS) && !defined(NDEBUG)
		TEMDEBUG GDKlockstatistics(1);
#endif
		GDKdebug = 0;
		strcpy(GDKdbpathStr,"dbpath");
		GDK_mmap_minsize = (size_t) 1 << 18;
		GDK_mmap_pagesize = (size_t) 1 << 16; 
		GDK_mem_maxsize = GDK_VM_MAXSIZE;
		GDK_vm_maxsize = GDK_VM_MAXSIZE;

		GDK_vm_trim = 1;

		GDK_mallocedbytes_estimate = 0;
		GDK_vm_cursize = 0;
		_MT_pagesize = 0;
		_MT_npages = 0;
#ifdef GDK_VM_KEEPHISTO
		memset((char*)GDK_vm_nallocs[MAX_BIT], 0, sizeof(GDK_vm_nallocs));
#endif
#ifdef GDK_MEM_KEEPHISTO
		memset((char*)GDK_nmallocs[MAX_BIT], 0, sizeof(GDK_nmallocs));
#endif
		GDKvmtrim_id =0;
		GDKnr_threads = 0;
		GDKnrofthreads = 0;
		memset((char*) GDKbatLock,0, sizeof(GDKbatLock));
		memset((char*) GDKbbpLock,0,sizeof(GDKbbpLock));

		memset((char*) GDKthreads, 0, sizeof(GDKthreads));
		memset((char*) THRdata, 0, sizeof(THRdata));
		memset((char*) THRprintbuf,0, sizeof(THRprintbuf));
		gdk_bbp_reset();
		MT_lock_unset(&GDKthreadLock);
		//gdk_system_reset(); CHECK OUT
	}
#ifndef HAVE_EMBEDDED
	MT_global_exit(status);
#endif
}

void
GDKexit(int status)
{
	if (GDKlockFile == NULL) {
#ifdef HAVE_EMBEDDED
		return;
#endif
		/* no database lock, so no threads, so exit now */
		exit(status);
	}
	GDKprepareExit();
	GDKreset(status);
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
	int fd;
	struct stat st;
	str gdklockpath = GDKfilepath(0, NULL, GDKLOCK, NULL);
	char GDKdirStr[PATHLENGTH];

	assert(GDKlockFile == NULL);
	assert(GDKdbpathStr != NULL);

	snprintf(GDKdirStr, PATHLENGTH, "%s%c", GDKdbpathStr, DIR_SEP);
	/*
	 * Obtain the global database lock.
	 */
	if (stat(GDKdbpathStr, &st) < 0 && GDKcreatedir(GDKdirStr) != GDK_SUCCEED) {
		GDKfatal("GDKlockHome: could not create %s\n", GDKdbpathStr);
	}
	if ((fd = MT_lockf(gdklockpath, F_TLOCK, 4, 1)) < 0) {
		GDKfatal("GDKlockHome: Database lock '%s' denied\n", GDKLOCK);
	}
	/* now we have the lock on the database */
	if ((GDKlockFile = fdopen(fd, "r+")) == NULL) {
		close(fd);
		GDKfatal("GDKlockHome: Could not open %s\n", GDKLOCK);
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
	GDKfree(gdklockpath);
}


static void
GDKunlockHome(void)
{
	if (GDKlockFile) {
		str gdklockpath = GDKfilepath(0, NULL, GDKLOCK, NULL);
		MT_lockf(gdklockpath, F_ULOCK, 4, 1);
		fclose(GDKlockFile);
		GDKlockFile = 0;
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
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
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
			GDKlog("%s", message);
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
		memset((char *) s, 0, sizeof(*s));
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
		if (diff > THREAD_STACK_SIZE - 16 * 1024)
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
		assert(bf != NULL);
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

#ifdef GDK_MEM_KEEPHISTO
#define heapinc(_memdelta)						\
	do {								\
		int _idx;						\
									\
		(void) ATOMIC_ADD(GDK_mallocedbytes_estimate, _memdelta, mbyteslock); \
		GDKmallidx(_idx, _memdelta);				\
		(void) ATOMIC_INC(GDK_nmallocs[_idx], mbyteslock);	\
	} while (0)
#define heapdec(memdelta)						\
	do {								\
		ssize_t _memdelta = (ssize_t) (memdelta);		\
		int _idx;						\
									\
		(void) ATOMIC_SUB(GDK_mallocedbytes_estimate, _memdelta, mbyteslock); \
		GDKmallidx(_idx, _memdelta);				\
		(void) ATOMIC_DEC(GDK_nmallocs[_idx], mbyteslock);	\
	} while (0)
#else
#define heapinc(_memdelta)						\
	(void) ATOMIC_ADD(GDK_mallocedbytes_estimate, _memdelta, mbyteslock)
#define heapdec(_memdelta)						\
	(void) ATOMIC_SUB(GDK_mallocedbytes_estimate, _memdelta, mbyteslock)
#endif

#ifdef GDK_VM_KEEPHISTO
#define meminc(vmdelta)							\
	do {								\
		ssize_t _vmdelta = (ssize_t) SEG_SIZE((vmdelta),MT_VMUNITLOG); \
		int _idx;						\
									\
		GDKmallidx(_idx, _vmdelta);				\
		(void) ATOMIC_INC(GDK_vm_nallocs[_idx], mbyteslock);	\
		(void) ATOMIC_ADD(GDK_vm_cursize, _vmdelta, mbyteslock); \
	} while (0)
#define memdec(vmdelta)							\
	do {								\
		ssize_t _vmdelta = (ssize_t) SEG_SIZE((vmdelta),MT_VMUNITLOG); \
		int _idx;						\
									\
		GDKmallidx(_idx, _vmdelta);				\
		(void) ATOMIC_DEC(GDK_vm_nallocs[_idx], mbyteslock);	\
		(void) ATOMIC_SUB(GDK_vm_cursize, _vmdelta, mbyteslock); \
	} while (0)
#else
#define meminc(vmdelta)							\
	(void) ATOMIC_ADD(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock)
#define memdec(vmdelta)							\
	(void) ATOMIC_SUB(GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG), mbyteslock)
#endif

#ifndef STATIC_CODE_ANALYSIS

static void
GDKmemdump(void)
{
	struct Mallinfo m = MT_mallinfo();

	MEMDEBUG {
		fprintf(stderr, "\n#mallinfo.arena = " SSZFMT "\n", (ssize_t) m.arena);
		fprintf(stderr, "#mallinfo.ordblks = " SSZFMT "\n", (ssize_t) m.ordblks);
		fprintf(stderr, "#mallinfo.smblks = " SSZFMT "\n", (ssize_t) m.smblks);
		fprintf(stderr, "#mallinfo.hblkhd = " SSZFMT "\n", (ssize_t) m.hblkhd);
		fprintf(stderr, "#mallinfo.hblks = " SSZFMT "\n", (ssize_t) m.hblks);
		fprintf(stderr, "#mallinfo.usmblks = " SSZFMT "\n", (ssize_t) m.usmblks);
		fprintf(stderr, "#mallinfo.fsmblks = " SSZFMT "\n", (ssize_t) m.fsmblks);
		fprintf(stderr, "#mallinfo.uordblks = " SSZFMT "\n", (ssize_t) m.uordblks);
		fprintf(stderr, "#mallinfo.fordblks = " SSZFMT "\n", (ssize_t) m.fordblks);
	}
#ifdef GDK_MEM_KEEPHISTO
	{
		int i;

		fprintf(stderr, "#memory histogram\n");
		for (i = 3; i < GDK_HISTO_MAX_BIT - 1; i++) {
			size_t j = 1 << i;

			fprintf(stderr, "# " SZFMT " " SZFMT "\n", j,
				ATOMIC_GET(GDK_nmallocs[i],
					   mbyteslock, "GDKmemdump"));
		}
	}
#endif
#ifdef GDK_VM_KEEPHISTO
	{
		int i;

		fprintf(stderr, "\n#virtual memory histogram\n");
		for (i = 12; i < GDK_HISTO_MAX_BIT - 1; i++) {
			size_t j = 1 << i;

			fprintf(stderr, "# " SZFMT " " SZFMT "\n", j,
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
GDKmemfail(const char *s, size_t len)
{
	int bak = GDKdebug;

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
	GDKmemdump();
/*	GDKdebug |= MEMMASK;  avoid debugging output */

	BBPtrim(BBPTRIM_ALL);

	GDKdebug = MIN(GDKdebug, bak);
	fprintf(stderr, "#%s(" SZFMT ") result [mem=" SZFMT ",vm=" SZFMT "]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
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
static inline void *
GDKmalloc_prefixsize(size_t size)
{
	ssize_t *s;

	if ((s = malloc(size + MALLOC_EXTRA_SPACE + GLIBC_BUG)) != NULL) {
		assert((((uintptr_t) s) & 7) == 0); /* no MISALIGN */
		s = (ssize_t*) ((char*) s + MALLOC_EXTRA_SPACE);
		s[-1] = (ssize_t) (size + MALLOC_EXTRA_SPACE);
	}
	return s;
}

void
GDKsetmemorylimit(lng nbytes)
{
	(void) nbytes;
#ifndef NDEBUG
	GDK_mallocedbytes_limit = nbytes;
#endif
}


/*
 * The emergency flag can be set to force a fatal error if needed.
 * Otherwise, the caller is able to deal with the lack of memory.
 */
#undef GDKmallocmax
void *
GDKmallocmax(size_t size, size_t *maxsize, int emergency)
{
	void *s;

	if (size == 0) {
#ifdef GDK_MEM_NULLALLOWED
		return NULL;
#else
		GDKfatal("GDKmallocmax: called with size " SZFMT "", size);
#endif
	}
#ifndef NDEBUG
	/* fail malloc for testing purposes depending on set limit */
	if (GDK_mallocedbytes_limit >= 0 &&
	    size > (size_t) GDK_mallocedbytes_limit) {
		return NULL;
	}
#endif
	size = (size + 7) & ~7;	/* round up to a multiple of eight */
	s = GDKmalloc_prefixsize(size);
	if (s == NULL) {
		GDKmemfail("GDKmalloc", size);
		s = GDKmalloc_prefixsize(size);
		if (s == NULL) {
			if (emergency == 0) {
				GDKerror("GDKmallocmax: failed for " SZFMT " bytes", size);
				return NULL;
			}
			GDKfatal("GDKmallocmax: failed for " SZFMT " bytes", size);
		} else {
			/* TODO why are we printing this on stderr? */
			fprintf(stderr, "#GDKmallocmax: recovery ok. Continuing..\n");
		}
	}
	*maxsize = size;
	heapinc(size + MALLOC_EXTRA_SPACE);
	return s;
}

#undef GDKmalloc
void *
GDKmalloc(size_t size)
{
	void *p = GDKmallocmax(size, &size, 0);
#ifndef NDEBUG
	DEADBEEFCHK if (p)
		memset(p, 0xBD, size);
#endif
	return p;
}

#undef GDKzalloc
void *
GDKzalloc(size_t size)
{
	size_t maxsize = size;
	void *p = GDKmallocmax(size, &maxsize, 0);
	if (p) {
		memset(p, 0, size);
#ifndef NDEBUG
		/* DeadBeef allocated area beyond what was requested */
		DEADBEEFCHK if (maxsize > size)
			memset((char *) p + size, 0xBD, maxsize - size);
#endif
	}
	return p;
}

#undef GDKfree
void
GDKfree(void *blk)
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

#undef GDKreallocmax
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
		GDKfree(blk);
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
			fprintf(stderr, "#GDKremallocmax: "
				"recovery ok. Continuing..\n");
		}
	}
	/* place MALLOC_EXTRA_SPACE bytes before it */
	assert((((uintptr_t) blk) & 4) == 0);
	blk = ((char *) blk) + MALLOC_EXTRA_SPACE;
	((ssize_t *) blk)[-1] = (ssize_t) newsize;

	/* adapt statistics */
	heapinc(newsize);
	heapdec(oldsize);
	*maxsize = size;
	return blk;
}

#undef GDKrealloc
ptr
GDKrealloc(void *blk, size_t size)
{
	size_t sz = size;

	return GDKreallocmax(blk, sz, &size, 0);
}

#undef GDKstrdup
char *
GDKstrdup(const char *s)
{
	int l = strLen(s);
	char *n = GDKmalloc(l);

	if (n)
		memcpy(n, s, l);
	return n;
}

#else

#define GDKmemfail(s, len)	/* nothing */

void *
GDKmallocmax(size_t size, size_t *maxsize, int emergency)
{
	void *ptr = malloc(size);
	*maxsize = size;
	if (ptr == 0 && emergency)
		GDKfatal("fatal\n");
	return ptr;
}

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
	void *ptr = malloc(size);
	if (ptr)
		memset(ptr, 0, size);
	else
		GDKerror("GDKzalloc: failed for " SZFMT " bytes", size);
	return ptr;
}

void *
GDKreallocmax(void *blk, size_t size, size_t *maxsize, int emergency)
{
	void *ptr = realloc(blk, size);
	*maxsize = size;
	if (ptr == 0) {
		if (emergency)
			GDKfatal("fatal\n");
		else
			GDKerror("GDKreallocmax: failed for "
				 SZFMT " bytes", size);
	}
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

#endif	/* STATIC_CODE_ANALYSIS */

#undef GDKstrndup
char *
GDKstrndup(const char *s, size_t n)
{
	char *r = GDKmalloc(n+1);

	if (r) {
		memcpy(r, s, n);
		r[n] = '\0';
	}
	return r;
}

/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
#undef GDKmmap
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret = MT_mmap(path, mode, len);

	if (ret == NULL) {
		GDKmemfail("GDKmmap", len);
		ret = MT_mmap(path, mode, len);
		if (ret != NULL) {
			fprintf(stderr, "#GDKmmap: recovery ok. Continuing..\n");
		}
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

	ret = MT_mremap(path, mode, old_address, old_size, new_size);
	if (ret == NULL) {
		GDKmemfail("GDKmremap", *new_size);
		ret = MT_mremap(path, mode, old_address, old_size, new_size);
		if (ret != NULL)
			fprintf(stderr, "#GDKmremap: recovery ok. Continuing..\n");
	}
	if (ret != NULL) {
		memdec(old_size);
		meminc(*new_size);
	}
	return ret;
}
