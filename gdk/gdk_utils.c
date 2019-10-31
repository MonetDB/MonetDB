/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
#include "monet_options.h"

#include "gdk.h"
#include "gdk_private.h"
#include "mutils.h"

static BAT *GDKkey = NULL;
static BAT *GDKval = NULL;
int GDKdebug = 0;
int GDKverbose = 0;

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
#ifdef BSD /* BSD macro is defined in sys/param.h */
# include <sys/sysctl.h>
#endif
#if defined(HAVE_SYS_RESOURCE_H) && defined(HAVE_GETRLIMIT)
#include <sys/resource.h>
#endif

#ifdef __CYGWIN__
#include <sysinfoapi.h>
#endif

#ifdef NATIVE_WIN32
#define chdir _chdir
#endif

static ATOMIC_TYPE GDKstopped = ATOMIC_VAR_INIT(0);
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

static bool
GDKenvironment(const char *dbpath)
{
	if (dbpath == NULL) {
		fprintf(stderr, "!GDKenvironment: database name missing.\n");
		return false;
	}
	if (strlen(dbpath) >= FILENAME_MAX) {
		fprintf(stderr, "!GDKenvironment: database name too long.\n");
		return false;
	}
	if (!MT_path_absolute(dbpath)) {
		fprintf(stderr, "!GDKenvironment: directory not an absolute path: %s.\n", dbpath);
		return false;
	}
	return true;
}

const char *
GDKgetenv(const char *name)
{
	if (GDKkey && GDKval) {
		BUN b = BUNfnd(GDKkey, (ptr) name);

		if (b != BUN_NONE) {
			BATiter GDKenvi = bat_iterator(GDKval);
			return BUNtvar(GDKenvi, b);
		}
	}
	return NULL;
}

bool
GDKgetenv_istext(const char *name, const char *text)
{
	const char *val = GDKgetenv(name);

	return val && strcasecmp(val, text) == 0;
}

bool
GDKgetenv_isyes(const char *name)
{
	return GDKgetenv_istext(name, "yes");
}

bool
GDKgetenv_istrue(const char *name)
{
	return GDKgetenv_istext(name, "true");
}

int
GDKgetenv_int(const char *name, int def)
{
	const char *val = GDKgetenv(name);

	if (val)
		return atoi(val);
	return def;
}

gdk_return
GDKsetenv(const char *name, const char *value)
{
	if (BUNappend(GDKkey, name, false) != GDK_SUCCEED ||
	    BUNappend(GDKval, value, false) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

gdk_return
GDKcopyenv(BAT **key, BAT **val, bool writable)
{
	BAT *k, *v;

	if (key == NULL || val == NULL) {
		GDKerror("GDKcopyenv: called incorrectly.\n");
		return GDK_FAIL;
	}
	k = COLcopy(GDKkey, GDKkey->ttype, writable, TRANSIENT);
	v = COLcopy(GDKval, GDKval->ttype, writable, TRANSIENT);
	if (k == NULL || v == NULL) {
		BBPreclaim(k);
		BBPreclaim(v);
		return GDK_FAIL;
	}
	*key = k;
	*val = v;
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
	(void) nr;
	GDKsyserror("! ERROR signal %d caught by thread %zu\n", nr, (size_t) MT_getpid());
}
#endif

#ifdef WIN32
static void
BATSIGabort(int nr)
{
	(void) nr;
	_Exit(3);		/* emulate Windows exit code without pop-up */
}
#endif

#ifndef NATIVE_WIN32
static int
BATSIGinit(void)
{
#ifdef SIGPIPE
	(void) signal(SIGPIPE, SIG_IGN);
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

#define SEG_SIZE(x,y)	((x)+(((x)&((1<<(y))-1))?(1<<(y))-((x)&((1<<(y))-1)):0))

/* This block is to provide atomic addition and subtraction to select
 * variables.  We use intrinsic functions (recognized and inlined by
 * the compiler) for both the GNU C compiler and Microsoft Visual
 * Studio.  By doing this, we avoid locking overhead.  There is also a
 * fall-back for other compilers. */
#include "matomic.h"
static ATOMIC_TYPE GDK_mallocedbytes_estimate = ATOMIC_VAR_INIT(0);
#ifndef NDEBUG
static volatile lng GDK_malloc_success_count = -1;
#endif
static ATOMIC_TYPE GDK_vm_cursize = ATOMIC_VAR_INIT(0);

size_t _MT_pagesize = 0;	/* variable holding page size */
size_t _MT_npages = 0;		/* variable holding memory size in pages */

static lng programepoch;

void
MT_init(void)
{
	programepoch = GDKusec();
#ifdef _MSC_VER
	{
		SYSTEM_INFO sysInfo;

		GetSystemInfo(&sysInfo);
		_MT_pagesize = sysInfo.dwPageSize;
	}
#elif defined(BSD) && defined(HW_PAGESIZE)
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
#elif defined(BSD) && defined(HW_MEMSIZE) && SIZEOF_SIZE_T == SIZEOF_LNG
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
#elif defined(BSD) && defined (HW_PHYSMEM64) && SIZEOF_SIZE_T == SIZEOF_LNG
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
#elif defined(BSD) && defined(HW_PHYSMEM)
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

#ifdef __linux__
	/* limit values to whatever cgroups gives us */
	FILE *fc;
	fc = fopen("/proc/self/cgroup", "r");
	if (fc != NULL) {
		char buf[1024];
		while (fgets(buf, (int) sizeof(buf), fc) != NULL) {
			char *p, *q;
			p = strchr(buf, ':');
			if (p == NULL)
				break;
			q = p + 1;
			p = strchr(q, ':');
			if (p == NULL)
				break;
			*p++ = 0;
			if (strstr(q, "memory") != NULL) {
				char pth[1024];
				FILE *f;
				q = strchr(p, '\n');
				if (q == NULL)
					break;
				*q = 0;
				q = stpconcat(pth, "/sys/fs/cgroup/memory",
					      p, NULL);
				/* sometimes the path in
				 * /proc/self/cgroup ends in "/" (or
				 * actually, is "/"); in all other
				 * cases add one */
				if (q[-1] != '/')
					*q++ = '/';
				/* limit of memory usage */
				strcpy(q, "memory.limit_in_bytes");
				f = fopen(pth, "r");
				if (f != NULL) {
					uint64_t mem;
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					fclose(f);
				}
				/* soft limit of memory usage */
				strcpy(q, "memory.soft_limit_in_bytes");
				f = fopen(pth, "r");
				if (f != NULL) {
					uint64_t mem;
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					fclose(f);
				}
				/* limit of memory+swap usage
				 * we use this as maximum virtual memory size */
				strcpy(q, "memory.memsw.limit_in_bytes");
				f = fopen(pth, "r");
				if (f != NULL) {
					uint64_t mem;
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) GDK_vm_maxsize) {
						GDK_vm_maxsize = (size_t) mem;
					}
					fclose(f);
				}
				break;

			}
		}
		fclose(fc);
	}
#endif

#if defined(HAVE_SYS_RESOURCE_H) && defined(HAVE_GETRLIMIT) && defined(RLIMIT_AS)
	struct rlimit l;
	/* address space (virtual memory) limit */
	if (getrlimit(RLIMIT_AS, &l) == 0
	    && l.rlim_cur != RLIM_INFINITY
	    && l.rlim_cur < GDK_vm_maxsize) {
		GDK_vm_maxsize = l.rlim_cur;
	}
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

static int THRinit(void);
static gdk_return GDKlockHome(int farmid);

#ifndef STATIC_CODE_ANALYSIS
#ifndef NDEBUG
static MT_Lock mallocsuccesslock = MT_LOCK_INITIALIZER("mallocsuccesslk");
#endif
#endif

void
GDKsetdebug(int debug)
{
	GDKdebug = debug;
}

void
GDKsetverbose(int verbose)
{
	GDKverbose = verbose;
}

gdk_return
GDKinit(opt *set, int setlen)
{
	static bool first = true;
	char *dbpath = mo_find_option(set, setlen, "gdk_dbpath");
	const char *p;
	opt *n;
	int i, nlen = 0;
	char buf[16];

	/* some sanity checks (should also find if symbols are not defined) */
	static_assert(sizeof(char) == SIZEOF_CHAR,
		      "error in configure: bad value for SIZEOF_CHAR");
	static_assert(sizeof(short) == SIZEOF_SHORT,
		      "error in configure: bad value for SIZEOF_SHORT");
	static_assert(sizeof(int) == SIZEOF_INT,
		      "error in configure: bad value for SIZEOF_INT");
	static_assert(sizeof(long) == SIZEOF_LONG,
		      "error in configure: bad value for SIZEOF_LONG");
	static_assert(sizeof(lng) == SIZEOF_LNG,
		      "error in configure: bad value for SIZEOF_LNG");
#ifdef HAVE_HGE
	static_assert(sizeof(hge) == SIZEOF_HGE,
		      "error in configure: bad value for SIZEOF_HGE");
#endif
	static_assert(sizeof(oid) == SIZEOF_OID,
		      "error in configure: bad value for SIZEOF_OID");
	static_assert(sizeof(void *) == SIZEOF_VOID_P,
		      "error in configure: bad value for SIZEOF_VOID_P");
	static_assert(sizeof(size_t) == SIZEOF_SIZE_T,
		      "error in configure: bad value for SIZEOF_SIZE_T");
	static_assert(SIZEOF_OID == SIZEOF_INT || SIZEOF_OID == SIZEOF_LNG,
		      "SIZEOF_OID should be equal to SIZEOF_INT or SIZEOF_LNG");

	if (first) {
		/* some things are really only initialized once */
		if (!MT_thread_init())
			return GDK_FAIL;

		for (i = 0; i <= BBP_BATMASK; i++) {
			char name[16];
			snprintf(name, sizeof(name), "GDKswapLock%d", i);
			MT_lock_init(&GDKbatLock[i].swap, name);
		}
		for (i = 0; i <= BBP_THREADMASK; i++) {
			char name[16];
			snprintf(name, sizeof(name), "GDKcacheLock%d", i);
			MT_lock_init(&GDKbbpLock[i].cache, name);
			snprintf(name, sizeof(name), "GDKtrimLock%d", i);
			MT_lock_init(&GDKbbpLock[i].trim, name);
			GDKbbpLock[i].free = 0;
		}
		if (mnstr_init() < 0)
			return GDK_FAIL;
		first = false;
	} else {
		/* BBP was locked by BBPexit() */
		BBPunlock();
	}
	errno = 0;
	if (!GDKinmemory() && !GDKenvironment(dbpath))
		return GDK_FAIL;

	MT_init_posix();
	if (THRinit() < 0)
		return GDK_FAIL;
#ifndef NATIVE_WIN32
	if (BATSIGinit() < 0)
		return GDK_FAIL;
#endif
#ifdef WIN32
	(void) signal(SIGABRT, BATSIGabort);
#if !defined(__MINGW32__) && !defined(__CYGWIN__)
	_set_abort_behavior(0, _CALL_REPORTFAULT | _WRITE_ABORT_MSG);
	_set_error_mode(_OUT_TO_STDERR);
#endif
#endif
	MT_init();

	/* now try to lock the database: go through all farms, and if
	 * we see a new directory, lock it */
	for (int farmid = 0; farmid < MAXFARMS; farmid++) {
		if (BBPfarms[farmid].dirname != NULL) {
			bool skip = false;
			for (int j = 0; j < farmid; j++) {
				if (BBPfarms[j].dirname != NULL &&
				    strcmp(BBPfarms[farmid].dirname, BBPfarms[j].dirname) == 0) {
					skip = true;
					break;
				}
			}
			if (!skip && GDKlockHome(farmid) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}

	/* Mserver by default takes 80% of all memory as a default */
	GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
	if (BBPinit() != GDK_SUCCEED)
		return GDK_FAIL;

	if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
		GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
		if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
			GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
	}

	n = (opt *) malloc(setlen * sizeof(opt));
	if (n == NULL)
		return GDK_FAIL;

	for (i = 0; i < setlen; i++) {
		bool done = false;

		for (int j = 0; j < nlen; j++) {
			if (strcmp(n[j].name, set[i].name) == 0) {
				if (n[j].kind < set[i].kind) {
					n[j] = set[i];
				}
				done = true;
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
			    (GDK_mmap_pagesize & (GDK_mmap_pagesize - 1)) != 0) {
				free(n);
				GDKerror("GDKinit: gdk_mmap_pagesize must be power of 2 between 2**12 and 2**20\n");
				return GDK_FAIL;
			}
		}
	}

	GDKkey = COLnew(0, TYPE_str, 100, TRANSIENT);
	GDKval = COLnew(0, TYPE_str, 100, TRANSIENT);
	if (GDKkey == NULL || GDKval == NULL) {
		free(n);
		GDKerror("GDKinit: Could not create environment BAT");
		return GDK_FAIL;
	}
	if (BBPrename(GDKkey->batCacheid, "environment_key") != 0 ||
	    BBPrename(GDKval->batCacheid, "environment_val") != 0) {
		free(n);
		GDKerror("GDKinit: BBPrename failed");
		return GDK_FAIL;
	}

	/* store options into environment BATs */
	for (i = 0; i < nlen; i++)
		if (GDKsetenv(n[i].name, n[i].value) != GDK_SUCCEED) {
			free(n);
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	free(n);

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0)
		GDKnr_threads = MT_check_nr_cores();

	if (!GDKinmemory()) {
		if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
			(p = strrchr(p, DIR_SEP)) != NULL) {
			if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED) {
				GDKerror("GDKinit: GDKsetenv failed");
				return GDK_FAIL;
			}
#if DIR_SEP != '/'		/* on Windows look for different separator */
		} else if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
				   (p = strrchr(p, '/')) != NULL) {
			if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED) {
				GDKerror("GDKinit: GDKsetenv failed");
				return GDK_FAIL;
			}
#endif
		}
	} else {
		if (GDKsetenv("gdk_dbname", ":inmemory") != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_vm_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_vm_maxsize);
		if (GDKsetenv("gdk_vm_maxsize", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mem_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mem_maxsize);
		if (GDKsetenv("gdk_mem_maxsize", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_minsize_persistent") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_minsize_persistent);
		if (GDKsetenv("gdk_mmap_minsize_persistent", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_minsize_transient") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_minsize_transient);
		if (GDKsetenv("gdk_mmap_minsize_transient", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_pagesize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_pagesize);
		if (GDKsetenv("gdk_mmap_pagesize", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("monet_pid") == NULL) {
		snprintf(buf, sizeof(buf), "%d", (int) getpid());
		if (GDKsetenv("monet_pid", buf) != GDK_SUCCEED) {
			GDKerror("GDKinit: GDKsetenv failed");
			return GDK_FAIL;
		}
	}
	if (GDKsetenv("revision", mercurial_revision()) != GDK_SUCCEED) {
		GDKerror("GDKinit: GDKsetenv failed");
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

int GDKnr_threads = 0;
static ATOMIC_TYPE GDKnrofthreads = ATOMIC_VAR_INIT(0);
static ThreadRec GDKthreads[THREADS];

bool
GDKexiting(void)
{
	return (bool) (ATOMIC_GET(&GDKstopped) > 0);
}

void
GDKprepareExit(void)
{
	if (ATOMIC_ADD(&GDKstopped, 1) > 0)
		return;

	THRDDEBUG dump_threads();
	join_detached_threads();
}

void
GDKreset(int status)
{
	MT_Id pid = MT_getpid();

	assert(GDKexiting());

	if (GDKkey) {
		BBPunfix(GDKkey->batCacheid);
		GDKkey = NULL;
	}
	if (GDKval) {
		BBPunfix(GDKval->batCacheid);
		GDKval = NULL;
	}

	join_detached_threads();

	if (status == 0) {
		/* they had their chance, now kill them */
		bool killed = false;
		MT_lock_set(&GDKthreadLock);
		for (Thread t = GDKthreads; t < GDKthreads + THREADS; t++) {
			MT_Id victim;
			if ((victim = (MT_Id) ATOMIC_GET(&t->pid)) != 0) {
				if (victim != pid) {
					int e;

					killed = true;
					e = MT_kill_thread(victim);
					fprintf(stderr, "#GDKexit: killing thread %d\n", e);
					(void) ATOMIC_DEC(&GDKnrofthreads);
				}
				GDKfree(t->name);
				t->name = NULL;
				ATOMIC_SET(&t->pid, 0);
			}
		}
		assert(ATOMIC_GET(&GDKnrofthreads) <= 1);
		/* all threads ceased running, now we can clean up */
		if (!killed) {
			/* we can't clean up after killing threads */
			BBPexit();
		}
		GDKlog(GET_GDKLOCK(PERSISTENT), GDKLOGOFF);

		for (int farmid = 0; farmid < MAXFARMS; farmid++) {
			if (BBPfarms[farmid].dirname != NULL) {
				bool skip = false;
				for (int j = 0; j < farmid; j++) {
					if (BBPfarms[j].dirname != NULL &&
					    strcmp(BBPfarms[farmid].dirname, BBPfarms[j].dirname) == 0) {
						skip = true;
						break;
					}
				}
				if (!skip)
					GDKunlockHome(farmid);
			}
		}

#ifdef LOCK_STATS
		TEMDEBUG GDKlockstatistics(1);
#endif
		GDKdebug = 0;
		GDK_mmap_minsize_persistent = MMAP_MINSIZE_PERSISTENT;
		GDK_mmap_minsize_transient = MMAP_MINSIZE_TRANSIENT;
		GDK_mmap_pagesize = MMAP_PAGESIZE;
		GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
		GDK_vm_maxsize = GDK_VM_MAXSIZE;
		GDKatomcnt = TYPE_str + 1;

		if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
			GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
			if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
				GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
		}

		GDKnr_threads = 0;
		ATOMIC_SET(&GDKnrofthreads, 0);
		close_stream((stream *) THRdata[0]);
		close_stream((stream *) THRdata[1]);
		for (int i = 0; i <= BBP_THREADMASK; i++) {
			GDKbbpLock[i].free = 0;
		}

		memset(THRdata, 0, sizeof(THRdata));
		gdk_bbp_reset();
		MT_lock_unset(&GDKthreadLock);
	}
	ATOMunknown_clean();
}

/* coverity[+kill] */
void
GDKexit(int status)
{
	if (!GDKinmemory() && GET_GDKLOCK(PERSISTENT) == NULL) {
#ifdef HAVE_EMBEDDED
		return;
#else
		/* no database lock, so no threads, so exit now */
		exit(status);
#endif
	}
	GDKprepareExit();
	GDKreset(status);
#ifndef HAVE_EMBEDDED
	exit(status);
#endif
}

/*
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 */

batlock_t GDKbatLock[BBP_BATMASK + 1];
bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
MT_Lock GDKnameLock = MT_LOCK_INITIALIZER("GDKnameLock");
MT_Lock GDKthreadLock = MT_LOCK_INITIALIZER("GDKthreadLock");
MT_Lock GDKtmLock = MT_LOCK_INITIALIZER("GDKtmLock");

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
static gdk_return
GDKlockHome(int farmid)
{
	int fd;
	struct stat st;
	char *gdklockpath;
	FILE *GDKlockFile;

	assert(BBPfarms[farmid].dirname != NULL);
	assert(BBPfarms[farmid].lock_file == NULL);

	if(!(gdklockpath = GDKfilepath(farmid, NULL, GDKLOCK, NULL))) {
		GDKerror("GDKlockHome: malloc failure\n");
		return GDK_FAIL;
	}

	/*
	 * Obtain the global database lock.
	 */
	if (stat(BBPfarms[farmid].dirname, &st) < 0 &&
	    GDKcreatedir(gdklockpath) != GDK_SUCCEED) {
		GDKerror("GDKlockHome: could not create %s\n",
			 BBPfarms[farmid].dirname);
		return GDK_FAIL;
	}
	if ((fd = MT_lockf(gdklockpath, F_TLOCK, 4, 1)) < 0) {
		GDKerror("GDKlockHome: Database lock '%s' denied\n",
			 gdklockpath);
		return GDK_FAIL;
	}

	/* now we have the lock on the database and are the only
	 * process allowed in this section */

	if ((GDKlockFile = fdopen(fd, "r+")) == NULL) {
		close(fd);
		GDKerror("GDKlockHome: Could not fdopen %s\n", gdklockpath);
		return GDK_FAIL;
	}

	/*
	 * Print the new process list in the global lock file.
	 */
	if (fseek(GDKlockFile, 0, SEEK_SET) == -1) {
		fclose(GDKlockFile);
		GDKerror("GDKlockHome: Error while setting the file pointer on %s\n", gdklockpath);
		return GDK_FAIL;
	}
	if (ftruncate(fileno(GDKlockFile), 0) < 0) {
		fclose(GDKlockFile);
		GDKerror("GDKlockHome: Could not truncate %s\n", gdklockpath);
		return GDK_FAIL;
	}
	if (fflush(GDKlockFile) == EOF) {
		fclose(GDKlockFile);
		GDKerror("GDKlockHome: Could not flush %s\n", gdklockpath);
		return GDK_FAIL;
	}
	GDKlog(GDKlockFile, GDKLOGON);
	GDKfree(gdklockpath);
	BBPfarms[farmid].lock_file = GDKlockFile;
	return GDK_SUCCEED;
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

		if (*prefix && dst < buf + GDKMAXERRLEN) {
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
		if (*suffix && dst < buf + GDKMAXERRLEN) {
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
		mnstr_printf(GDKout, "%s%.*s%s", prefix,
			     (int) messagelen, message, suffix);
	}
	fprintf(stderr, "#%s:%s%.*s%s",
		MT_thread_getname(),
		prefix[0] == '#' ? prefix + 1 : prefix,
		(int) messagelen, message, suffix);
}

/* print an error or warning message, making sure the message ends in
 * a newline, and also that every line in the message (if there are
 * multiple), starts with an exclamation point.
 * One of the problems complicating this whole issue is that each line
 * should be printed using a single call to mnstr_printf, and moreover,
 * the format string should start with a "!".
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
	int err = errno;
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);
	va_list ap;

	if (strncmp(format, GDKERROR, len) == 0) {
		len = 0;
	} else {
		strncpy(message, GDKERROR, sizeof(message));
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);
	if (err > 0 && err < 1024) {
		size_t len1;
		size_t len2;
		size_t len3;
		char *osmsg;
		osmsg = strerror(err);
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

#ifdef NATIVE_WIN32
void
GDKwinerror(const char *format, ...)
{
	int err = GetLastError();
	char message[GDKERRLEN];
	size_t len = strlen(GDKERROR);
	va_list ap;

	if (strncmp(format, GDKERROR, len) == 0) {
		len = 0;
	} else {
		strncpy(message, GDKERROR, sizeof(message));
	}
	va_start(ap, format);
	vsnprintf(message + len, sizeof(message) - (len + 2), format, ap);
	va_end(ap);

	size_t len1;
	size_t len2;
	size_t len3;
	char *osmsg;
	char osmsgbuf[256];
	osmsg = osmsgbuf;
	FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, err,
		      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		      (LPTSTR) osmsgbuf, sizeof(osmsgbuf), NULL);
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
	GDKaddbuf(message);

	SetLastError(0);
}
#endif

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

#ifndef STATIC_CODE_ANALYSIS
	if (GDKfataljumpenable) {
		// in embedded mode, we really don't want to kill our host
		GDKfatalmsg = GDKstrdup(message);
		longjmp(GDKfataljump, 42);
	} else
#endif
	{
		fputs(message, stderr);
		fputs("\n", stderr);
		fflush(stderr);

		/*
		 * Real errors should be saved in the log file for post-crash
		 * inspection.
		 */
		if (GDKexiting()) {
			fflush(stdout);
			exit(1);
		} else {
			GDKlog(GET_GDKLOCK(PERSISTENT), "%s", message);
#ifdef COREDUMP
			abort();
#else
			GDKexit(1);
#endif
		}
	}
}


lng
GDKusec(void)
{
	/* Return the time in microseconds since an epoch.  The epoch
	 * is currently midnight at the start of January 1, 1970, UTC. */
#if defined(NATIVE_WIN32)
	FILETIME ft;
	ULARGE_INTEGER f;
	GetSystemTimeAsFileTime(&ft); /* time since Jan 1, 1601 */
	f.LowPart = ft.dwLowDateTime;
	f.HighPart = ft.dwHighDateTime;
	/* there are 369 years, of which 89 are leap years from
	 * January 1, 1601 to January 1, 1970 which makes 134774 days;
	 * multiply that with the number of seconds in a day and the
	 * number of 100ns units in a second; subtract that from the
	 * value for the current time since January 1, 1601 to get the
	 * time since the Unix epoch */
	f.QuadPart -= LL_CONSTANT(134774) * 24 * 60 * 60 * 10000000;
	/* and convert to microseconds */
	return (lng) (f.QuadPart / 10);
#elif defined(HAVE_CLOCK_GETTIME)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (lng) (ts.tv_sec * LL_CONSTANT(1000000) + ts.tv_nsec / 1000);
#elif defined(HAVE_GETTIMEOFDAY)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (lng) (tv.tv_sec * LL_CONSTANT(1000000) + tv.tv_usec);
#elif defined(HAVE_FTIME)
	struct timeb tb;
	ftime(&tb);
	return (lng) (tb.time * LL_CONSTANT(1000000) + tb.millitm * LL_CONSTANT(1000));
#else
	/* last resort */
	return (lng) (time(NULL) * LL_CONSTANT(1000000));
#endif
}


int
GDKms(void)
{
	/* wraps around after a bit over 24 days */
	return (int) ((GDKusec() - programepoch) / 1000);
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
void *THRdata[THREADDATA] = { 0 };

Thread
THRget(int tid)
{
	assert(0 < tid && tid <= THREADS);
	return &GDKthreads[tid - 1];
}

#if defined(_MSC_VER) && _MSC_VER >= 1900
#pragma warning(disable : 4172)
#endif
static inline uintptr_t
THRsp(void)
{
	int l = 0;
	uintptr_t sp = (uintptr_t) (&l);

	return sp;
}

static inline Thread
GDK_find_self(void)
{
	return (Thread) MT_thread_getdata();
}

static Thread
THRnew(const char *name, MT_Id pid)
{
	char *nme = GDKstrdup(name);

	if (nme == NULL) {
		IODEBUG fprintf(stderr, "#THRnew: malloc failure\n");
		GDKerror("THRnew: malloc failure\n");
		return NULL;
	}
	for (Thread s = GDKthreads; s < GDKthreads + THREADS; s++) {
		ATOMIC_BASE_TYPE npid = 0;
		if (ATOMIC_CAS(&s->pid, &npid, pid)) {
			/* successfully allocated, fill in rest */
			s->data[0] = THRdata[0];
			s->data[1] = THRdata[1];
			s->sp = THRsp();
			s->name = nme;
			PARDEBUG fprintf(stderr, "#%x %zu sp = %zu\n",
					 (unsigned) s->tid,
					 (size_t) ATOMIC_GET(&s->pid),
					 (size_t) s->sp);
			PARDEBUG fprintf(stderr, "#nrofthreads %d\n",
					 (int) ATOMIC_GET(&GDKnrofthreads) + 1);
			return s;
		}
	}
	GDKfree(nme);
	IODEBUG fprintf(stderr, "#THRnew: too many threads\n");
	GDKerror("THRnew: too many threads\n");
	return NULL;
}

struct THRstart {
	void (*func) (void *);
	void *arg;
	MT_Sema sem;
	Thread thr;
};

static void
THRstarter(void *a)
{
	struct THRstart *t = a;
	void (*func) (void *) = t->func;
	void *arg = t->arg;

	MT_sema_down(&t->sem);
	t->thr->sp = THRsp();
	MT_thread_setdata(t->thr);
	(*func)(arg);
	THRdel(t->thr);
	MT_sema_destroy(&t->sem);
	GDKfree(a);
}

MT_Id
THRcreate(void (*f) (void *), void *arg, enum MT_thr_detach d, const char *name)
{
	MT_Id pid;
	Thread s;
	struct THRstart *t;
	static ATOMIC_TYPE ctr = ATOMIC_VAR_INIT(0);
	char semname[16];
	int len;

	if ((t = GDKmalloc(sizeof(*t))) == NULL)
		return 0;
	if ((s = THRnew(name, ~(MT_Id)0)) == NULL) {
		GDKfree(t);
		return 0;
	}
	*t = (struct THRstart) {
		.func = f,
		.arg = arg,
		.thr = s,
	};
	len = snprintf(semname, sizeof(semname), "THRcreate%" PRIu64, (uint64_t) ATOMIC_INC(&ctr));
	if (len == -1 || len > (int) sizeof(semname)) {
		IODEBUG fprintf(stderr, "#THRcreate: semaphore name is too large\n");
		GDKerror("THRcreate: semaphore name is too large\n");
		GDKfree(t);
		GDKfree(s->name);
		s->name = NULL;
		ATOMIC_SET(&s->pid, 0); /* deallocate */
		return 0;
	}
	MT_sema_init(&t->sem, 0, semname);
	if (MT_create_thread(&pid, THRstarter, t, d, name) != 0) {
		GDKerror("THRcreate: could not start thread\n");
		MT_sema_destroy(&t->sem);
		GDKfree(t);
		GDKfree(s->name);
		s->name = NULL;
		ATOMIC_SET(&s->pid, 0); /* deallocate */
		return 0;
	}
	/* must not fail after this: the thread has been started */
	(void) ATOMIC_INC(&GDKnrofthreads);
	ATOMIC_SET(&s->pid, pid);
	/* send new thread on its way */
	MT_sema_up(&t->sem);
	return pid;
}

void
THRdel(Thread t)
{
	assert(GDKthreads <= t && t < GDKthreads + THREADS);
	MT_thread_setdata(NULL);
	PARDEBUG fprintf(stderr, "#pid = %zu, disconnected, %d left\n",
			 (size_t) ATOMIC_GET(&t->pid),
			 (int) ATOMIC_GET(&GDKnrofthreads));

	GDKfree(t->name);
	t->name = NULL;
	for (int i = 0; i < THREADDATA; i++)
		t->data[i] = NULL;
	t->sp = 0;
	ATOMIC_SET(&t->pid, 0);	/* deallocate */
	(void) ATOMIC_DEC(&GDKnrofthreads);
}

int
THRhighwater(void)
{
	uintptr_t c;
	Thread s;
	size_t diff;
	int rc = 0;

	s = GDK_find_self();
	if (s != NULL) {
		c = THRsp();
		diff = c < s->sp ? s->sp - c : c - s->sp;
		if (diff > THREAD_STACK_SIZE - 80 * 1024)
			rc = 1;
	}
	return rc;
}

/*
 * I/O is organized per thread, because users may gain access through
 * the network.  The code below should be improved to gain speed.
 */

static int
THRinit(void)
{
	int i = 0;
	Thread s;
	static bool first = true;

	if ((THRdata[0] = (void *) file_wastream(stdout, "stdout")) == NULL)
		return -1;
	if ((THRdata[1] = (void *) file_rastream(stdin, "stdin")) == NULL) {
		mnstr_destroy(THRdata[0]);
		THRdata[0] = NULL;
		return -1;
	}
	if (first) {
		for (i = 0; i < THREADS; i++) {
			GDKthreads[i].tid = i + 1;
			ATOMIC_INIT(&GDKthreads[i].pid, 0);
		}
		first = false;
	}
	if ((s = THRnew("main thread", MT_getpid())) == NULL) {
		mnstr_destroy(THRdata[0]);
		THRdata[0] = NULL;
		mnstr_destroy(THRdata[1]);
		THRdata[1] = NULL;
		return -1;
	}
	(void) ATOMIC_INC(&GDKnrofthreads);
	MT_thread_setdata(s);
	return 0;
}

void
THRsetdata(int n, ptr val)
{
	Thread s;

	s = GDK_find_self();
	if (s) {
		assert(val == NULL || s->data[n] == NULL);
		s->data[n] = val;
	}
}

void *
THRgetdata(int n)
{
	Thread s;
	void *d;

	s = GDK_find_self();
	d = s ? s->data[n] : THRdata[n];
	return d;
}

int
THRgettid(void)
{
	Thread s;
	int t;

	s = GDK_find_self();
	t = s ? s->tid : 1;
	return t;
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

size_t
GDKmem_cursize(void)
{
	/* RAM/swapmem that Monet is really using now */
	return (size_t) ATOMIC_GET(&GDK_mallocedbytes_estimate);
}

size_t
GDKvm_cursize(void)
{
	/* current Monet VM address space usage */
	return (size_t) ATOMIC_GET(&GDK_vm_cursize) + GDKmem_cursize();
}

#define heapinc(_memdelta)						\
	(void) ATOMIC_ADD(&GDK_mallocedbytes_estimate, _memdelta)
#define heapdec(_memdelta)						\
	(void) ATOMIC_SUB(&GDK_mallocedbytes_estimate, _memdelta)

#define meminc(vmdelta)							\
	(void) ATOMIC_ADD(&GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG))
#define memdec(vmdelta)							\
	(void) ATOMIC_SUB(&GDK_vm_cursize, (ssize_t) SEG_SIZE((vmdelta), MT_VMUNITLOG))

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

	fprintf(stderr, "#%s(%zu) fails, try to free up space [memory in use=%zu,virtual memory in use=%zu]\n", s, len, GDKmem_cursize(), GDKvm_cursize());
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
		GDKerror("GDKmalloc_internal: failed for %zu bytes", size);
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
		GDKerror("GDKrealloc: failed for %zu bytes", size);
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
		GDKerror("GDKmalloc: failed for %zu bytes", size);
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
		GDKerror("GDKzalloc: failed for %zu bytes", size);
	return ptr;
}

void *
GDKrealloc(void *ptr, size_t size)
{
	void *p = realloc(ptr, size);
	if (p == NULL)
		GDKerror("GDKrealloc: failed for %zu bytes", size);
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
