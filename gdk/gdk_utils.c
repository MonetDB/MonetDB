/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
		TRC_CRITICAL(GDK, "Database name missing.\n");
		return false;
	}
	if (strlen(dbpath) >= FILENAME_MAX) {
		TRC_CRITICAL(GDK, "Database name too long.\n");
		return false;
	}
	if (!GDKembedded() && !MT_path_absolute(dbpath)) {
		TRC_CRITICAL(GDK, "Directory not an absolute path: %s.\n", dbpath);
		return false;
	}
	return true;
}

static struct orig_value {
	struct orig_value *next;
	char *value;
	char key[];
} *orig_value;
static MT_Lock GDKenvlock = MT_LOCK_INITIALIZER(GDKenvlock);

const char *
GDKgetenv(const char *name)
{
	MT_lock_set(&GDKenvlock);
	for (struct orig_value *ov = orig_value; ov; ov = ov->next) {
		if (strcmp(ov->key, name) == 0) {
			MT_lock_unset(&GDKenvlock);
			return ov->value;
		}
	}
	MT_lock_unset(&GDKenvlock);
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

#define ESCAPE_CHAR	'%'

static bool
isutf8(const char *v, size_t *esclen)
{
	size_t n = 1;
	int nutf8 = 0;
	int m = 0;
	for (size_t i = 0; v[i]; i++) {
		if (nutf8 > 0) {
			if ((v[i] & 0xC0) != 0x80 ||
			    (m != 0 && (v[i] & m) == 0))
				goto badutf8;
			m = 0;
			nutf8--;
		} else if ((v[i] & 0xE0) == 0xC0) {
			nutf8 = 1;
			if ((v[i] & 0x1E) == 0)
				goto badutf8;
		} else if ((v[i] & 0xF0) == 0xE0) {
			nutf8 = 2;
			if ((v[i] & 0x0F) == 0)
				m = 0x20;
		} else if ((v[i] & 0xF8) == 0xF0) {
			nutf8 = 3;
			if ((v[i] & 0x07) == 0)
				m = 0x30;
		} else if ((v[i] & 0x80) != 0) {
			goto badutf8;
		}
	}
	*esclen = 0;
	return true;
  badutf8:
	for (size_t i = 0; v[i]; i++) {
		if (v[i] & 0x80 || v[i] == ESCAPE_CHAR)
			n += 3;
		else
			n++;
	}
	*esclen = n;
	return false;
}

gdk_return
GDKsetenv(const char *name, const char *value)
{
	static const char hexdigits[] = "0123456789abcdef";
	char *conval = NULL;
	size_t esclen = 0;
	if (!isutf8(value, &esclen)) {
		size_t j = strlen(name) + 1;
		struct orig_value *ov = GDKmalloc(offsetof(struct orig_value, key) + j + strlen(value) + 1);
		if (ov == NULL)
			return GDK_FAIL;
		strcpy(ov->key, name);
		ov->value = ov->key + j;
		strcpy(ov->value, value);
		conval = GDKmalloc(esclen);
		if (conval == NULL) {
			GDKfree(ov);
			return GDK_FAIL;
		}
		j = 0;
		for (size_t i = 0; value[i]; i++) {
			if (value[i] & 0x80 || value[i] == ESCAPE_CHAR) {
				conval[j++] = ESCAPE_CHAR;
				conval[j++] = hexdigits[(unsigned char) value[i] >> 4];
				conval[j++] = hexdigits[(unsigned char) value[i] & 0xF];
			} else {
				conval[j++] = value[i];
			}
		}
		conval[j] = 0;
		MT_lock_set(&GDKenvlock);
		ov->next = orig_value;
		orig_value = ov;
		/* remove previous value if present (later in list) */
		for (ov = orig_value; ov->next; ov = ov->next) {
			if (strcmp(ov->next->key, name) == 0) {
				struct orig_value *ovn = ov->next;
				ov->next = ovn->next;
				GDKfree(ovn);
			}
		}
		MT_lock_unset(&GDKenvlock);
	} else {
		/* remove previous value if present */
		MT_lock_set(&GDKenvlock);
		for (struct orig_value **ovp = &orig_value; *ovp; ovp = &(*ovp)->next) {
			if (strcmp((*ovp)->key, name) == 0) {
				struct orig_value *ov = *ovp;
				*ovp = ov->next;
				GDKfree(ov);
				break;
			}
		}
		MT_lock_unset(&GDKenvlock);
	}
	gdk_return rc = BUNappend(GDKkey, name, false);
	if (rc == GDK_SUCCEED)
		rc = BUNappend(GDKval, conval ? conval : value, false);
	GDKfree(conval);
	return rc;
}

gdk_return
GDKcopyenv(BAT **key, BAT **val, bool writable)
{
	BAT *k, *v;

	if (key == NULL || val == NULL) {
		GDKerror("called incorrectly.\n");
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
	vsnprintf(buf, sizeof(buf), format, ap);
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
	ctm = ctime_r(&tm, tbuf);
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
static void
BATSIGinit(void)
{
#ifdef SIGPIPE
	(void) signal(SIGPIPE, SIG_IGN);
#endif
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
	char buf[1024];
	char cgr1[1024] = "/sys/fs/cgroup/memory";
	char cgr2[1024] = "/sys/fs/cgroup";
	fc = fopen("/proc/self/mountinfo", "r");
	if (fc != NULL) {
		while (fgets(buf, (int) sizeof(buf), fc) != NULL) {
			char *p, *cgr;
			if ((p = strstr(buf, " - cgroup ")) != NULL &&
			    strstr(p, "memory") != NULL)
				cgr = cgr1;
			else if (strstr(buf, " - cgroup2 ") != NULL)
				cgr = cgr2;
			else
				continue;
			/* buf points at mount ID */
			p = strchr(buf, ' ');
			if (p == NULL)
				break;
			p++;
			/* p points at parent ID */
			p = strchr(p, ' ');
			if (p == NULL)
				break;
			p++;
			/* p points at major:minor */
			p = strchr(p, ' ');
			if (p == NULL)
				break;
			p++;
			/* p points at root */
			p = strchr(p, ' ');
			if (p == NULL)
				break;
			p++;
			/* p points at mount point */
			char *dir = p;
			p = strchr(p, ' ');
			if (p == NULL)
				break;
			*p = 0;
			strcpy_len(cgr, dir, 1024);
		}
		fclose(fc);
	}
	fc = fopen("/proc/self/cgroup", "r");
	if (fc != NULL) {
		/* each line is of the form:
		 * hierarchy-ID:controller-list:cgroup-path
		 *
		 * For cgroup v1, the hierarchy-ID refers to the
		 * second column in /proc/cgroups (which we ignore)
		 * and the controller-list is a comma-separated list
		 * of the controllers bound to the hierarchy.  We look
		 * for the "memory" controller and use its
		 * cgroup-path.  We ignore the other lines.
		 *
		 * For cgroup v2, the hierarchy-ID is 0 and the
		 * controller-list is empty.  We just use the
		 * cgroup-path.
		 *
		 * We use the first line that we can match (either v1
		 * or v2) and for which we can open any of the files
		 * that we are looking for.
		 */
		while (fgets(buf, (int) sizeof(buf), fc) != NULL) {
			char pth[1024];
			char *p, *q;
			bool success = false; /* true if we can open any file */
			FILE *f;
			uint64_t mem;

			p = strchr(buf, '\n');
			if (p == NULL)
				break;
			*p = 0;
			if (strncmp(buf, "0::", 3) == 0) {
				size_t l;

				/* cgroup v2 entry */
				l = strconcat_len(pth, sizeof(pth),
						  cgr2, buf + 3, "/", NULL);
				/* hard limit */
				strcpy(pth + l, "memory.max");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1 && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					/* assume "max" if not a number */
					fclose(f);
				}
				/* soft high limit */
				strcpy(pth + l, "memory.high");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1 && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					/* assume "max" if not a number */
					fclose(f);
				}
				/* soft low limit */
				strcpy(pth + l, "memory.low");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1 && mem > 0 && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					/* assume "max" if not a number */
					fclose(f);
				}
				/* limit of memory+swap usage
				 * we use this as maximum virtual memory size */
				strcpy(pth + l, "memory.swap.max");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) GDK_vm_maxsize) {
						GDK_vm_maxsize = (size_t) mem;
					}
					success = true;
					fclose(f);
				}
			} else {
				/* cgroup v1 entry */
				p = strchr(buf, ':');
				if (p == NULL)
					break;
				q = p + 1;
				p = strchr(q, ':');
				if (p == NULL)
					break;
				*p++ = 0;
				if (strstr(q, "memory") == NULL)
					continue;
				/* limit of memory usage */
				strconcat_len(pth, sizeof(pth),
					      cgr1, p,
					      "/memory.limit_in_bytes",
					      NULL);
				f = fopen(pth, "r");
				if (f == NULL) {
					strconcat_len(pth, sizeof(pth),
						      cgr1,
						      "/memory.limit_in_bytes",
						      NULL);
					f = fopen(pth, "r");
				}
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					fclose(f);
				}
				/* soft limit of memory usage */
				strconcat_len(pth, sizeof(pth),
					      cgr1, p,
					      "/memory.soft_limit_in_bytes",
					      NULL);
				f = fopen(pth, "r");
				if (f == NULL) {
					strconcat_len(pth, sizeof(pth),
						      cgr1,
						      "/memory.soft_limit_in_bytes",
						      NULL);
					f = fopen(pth, "r");
				}
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					fclose(f);
				}
				/* limit of memory+swap usage
				 * we use this as maximum virtual memory size */
				strconcat_len(pth, sizeof(pth),
					      cgr1, p,
					      "/memory.memsw.limit_in_bytes",
					      NULL);
				f = fopen(pth, "r");
				if (f == NULL) {
					strconcat_len(pth, sizeof(pth),
						      cgr1,
						      "/memory.memsw.limit_in_bytes",
						      NULL);
					f = fopen(pth, "r");
				}
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem < (uint64_t) GDK_vm_maxsize) {
						GDK_vm_maxsize = (size_t) mem;
					}
					success = true;
					fclose(f);
				}
			}
			if (success)
				break;
		}
		fclose(fc);
	}
#endif

#if defined(HAVE_SYS_RESOURCE_H) && defined(HAVE_GETRLIMIT) && defined(RLIMIT_AS)
	struct rlimit l;
	/* address space (virtual memory) limit */
	if (getrlimit(RLIMIT_AS, &l) == 0
	    && l.rlim_cur != (rlim_t)RLIM_INFINITY
	    && (size_t)l.rlim_cur < GDK_vm_maxsize) {
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

#ifndef __COVERITY__
#ifndef NDEBUG
static MT_Lock mallocsuccesslock = MT_LOCK_INITIALIZER(mallocsuccesslock);
#endif
#endif

void
GDKsetdebug(int debug)
{
	GDKdebug = debug;
	if (debug & ACCELMASK)
		GDKtracer_set_component_level("accelerator", "debug");
	else
		GDKtracer_reset_component_level("accelerator");
	if (debug & ALGOMASK)
		GDKtracer_set_component_level("algo", "debug");
	else
		GDKtracer_reset_component_level("algo");
	if (debug & ALLOCMASK)
		GDKtracer_set_component_level("alloc", "debug");
	else
		GDKtracer_reset_component_level("alloc");
	if (debug & BATMASK)
		GDKtracer_set_component_level("bat", "debug");
	else
		GDKtracer_reset_component_level("bat");
	if (debug & CHECKMASK)
		GDKtracer_set_component_level("check", "debug");
	else
		GDKtracer_reset_component_level("check");
	if (debug & DELTAMASK)
		GDKtracer_set_component_level("delta", "debug");
	else
		GDKtracer_reset_component_level("delta");
	if (debug & HEAPMASK)
		GDKtracer_set_component_level("heap", "debug");
	else
		GDKtracer_reset_component_level("heap");
	if (debug & IOMASK)
		GDKtracer_set_component_level("io", "debug");
	else
		GDKtracer_reset_component_level("io");
	if (debug & PARMASK)
		GDKtracer_set_component_level("par", "debug");
	else
		GDKtracer_reset_component_level("par");
	if (debug & PERFMASK)
		GDKtracer_set_component_level("perf", "debug");
	else
		GDKtracer_reset_component_level("perf");
	if (debug & TEMMASK)
		GDKtracer_set_component_level("tem", "debug");
	else
		GDKtracer_reset_component_level("tem");
	if (debug & THRDMASK)
		GDKtracer_set_component_level("thrd", "debug");
	else
		GDKtracer_reset_component_level("thrd");
}

int
GDKgetdebug(void)
{
	int debug = GDKdebug;
	const char *lvl;
	lvl = GDKtracer_get_component_level("accelerator");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= ACCELMASK;
	lvl = GDKtracer_get_component_level("algo");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= ALGOMASK;
	lvl = GDKtracer_get_component_level("alloc");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= ALLOCMASK;
	lvl = GDKtracer_get_component_level("bat");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= BATMASK;
	lvl = GDKtracer_get_component_level("check");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= CHECKMASK;
	lvl = GDKtracer_get_component_level("delta");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= DELTAMASK;
	lvl = GDKtracer_get_component_level("heap");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= HEAPMASK;
	lvl = GDKtracer_get_component_level("io");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= IOMASK;
	lvl = GDKtracer_get_component_level("par");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= PARMASK;
	lvl = GDKtracer_get_component_level("perf");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= PERFMASK;
	lvl = GDKtracer_get_component_level("tem");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= TEMMASK;
	lvl = GDKtracer_get_component_level("thrd");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= THRDMASK;
	return debug;
}

static bool Mbedded = true;
bool
GDKembedded(void)
{
	return Mbedded;
}

gdk_return
GDKinit(opt *set, int setlen, bool embedded)
{
	static bool first = true;
	const char *dbpath;
	const char *dbtrace;
	const char *p;
	opt *n;
	int i, nlen = 0;
	char buf[16];

	if (GDKinmemory(0)) {
		dbpath = dbtrace = NULL;
	} else {
		dbpath = mo_find_option(set, setlen, "gdk_dbpath");
		dbtrace = mo_find_option(set, setlen, "gdk_dbtrace");
	}
	Mbedded = embedded;
	/* some sanity checks (should also find if symbols are not defined) */
	static_assert(sizeof(int) == sizeof(int32_t),
		      "int is not equal in size to int32_t");
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
	static_assert(sizeof(dbl) == SIZEOF_DOUBLE,
		      "error in configure: bad value for SIZEOF_DOUBLE");
	static_assert(sizeof(oid) == SIZEOF_OID,
		      "error in configure: bad value for SIZEOF_OID");
	static_assert(sizeof(void *) == SIZEOF_VOID_P,
		      "error in configure: bad value for SIZEOF_VOID_P");
	static_assert(sizeof(size_t) == SIZEOF_SIZE_T,
		      "error in configure: bad value for SIZEOF_SIZE_T");
	static_assert(SIZEOF_OID == SIZEOF_INT || SIZEOF_OID == SIZEOF_LNG,
		      "SIZEOF_OID should be equal to SIZEOF_INT or SIZEOF_LNG");
	static_assert(sizeof(uuid) == 16,
		      "sizeof(uuid) should be equal to 16");

	if (first) {
		/* some things are really only initialized once */
		if (!MT_thread_init()) {
			TRC_CRITICAL(GDK, "MT_thread_init failed\n");
			return GDK_FAIL;
		}

		for (i = 0; i <= BBP_BATMASK; i++) {
			char name[MT_NAME_LEN];
			snprintf(name, sizeof(name), "GDKswapLock%d", i);
			MT_lock_init(&GDKbatLock[i].swap, name);
		}
		for (i = 0; i <= BBP_THREADMASK; i++) {
			char name[MT_NAME_LEN];
			snprintf(name, sizeof(name), "GDKcacheLock%d", i);
			MT_lock_init(&GDKbbpLock[i].cache, name);
			snprintf(name, sizeof(name), "GDKtrimLock%d", i);
			MT_lock_init(&GDKbbpLock[i].trim, name);
			GDKbbpLock[i].free = 0;
		}
		if (mnstr_init() < 0) {
			TRC_CRITICAL(GDK, "mnstr_init failed\n");
			return GDK_FAIL;
		}
		first = false;
	} else {
		/* BBP was locked by BBPexit() */
		BBPunlock();
	}
	GDKtracer_init(dbpath, dbtrace);
	errno = 0;
	if (!GDKinmemory(0) && !GDKenvironment(dbpath))
		return GDK_FAIL;

	MT_init_posix();
	if (THRinit() < 0)
		return GDK_FAIL;
#ifndef NATIVE_WIN32
	BATSIGinit();
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
#if SIZEOF_SIZE_T == 4
	if ((double) MT_npages() * (double) MT_pagesize() * 0.815 >= (double) GDK_VM_MAXSIZE)
		GDK_mem_maxsize = GDK_VM_MAXSIZE;
	else
#endif
	GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
	if (BBPinit() != GDK_SUCCEED)
		return GDK_FAIL;

	if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
		GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
		if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
			GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
	}

	n = (opt *) malloc(setlen * sizeof(opt));
	if (n == NULL) {
		GDKsyserror("malloc failed\n");
		return GDK_FAIL;
	}

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
				TRC_CRITICAL(GDK, "gdk_mmap_pagesize must be power of 2 between 2**12 and 2**20\n");
				return GDK_FAIL;
			}
		}
	}

	GDKkey = COLnew(0, TYPE_str, 100, TRANSIENT);
	GDKval = COLnew(0, TYPE_str, 100, TRANSIENT);
	if (GDKkey == NULL || GDKval == NULL) {
		free(n);
		TRC_CRITICAL(GDK, "Could not create environment BATs");
		return GDK_FAIL;
	}
	if (BBPrename(GDKkey->batCacheid, "environment_key") != 0 ||
	    BBPrename(GDKval->batCacheid, "environment_val") != 0) {
		free(n);
		TRC_CRITICAL(GDK, "BBPrename of environment BATs failed");
		return GDK_FAIL;
	}

	/* store options into environment BATs */
	for (i = 0; i < nlen; i++)
		if (GDKsetenv(n[i].name, n[i].value) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv %s failed", n[i].name);
			free(n);
			return GDK_FAIL;
		}
	free(n);

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0)
		GDKnr_threads = MT_check_nr_cores();

	if (!GDKinmemory(0)) {
		if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
			(p = strrchr(p, DIR_SEP)) != NULL) {
			if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED) {
				TRC_CRITICAL(GDK, "GDKsetenv gdk_dbname failed");
				return GDK_FAIL;
			}
#if DIR_SEP != '/'		/* on Windows look for different separator */
		} else if ((p = GDKgetenv("gdk_dbpath")) != NULL &&
				   (p = strrchr(p, '/')) != NULL) {
			if (GDKsetenv("gdk_dbname", p + 1) != GDK_SUCCEED) {
				TRC_CRITICAL(GDK, "GDKsetenv gdk_dbname failed");
				return GDK_FAIL;
			}
#endif
		}
	} else if (GDKgetenv("gdk_dbname") == NULL) {
		if (GDKsetenv("gdk_dbname", ":memory:") != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_dbname failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_vm_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_vm_maxsize);
		if (GDKsetenv("gdk_vm_maxsize", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_vm_maxsize failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mem_maxsize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mem_maxsize);
		if (GDKsetenv("gdk_mem_maxsize", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_mem_maxsize failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_minsize_persistent") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_minsize_persistent);
		if (GDKsetenv("gdk_mmap_minsize_persistent", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_mmap_minsize_persistent failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_minsize_transient") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_minsize_transient);
		if (GDKsetenv("gdk_mmap_minsize_transient", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_mmap_minsize_transient failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("gdk_mmap_pagesize") == NULL) {
		snprintf(buf, sizeof(buf), "%zu", GDK_mmap_pagesize);
		if (GDKsetenv("gdk_mmap_pagesize", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_mmap_pagesize failed");
			return GDK_FAIL;
		}
	}
	if (GDKgetenv("monet_pid") == NULL) {
		snprintf(buf, sizeof(buf), "%d", (int) getpid());
		if (GDKsetenv("monet_pid", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv monet_pid failed");
			return GDK_FAIL;
		}
	}
	if (GDKsetenv("revision", mercurial_revision()) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "GDKsetenv revision failed");
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

int GDKnr_threads = 0;
static ATOMIC_TYPE GDKnrofthreads = ATOMIC_VAR_INIT(0);
static struct threadStruct GDKthreads[THREADS];

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

	TRC_DEBUG_IF(THRD)
		dump_threads();
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

	MT_lock_set(&GDKenvlock);
	while (orig_value) {
		struct orig_value *ov = orig_value;
		orig_value = orig_value->next;
		GDKfree(ov);
	}
	MT_lock_unset(&GDKenvlock);

	if (status == 0) {
		/* they had their chance, now kill them */
		bool killed = false;
		MT_lock_set(&GDKthreadLock);
		for (Thread t = GDKthreads; t < GDKthreads + THREADS; t++) {
			MT_Id victim;
			if ((victim = (MT_Id) ATOMIC_GET(&t->pid)) != 0) {
				if (pid && victim != pid) {
					int e;

					killed = true;
					e = MT_kill_thread(victim);
					TRC_INFO(GDK, "Killing thread: %d\n", e);
					(void) ATOMIC_DEC(&GDKnrofthreads);
				}
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
				if (BBPfarms[farmid].dirname) {
					GDKfree((char*)BBPfarms[farmid].dirname);
					BBPfarms[farmid].dirname = NULL;
				}
			}
		}

#ifdef LOCK_STATS
		TRC_DEBUG_IF(TEM) GDKlockstatistics(1);
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

	/* stop GDKtracer */
	GDKtracer_stop();
}

/* coverity[+kill] */
void
GDKexit(int status)
{
	if (!GDKinmemory(0) && GET_GDKLOCK(PERSISTENT) == NULL) {
		/* stop GDKtracer */
		GDKtracer_stop();

		/* no database lock, so no threads, so exit now */
		if (!GDKembedded())
			exit(status);
	}
	GDKprepareExit();
	GDKreset(status);
	if (!GDKembedded())
		exit(status);
}

/*
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 */

batlock_t GDKbatLock[BBP_BATMASK + 1];
bbplock_t GDKbbpLock[BBP_THREADMASK + 1];
MT_Lock GDKnameLock = MT_LOCK_INITIALIZER(GDKnameLock);
MT_Lock GDKthreadLock = MT_LOCK_INITIALIZER(GDKthreadLock);
MT_Lock GDKtmLock = MT_LOCK_INITIALIZER(GDKtmLock);

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

	if ((gdklockpath = GDKfilepath(farmid, NULL, GDKLOCK, NULL)) == NULL) {
		return GDK_FAIL;
	}

	/*
	 * Obtain the global database lock.
	 */
	if (MT_stat(BBPfarms[farmid].dirname, &st) < 0 &&
	    GDKcreatedir(gdklockpath) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "could not create %s\n",
			 BBPfarms[farmid].dirname);
		GDKfree(gdklockpath);
		return GDK_FAIL;
	}
	if ((fd = MT_lockf(gdklockpath, F_TLOCK)) < 0) {
		TRC_CRITICAL(GDK, "Database lock '%s' denied\n",
			 gdklockpath);
		GDKfree(gdklockpath);
		return GDK_FAIL;
	}

	/* now we have the lock on the database and are the only
	 * process allowed in this section */

	if ((GDKlockFile = fdopen(fd, "r+")) == NULL) {
		GDKsyserror("Could not fdopen %s\n", gdklockpath);
		close(fd);
		GDKfree(gdklockpath);
		return GDK_FAIL;
	}

	/*
	 * Print the new process list in the global lock file.
	 */
	if (fseek(GDKlockFile, 0, SEEK_SET) == -1) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Error while setting the file pointer on %s\n", gdklockpath);
		GDKfree(gdklockpath);
		return GDK_FAIL;
	}
	if (ftruncate(fileno(GDKlockFile), 0) < 0) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Could not truncate %s\n", gdklockpath);
		GDKfree(gdklockpath);
		return GDK_FAIL;
	}
	if (fflush(GDKlockFile) == EOF) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Could not flush %s\n", gdklockpath);
		GDKfree(gdklockpath);
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
		if (gdklockpath)
			MT_lockf(gdklockpath, F_ULOCK);
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

#define GDKERRLEN	(1024+512)

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

	GDKtracer_set_component_level("io", "debug");
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

#ifndef __COVERITY__
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
			exit(1);
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
	(void) clock_gettime(CLOCK_REALTIME, &ts);
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
	for (Thread s = GDKthreads; s < GDKthreads + THREADS; s++) {
		ATOMIC_BASE_TYPE npid = 0;
		if (ATOMIC_CAS(&s->pid, &npid, pid)) {
			/* successfully allocated, fill in rest */
			s->data[0] = THRdata[0];
			s->data[1] = THRdata[1];
			s->sp = THRsp();
			strcpy_len(s->name, name, sizeof(s->name));
			TRC_DEBUG(PAR, "%x %zu sp = %zu\n",
				  (unsigned) s->tid,
				  (size_t) ATOMIC_GET(&s->pid),
				  (size_t) s->sp);
			TRC_DEBUG(PAR, "Number of threads: %d\n",
				  (int) ATOMIC_GET(&GDKnrofthreads) + 1);
			return s;
		}
	}
	TRC_DEBUG(IO_, "Too many threads\n");
	GDKerror("too many threads\n");
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
	char semname[32];
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
		TRC_WARNING(IO_, "Semaphore name is too large\n");
	}
	MT_sema_init(&t->sem, 0, semname);
	if (MT_create_thread(&pid, THRstarter, t, d, name) != 0) {
		GDKerror("could not start thread\n");
		MT_sema_destroy(&t->sem);
		GDKfree(t);
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
	TRC_DEBUG(PAR, "pid = %zu, disconnected, %d left\n",
		  (size_t) ATOMIC_GET(&t->pid),
		  (int) ATOMIC_GET(&GDKnrofthreads));

	t->name[0] = 0;
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

	if ((THRdata[0] = (void *) stdout_wastream()) == NULL) {
		TRC_CRITICAL(GDK, "malloc for stdout failed\n");
		return -1;
	}
	if ((THRdata[1] = (void *) stdin_rastream()) == NULL) {
		TRC_CRITICAL(GDK, "malloc for stdin failed\n");
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
		TRC_CRITICAL(GDK, "THRnew failed\n");
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

const char *
GDKversion(void)
{
	return MONETDB_VERSION;
}

const char *
GDKlibversion(void)
{
	return GDK_VERSION;
}

inline size_t
GDKmem_cursize(void)
{
	/* RAM/swapmem that Monet is really using now */
	return (size_t) ATOMIC_GET(&GDK_mallocedbytes_estimate);
}

inline size_t
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
		GDKerror("allocation failed because of testing limit\n");
		return NULL;
	}
#endif
	if (GDKvm_cursize() + size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory\n");
		return NULL;
	}

	/* pad to multiple of eight bytes and add some extra space to
	 * write real size in front; when debugging, also allocate
	 * extra space for check bytes */
	nsize = (size + 7) & ~7;
	if ((s = malloc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE)) == NULL) {
		GDKsyserror("malloc failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", size, GDKmem_cursize(), GDKvm_cursize());;
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
	    GDKvm_cursize() + nsize - asize >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
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
		GDKsyserror("realloc failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", size, GDKmem_cursize(), GDKvm_cursize());;
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

/* return how much memory was allocated; the argument must be a value
 * returned by GDKmalloc, GDKzalloc, GDKrealloc, GDKstrdup, or
 * GDKstrndup */
size_t
GDKmallocated(const void *s)
{
	return ((const size_t *) s)[-1]; /* how much allocated last */
}

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

	if (GDKvm_cursize() + len >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("requested too much virtual memory; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", len, GDKmem_cursize(), GDKvm_cursize());
		return NULL;
	}
	ret = MT_mmap(path, mode, len);
	if (ret != NULL)
		meminc(len);
	else
		GDKerror("requesting virtual memory failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", len, GDKmem_cursize(), GDKvm_cursize());
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
	    GDKvm_cursize() + *new_size - old_size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("requested too much virtual memory; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", *new_size, GDKmem_cursize(), GDKvm_cursize());
		return NULL;
	}
	ret = MT_mremap(path, mode, old_address, old_size, new_size);
	if (ret != NULL) {
		memdec(old_size);
		meminc(*new_size);
	} else {
		GDKerror("requesting virtual memory failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", *new_size, GDKmem_cursize(), GDKvm_cursize());
	}
	return ret;
}
