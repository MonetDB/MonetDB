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
ATOMIC_TYPE GDKdebug = ATOMIC_VAR_INIT(0);

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

/* when the number of updates to a BAT is less than 1 in this number, we
 * keep the unique_est property */
BUN gdk_unique_estimate_keep_fraction = GDK_UNIQUE_ESTIMATE_KEEP_FRACTION; /* should become a define once */
/* if the number of unique values is less than 1 in this number, we
 * destroy the hash rather than update it in HASH{append,insert,delete} */
BUN hash_destroy_uniques_fraction = HASH_DESTROY_UNIQUES_FRACTION;     /* likewise */
/* if the estimated number of unique values is less than 1 in this
 * number, don't build a hash table to do a hashselect */
dbl no_hash_select_fraction = NO_HASH_SELECT_FRACTION;           /* same here */
/* if the hash chain is longer than this number, we delete the hash
 * rather than maintaining it in HASHdelete */
BUN hash_destroy_chain_length = HASH_DESTROY_CHAIN_LENGTH;

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
		BUN b = BUNfnd(GDKkey, name);

		if (b != BUN_NONE) {
			BATiter GDKenvi = bat_iterator(GDKval);
			const char *v = BUNtvar(GDKenvi, b);
			bat_iterator_end(&GDKenvi);
			return v;
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
	BUN p = BUNfnd(GDKkey, name);
	gdk_return rc;
	if (p != BUN_NONE) {
		rc = BUNreplace(GDKval, p + GDKval->hseqbase,
				conval ? conval : value, false);
	} else {
		rc = BUNappend(GDKkey, name, false);
		if (rc == GDK_SUCCEED) {
			rc = BUNappend(GDKval, conval ? conval : value, false);
			if (rc != GDK_SUCCEED) {
				/* undo earlier successful append to
				 * keep bats aligned (this can't really
				 * fail, but we must check the result
				 * anyway) */
				if (BUNdelete(GDKkey, GDKkey->hseqbase + GDKkey->batCount - 1) != GDK_SUCCEED)
					GDKerror("deleting key failed after failed value append");
			}
		}
	}
	assert(BATcount(GDKval) == BATcount(GDKkey));
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
__attribute__((__format__(__printf__, 2, 3)))
static void
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
 */
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
#ifdef HAVE_SIGACTION
	struct sigaction sa;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
#ifdef SIGPIPE
	sa.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sa, NULL);
#endif
#ifdef SIGHUP
	sa.sa_handler = GDKtracer_reinit_basic;
	sigaction(SIGHUP, &sa, NULL);
#endif
#ifdef WIN32
	sa.sa_handler = BATSIGabort;
	sigaction(SIGABRT, &sa, NULL);
#endif
#else
#ifdef SIGPIPE
	(void) signal(SIGPIPE, SIG_IGN);
#endif
#ifdef SIGHUP
	// Register signal to GDKtracer (logrotate)
	(void) signal(SIGHUP, GDKtracer_reinit_basic);
#endif
#ifdef WIN32
	(void) signal(SIGABRT, BATSIGabort);
#endif
#endif
#if defined(WIN32) && !defined(__MINGW32__) && !defined(__CYGWIN__)
	_set_abort_behavior(0, _CALL_REPORTFAULT | _WRITE_ABORT_MSG);
	_set_error_mode(_OUT_TO_STDERR);
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

#define SEG_SIZE(x)	((ssize_t) (((x) + _MT_pagesize - 1) & ~(_MT_pagesize - 1)))

/* This block is to provide atomic addition and subtraction to select
 * variables.  We use intrinsic functions (recognized and inlined by
 * the compiler) for both the GNU C compiler and Microsoft Visual
 * Studio.  By doing this, we avoid locking overhead.  There is also a
 * fall-back for other compilers. */
#include "matomic.h"
static ATOMIC_TYPE GDK_mallocedbytes_estimate = ATOMIC_VAR_INIT(0);
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
				/* cgroup v2 entry */
				p = stpcpy(pth, cgr2);
				q = stpcpy(stpcpy(p, buf + 3), "/");
				/* hard limit */
				strcpy(q, "memory.max");
				f = fopen(pth, "r");
				while (f == NULL && q > p) {
					/* go up the hierarchy until we
					 * find the file or the
					 * hierarchy runs out */
					*--q = 0; /* zap the slash */
					q = strrchr(p, '/');
					if (q == NULL || q == p) {
						/* position after the slash */
						q = p + 1;
						break;
					}
					strcpy(++q, "memory.max");
					f = fopen(pth, "r");
				}
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem > 0
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					/* assume "max" if not a number */
					fclose(f);
				}
				/* soft high limit */
				strcpy(q, "memory.high");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem > 0
					    && mem < (uint64_t) _MT_pagesize * _MT_npages) {
						_MT_npages = (size_t) (mem / _MT_pagesize);
					}
					success = true;
					/* assume "max" if not a number */
					fclose(f);
				}
				/* limit of swap usage, hard limit
				 * we use this, together with
				 * memory.high, as maximum virtual
				 * memory size */
				strcpy(q, "memory.swap.max");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem > 0
					    && (mem += _MT_npages * _MT_pagesize) < (uint64_t) GDK_vm_maxsize) {
						GDK_vm_maxsize = (size_t) mem;
					}
					success = true;
					fclose(f);
				}
#if 0 /* not sure about using this one */
				/* limit of swap usage, soft limit */
				strcpy(q, "memory.swap.high");
				f = fopen(pth, "r");
				if (f != NULL) {
					if (fscanf(f, "%" SCNu64, &mem) == 1
					    && mem > 0
					    && (mem += _MT_npages * _MT_pagesize) < (uint64_t) GDK_vm_maxsize) {
						GDK_vm_maxsize = (size_t) mem;
					}
					success = true;
					fclose(f);
				}
#endif
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
					    && mem > 0
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
					    && mem > 0
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
					    && mem > 0
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

void
GDKsetdebug(unsigned debug)
{
	ATOMIC_SET(&GDKdebug, debug);
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
	if (debug & LOADMASK)
		GDKtracer_set_component_level("mal_loader", "debug");
	else
		GDKtracer_reset_component_level("mal_loader");
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
	if (debug & TMMASK)
		GDKtracer_set_component_level("tm", "debug");
	else
		GDKtracer_reset_component_level("tm");
}

unsigned
GDKgetdebug(void)
{
	ATOMIC_BASE_TYPE debug = ATOMIC_GET(&GDKdebug);
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
	lvl = GDKtracer_get_component_level("mal_loader");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= LOADMASK;
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
	lvl = GDKtracer_get_component_level("tm");
	if (lvl && strcmp(lvl, "debug") == 0)
		debug |= TMMASK;
	return (unsigned) debug;
}

static bool Mbedded = true;
bool
GDKembedded(void)
{
	return Mbedded;
}

static MT_Id mainpid;

gdk_return
GDKinit(opt *set, int setlen, bool embedded, const char *caller_revision)
{
	static bool first = true;
	const char *dbpath;
	const char *dbtrace;
	const char *p;
	opt *n;
	int i, nlen = 0;
	char buf[16];

	if (caller_revision) {
		p = mercurial_revision();
		if (p && strcmp(p, caller_revision) != 0) {
			GDKerror("incompatible versions: caller is %s, GDK is %s\n", caller_revision, p);
			return GDK_FAIL;
		}
	}

	ATOMIC_SET(&GDKstopped, 0);

	if (BBPchkfarms() != GDK_SUCCEED)
		return GDK_FAIL;

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
			snprintf(name, sizeof(name), "GDKswapCond%d", i);
			MT_cond_init(&GDKbatLock[i].cond, name);
		}
		if (mnstr_init() < 0) {
			TRC_CRITICAL(GDK, "mnstr_init failed\n");
			return GDK_FAIL;
		}
	} else {
		/* BBP was locked by BBPexit() */
		//BBPunlock();
	}
	mainpid = MT_getpid();

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
	const char *allow = mo_find_option(set, setlen, "allow_hge_upgrade");
	const char *procwalxit = mo_find_option(set, setlen, "process-wal-and-exit");
	if (BBPinit(allow && strcmp(allow, "yes") == 0, procwalxit && strcmp(procwalxit, "yes") == 0) != GDK_SUCCEED)
		return GDK_FAIL;
	first = false;

	if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
		GDK_mmap_minsize_transient = MAX(MMAP_PAGESIZE, GDK_mem_maxsize / 16);
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
			if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient)
				GDK_mmap_minsize_transient = MAX(MMAP_PAGESIZE, GDK_mem_maxsize / 16);
			if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
				GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
		} else if (strcmp("gdk_vm_maxsize", n[i].name) == 0) {
			GDK_vm_maxsize = (size_t) strtoll(n[i].value, NULL, 10);
			GDK_vm_maxsize = MAX(1 << 30, GDK_vm_maxsize);
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
	if (BBPrename(GDKkey, "environment_key") != 0 ||
	    BBPrename(GDKval, "environment_val") != 0) {
		free(n);
		TRC_CRITICAL(GDK, "BBPrename of environment BATs failed");
		return GDK_FAIL;
	}
	BBP_pid(GDKkey->batCacheid) = 0;
	BBP_pid(GDKval->batCacheid) = 0;

	/* store options into environment BATs */
	for (i = 0; i < nlen; i++)
		if (GDKsetenv(n[i].name, n[i].value) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv %s failed", n[i].name);
			free(n);
			return GDK_FAIL;
		}
	free(n);

	GDKnr_threads = GDKgetenv_int("gdk_nr_threads", 0);
	if (GDKnr_threads == 0) {
		GDKnr_threads = MT_check_nr_cores();
		snprintf(buf, sizeof(buf), "%d", GDKnr_threads);
		if (GDKsetenv("gdk_nr_threads", buf) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "GDKsetenv gdk_nr_threads failed");
			return GDK_FAIL;
		}
	}
	if (GDKnr_threads > THREADS)
		GDKnr_threads = THREADS;

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
		if (GDKsetenv("gdk_dbname", "in-memory") != GDK_SUCCEED) {
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
	gdk_unique_estimate_keep_fraction = 0;
	if ((p = GDKgetenv("gdk_unique_estimate_keep_fraction")) != NULL)
		gdk_unique_estimate_keep_fraction = (BUN) strtoll(p, NULL, 10);
	if (gdk_unique_estimate_keep_fraction == 0)
		gdk_unique_estimate_keep_fraction = GDK_UNIQUE_ESTIMATE_KEEP_FRACTION;
	hash_destroy_uniques_fraction = 0;
	if ((p = GDKgetenv("hash_destroy_uniques_fraction")) != NULL)
		hash_destroy_uniques_fraction = (BUN) strtoll(p, NULL, 10);
	if (hash_destroy_uniques_fraction == 0)
		hash_destroy_uniques_fraction = HASH_DESTROY_UNIQUES_FRACTION;
	no_hash_select_fraction = 0;
	if ((p = GDKgetenv("no_hash_select_fraction")) != NULL)
		no_hash_select_fraction = (dbl) strtoll(p, NULL, 10);
	if (no_hash_select_fraction == 0)
		no_hash_select_fraction = NO_HASH_SELECT_FRACTION;
	hash_destroy_chain_length = 0;
	if ((p = GDKgetenv("hash_destroy_chain_length")) != NULL)
		hash_destroy_chain_length = (BUN) strtoll(p, NULL, 10);
	if (hash_destroy_chain_length == 0)
		hash_destroy_chain_length = HASH_DESTROY_CHAIN_LENGTH;

	return GDK_SUCCEED;
}

int GDKnr_threads = 0;
static ATOMIC_TYPE GDKnrofthreads = ATOMIC_VAR_INIT(0);

bool
GDKexiting(void)
{
	return (bool) (ATOMIC_GET(&GDKstopped) > 0);
}

void
GDKprepareExit(void)
{
	ATOMIC_ADD(&GDKstopped, 1);

	if (MT_getpid() == mainpid) {
		TRC_DEBUG_IF(THRD)
			dump_threads();
		join_detached_threads();
	}
}

void
GDKreset(int status)
{
	assert(GDKexiting());

	if (GDKembedded())
		// In the case of a restarted embedded database, GDKstopped has to be reset as well.
		ATOMIC_SET(&GDKstopped, 0);

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
		bool killed = MT_kill_threads();
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
		ATOMIC_SET(&GDKdebug, 0);
		GDK_mmap_minsize_persistent = MMAP_MINSIZE_PERSISTENT;
		GDK_mmap_minsize_transient = MMAP_MINSIZE_TRANSIENT;
		GDK_mmap_pagesize = MMAP_PAGESIZE;
		GDK_mem_maxsize = (size_t) ((double) MT_npages() * (double) MT_pagesize() * 0.815);
		GDK_vm_maxsize = GDK_VM_MAXSIZE;
		GDKatomcnt = TYPE_blob + 1;

		if (GDK_mem_maxsize / 16 < GDK_mmap_minsize_transient) {
			GDK_mmap_minsize_transient = GDK_mem_maxsize / 16;
			if (GDK_mmap_minsize_persistent > GDK_mmap_minsize_transient)
				GDK_mmap_minsize_persistent = GDK_mmap_minsize_transient;
		}

		GDKnr_threads = 0;
		ATOMIC_SET(&GDKnrofthreads, 0);
		close_stream(GDKstdout);
		close_stream(GDKstdin);
		GDKstdout = NULL;
		GDKstdin = NULL;

		gdk_bbp_reset();
	}
	ATOMunknown_clean();

	/* stop GDKtracer */
	GDKtracer_stop();
}

/*
 * All semaphores used by the application should be mentioned here.
 * They are initialized during system initialization.
 */

batlock_t GDKbatLock[BBP_BATMASK + 1];

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
	char gdklockpath[1024];
	FILE *GDKlockFile;

	assert(BBPfarms[farmid].dirname != NULL);
	assert(BBPfarms[farmid].lock_file == NULL);

	if (GDKfilepath(gdklockpath, sizeof(gdklockpath), farmid, NULL, GDKLOCK, NULL) != GDK_SUCCEED)
		return GDK_FAIL;

	/*
	 * Obtain the global database lock.
	 */
	if (MT_stat(BBPfarms[farmid].dirname, &st) < 0 &&
	    GDKcreatedir(gdklockpath) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "could not create %s\n",
			 BBPfarms[farmid].dirname);
		return GDK_FAIL;
	}
	if ((fd = MT_lockf(gdklockpath, F_TLOCK)) < 0) {
		TRC_CRITICAL(GDK, "Database lock '%s' denied\n",
			 gdklockpath);
		return GDK_FAIL;
	}

	/* now we have the lock on the database and are the only
	 * process allowed in this section */

	if ((GDKlockFile = fdopen(fd, "r+")) == NULL) {
		GDKsyserror("Could not fdopen %s\n", gdklockpath);
		close(fd);
		return GDK_FAIL;
	}

	/*
	 * Print the new process list in the global lock file.
	 */
	if (fseek(GDKlockFile, 0, SEEK_SET) == -1) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Error while setting the file pointer on %s\n", gdklockpath);
		return GDK_FAIL;
	}
	if (ftruncate(fileno(GDKlockFile), 0) < 0) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Could not truncate %s\n", gdklockpath);
		return GDK_FAIL;
	}
	if (fflush(GDKlockFile) == EOF) {
		fclose(GDKlockFile);
		TRC_CRITICAL(GDK, "Could not flush %s\n", gdklockpath);
		return GDK_FAIL;
	}
	GDKlog(GDKlockFile, GDKLOGON);
	BBPfarms[farmid].lock_file = GDKlockFile;
	return GDK_SUCCEED;
}


static void
GDKunlockHome(int farmid)
{
	if (BBPfarms[farmid].lock_file) {
		char gdklockpath[MAXPATH];

		if (GDKfilepath(gdklockpath, sizeof(gdklockpath), farmid, NULL, GDKLOCK, NULL) == GDK_SUCCEED)
			MT_lockf(gdklockpath, F_ULOCK);
		fclose(BBPfarms[farmid].lock_file);
		BBPfarms[farmid].lock_file = NULL;
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
 * Note that the users should have gained exclusive access already.  A
 * new entry is initialized automatically when not found.  Its file
 * descriptors are the same as for the server and should be
 * subsequently reset.
 */
stream *GDKstdout;
stream *GDKstdin;

static int
THRinit(void)
{
	if ((GDKstdout = stdout_wastream()) == NULL) {
		TRC_CRITICAL(GDK, "malloc for stdout failed\n");
		return -1;
	}
	if ((GDKstdin = stdin_rastream()) == NULL) {
		TRC_CRITICAL(GDK, "malloc for stdin failed\n");
		mnstr_destroy(GDKstdout);
		GDKstdout = NULL;
		return -1;
	}
	struct freebats *t = MT_thread_getfreebats();
	t->freebats = 0;
	t->nfreebats = 0;
	return 0;
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
	ATOMIC_ADD(&GDK_mallocedbytes_estimate, _memdelta)
#ifndef NDEBUG
#define heapdec(_memdelta)							\
	do {								\
		ATOMIC_BASE_TYPE old = ATOMIC_SUB(&GDK_mallocedbytes_estimate, _memdelta); \
		assert(old >= (ATOMIC_BASE_TYPE) _memdelta);		\
	} while (0)
#else
#define heapdec(_memdelta)						\
	ATOMIC_SUB(&GDK_mallocedbytes_estimate, _memdelta)
#endif

#define meminc(vmdelta)							\
	ATOMIC_ADD(&GDK_vm_cursize, SEG_SIZE(vmdelta))
#ifndef NDEBUG
#define memdec(vmdelta)							\
	do {								\
		ssize_t diff = SEG_SIZE(vmdelta);			\
		ATOMIC_BASE_TYPE old = ATOMIC_SUB(&GDK_vm_cursize, diff); \
		assert(old >= (ATOMIC_BASE_TYPE) diff);			\
	} while (0)
#else
#define memdec(vmdelta)							\
	ATOMIC_SUB(&GDK_vm_cursize, SEG_SIZE(vmdelta))
#endif

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

#if defined(NDEBUG) || defined(SANITIZER)
#define DEBUG_SPACE	0
#else
#define DEBUG_SPACE	16
#endif

/* malloc smaller than this aren't subject to the GDK_vm_maxsize test */
#define SMALL_MALLOC	256

static void *
GDKmalloc_internal(size_t size, bool clear)
{
	void *s;
	size_t nsize;

	assert(size != 0);
#ifndef SIZE_CHECK_IN_HEAPS_ONLY
	if (size > SMALL_MALLOC &&
	    GDKvm_cursize() + size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory\n");
		return NULL;
	}
#endif

	/* pad to multiple of eight bytes and add some extra space to
	 * write real size in front; when debugging, also allocate
	 * extra space for check bytes */
	nsize = (size + 7) & ~7;
	if (clear)
		s = calloc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE, 1);
	else
		s = malloc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	if (s == NULL) {
		GDKsyserror("malloc failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", size, GDKmem_cursize(), GDKvm_cursize());;
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);

	heapinc(nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);

	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#if !defined(NDEBUG) && !defined(SANITIZER)
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

	if ((s = GDKmalloc_internal(size, false)) == NULL)
		return NULL;
#if !defined(NDEBUG) && !defined(SANITIZER)
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
	return GDKmalloc_internal(size, true);
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

	if ((p = GDKmalloc_internal(size, false)) == NULL)
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
	if ((p = GDKmalloc_internal(size + 1, false)) == NULL)
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

#if !defined(NDEBUG) && !defined(SANITIZER)
	size_t *p = s;
	assert((asize & 2) == 0);   /* check against duplicate free */
	size_t size = p[-2];
	assert(((size + 7) & ~7) + MALLOC_EXTRA_SPACE + DEBUG_SPACE == asize);
	/* check for out-of-bounds writes */
	for (size_t i = size; i < asize - MALLOC_EXTRA_SPACE; i++)
		assert(((char *) s)[i] == '\xBD');
	p[-1] |= 2;		/* indicate area is freed */

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
#if !defined(NDEBUG) && !defined(SANITIZER)
	size_t osize;
#endif
	size_t *os = s;

	assert(size != 0);

	if (s == NULL)
		return GDKmalloc(size);

	nsize = (size + 7) & ~7;
	asize = os[-1];		/* how much allocated last */

#ifndef SIZE_CHECK_IN_HEAPS_ONLY
	if (size > SMALL_MALLOC &&
	    nsize > asize &&
	    GDKvm_cursize() + nsize - asize >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("allocating too much memory\n");
		return NULL;
	}
#endif
#if !defined(NDEBUG) && !defined(SANITIZER)
	assert((asize & 2) == 0);   /* check against duplicate free */
	/* check for out-of-bounds writes */
	osize = os[-2];		/* how much asked for last */
	assert(((osize + 7) & ~7) + MALLOC_EXTRA_SPACE + DEBUG_SPACE == asize);
	for (size_t i = osize; i < asize - MALLOC_EXTRA_SPACE; i++)
		assert(((char *) s)[i] == '\xBD');
	/* if shrinking, write debug pattern into to-be-freed memory */
	DEADBEEFCHK if (size < osize)
		memset((char *) s + size, '\xDB', osize - size);
	os[-1] |= 2;		/* indicate area is freed */
#endif
	s = realloc((char *) s - MALLOC_EXTRA_SPACE,
		    nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE);
	if (s == NULL) {
#if !defined(NDEBUG) && !defined(SANITIZER)
		os[-1] &= ~2;	/* not freed after all */
		assert(os[-1] == asize);
		assert(os[-2] == osize);
#endif
		GDKsyserror("realloc failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", size, GDKmem_cursize(), GDKvm_cursize());;
		return NULL;
	}
	s = (void *) ((char *) s + MALLOC_EXTRA_SPACE);
	/* just before the pointer that we return, write how much we
	 * asked of malloc */
	((size_t *) s)[-1] = nsize + MALLOC_EXTRA_SPACE + DEBUG_SPACE;
#if !defined(NDEBUG) && !defined(SANITIZER)
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

/*
 * @- virtual memory
 * allocations affect only the logical VM resources.
 */
#undef GDKmmap
void *
GDKmmap(const char *path, int mode, size_t len)
{
	void *ret;

#ifndef SIZE_CHECK_IN_HEAPS_ONLY
	if (GDKvm_cursize() + len >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("requested too much virtual memory; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", len, GDKmem_cursize(), GDKvm_cursize());
		return NULL;
	}
#endif
	ret = MT_mmap(path, mode, len);
	if (ret != NULL) {
		if (mode & MMAP_COPY)
			heapinc(len);
		else
			meminc(len);
	} else
		GDKerror("requesting virtual memory failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", len, GDKmem_cursize(), GDKvm_cursize());
	return ret;
}

#undef GDKmunmap
gdk_return
GDKmunmap(void *addr, int mode, size_t size)
{
	int ret;

	ret = MT_munmap(addr, size);
	if (ret == 0) {
		if (mode & MMAP_COPY)
			heapdec(size);
		else
			memdec(size);
	}
	return ret == 0 ? GDK_SUCCEED : GDK_FAIL;
}

#undef GDKmremap
void *
GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *ret;

#ifndef SIZE_CHECK_IN_HEAPS_ONLY
	if (*new_size > old_size &&
	    GDKvm_cursize() + *new_size - old_size >= GDK_vm_maxsize &&
	    !MT_thread_override_limits()) {
		GDKerror("requested too much virtual memory; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", *new_size, GDKmem_cursize(), GDKvm_cursize());
		return NULL;
	}
#endif
	ret = MT_mremap(path, mode, old_address, old_size, new_size);
	if (ret != NULL) {
		if (mode & MMAP_COPY) {
			heapdec(old_size);
			heapinc(*new_size);
		} else {
			memdec(old_size);
			meminc(*new_size);
		}
	} else {
		GDKerror("requesting virtual memory failed; memory requested: %zu, memory in use: %zu, virtual memory in use: %zu\n", *new_size, GDKmem_cursize(), GDKvm_cursize());
	}
	return ret;
}

/* print some potentially interesting information */
struct prinfocb {
	struct prinfocb *next;
	void (*func)(void);
} *prinfocb;

void
GDKprintinforegister(void (*func)(void))
{
	struct prinfocb *p = GDKmalloc(sizeof(struct prinfocb));
	if (p == NULL) {
		GDKerror("cannot register USR1 printing function.\n");
		return;
	}
	p->func = func;
	p->next = NULL;
	struct prinfocb **pp = &prinfocb;
	while (*pp != NULL)
		pp = &(*pp)->next;
	*pp = p;
}

void
GDKprintinfo(void)
{
	size_t allocated = (size_t) ATOMIC_GET(&GDK_mallocedbytes_estimate);
	size_t vmallocated = (size_t) ATOMIC_GET(&GDK_vm_cursize);

	printf("SIGUSR1 info start\n");
	printf("Virtual memory allocated: %zu, of which %zu with malloc\n",
	       vmallocated + allocated, allocated);
#ifdef WITH_MALLOC
#ifdef WITH_JEMALLOC
	size_t jeallocated = 0, jeactive = 0, jemapped = 0, jeresident = 0, jeretained = 0;
	if (mallctl("stats.allocated", &jeallocated, &(size_t){sizeof(jeallocated)}, NULL, 0) == 0 &&
	    mallctl("stats.active", &jeactive, &(size_t){sizeof(jeactive)}, NULL, 0) == 0 &&
	    mallctl("stats.mapped", &jemapped, &(size_t){sizeof(jemapped)}, NULL, 0) == 0 &&
	    mallctl("stats.resident", &jeresident, &(size_t){sizeof(jeresident)}, NULL, 0) == 0 &&
	    mallctl("stats.retained", &jeretained, &(size_t){sizeof(jeretained)}, NULL, 0) == 0)
		printf("JEmalloc: allocated %zu, active %zu, mapped %zu, resident %zu, retained %zu\n", jeallocated, jeactive, jemapped, jeresident, jeretained);
#endif
#elif defined(HAVE_MALLINFO2)
	struct mallinfo2 mi = mallinfo2();
	printf("mallinfo: arena %zu, ordblks %zu, smblks %zu, hblks %zu, hblkhd %zu, fsmblks %zu, uordblks %zu, fordblks %zu, keepcost %zu\n",
	       mi.arena, mi.ordblks, mi.smblks, mi.hblks, mi.hblkhd, mi.fsmblks, mi.uordblks, mi.fordblks, mi.keepcost);
	printf("   total allocated (arena+hblkhd): %zu\n", mi.arena + mi.hblkhd);
#endif
	printf("gdk_vm_maxsize: %zu, gdk_mem_maxsize: %zu\n",
	       GDK_vm_maxsize, GDK_mem_maxsize);
	printf("gdk_mmap_minsize_persistent %zu, gdk_mmap_minsize_transient %zu\n",
	       GDK_mmap_minsize_persistent, GDK_mmap_minsize_transient);
#ifdef __linux__
	int fd = open("/proc/self/statm", O_RDONLY | O_CLOEXEC);
	if (fd >= 0) {
		char buf[512];
		ssize_t s = read(fd, buf, sizeof(buf) - 1);
		close(fd);
		if (s > 0) {
			assert((size_t) s < sizeof(buf));
			size_t size, resident, shared;
			buf[s] = 0;
			if (sscanf(buf, "%zu %zu %zu", &size, &resident, &shared) == 3) {
				size *= MT_pagesize();
				resident *= MT_pagesize();
				shared *= MT_pagesize();
				printf("Virtual size: %zu, anonymous RSS: %zu, shared RSS: %zu (together: %zu)\n",
				       size, resident - shared, shared, resident);
			}
		}
	}
#endif
	BBPprintinfo();
#ifdef LOCK_STATS
	GDKlockstatistics(3);
#endif
	dump_threads();
	for (struct prinfocb *p = prinfocb; p; p = p->next)
		(*p->func)();
	printf("SIGUSR1 info end\n");
}

void (*GDKtriggerusr1)(void);
void
GDKusr1triggerCB(void (*func)(void))
{
	GDKtriggerusr1 = func;
}

exception_buffer *
eb_init(exception_buffer *eb)
{
	if (eb) {
		eb->enabled = 0;
		eb->code = 0;
		eb->msg = NULL;
	}
	return eb;
}

void
eb_error(exception_buffer *eb, const char *msg, int val)
{
	eb->code = val;
	eb->msg = msg;
	eb->enabled = 0;			/* not any longer... */
#ifdef HAVE_SIGLONGJMP
	siglongjmp(eb->state, eb->code);
#else
	longjmp(eb->state, eb->code);
#endif
}

#define SA_BLOCK (128*1024)

typedef struct freed_t {
	struct freed_t *n;
	size_t sz;
} freed_t;

static void
sa_destroy_freelist(freed_t *f)
{
	while(f) {
		freed_t *n = f->n;
		GDKfree(f);
		f = n;
	}
}

static void
sa_free(allocator *pa, void *blk)
{
	assert(!pa->pa);
	size_t i;

	for(i = 0; i < pa->nr; i++) {
		if (pa->blks[i] == blk)
			break;
	}
	assert (i < pa->nr);
	for (; i < pa->nr-1; i++)
		pa->blks[i] = pa->blks[i+1];
	pa->nr--;

	size_t sz = GDKmallocated(blk);
	if (sz > (SA_BLOCK + 32)) {
		GDKfree(blk);
	} else {
		freed_t *f = blk;
		f->n = pa->freelist;
		f->sz = sz;

		pa->freelist = f;
	}
}

static void *
sa_use_freed(allocator *pa, size_t sz)
{
	(void)sz;

	freed_t *f = pa->freelist;
	pa->freelist = f->n;
	return f;
}

#define round16(sz) ((sz+15)&~15)

allocator *
sa_create(allocator *pa)
{
	const size_t initial_blks_size = 1024;
	// The above choice only consumes only 8 of the 128 kB of the first
	// block but allows for up to 128 MB of memory to be allocated before a
	// blks reallocation is needed. This is useful because blks reallocations
	// consume memory from the parent allocator that currently will not be reused
	// until the parent allocator itself is destroyed.

	char *first_block = pa?(char*)sa_alloc(pa, SA_BLOCK):(char*)GDKmalloc(SA_BLOCK);
	if (first_block == NULL)
		return NULL;

	// The start of the first block holds our bookkeeping.
	// First our blks array, until that needs to be reallocated.
	// Then the allocator struct itself.
	//
	// It's important that the blks come first so we can easily
	// check if the blks have been reallocated by comparing pointers.
	size_t reserved = 0;
	char **blks = (char**)first_block;
	reserved += round16(sizeof(*blks) * initial_blks_size);
	allocator *sa = (allocator*)(first_block + reserved);
	reserved += round16(sizeof(allocator));

	eb_init(&sa->eb);
	sa->pa = pa;
	sa->size = initial_blks_size;
	sa->nr = 1;
	sa->blks = blks;
	sa->blks[0] = first_block;
	sa->first_block = first_block;
	sa->used = reserved;
	sa->reserved = sa->used;
	sa->freelist = NULL;
	sa->usedmem = SA_BLOCK;

	return sa;
}

allocator *
sa_reset(allocator *sa)
{
	for (size_t i = 0; i < sa->nr; i++) {
		char *blk = sa->blks[i];
		if (blk == sa->first_block) {
			// sa_alloc sometimes shuffles the blocks around,
			// move first_block back to the start and don't deallocate it
			// because it holds our bookkeeping.
			sa->blks[i] = sa->blks[0];
			sa->blks[0] = sa->first_block;
		}
		else {
			// Discard all other blocks.
			if (!sa->pa)
				GDKfree(blk);
			else
				sa_free(sa->pa, blk);
		}
	}
	sa->nr = 1;
	sa->used = sa->reserved;
	sa->usedmem = SA_BLOCK;
	return sa;
}

#undef sa_realloc
#undef sa_alloc
void *
sa_realloc(allocator *sa, void *p, size_t sz, size_t oldsz)
{
	void *r = sa_alloc(sa, sz);

	if (r)
		memcpy(r, p, oldsz);
	return r;
}

void *
sa_alloc(allocator *sa, size_t sz)
{
	char *r;
	sz = round16(sz);
	if (sz > (SA_BLOCK-sa->used)) {
		if (sa->pa)
			r = (char*)sa_alloc(sa->pa, (sz > SA_BLOCK ? sz : SA_BLOCK));
		else if (sz <= SA_BLOCK && sa->freelist) {
			r = sa_use_freed(sa, SA_BLOCK);
		} else
			r = GDKmalloc(sz > SA_BLOCK ? sz : SA_BLOCK);
		if (r == NULL) {
			if (sa->eb.enabled)
				eb_error(&sa->eb, "out of memory", 1000);
			return NULL;
		}
		if (sa->nr >= sa->size) {
			char **tmp;
			size_t osz = sa->size;
			sa->size *=2;
			// Here we rely on the initial blks being at the start of the first block
			if ((char*)sa->blks == sa->first_block) {
			        tmp = sa->pa?(char**)sa_alloc(sa->pa, sizeof(char*) * sa->size):(char**)GDKmalloc(sizeof(char*) * sa->size);
				if (tmp != NULL)
					memcpy(tmp, sa->blks, osz * sizeof(char*));
			} else if (sa->pa) {
				tmp = (char**)sa_realloc(sa->pa, sa->blks, sizeof(char*) * sa->size, sizeof(char*) * osz);
			} else {
				tmp = GDKrealloc(sa->blks, sizeof(char*) * sa->size);
			}
			if (tmp == NULL) {
				sa->size /= 2; /* undo */
				if (sa->eb.enabled)
					eb_error(&sa->eb, "out of memory", 1000);
				if (!sa->pa)
					GDKfree(r);
				return NULL;
			}
			sa->blks = tmp;
		}
		if (sz >= SA_BLOCK) {
			// The request is large so it gets its own block.
			// We put it 'under' the current block because
			// there may still be plenty of usable space there.
			sa->blks[sa->nr] = sa->blks[sa->nr-1];
			sa->blks[sa->nr-1] = r;
			sa->nr ++;
			sa->usedmem += sz;
		} else {
			sa->blks[sa->nr] = r;
			sa->nr ++;
			sa->used = sz;
			sa->usedmem += SA_BLOCK;
		}
	} else {
		r = sa->blks[sa->nr-1] + sa->used;
		sa->used += sz;
	}
	return r;
}

#undef sa_zalloc
void *
sa_zalloc(allocator *sa, size_t sz)
{
	void *r = sa_alloc(sa, sz);

	if (r)
		memset(r, 0, sz);
	return r;
}

void
sa_destroy(allocator *sa)
{
	sa_reset(sa);
	sa_destroy_freelist(sa->freelist);

	// Here we rely on the initial blks being at the start of the first block
	if ((char*)sa->blks != sa->first_block) {
		if (sa->pa == NULL)
			GDKfree(sa->blks);
	}


	// now we know first_block is all that's left.
	if (sa->pa)
		sa_free(sa->pa, sa->first_block);
	else
		GDKfree(sa->first_block);
}

#undef sa_strndup
char *
sa_strndup(allocator *sa, const char *s, size_t l)
{
	char *r = sa_alloc(sa, l+1);

	if (r) {
		memcpy(r, s, l);
		r[l] = 0;
	}
	return r;
}

#undef sa_strdup
char *
sa_strdup(allocator *sa, const char *s)
{
	return sa_strndup(sa, s, strlen(s));
}

char *
sa_strconcat(allocator *sa, const char *s1, const char *s2)
{
	size_t l1 = strlen(s1);
	size_t l2 = strlen(s2);
	char *r = sa_alloc(sa, l1+l2+1);

	if (l1)
		memcpy(r, s1, l1);
	if (l2)
		memcpy(r+l1, s2, l2);
	r[l1+l2] = 0;
	return r;
}

size_t
sa_size(allocator *sa)
{
	return sa->usedmem;
}

exception_buffer *
sa_get_eb(allocator *sa)
{
	return &sa->eb;
}
