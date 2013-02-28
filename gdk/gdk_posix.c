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
 * @a Niels Nes, Peter Boncz
 * @* System Independent Layer
 *
 * GDK is built on Posix. Exceptions are made for memory mapped files
 * and anonymous virtual memory, for which somewhat higher-level
 * functions are defined here.  Most of this file concerns itself with
 * emulation of Posix functionality on the WIN32 native platform.
 */
#include "monetdb_config.h"
#include "gdk.h"        /* includes gdk_posix.h */
#include "gdk_private.h"
#include "mutils.h"
#include <stdio.h>
#include <unistd.h>		/* sbrk on Solaris */
#include <string.h>     /* strncpy */

#ifdef __hpux
extern char *sbrk(int);
#endif

#ifdef HAVE_FCNTL_H
# include <fcntl.h>
#endif
#ifdef HAVE_PROCFS_H
# include <procfs.h>
#endif
#ifdef HAVE_MACH_TASK_H
# include <mach/task.h>
#endif
#ifdef HAVE_MACH_MACH_INIT_H
# include <mach/mach_init.h>
#endif
#if defined(HAVE_KVM_H) && defined(HAVE_SYS_SYSCTL_H)
# include <kvm.h>
# include <sys/param.h>
# include <sys/sysctl.h>
# include <sys/user.h>
#endif

#if defined(DEBUG_ALLOC) && SIZEOF_VOID_P > 4
#undef DEBUG_ALLOC
#endif

#ifdef WIN32
int GDK_mem_pagebits = 16;	/* on windows, the mmap addresses can be set by the 64KB */
#else
int GDK_mem_pagebits = 14;	/* on linux, 4KB pages can be addressed (but we use 16KB) */
#endif

#ifndef MAP_NORESERVE
# define MAP_NORESERVE		MAP_PRIVATE
#endif

#define MMAP_ADVISE		7
#define MMAP_WRITABLE		(MMAP_WRITE|MMAP_COPY)

/* DDALERT: AIX4.X 64bits needs HAVE_SETENV==0 due to a AIX bug, but
 * it probably isn't detected so by configure */

#ifndef HAVE_SETENV
int
setenv(const char *name, const char *value, int overwrite)
{
	int ret = 0;

	if (overwrite || getenv(name) == NULL) {
		char *p = (char *) GDKmalloc(2 + strlen(name) + strlen(value));

		if (p == NULL)
			return -1;
		strcpy(p, name);
		strcat(p, "=");
		strcat(p, value);
		ret = putenv(p);
		/* GDKfree(p); LEAK INSERTED DUE TO SOME WEIRD CRASHES */
	}
	return ret;
}
#endif

char *MT_heapbase = NULL;


/* Crude VM buffer management that keep a list of all memory mapped
 * regions.
 *
 * a.k.a. "helping stupid VM implementations that ignore VM advice"
 *
 * The main goal is to be able to tell the OS to please stop buffering
 * all memory mapped pages when under pressure. A major problem is
 * materialization of large results in newly created memory mapped
 * files. Operating systems tend to cache all dirty pages, such that
 * when memory is out, all pages are dirty and cannot be unloaded
 * quickly. The VM panic occurs and comatose OS states may be
 * observed.  This is in spite of our use of
 * madvise(MADV_SEQUENTIAL). That is; we would want that the OS drops
 * pages after we've passed them. That does not happen; pages are
 * retained and pollute the buffer cache.
 *
 * Regrettably, at this level, we don't know anything about how Monet
 * is using the mmapped regions. Monet code is totally oblivious of
 * any I/O; that's why it is so easy to create CPU efficient code in
 * Monet.
 *
 * The current solution focuses on large writable maps. These often
 * represent newly created BATs, that are the result of some (running)
 * operator. We assume two things here:
 * - the BAT is created in sequential fashion (always almost true)
 * - afterwards, this BAT is used in sequential fashion (often true)
 *
 * A VMtrim thread keeps an eye on the RSS (memory pressure) and large
 * writable memory maps. If RSS approaches mem_maxsize(), it starts to
 * *worry*, and starts to write dirty data from these writable maps to
 * disk in 128MB tiles. So, if memory pressure rises further in the
 * near future, the OS has some option to release memory pages cheaply
 * (i.e. without needing I/O). This is also done explicitly by the
 * VM-thread: when RSS exceeds mem_maxsize() is explicitly asks the OS
 * to release pages.  The reason is that Linux is not smart enough to
 * do even this. Anyway..
 *
 * The way to free pages explicitly in Linux is to call
 * posix_fadvise(..,MADV_DONTNEED).  Particularly,
 * posix_madvise(..,POSIX_MADV_DONTNEED) which is supported and
 * documented doesn't work on Linux. But we do both posix_madvise and
 * posix_fadvise, so on other unix systems that don't support
 * posix_fadvise, posix_madvise still might work.  On Windows, to our
 * knowledge, there is no way to tell it stop buffering a memory
 * mapped region. msync (FlushViewOfFile) does work, though. So let's
 * hope the VM paging algorithm behaves better than Linux which just
 * runs off the cliff and if MonetDB does not prevent RSS from being
 * too high, enters coma.
 *
 * We will only be able to sensibly test this on Windows64. On
 * Windows32, mmap sizes do not significantly exceed RAM sizes so
 * MonetDB swapping actually will not happen (of course, you've got
 * this nasty problem of VM fragemntation and failing mmaps instead).
 *
 * In principle, page tiles are saved sequentially, and behind it, but
 * never overtaking it, is an "unload-cursor" that frees the pages if
 * that is needed to keep RSS down.  There is a tweak in the
 * algorithm, that re-sets the unload-cursor if it seems that all
 * tiles to the end have been saved (whether a tile is actually saved
 * is determined by timing the sync action). This means that the
 * producing operator is ready creating the BAT, and we assume it is
 * going to be used sequentially afterwards.  In that case, we should
 * start unloading right after the 'read-cursor', that is, from the
 * start.
 *
 * EXAMPLE
 * D = dirty tile
 * s = saved tile (i.e. clean)
 * u = unloaded tile
 * L = tile that is being loaded
 *
 *           +--> operator produces  BAT
 * (1) DDDDDD|......................................| end of reserved mmap
 *                      ____|RSS
 *                     |
 *                     | at 3/4 of RSS consumed we start to worry
 *                     +--> operator produces BAT
 * (2) DDDDDDDDDDDDDDDD|............................|
 *                    s<----------------------------- VM backwards save thread
 *                    |
 *                    + first tile of which saving costs anything
 *
 *                        +--> operator produces BAT
 * (3) DDDDDDDDDDDDDDDss|D|.........................|
 *     VM-thread save ->|
 *
 * When the RSS target is exceeded, we start unloading tiles..
 *
 *                     +-->  VM-thread unload starts at *second* 's'
 *                     |
 *                     |    +--> operator produces BAT
 * (4) DDDDDDDDDDDDDDDsus|DD|........................|
 *     VM-thread save -->|  | RSS = Full!
 *
 *                                  +-- 0 => save costs nothing!!
 *     VM-thread save ------------->|        assume bat complete
 * (5) DDDDDDDDDDDDDDDsuuuuuuuuussss0................|
 *                    |<-------- re-set unload cursor
 *                    +--- first tile was not unloaded.
 *
 * later.. some other operator sequentially reads the bat
 * first part is 'D', that is, nicely cached.
 *
 *     ---read------->|
 * (6) DDDDDDDDDDDDDDDsuuuuuuuuussss0................|
 *
 * now we're hitting the unloaded region. the query becomes
 * I/O read bound here (typically 20% CPU utilization).
 *
 *     ---read-------->|
 * (7) DDDDDDDDDDDDDDDuLuuuuuuuussss0................|
 *                   /  \
 *      unload cursor    load cursor
 *
 *     ---read---------------->|
 * (8) DDDDDDDDDDDDDDDuuuuuuuuuLssss0................|
 *                           /  \
 *              unload cursor    load cursor
 *
 *     ---read--------------------->| done
 * (9) DDDDDDDDDDDDDDDuuuuuuuuuLssss0................|
 *                              ****
 *                              last part still cached
 *
 * note: if we would not have re-setted the unload cursor (5)
 *       the last part would have been lost due to continuing
 *       RSS pressure from the 'L' read-cursor.
 *
 * If multiple write-mmaps exist, we do unload-tile and save-tile
 * selection on a round-robin basis among them.
 *
 * Of course, this is a simple solution for simple cases only.
 * (a) if the bat is produced too fast, (or your disk is too slow)
 *     RSS will exceeds its limit and Linux will go into swapping.
 * (b) if your data is not produced and read sequentially.
 *     Examples are sorting or clustering on huge datasets.
 * (c) if RSS pressure is due to large read-maps, rather than
 *     intermediate results.
 *
 * Two crude suggestions:
 * - If we are under RSS pressure without unloadable tiles and with
 *   savable tiles, we should consider suspending *all* other threads
 *   until we manage to unload a tile.
 * - if there are no savable tiles (or in case of read-only maps)
 *   we could resort to saving and unloading random tiles.
 *
 * To do better, our BAT algorithms should provide even more detailed
 * advice on their access patterns, which may even consist of pointers
 * to the cursors (i.e. pointers to b->batBuns->free or the cursors
 * in radix-cluster), which an enhanced version of this thread might
 * take into account.
 *
 * [Kersten] The memory map table should be aligned to the number of
 * mapped files. In more recent applications, such as the SkyServer
 * this may be around 2000 BATs easily.
 */

#ifdef HAVE_PTHREAD_H
/* pthread.h on Windows includes config.h if HAVE_CONFIG_H is set */
#undef HAVE_CONFIG_H
#include <sched.h>
#include <pthread.h>
#endif
#ifdef HAVE_SEMAPHORE_H
#include <semaphore.h>
#endif

#ifndef NATIVE_WIN32
#ifdef HAVE_POSIX_FADVISE
#ifdef HAVE_UNAME
#include <sys/utsname.h>
#endif
#endif

void
MT_init_posix(void)
{
	MT_heapbase = (char *) sbrk(0);
}

/* return RSS in bytes */
size_t
MT_getrss(void)
{
#if defined(HAVE_PROCFS_H) && defined(__sun__)
	/* retrieve RSS the Solaris way (2.6+) */
	int fd;
	psinfo_t psbuff;

	fd = open("/proc/self/psinfo", O_RDONLY);
	if (fd >= 0) {
		if (read(fd, &psbuff, sizeof(psbuff)) == sizeof(psbuff)) {
			close(fd);
			return psbuff.pr_rssize * 1024;
		}
		close(fd);
	}
#elif defined(HAVE_TASK_INFO)
	/* Darwin/MACH call for process' RSS */
	task_t task = mach_task_self();
	struct task_basic_info_64 t_info;
	mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_64_COUNT;

	if (task_info(task, TASK_BASIC_INFO_64, (task_info_t)&t_info, &t_info_count) != KERN_INVALID_POLICY)
		return t_info.resident_size;  /* bytes */
#elif defined(HAVE_KVM_H) && defined(HAVE_SYS_SYSCTL_H)
	/* get RSS on FreeBSD and NetBSD */
	struct kinfo_proc *ki;
	int ski = 1;
	kvm_t *kd;
	size_t rss = 0;

	kd = kvm_open(NULL, "/dev/null", NULL, O_RDONLY, "kvm_open");
	if (kd == NULL)
		return 0;

	ki = kvm_getprocs(kd, KERN_PROC_PID, getpid(), &ski);
	if (ki == NULL) {
		kvm_close(kd);
		return 0;
	}

#ifdef __NetBSD__		/* should we use configure for this? */
	/* see bug 3217 */
	rss = ki->kp_eproc.e_vm.vm_rssize;
#else
	rss = ki->ki_rssize;
#endif

	kvm_close(kd);

	return rss * MT_pagesize();
#elif defined(__linux__)
	/* get RSS on Linux */
	int fd;

	fd = open("/proc/self/stat", O_RDONLY);
	if (fd >= 0) {
		char buf[1024], *r = buf;
		ssize_t i, sz = read(fd, buf, 1024);

		close(fd);
		if (sz > 0) {
			for (i = 0; i < 23; i++) {
				while (*r && (*r == ' ' || *r == '\t'))
					r++;
				while (*r && (*r != ' ' && *r != '\t'))
					r++;
			}
			while (*r && (*r == ' ' || *r == '\t'))
				r++;
			return ((size_t) atol(r)) * MT_pagesize();
		}
	}
#endif
	return 0;
}


char *
MT_heapcur(void)
{
	return (char *) sbrk(0);
}

void *
MT_mmap(const char *path, int mode, size_t len)
{
	int fd = open(path, O_CREAT | ((mode & MMAP_WRITE) ? O_RDWR : O_RDONLY), MONETDB_MODE);
	void *ret = (void *) -1L;

	if (fd >= 0) {
		ret = mmap(NULL,
			   len,
			   ((mode & MMAP_WRITABLE) ? PROT_WRITE : 0) | PROT_READ,
			   (mode & MMAP_COPY) ? (MAP_PRIVATE | MAP_NORESERVE) : MAP_SHARED,
			   fd,
			   0);
		close(fd);
	}
	return ret;
}

int
MT_munmap(void *p, size_t len)
{
	int ret = munmap(p, len);

#ifdef MMAP_DEBUG
	mnstr_printf(GDKstdout, "#munmap(" LLFMT "," LLFMT ",%d) = %d\n", (long long) p, (long long) len, ret);
#endif
	return ret;
}

int
MT_msync(void *p, size_t off, size_t len, int mode)
{
	int ret = msync(((char *) p) + off, len, (mode & MMAP_SYNC) ? MS_SYNC : ((mode & MMAP_ASYNC) ? MS_ASYNC : MS_INVALIDATE));

#ifdef MMAP_DEBUG
	mnstr_printf(GDKstdout,
		      "#msync(" LLFMT "," LLFMT ",%s) = %d\n",
		      (long long) p, (long long) len,
		      (mode & MMAP_SYNC) ? "MS_SYNC" : ((mode & MMAP_ASYNC) ? "MS_ASYNC" : "MS_INVALIDATE"),
		      ret);
#endif
	if (ret < 0)
		return errno;
	return ret;
}

struct Mallinfo
MT_mallinfo(void)
{
	struct Mallinfo _ret;

#ifdef HAVE_USEFUL_MALLINFO
	struct mallinfo m;

	m = mallinfo();
	_ret.arena = m.arena;
	_ret.ordblks = m.ordblks;
	_ret.smblks = m.smblks;
	_ret.hblks = m.hblks;
	_ret.hblkhd = m.hblkhd;
	_ret.usmblks = m.usmblks;
	_ret.fsmblks = m.fsmblks;
	_ret.uordblks = m.uordblks;
	_ret.fordblks = m.fordblks;
	_ret.keepcost = m.keepcost;
#else
	memset(&_ret, 0, sizeof(_ret));
#endif
	return _ret;
}

int
MT_path_absolute(const char *pathname)
{
	return (*pathname == DIR_SEP);
}

#ifdef HAVE_DLFCN_H
# include <dlfcn.h>
#endif

void *
mdlopen(const char *library, int mode)
{
	(void) library;
	return dlopen(NULL, mode);
}

#ifdef WIN32
#include <windows.h>
#endif

#else /* WIN32 native */

#ifndef BUFSIZ
#define BUFSIZ 1024
#endif

#undef _errno
#undef stat
#undef rmdir
#undef mkdir

#include <windows.h>

#ifdef _MSC_VER
#include <io.h>
#endif /* _MSC_VER */
#include <Psapi.h>

#define MT_SMALLBLOCK 256

static LONG WINAPI
MT_ignore_exceptions(struct _EXCEPTION_POINTERS *ExceptionInfo)
{
	(void) ExceptionInfo;
	return EXCEPTION_EXECUTE_HANDLER;
}

static pthread_mutex_t MT_mmap_lock;

void
MT_init_posix(void)
{
	MT_heapbase = 0;
	SetUnhandledExceptionFilter(MT_ignore_exceptions);
	pthread_mutex_init(&MT_mmap_lock, 0);
}

size_t
MT_getrss(void)
{
	PROCESS_MEMORY_COUNTERS ctr;

	if (GetProcessMemoryInfo(GetCurrentProcess(), &ctr, sizeof(ctr)))
		return ctr.WorkingSetSize;
	return 0;
}

char *
MT_heapcur(void)
{
	return (char *) 0;
}

/* Windows mmap keeps a global list of base addresses for complex
 * (remapped) memory maps the reason is that each remapped segment
 * needs to be unmapped separately in the end. */

void *
MT_mmap(const char *path, int mode, size_t len)
{
	DWORD mode0 = FILE_READ_ATTRIBUTES | FILE_READ_DATA;
	DWORD mode1 = FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD mode2 = mode & MMAP_ADVISE;
	DWORD mode3 = PAGE_READONLY;
	int mode4 = FILE_MAP_READ;
	SECURITY_ATTRIBUTES sa;
	HANDLE h1, h2;
	void *ret;

	if (mode & MMAP_WRITE) {
		mode0 |= FILE_APPEND_DATA | FILE_WRITE_ATTRIBUTES | FILE_WRITE_DATA;
	}
	if (mode2 == MMAP_RANDOM || mode2 == MMAP_DONTNEED) {
		mode2 = FILE_FLAG_RANDOM_ACCESS;
	} else if (mode2 == MMAP_SEQUENTIAL || mode2 == MMAP_WILLNEED) {
		mode2 = FILE_FLAG_SEQUENTIAL_SCAN;
	} else {
		mode2 = FILE_FLAG_NO_BUFFERING;
	}
	if (mode & MMAP_SYNC) {
		mode2 |= FILE_FLAG_WRITE_THROUGH;
	}
	if (mode & MMAP_COPY) {
		mode3 = PAGE_WRITECOPY;
		mode4 = FILE_MAP_COPY;
	} else if (mode & MMAP_WRITE) {
		mode3 = PAGE_READWRITE;
		mode4 = FILE_MAP_WRITE;
	}
	sa.nLength = sizeof(SECURITY_ATTRIBUTES);
	sa.bInheritHandle = TRUE;
	sa.lpSecurityDescriptor = 0;

	h1 = CreateFile(path, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
	if (h1 == INVALID_HANDLE_VALUE) {
		(void) SetFileAttributes(path, FILE_ATTRIBUTE_NORMAL);
		h1 = CreateFile(path, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
		if (h1 == INVALID_HANDLE_VALUE) {
			GDKsyserror("MT_mmap: CreateFile('%s', %lu, %lu, &sa, %lu, %lu, NULL) failed\n",
				    path, mode0, mode1, (DWORD) OPEN_ALWAYS, mode2);
			return (void *) -1;
		}
	}

	h2 = CreateFileMapping(h1, &sa, mode3, (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)), (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)), NULL);
	if (h2 == NULL) {
		GDKsyserror("MT_mmap: CreateFileMapping(" PTRFMT ", &sa, %lu, %lu, %lu, NULL) failed\n",
			    PTRFMTCAST h1, mode3,
			    (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)),
			    (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)));
		CloseHandle(h1);
		return (void *) -1;
	}
	CloseHandle(h1);

	ret = MapViewOfFileEx(h2, mode4, (DWORD) 0, (DWORD) 0, len, NULL);
	CloseHandle(h2);

	return ret ? ret : (void *) -1;
}

int
MT_munmap(void *p, size_t dummy)
{
	int ret = 0;

	(void) dummy;
	/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
	 * while Unix's   munmap          returns success==0, error==-1. */
	return -(UnmapViewOfFile(p) == 0);
}

int
MT_msync(void *p, size_t off, size_t len, int mode)
{
	int ret = 0;

	(void) mode;
	/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
	 * while Unix's   munmap          returns success==0, error==-1. */
	return -(FlushViewOfFile(((char *) p) + off, len) == 0);
}

#ifndef _HEAPOK			/* MinGW */
#define _HEAPEMPTY      (-1)
#define _HEAPOK         (-2)
#define _HEAPBADBEGIN   (-3)
#define _HEAPBADNODE    (-4)
#define _HEAPEND        (-5)
#define _HEAPBADPTR     (-6)
#endif

struct Mallinfo
MT_mallinfo(void)
{
	struct Mallinfo _ret;
	_HEAPINFO hinfo;
	int heapstatus;

	hinfo._pentry = NULL;
	memset(&_ret, 0, sizeof(_ret));

	while ((heapstatus = _heapwalk(&hinfo)) == _HEAPOK) {
		_ret.arena += hinfo._size;
		if (hinfo._size > MT_SMALLBLOCK) {
			_ret.smblks++;
			if (hinfo._useflag == _USEDENTRY) {
				_ret.usmblks += hinfo._size;
			} else {
				_ret.fsmblks += hinfo._size;
			}
		} else {
			_ret.ordblks++;
			if (hinfo._useflag == _USEDENTRY) {
				_ret.uordblks += hinfo._size;
			} else {
				_ret.fordblks += hinfo._size;
			}
		}
	}
	if (heapstatus == _HEAPBADPTR || heapstatus == _HEAPBADBEGIN || heapstatus == _HEAPBADNODE) {

		mnstr_printf(GDKstdout, "#mallinfo(): heap is corrupt.");
	}
	_heapmin();
	return _ret;
}

int
MT_path_absolute(const char *pathname)
{
	char *drive_end = strchr(pathname, ':');
	char *path_start = strchr(pathname, '\\');

	if (path_start == NULL) {
		return 0;
	}
	return (path_start == pathname || drive_end == (path_start - 1));
}


#ifndef HAVE_FTRUNCATE
int
ftruncate(int fd, off_t size)
{
	HANDLE hfile;
	unsigned int curpos;

	if (fd < 0)
		return -1;

	hfile = (HANDLE) _get_osfhandle(fd);
	curpos = SetFilePointer(hfile, 0, NULL, FILE_CURRENT);
	if (curpos == 0xFFFFFFFF ||
	    SetFilePointer(hfile, (LONG) size, NULL, FILE_BEGIN) == 0xFFFFFFFF ||
	    !SetEndOfFile(hfile)) {
		int error = GetLastError();

		if (error && error != ERROR_INVALID_HANDLE)
			SetLastError(ERROR_OPEN_FAILED);	/* enforce EIO */
		return -1;
	}

	return 0;
}
#endif

#ifndef HAVE_GETTIMEOFDAY
static int nodays[12] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

#define LEAPYEAR(y) ((((y)%4)==0 && ((y)%100)!=0) || ((y)%400)==0)
#define NODAYS(m,y) (((m)!=2)?nodays[(m)-1]:LEAPYEAR(y)?29:28)

int
gettimeofday(struct timeval *tv, int *ignore_zone)
{
	unsigned int year, day, month;
	SYSTEMTIME st;

	(void) ignore_zone;
	GetSystemTime(&st);
	day = 0;
	for (year = 1970; year < st.wYear; year++)
		day += LEAPYEAR(year) ? 366 : 365;

	for (month = 1; month < st.wMonth; month++)
		day += NODAYS(month, st.wYear);

	day += st.wDay;
	tv->tv_sec = 60 * (day * 24 * 60 + st.wMinute) + st.wSecond;
	tv->tv_usec = 1000 * st.wMilliseconds;
	return 0;
}
#endif

void *
mdlopen(const char *library, int mode)
{
	(void) mode;
	return GetModuleHandle(library);
}

void *
dlopen(const char *file, int mode)
{
	(void) mode;
	if (file != NULL) {
		return (void *) LoadLibrary(file);
	}
	return GetModuleHandle(NULL);
}

int
dlclose(void *handle)
{
	if (handle != NULL) {
		return FreeLibrary((HINSTANCE) handle);
	}
	return -1;
}

void *
dlsym(void *handle, const char *name)
{
	if (handle != NULL) {
		return (void *) GetProcAddress((HINSTANCE) handle, name);
	}
	return NULL;
}

char *
dlerror(void)
{
	static char msg[1024];

	FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, GetLastError(), 0, msg, sizeof(msg), NULL);
	return msg;
}

/* dir manipulations fail in WIN32 if file name contains trailing
 * slashes; work around this */
static char *
reduce_dir_name(const char *src, char *dst, size_t cap)
{
	size_t len = strlen(src);
	char *buf = dst;

	if (len >= cap)
		buf = malloc(len + 1);
	while (--len > 0 && src[len - 1] != ':' && src[len] == DIR_SEP)
		;
	for (buf[++len] = 0; len > 0; buf[len] = src[len])
		len--;
	return buf;
}

#undef _stat64
int
win_stat(const char *pathname, struct _stat64 *st)
{
	char buf[128], *p = reduce_dir_name(pathname, buf, sizeof(buf));
	int ret = _stat64(p, st);

	if (p != buf)
		free(p);
	return ret;
}

int
win_rmdir(const char *pathname)
{
	char buf[128], *p = reduce_dir_name(pathname, buf, sizeof(buf));
	int ret = _rmdir(p);

	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG THRprintf(GDKstdout, "retry rmdir %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _rmdir(p);
	}
	if (p != buf)
		free(p);
	return ret;
}

int
win_unlink(const char *pathname)
{
	int ret = _unlink(pathname);
	if (ret < 0) {
		/* Vista is paranoid: we cannot delete read-only files
		 * owned by ourselves. Vista somehow also sets these
		 * files to read-only.
		 */
		(void) SetFileAttributes(pathname, FILE_ATTRIBUTE_NORMAL);
		ret = _unlink(pathname);
	}
	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG THRprintf(GDKstdout, "retry unlink %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _unlink(pathname);
	}
	return ret;
}

#undef rename
int
win_rename(const char *old, const char *new)
{
	int ret = rename(old, new);

	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG THRprintf(GDKstdout, "#retry rename %s %s\n", old, new);
		MT_sleep_ms(100);	/* wait a little */
		ret = rename(old, new);
	}
	return ret;
}

int
win_mkdir(const char *pathname, const int mode)
{
	char buf[128], *p = reduce_dir_name(pathname, buf, sizeof(buf));
	int ret = _mkdir(p);

	(void) mode;
	if (p != buf)
		free(p);
	return ret;
}

#if _WIN32_WINNT >= 0x500
/* NTFS does support symbolic links */
int
win_link(const char *oldpath, const char *newpath)
{
	return CreateHardLink(newpath, oldpath, NULL) ? -1 : 0;
}
#endif

typedef struct {
	int w;			/* windows version of error */
	const char *s;		/* text of windows version */
	int e;			/* errno version of error */
} win_errmap_t;

#ifndef EBADRQC
#define EBADRQC 56
#endif
#ifndef ENODATA
#define ENODATA 61
#endif
#ifndef ENONET
#define ENONET 64
#endif
#ifndef ENOTUNIQ
#define ENOTUNIQ 76
#endif
#ifndef ECOMM
#define ECOMM 70
#endif
#ifndef ENOLINK
#define ENOLINK 67
#endif
win_errmap_t win_errmap[] = {
	{ERROR_INVALID_FUNCTION, "ERROR_INVALID_FUNCTION", EBADRQC},
	{ERROR_FILE_NOT_FOUND, "ERROR_FILE_NOT_FOUND", ENOENT},
	{ERROR_PATH_NOT_FOUND, "ERROR_PATH_NOT_FOUND", ENOENT},
	{ERROR_TOO_MANY_OPEN_FILES, "ERROR_TOO_MANY_OPEN_FILES", EMFILE},
	{ERROR_ACCESS_DENIED, "ERROR_ACCESS_DENIED", EACCES},
	{ERROR_INVALID_HANDLE, "ERROR_INVALID_HANDLE", EBADF},
	{ERROR_NOT_ENOUGH_MEMORY, "ERROR_NOT_ENOUGH_MEMORY", ENOMEM},
	{ERROR_INVALID_DATA, "ERROR_INVALID_DATA", EINVAL},
	{ERROR_OUTOFMEMORY, "ERROR_OUTOFMEMORY", ENOMEM},
	{ERROR_INVALID_DRIVE, "ERROR_INVALID_DRIVE", ENODEV},
	{ERROR_NOT_SAME_DEVICE, "ERROR_NOT_SAME_DEVICE", EXDEV},
	{ERROR_NO_MORE_FILES, "ERROR_NO_MORE_FILES", ENFILE},
	{ERROR_WRITE_PROTECT, "ERROR_WRITE_PROTECT", EROFS},
	{ERROR_BAD_UNIT, "ERROR_BAD_UNIT", ENODEV},
	{ERROR_SHARING_VIOLATION, "ERROR_SHARING_VIOLATION", EACCES},
	{ERROR_LOCK_VIOLATION, "ERROR_LOCK_VIOLATION", EACCES},
	{ERROR_SHARING_BUFFER_EXCEEDED, "ERROR_SHARING_BUFFER_EXCEEDED", ENOLCK},
	{ERROR_HANDLE_EOF, "ERROR_HANDLE_EOF", ENODATA},
	{ERROR_HANDLE_DISK_FULL, "ERROR_HANDLE_DISK_FULL", ENOSPC},
	{ERROR_NOT_SUPPORTED, "ERROR_NOT_SUPPORTED", ENOSYS},
	{ERROR_REM_NOT_LIST, "ERROR_REM_NOT_LIST", ENONET},
	{ERROR_DUP_NAME, "ERROR_DUP_NAME", ENOTUNIQ},
	{ERROR_BAD_NETPATH, "ERROR_BAD_NETPATH", ENXIO},
	{ERROR_FILE_EXISTS, "ERROR_FILE_EXISTS", EEXIST},
	{ERROR_CANNOT_MAKE, "ERROR_CANNOT_MAKE", EPERM},
	{ERROR_INVALID_PARAMETER, "ERROR_INVALID_PARAMETER", EINVAL},
	{ERROR_NO_PROC_SLOTS, "ERROR_NO_PROC_SLOTS", EAGAIN},
	{ERROR_BROKEN_PIPE, "ERROR_BROKEN_PIPE", EPIPE},
	{ERROR_OPEN_FAILED, "ERROR_OPEN_FAILED", EIO},
	{ERROR_NO_MORE_SEARCH_HANDLES, "ERROR_NO_MORE_SEARCH_HANDLES", ENFILE},
	{ERROR_CALL_NOT_IMPLEMENTED, "ERROR_CALL_NOT_IMPLEMENTED", ENOSYS},
	{ERROR_INVALID_NAME, "ERROR_INVALID_NAME", ENOENT},
	{ERROR_WAIT_NO_CHILDREN, "ERROR_WAIT_NO_CHILDREN", ECHILD},
	{ERROR_CHILD_NOT_COMPLETE, "ERROR_CHILD_NOT_COMPLETE", EBUSY},
	{ERROR_DIR_NOT_EMPTY, "ERROR_DIR_NOT_EMPTY", ENOTEMPTY},
	{ERROR_SIGNAL_REFUSED, "ERROR_SIGNAL_REFUSED", EIO},
	{ERROR_BAD_PATHNAME, "ERROR_BAD_PATHNAME", EINVAL},
	{ERROR_SIGNAL_PENDING, "ERROR_SIGNAL_PENDING", EBUSY},
	{ERROR_MAX_THRDS_REACHED, "ERROR_MAX_THRDS_REACHED", EAGAIN},
	{ERROR_BUSY, "ERROR_BUSY", EBUSY},
	{ERROR_ALREADY_EXISTS, "ERROR_ALREADY_EXISTS", EEXIST},
	{ERROR_NO_SIGNAL_SENT, "ERROR_NO_SIGNAL_SENT", EIO},
	{ERROR_FILENAME_EXCED_RANGE, "ERROR_FILENAME_EXCED_RANGE", EINVAL},
	{ERROR_META_EXPANSION_TOO_LONG, "ERROR_META_EXPANSION_TOO_LONG", EINVAL},
	{ERROR_INVALID_SIGNAL_NUMBER, "ERROR_INVALID_SIGNAL_NUMBER", EINVAL},
	{ERROR_THREAD_1_INACTIVE, "ERROR_THREAD_1_INACTIVE", EINVAL},
	{ERROR_BAD_PIPE, "ERROR_BAD_PIPE", EINVAL},
	{ERROR_PIPE_BUSY, "ERROR_PIPE_BUSY", EBUSY},
	{ERROR_NO_DATA, "ERROR_NO_DATA", EPIPE},
	{ERROR_PIPE_NOT_CONNECTED, "ERROR_PIPE_NOT_CONNECTED", ECOMM},
	{ERROR_MORE_DATA, "ERROR_MORE_DATA", EAGAIN},
	{ERROR_DIRECTORY, "ERROR_DIRECTORY", EISDIR},
	{ERROR_PIPE_CONNECTED, "ERROR_PIPE_CONNECTED", EBUSY},
	{ERROR_PIPE_LISTENING, "ERROR_PIPE_LISTENING", ECOMM},
	{ERROR_NO_TOKEN, "ERROR_NO_TOKEN", EINVAL},
	{ERROR_PROCESS_ABORTED, "ERROR_PROCESS_ABORTED", EFAULT},
	{ERROR_BAD_DEVICE, "ERROR_BAD_DEVICE", ENODEV},
	{ERROR_BAD_USERNAME, "ERROR_BAD_USERNAME", EINVAL},
	{ERROR_NOT_CONNECTED, "ERROR_NOT_CONNECTED", ENOLINK},
	{ERROR_OPEN_FILES, "ERROR_OPEN_FILES", EAGAIN},
	{ERROR_ACTIVE_CONNECTIONS, "ERROR_ACTIVE_CONNECTIONS", EAGAIN},
	{ERROR_DEVICE_IN_USE, "ERROR_DEVICE_IN_USE", EAGAIN},
	{ERROR_INVALID_AT_INTERRUPT_TIME, "ERROR_INVALID_AT_INTERRUPT_TIME", EINTR},
	{ERROR_IO_DEVICE, "ERROR_IO_DEVICE", EIO},
};

#define GDK_WIN_ERRNO_TLS 13

int *
win_errno(void)
{
	/* get address of thread-local Posix errno; refresh its value
	 * from WIN32 error code */
	int i, err = GetLastError() & 0xff;
	int *result = TlsGetValue(GDK_WIN_ERRNO_TLS);

	if (result == NULL) {
		result = (int *) malloc(sizeof(int));
		*result = 0;
		TlsSetValue(GDK_WIN_ERRNO_TLS, result);
	}
	for (i = 0; win_errmap[i].w != 0; ++i) {
		if (err == win_errmap[i].w) {
			*result = win_errmap[i].e;
			break;
		}
	}
	SetLastError(err);
	return result;
}
#endif

#ifndef WIN32

void
MT_sleep_ms(unsigned int ms)
{
#ifdef HAVE_NANOSLEEP_dont_use
	struct timespec ts;

	ts.tv_sec = (time_t) (ms / 1000);
	ts.tv_nsec = 1000000 * (ms % 1000);
	while (nanosleep(&ts, &ts) == -1 && errno == EINTR)
		;
#else
	struct timeval tv;

	tv.tv_sec = ms / 1000;
	tv.tv_usec = 1000 * (ms % 1000);
	(void) select(0, NULL, NULL, NULL, &tv);
#endif
}

#else /* WIN32 */

void
MT_sleep_ms(unsigned int ms)
{
	Sleep(ms);
}

#endif
