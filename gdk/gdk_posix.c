/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
#include "gdk.h"		/* includes gdk_posix.h */
#include "gdk_private.h"
#include "mutils.h"
#include <unistd.h>
#include <string.h>     /* strncpy */

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
#if defined(HAVE_KVM_H)
# include <kvm.h>
# include <sys/param.h>
# include <sys/sysctl.h>
# include <sys/user.h>
#endif

#if defined(__GNUC__) && defined(HAVE_VALGRIND)
#include <valgrind.h>
#else
#define VALGRIND_MALLOCLIKE_BLOCK(addr, sizeB, rzB, is_zeroed)
#define VALGRIND_FREELIKE_BLOCK(addr, rzB)
#define VALGRIND_RESIZEINPLACE_BLOCK(addr, oldSizeB, newSizeB, rzB)
#endif

#ifndef MAP_NORESERVE
# define MAP_NORESERVE		MAP_PRIVATE
#endif
#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
#define MAP_ANONYMOUS		MAP_ANON
#endif

#define MMAP_ADVISE		7
#define MMAP_WRITABLE		(MMAP_WRITE|MMAP_COPY)

#ifndef O_CLOEXEC
#ifdef _O_NOINHERIT
#define O_CLOEXEC _O_NOINHERIT	/* Windows */
#else
#define O_CLOEXEC 0
#endif
#endif

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
}

/* return RSS in bytes */
size_t
MT_getrss(void)
{
#if defined(HAVE_PROCFS_H) && defined(__sun__)
	/* retrieve RSS the Solaris way (2.6+) */
	int fd;
	psinfo_t psbuff;

	fd = open("/proc/self/psinfo", O_RDONLY | O_CLOEXEC);
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
#elif defined(HAVE_KVM_H)
	/* get RSS on FreeBSD and NetBSD */
	struct kinfo_proc *ki;
	int ski = 1;
	kvm_t *kd;
	size_t rss = 0;

	kd = kvm_open(NULL, "/dev/null", NULL, O_RDONLY, "kvm_open");
	if (kd != NULL) {
		ki = kvm_getprocs(kd, KERN_PROC_PID, getpid(), &ski);
		if (ki != NULL) {
#ifdef __NetBSD__		/* should we use configure for this? */
			/* see bug 3217 */
			rss = ki->kp_eproc.e_vm.vm_rssize;
#else
			rss = ki->ki_rssize;
#endif
			kvm_close(kd);

			return rss * MT_pagesize();
		} else {
			kvm_close(kd);
		}
	}
#elif defined(__linux__)
	/* get RSS on Linux */
	int fd;

	fd = open("/proc/self/stat", O_RDONLY | O_CLOEXEC);
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

void *
MT_mmap(const char *path, int mode, size_t len)
{
	int fd;
	void *ret;

	fd = open(path, O_CREAT | ((mode & MMAP_WRITE) ? O_RDWR : O_RDONLY) | O_CLOEXEC, MONETDB_MODE);
	if (fd < 0) {
		GDKsyserror("open %s failed\n", path);
		return MAP_FAILED;
	}
	ret = mmap(NULL,
		   len,
		   ((mode & MMAP_WRITABLE) ? PROT_WRITE : 0) | PROT_READ,
		   (mode & MMAP_COPY) ? (MAP_PRIVATE | MAP_NORESERVE) : MAP_SHARED,
		   fd,
		   0);
	if (ret == MAP_FAILED) {
		GDKsyserror("mmap(%s,%zu) failed\n", path, len);
		ret = NULL;
	}
	close(fd);
	VALGRIND_MALLOCLIKE_BLOCK(ret, len, 0, 1);
	return ret;
}

int
MT_munmap(void *p, size_t len)
{
	int ret = munmap(p, len);

	if (ret < 0)
		GDKsyserror("munmap(%p,%zu) failed\n", p, len);
	VALGRIND_FREELIKE_BLOCK(p, 0);
	return ret;
}

/* expand or shrink a memory map (ala realloc).
 * the address returned may be different from the address going in.
 * in case of failure, the old address is still mapped and NULL is returned.
 */
void *
MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *p;
	int fd = -1;
	int flags = mode & MMAP_COPY ? MAP_PRIVATE : MAP_SHARED;
	int prot = PROT_WRITE | PROT_READ;

	/* round up to multiple of page size */
	*new_size = (*new_size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);

	/* doesn't make sense for us to extend read-only memory map */
	assert(mode & MMAP_WRITABLE);

	if (*new_size < old_size) {
#ifndef __COVERITY__	/* hide this from static code analyzer */
		/* shrink */
		VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
		if (munmap((char *) old_address + *new_size,
			   old_size - *new_size) < 0) {
			GDKsyserror("MT_mremap(%s,%p,%zu,%zu): munmap() failed\n", path?path:"NULL", old_address, old_size, *new_size);
			/* even though the system call failed, we
			 * don't need to propagate the error up: the
			 * address should still work in the same way
			 * as it did before */
			return old_address;
		}
		if (path && truncate(path, *new_size) < 0)
			TRC_WARNING(GDK, "MT_mremap(%s): truncate failed: %s\n",
				    path, GDKstrerror(errno, (char[64]){0}, 64));
#endif	/* !__COVERITY__ */
		return old_address;
	}
	if (*new_size == old_size) {
		/* do nothing */
		return old_address;
	}

	if (!(mode & MMAP_COPY) && path != NULL) {
		/* "normal" memory map */

		if ((fd = open(path, O_RDWR | O_CLOEXEC)) < 0) {
			GDKsyserror("MT_mremap(%s,%p,%zu,%zu): open failed\n",
				    path, old_address, old_size, *new_size);
			return NULL;
		}
		if (GDKextendf(fd, *new_size, path) != GDK_SUCCEED) {
			close(fd);
			TRC_ERROR(GDK, "MT_mremap(%s,%p,%zu,%zu): GDKextendf() failed\n", path, old_address, old_size, *new_size);
			return NULL;
		}
#ifdef HAVE_MREMAP
		/* on Linux it's easy */
		p = mremap(old_address, old_size, *new_size, MREMAP_MAYMOVE);
#ifdef HAVE_VALGRIND
		if (p != MAP_FAILED) {
			if (p == old_address) {
				VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
			} else {
				VALGRIND_FREELIKE_BLOCK(old_address, 0);
				VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
			}
		}
#endif
#else
		/* try to map extension at end of current map */
		p = mmap((char *) old_address + old_size, *new_size - old_size,
			 prot, flags, fd, old_size);
		/* if it failed, there is no point trying a full mmap:
		 * that too won't fit */
		if (p != MAP_FAILED) {
			if (p == (char *) old_address + old_size) {
				/* we got the requested address, make
				 * sure we return the correct (old)
				 * address */
				VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
				p = old_address;
			} else {
				/* we got some other address: discard
				 * it and make full mmap */
				if (munmap(p, *new_size - old_size) < 0)
					GDKsyserror("munmap");
#ifdef NO_MMAP_ALIASING
				if (msync(old_address, old_size, MS_SYNC) < 0)
					GDKsyserror("msync");
#endif
				/* first create full mmap, then, if
				 * successful, remove old mmap */
				p = mmap(NULL, *new_size, prot, flags, fd, 0);
				if (p != MAP_FAILED) {
					VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
					if (munmap(old_address, old_size) < 0)
						GDKsyserror("munmap");
					VALGRIND_FREELIKE_BLOCK(old_address, 0);
				}
			}
		}
#endif	/* HAVE_MREMAP */
	} else {
		/* "copy-on-write" or "anonymous" memory map */
#ifdef MAP_ANONYMOUS
		flags |= MAP_ANONYMOUS;
#else
		if ((fd = open("/dev/zero", O_RDWR | O_CLOEXEC)) < 0) {
			GDKsyserror("MT_mremap(%s,%p,%zu,%zu): "
				    "open('/dev/zero') failed\n",
				    path ? path : "NULL", old_address,
				    old_size, *new_size);
			return NULL;
		}
#endif
		/* try to map an anonymous area as extent to the
		 * current map */
		p = mmap((char *) old_address + old_size, *new_size - old_size,
			 prot, flags, fd, 0);
		/* no point trying a full map if this didn't work:
		 * there isn't enough space */
		if (p != MAP_FAILED) {
			if (p == (char *) old_address + old_size) {
				/* we got the requested address, make
				 * sure we return the correct (old)
				 * address */
				VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
				p = old_address;
			} else {
				/* we got some other address: discard
				 * it and make full mmap */
				if (munmap(p, *new_size - old_size) < 0)
					GDKsyserror("munmap");
#ifdef HAVE_MREMAP
				/* first get an area large enough for
				 * *new_size */
				p = mmap(NULL, *new_size, prot, flags, fd, 0);
				if (p != MAP_FAILED) {
					/* then overlay old mmap over new */
					void *q;

					q = mremap(old_address, old_size,
						   old_size,
						   MREMAP_FIXED | MREMAP_MAYMOVE,
						   p);
					assert(q == p || q == MAP_FAILED);
					if (q == MAP_FAILED) {
						int e = errno;
						/* we didn't expect this... */
						if (munmap(p, *new_size) < 0)
							GDKsyserror("munmap");
						p = MAP_FAILED;
						errno = e;
					}
#ifdef HAVE_VALGRIND
					else {
						VALGRIND_FREELIKE_BLOCK(old_size, 0);
						VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
					}
#endif
				}
#else
				p = MAP_FAILED;
				if (path == NULL ||
				    *new_size <= GDK_mmap_minsize_persistent) {
					/* size not too big yet or
					 * anonymous, try to make new
					 * anonymous mmap and copy
					 * data over */
					p = mmap(NULL, *new_size, prot, flags,
						 fd, 0);
					if (p != MAP_FAILED) {
						VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 0);
						memcpy(p, old_address,
						       old_size);
						munmap(old_address, old_size);
						VALGRIND_FREELIKE_BLOCK(old_address, 0);
					}
					/* if it failed, try alternative */
				}
				if (p == MAP_FAILED && path != NULL) {
					/* write data to disk, then
					 * mmap it to new address */
					if (fd >= 0)
						close(fd);
					fd = -1;
					p = malloc(strlen(path) + 5);
					if (p == NULL){
						GDKsyserror("MT_mremap(%s,%p,%zu,%zu): fd < 0\n", path, old_address, old_size, *new_size);
						return NULL;
					}

					strcat(strcpy(p, path), ".tmp");
					fd = open(p, O_RDWR | O_CREAT | O_CLOEXEC,
						  MONETDB_MODE);
					if (fd < 0) {
						GDKsyserror("MT_mremap(%s,%p,%zu,%zu): fd < 0\n", path, old_address, old_size, *new_size);
						free(p);
						return NULL;
					}
					free(p);
					if (write(fd, old_address,
						  old_size) < 0 ||
#ifdef HAVE_FALLOCATE
					    /* prefer Linux-specific
					     * fallocate over standard
					     * posix_fallocate, since
					     * glibc uses a rather
					     * slow method of
					     * allocating the file if
					     * the file system doesn't
					     * support the operation,
					     * we just use ftruncate
					     * in that case */
					    (fallocate(fd, 0, (off_t) old_size, (off_t) *new_size - (off_t) old_size) < 0 && (errno != EOPNOTSUPP || ftruncate(fd, (off_t) *new_size) < 0))
#else
#ifdef HAVE_POSIX_FALLOCATE
					    /* posix_fallocate returns
					     * error number on
					     * failure, not -1, and if
					     * it returns EINVAL, the
					     * underlying file system
					     * may not support the
					     * operation, so we then
					     * need to try
					     * ftruncate */
					    ((errno = posix_fallocate(fd, (off_t) old_size, (off_t) *new_size - (off_t) old_size)) == EINVAL ? ftruncate(fd, (off_t) *new_size) < 0 : errno != 0)
#else
					    ftruncate(fd, (off_t) *new_size) < 0
#endif
#endif
						) {
						GDKsyserror("MT_mremap(%s,%p,%zu,%zu): write() or "
#ifdef HAVE_FALLOCATE
							    "fallocate()"
#else
#ifdef HAVE_POSIX_FALLOCATE
							    "posix_fallocate()"
#else
							    "ftruncate()"
#endif
#endif
							    " failed\n", path, old_address, old_size, *new_size);
						/* extending failed:
						 * free any disk space
						 * allocated in the
						 * process */
						if (ftruncate(fd, (off_t) old_size) < 0)
							GDKsyserror("MT_mremap(%s,%p,%zu,%zu): ftruncate() failed\n", path, old_address, old_size, *new_size);
						close(fd);
						return NULL;
					}
					p = mmap(NULL, *new_size, prot, flags,
						 fd, 0);
					if (p != MAP_FAILED) {
						VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
						munmap(old_address, old_size);
						VALGRIND_FREELIKE_BLOCK(old_address, 0);
					}
				}
#endif	/* HAVE_MREMAP */
			}
		}
	}
	if (p == MAP_FAILED)
		GDKsyserror("MT_mremap(%s,%p,%zu,%zu): p == MAP_FAILED\n", path?path:"NULL", old_address, old_size, *new_size);
	if (fd >= 0)
		close(fd);
	return p == MAP_FAILED ? NULL : p;
}

int
MT_msync(void *p, size_t len)
{
	int ret = msync(p, len, MS_SYNC);

	if (ret < 0)
		GDKsyserror("msync failed\n");
	return ret;
}

bool
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
	(void)library; /* Not used because of MacOs not handling dlopen on linked library */
	return dlopen(NULL, mode);
}

#else /* WIN32 native */

#ifndef BUFSIZ
#define BUFSIZ 1024
#endif

#undef _errno

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

void
MT_init_posix(void)
{
	SetUnhandledExceptionFilter(MT_ignore_exceptions);
}

size_t
MT_getrss(void)
{
	PROCESS_MEMORY_COUNTERS ctr;
	if (GetProcessMemoryInfo(GetCurrentProcess(), &ctr, sizeof(ctr)))
		return ctr.WorkingSetSize;
	return 0;
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
	wchar_t *wpath = utf8towchar(path);
	if (wpath == NULL)
		return NULL;

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
	mode2 |= FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;
	sa.nLength = sizeof(SECURITY_ATTRIBUTES);
	sa.bInheritHandle = TRUE;
	sa.lpSecurityDescriptor = 0;

	h1 = CreateFileW(wpath, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
	if (h1 == INVALID_HANDLE_VALUE) {
		(void) SetFileAttributesW(wpath, FILE_ATTRIBUTE_NORMAL);
		h1 = CreateFileW(wpath, mode0, mode1, &sa, OPEN_ALWAYS, mode2, NULL);
		if (h1 == INVALID_HANDLE_VALUE) {
			free(wpath);
			GDKwinerror("CreateFile('%s', %lu, %lu, &sa, %lu, %lu, NULL) failed\n",
				    path, (unsigned long) mode0, (unsigned long) mode1, (unsigned long) OPEN_ALWAYS, (unsigned long) mode2);
			return NULL;
		}
	}
	free(wpath);

	h2 = CreateFileMapping(h1, &sa, mode3, (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)), (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)), NULL);
	if (h2 == NULL) {
		GDKwinerror("CreateFileMapping(%p, &sa, %lu, %lu, %lu, NULL) failed\n",
			    h1, (unsigned long) mode3,
			    (unsigned long) (((unsigned __int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)),
			    (unsigned long) (len & LL_CONSTANT(0xFFFFFFFF)));
		CloseHandle(h1);
		return NULL;
	}
	CloseHandle(h1);

	ret = MapViewOfFileEx(h2, mode4, (DWORD) 0, (DWORD) 0, len, NULL);
	if (ret == NULL)
		errno = winerror(GetLastError());
	CloseHandle(h2);

	return ret;
}

int
MT_munmap(void *p, size_t dummy)
{
	int ret;

	(void) dummy;
	/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
	 * while Unix's   munmap          returns success==0, error==-1. */
	ret = UnmapViewOfFile(p);
	if (ret == 0) {
		GDKwinerror("UnmapViewOfFile failed\n");
		return -1;
	}
	return 0;
}

void *
MT_mremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *p;

	/* doesn't make sense for us to extend read-only memory map */
	assert(mode & MMAP_WRITABLE);

	/* round up to multiple of page size */
	*new_size = (*new_size + GDK_mmap_pagesize - 1) & ~(GDK_mmap_pagesize - 1);

	if (old_size >= *new_size) {
		*new_size = old_size;
		return old_address;	/* don't bother shrinking */
	}
	if (GDKextend(path, *new_size) != GDK_SUCCEED) {
		TRC_ERROR(GDK, "MT_mremap(%s,%p,%zu,%zu): GDKextend() failed\n", path?path:"NULL", old_address, old_size, *new_size);
		return NULL;
	}
	if (path && !(mode & MMAP_COPY))
		MT_munmap(old_address, old_size);
	p = MT_mmap(path, mode, *new_size);
	if (p != NULL && (path == NULL || (mode & MMAP_COPY))) {
		memcpy(p, old_address, old_size);
		MT_munmap(old_address, old_size);
	}

	if (p == NULL)
		TRC_ERROR(GDK, "MT_mremap(%s,%p,%zu,%zu): p == NULL\n", path?path:"NULL", old_address, old_size, *new_size);
	return p;
}

int
MT_msync(void *p, size_t len)
{
	int ret;

	/*       Windows' FlushViewOfFile returns success!=0, error== 0,
	 * while Unix's   munmap          returns success==0, error==-1. */
	ret = FlushViewOfFile(p, len);
	if (ret == 0) {
		GDKwinerror("FlushViewOfFile failed\n");
		return -1;
	}
	return 0;
}

bool
MT_path_absolute(const char *pathname)
{
	/* drive letter, colon, directory separator */
	return (((('a' <= pathname[0] && pathname[0] <= 'z') ||
		  ('A' <= pathname[0] && pathname[0] <= 'Z')) &&
		 pathname[1] == ':' &&
		 (pathname[2] == '/' || pathname[2] == '\\')) ||
		(pathname[0] == '\\')); // && pathname[1] == '\\'));
}

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
mdlopen(const char *file, int mode)
{
	return dlopen(file, mode);
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
#endif

void
MT_sleep_ms(unsigned int ms)
{
#ifdef NATIVE_WIN32
	Sleep(ms);
#else
#ifdef HAVE_NANOSLEEP
	(void) nanosleep(&(struct timespec) {.tv_sec = ms / 1000,
				.tv_nsec = ms == 1 ? 1000 : (long) (ms % 1000) * 1000000,},
		NULL);
#else
	(void) select(0, NULL, NULL, NULL,
		      &(struct timeval) {.tv_sec = ms / 1000,
				      .tv_usec = ms == 1 ? 1 : (ms % 1000) * 1000,});
#endif
#endif
}

#if !defined(HAVE_LOCALTIME_R) || !defined(HAVE_GMTIME_R) || !defined(HAVE_ASCTIME_R) || !defined(HAVE_CTIME_R)
static MT_Lock timelock = MT_LOCK_INITIALIZER(timelock);
#endif

#ifndef HAVE_LOCALTIME_R
struct tm *
localtime_r(const time_t *restrict timep, struct tm *restrict result)
{
	struct tm *tmp;
	MT_lock_set(&timelock);
	tmp = localtime(timep);
	if (tmp)
		*result = *tmp;
	MT_lock_unset(&timelock);
	return tmp ? result : NULL;
}
#endif

#ifndef HAVE_GMTIME_R
struct tm *
gmtime_r(const time_t *restrict timep, struct tm *restrict result)
{
	struct tm *tmp;
	MT_lock_set(&timelock);
	tmp = gmtime(timep);
	if (tmp)
		*result = *tmp;
	MT_lock_unset(&timelock);
	return tmp ? result : NULL;
}
#endif

#ifndef HAVE_ASCTIME_R
char *
asctime_r(const struct tm *restrict tm, char *restrict buf)
{
	char *tmp;
	MT_lock_set(&timelock);
	tmp = asctime(tm);
	if (tmp)
		strcpy(buf, tmp);
	MT_lock_unset(&timelock);
	return tmp ? buf : NULL;
}
#endif

#ifndef HAVE_CTIME_R
char *
ctime_r(const time_t *restrict t, char *restrict buf)
{
	char *tmp;
	MT_lock_set(&timelock);
	tmp = ctime(t);
	if (tmp)
		strcpy(buf, tmp);
	MT_lock_unset(&timelock);
	return tmp ? buf : NULL;
}
#endif

#ifndef HAVE_STRERROR_R
static MT_Lock strerrlock = MT_LOCK_INITIALIZER(strerrlock);

int
strerror_r(int errnum, char *buf, size_t buflen)
{
	char *msg;
	MT_lock_set(&strerrlock);
	msg = strerror(errnum);
	strcpy_len(buf, msg, buflen);
	MT_lock_unset(&strerrlock);
	return 0;
}
#endif
