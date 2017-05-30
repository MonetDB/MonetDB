/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include <stdio.h>
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
#if defined(HAVE_KVM_H) && defined(HAVE_SYS_SYSCTL_H)
# include <kvm.h>
# include <sys/param.h>
# include <sys/sysctl.h>
# include <sys/user.h>
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


void *
MT_mmap(const char *path, int mode, size_t len)
{
	int fd;
	void *ret;

	fd = open(path, O_CREAT | ((mode & MMAP_WRITE) ? O_RDWR : O_RDONLY), MONETDB_MODE);
	if (fd < 0) {
		GDKsyserror("MT_mmap: open %s failed\n", path);
		return MAP_FAILED;
	}
	ret = mmap(NULL,
		   len,
		   ((mode & MMAP_WRITABLE) ? PROT_WRITE : 0) | PROT_READ,
		   (mode & MMAP_COPY) ? (MAP_PRIVATE | MAP_NORESERVE) : MAP_SHARED,
		   fd,
		   0);
	if (ret == MAP_FAILED) {
		GDKsyserror("MT_mmap: mmap(%s,"SZFMT") failed\n", path, len);
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
		GDKsyserror("MT_munmap: munmap(" PTRFMT "," SZFMT ") failed\n",
			    PTRFMTCAST p, len);
	VALGRIND_FREELIKE_BLOCK(p, 0);
#ifdef MMAP_DEBUG
	fprintf(stderr, "#munmap(" PTRFMT "," SZFMT ") = %d\n", PTRFMTCAST p, len, ret);
#endif
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
#ifndef STATIC_CODE_ANALYSIS	/* hide this from static code analyzer */
		/* shrink */
		VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
		if (munmap((char *) old_address + *new_size,
			   old_size - *new_size) < 0) {
			GDKsyserror("MT_mremap: munmap("PTRFMT","SZFMT") failed\n",
				    PTRFMTCAST ((char *) old_address + *new_size),
				    old_size - *new_size);
			fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): munmap() failed\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
			/* even though the system call failed, we
			 * don't need to propagate the error up: the
			 * address should still work in the same way
			 * as it did before */
			return old_address;
		}
		if (path && truncate(path, *new_size) < 0)
			fprintf(stderr, "#MT_mremap(%s): truncate failed\n", path);
#ifdef MMAP_DEBUG
		fprintf(stderr, "MT_mremap(%s,"PTRFMT","SZFMT","SZFMT") -> shrinking\n", path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
#endif
#endif	/* !STATIC_CODE_ANALYSIS */
		return old_address;
	}
	if (*new_size == old_size) {
		/* do nothing */
#ifdef MMAP_DEBUG
		fprintf(stderr, "MT_mremap(%s,"PTRFMT","SZFMT","SZFMT") -> unchanged\n", path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
#endif
		return old_address;
	}

	if (!(mode & MMAP_COPY) && path != NULL) {
		/* "normal" memory map */

		if ((fd = open(path, O_RDWR)) < 0) {
			GDKsyserror("MT_mremap: open(%s) failed\n", path);
			fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): open() failed\n", __FILE__, __LINE__, path, PTRFMTCAST old_address, old_size, *new_size);
			return NULL;
		}
		if (GDKextendf(fd, *new_size, path) != GDK_SUCCEED) {
			close(fd);
			fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): GDKextendf() failed\n", __FILE__, __LINE__, path, PTRFMTCAST old_address, old_size, *new_size);
			return NULL;
		}
#ifdef HAVE_MREMAP
		/* on Linux it's easy */
		p = mremap(old_address, old_size, *new_size, MREMAP_MAYMOVE);
		if (p == MAP_FAILED)
			GDKsyserror("MT_mremap: mremap("PTRFMT","SZFMT","SZFMT") failed\n",
				    PTRFMTCAST old_address, old_size,
				    *new_size);
#ifdef HAVE_VALGRIND
		if (p == old_address) {
			VALGRIND_RESIZEINPLACE_BLOCK(old_address, old_size, *new_size, 0);
		} else {
			VALGRIND_FREELIKE_BLOCK(old_address, 0);
			VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
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
				munmap(p, *new_size - old_size);
#ifdef NO_MMAP_ALIASING
				msync(old_address, old_size, MS_SYNC);
#endif
				/* first create full mmap, then, if
				 * successful, remove old mmap */
				p = mmap(NULL, *new_size, prot, flags, fd, 0);
				if (p != MAP_FAILED) {
					VALGRIND_MALLOCLIKE_BLOCK(p, *new_size, 0, 1);
					munmap(old_address, old_size);
					VALGRIND_FREELIKE_BLOCK(old_address, 0);
				}
			}
		}
		if (p == MAP_FAILED)
			GDKsyserror("MT_mremap: mmap failed\n");
#endif	/* HAVE_MREMAP */
		close(fd);
	} else {
		/* "copy-on-write" or "anonymous" memory map */
#ifdef MAP_ANONYMOUS
		flags |= MAP_ANONYMOUS;
#else
		if ((fd = open("/dev/zero", O_RDWR)) < 0) {
			GDKsyserror("MT_mremap: open(/dev/zero) failed\n");
			fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): open('/dev/zero') failed\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
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
				munmap(p, *new_size - old_size);
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
						/* we didn't expect this... */
						munmap(p, *new_size);
						p = MAP_FAILED;
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
#ifdef HAVE_POSIX_FALLOCATE
					int rt;
#endif
					/* write data to disk, then
					 * mmap it to new address */
					if (fd >= 0)
						close(fd);
					p = malloc(strlen(path) + 5);
					if (p == NULL){
						GDKsyserror("MT_mremap: malloc() failed\n");
						fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): fd < 0\n", __FILE__, __LINE__, path, PTRFMTCAST old_address, old_size, *new_size);
						return NULL;
					}

					strcat(strcpy(p, path), ".tmp");
					fd = open(p, O_RDWR | O_CREAT,
						  MONETDB_MODE);
					if (fd < 0) {
						GDKsyserror("MT_mremap: open(%s) failed\n", (char *) p);
						free(p);
						fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): fd < 0\n", __FILE__, __LINE__, path, PTRFMTCAST old_address, old_size, *new_size);
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
					    ((rt = posix_fallocate(fd, (off_t) old_size, (off_t) *new_size - (off_t) old_size)) == EINVAL ? ftruncate(fd, (off_t) *new_size) < 0 : rt != 0)
#else
					    ftruncate(fd, (off_t) *new_size) < 0
#endif
#endif
						) {
						int err = errno;
						/* extending failed:
						 * free any disk space
						 * allocated in the
						 * process */
						(void) ftruncate(fd, (off_t) old_size);
						errno = err; /* restore for error message */
						GDKsyserror("MT_mremap: growing file failed\n");
						close(fd);
						fprintf(stderr,
							"= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): write() or "
#ifdef HAVE_FALLOCATE
							"fallocate()"
#else
#ifdef HAVE_POSIX_FALLOCATE
							"posix_fallocate()"
#else
							"ftruncate()"
#endif
#endif
							" failed\n", __FILE__, __LINE__, path, PTRFMTCAST old_address, old_size, *new_size);
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
		if (p == MAP_FAILED)
			GDKsyserror("MT_mremap: mmap failed\n");
		if (fd >= 0)
			close(fd);
	}
#ifdef MMAP_DEBUG
	fprintf(stderr, "MT_mremap(%s,"PTRFMT","SZFMT","SZFMT") -> "PTRFMT"%s\n", path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size, PTRFMTCAST p, path && mode & MMAP_COPY ? " private" : "");
#endif
	if (p == MAP_FAILED)
		fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): p == MAP_FAILED\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
	return p == MAP_FAILED ? NULL : p;
}

int
MT_msync(void *p, size_t len)
{
	int ret = msync(p, len, MS_SYNC);

	if (ret < 0)
		GDKsyserror("MT_msync: msync failed\n");
#ifdef MMAP_DEBUG
	fprintf(stderr,
		     "#msync(" PTRFMT "," SZFMT ",MS_SYNC) = %d\n",
		     PTRFMTCAST p, len, ret);
#endif
	return ret;
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

void
MT_init_posix(void)
{
	SetUnhandledExceptionFilter(MT_ignore_exceptions);
}

size_t
MT_getrss(void)
{
#ifdef _WIN64
	PROCESS_MEMORY_COUNTERS ctr;
	if (GetProcessMemoryInfo(GetCurrentProcess(), &ctr, sizeof(ctr)))
		return ctr.WorkingSetSize;
#endif
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
			errno = winerror(GetLastError());
			GDKsyserror("MT_mmap: CreateFile('%s', %lu, %lu, &sa, %lu, %lu, NULL) failed\n",
				    path, mode0, mode1, (DWORD) OPEN_ALWAYS, mode2);
			return NULL;
		}
	}

	h2 = CreateFileMapping(h1, &sa, mode3, (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)), (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)), NULL);
	if (h2 == NULL) {
		errno = winerror(GetLastError());
		GDKsyserror("MT_mmap: CreateFileMapping(" PTRFMT ", &sa, %lu, %lu, %lu, NULL) failed\n",
			    PTRFMTCAST h1, mode3,
			    (DWORD) (((__int64) len >> 32) & LL_CONSTANT(0xFFFFFFFF)),
			    (DWORD) (len & LL_CONSTANT(0xFFFFFFFF)));
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
		errno = winerror(GetLastError());
		GDKsyserror("MT_munmap failed\n");
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
		fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): GDKextend() failed\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
		return NULL;
	}
	if (path && !(mode & MMAP_COPY))
		MT_munmap(old_address, old_size);
	p = MT_mmap(path, mode, *new_size);
	if (p != NULL && (path == NULL || (mode & MMAP_COPY))) {
		memcpy(p, old_address, old_size);
		MT_munmap(old_address, old_size);
	}
#ifdef MMAP_DEBUG
	fprintf(stderr, "MT_mremap(%s,"PTRFMT","SZFMT","SZFMT") -> "PTRFMT"\n", path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size, PTRFMTCAST p);
#endif
	if (p == NULL)
		fprintf(stderr, "= %s:%d: MT_mremap(%s,"PTRFMT","SZFMT","SZFMT"): p == NULL\n", __FILE__, __LINE__, path?path:"NULL", PTRFMTCAST old_address, old_size, *new_size);
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
		errno = winerror(GetLastError());
		GDKsyserror("MT_msync: FlushViewOfFile failed\n");
		return -1;
	}
	return 0;
}

int
MT_path_absolute(const char *pathname)
{
	/* drive letter, colon, directory separator */
	return (((('a' <= pathname[0] && pathname[0] <= 'z') ||
		  ('A' <= pathname[0] && pathname[0] <= 'Z')) &&
		 pathname[1] == ':' &&
		 (pathname[2] == '/' || pathname[2] == '\\')) ||
		(pathname[0] == '\\' && pathname[1] == '\\'));
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

/* dir manipulations fail in WIN32 if file name contains trailing
 * slashes; work around this */
static char *
reduce_dir_name(const char *src, char *dst, size_t cap)
{
	size_t len = strlen(src);
	char *buf = dst;

	if (len >= cap)
		buf = malloc(len + 1);
	if (buf == NULL)
		return NULL;
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
		IODEBUG fprintf(stderr, "retry rmdir %s\n", pathname);
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
		IODEBUG fprintf(stderr, "retry unlink %s\n", pathname);
		MT_sleep_ms(100);	/* wait a little */
		ret = _unlink(pathname);
	}
	return ret;
}

#undef rename
int
win_rename(const char *old, const char *dst)
{
	int ret;

	ret = rename(old, dst);
	if (ret == 0 || (ret < 0 && errno == ENOENT))
		return ret;
	if (ret < 0 && errno == EEXIST) {
		(void) win_unlink(dst);
		ret = rename(old, dst);
	}

	if (ret < 0 && errno != ENOENT) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG fprintf(stderr, "#retry rename %s %s\n", old, dst);
		MT_sleep_ms(100);	/* wait a little */
		ret = rename(old, dst);
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
