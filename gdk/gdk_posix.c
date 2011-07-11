/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f gdk_posix
 * @a Niels Nes, Peter Boncz
 * @* System Independent Layer
 *
 * GDK is built on Posix. Exceptions are made for memory mapped files and
 * anonymous virtual memory, for which somewhat higher-level functions are
 * defined here.
 * Most of this file concerns itself with emulation of Posix functionality on
 * the WIN32 native platform.
 * @-
 */
#include "monetdb_config.h"
#include "gdk.h"        /* includes gdk_posix.h */
#include "gdk_private.h"
#include "mutils.h"
#include <stdio.h>
#include <unistd.h>		/* sbrk on Solaris */
#include <string.h>     /* strncpy */

#if defined(__hpux)
extern char *sbrk(int);
#endif

#ifdef HAVE_FCNTL_H
# include <fcntl.h>
#endif
#ifdef HAVE_PROCFS_H
# include <procfs.h>
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
# define MAP_NORESERVE 		MAP_PRIVATE
#endif

#define MMAP_ADVISE		7
#define MMAP_WRITABLE		(MMAP_WRITE|MMAP_COPY)

/* DDALERT: AIX4.X 64bits needs HAVE_SETENV==0 due to a AIX bug, but it probably isn't detected so by configure */

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


/* Crude VM buffer management that keep a list of all memory mapped regions.
 *
 * a.k.a. "helping stupid VM implementations that ignore VM advice"
 *
 * The main goal is to be able to tell the OS to please stop buffering all memory
 * mapped pages when under pressure. A major problem is materialization of large
 * results in newly created memory mapped files. Operating systems tend to cache
 * all dirty pages, such that when memory is out, all pages are dirty and cannot
 * be unloaded quickly. The VM panic occurs and comatose OS states may be observed.
 * This is in spite of our use of madvise(MADV_SEQUENTIAL). That is; we would want
 * that the OS drops pages after we've passed them. That does not happen; pages are
 * retained and pollute the buffer cache.
 *
 * Regrettably, at this level, we don't know anything about how Monet is using the
 * mmapped regions. Monet code is totally oblivious of any I/O; that's why it is
 * so easy to create CPU efficient code in Monet.
 *
 * The current solution focuses on large writable maps. These often represent
 * newly created BATs, that are the result of some (running) operator. We
 * assume two things here:
 * - the BAT is created in sequential fashion (always almost true)
 * - afterwards, this BAT is used in sequential fashion (often true)
 *
 * A VMtrim thread keeps an eye on the RSS (memory pressure) and large writable
 * memory maps. If RSS approaches mem_maxsize(), it starts to *worry*, and starts
 * to write dirty data from these writable maps to disk in 128MB tiles. So, if
 * memory pressure rises further in the near future, the OS has some option to release
 * memory pages cheaply (i.e. without needing I/O). This is also done explicitly by the
 * VM-thread: when RSS exceeds mem_maxsize() is explicitly asks the OS to release pages.
 * The reason is that Linux is not smart enough to do even this. Anyway..
 *
 * The way to free pages explicitly in Linux is to call posix_fadvise(..,MADV_DONTNEED).
 * Particularly, posix_madvise(..,POSIX_MADV_DONTNEED) which is supported and documented
 * doesn't work on Linux. But we do both posix_madvise and posix_fadvise, so on other unix
 * systems that don't support posix_fadvise, posix_madvise still might work.
 * On Windows, to our knowledge, there is no way to tell it stop buffering
 * a memory mapped region. msync (FlushViewOfFile) does work, though. So let's
 * hope the VM paging algorithm behaves better than Linux which just runs off
 * the cliff and if MonetDB does not prevent RSS from being too high, enters coma.
 *
 * We will only be able to sensibly test this on Windows64. On Windows32, mmap sizes
 * do not significantly exceed RAM sizes so MonetDB swapping actually will not happen
 * (of course, you've got this nasty problem of VM fragemntation and failing mmaps instead).
 *
 * In principle, page tiles are saved sequentially, and behind it, but never overtaking
 * it, is an "unload-cursor" that frees the pages if that is needed to keep RSS down.
 * There is a tweak in the algorithm, that re-sets the unload-cursor if it seems
 * that all tiles to the end have been saved (whether a tile is actually saved is
 * determined by timing the sync action). This means that the producing operator
 * is ready creating the BAT, and we assume it is going to be used sequentially afterwards.
 * In that case, we should start unloading right after the 'read-cursor', that is,
 * from the start.
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

typedef struct {
	char path[128];		/* mapped file, retained for debugging */
	char *base;		/* base address */
	size_t len;		/* length of map */
	size_t first_tile;	/* from here we started saving tiles */
	size_t save_tile;	/* next tile to save */
	size_t unload_tile;	/* next tile to unload */
	int wrap_tile;		/* unloading has reached end of map? */
	int last_tile;		/* saving has reached end of map? */
	int fd;			/* open fd (==-1 for anon vm), retained to give posix_fadvise */
	int usecnt;		/* number of threads accessing the heap now */
	int random;		/* number of threads accessing the heap randomly now */
	int writable;
	int next;
} MT_mmap_t;

#ifdef HAVE_POSIX_FADVISE
static int do_not_use_posix_fadvise = 0;
#endif

MT_mmap_t MT_mmap_tab[MT_MMAP_BUFSIZE];
int MT_mmap_cur = -1, MT_mmap_busy = -1, MT_mmap_first = -1, MT_mmap_free = 0;

pthread_mutex_t MT_mmap_lock, MT_mmap_relock;

static void
MT_mmap_empty(int i)
{
	MT_mmap_tab[i].path[0] = 0;
	MT_mmap_tab[i].base = NULL;
	MT_mmap_tab[i].len = 0;
	MT_mmap_tab[i].writable = 0;
	MT_mmap_tab[i].fd = -1;
	MT_mmap_tab[i].usecnt = 0;
	MT_mmap_tab[i].random = 0;
}

static void
MT_mmap_init(void)
{
	int i;

	/* create lock */
	pthread_mutex_init(&MT_mmap_lock, 0);
	pthread_mutex_init(&MT_mmap_relock, 0);

	for (i = 0; i < MT_MMAP_BUFSIZE; i++) {
		MT_mmap_tab[i].next = i + 1;
		MT_mmap_empty(i);
	}
	MT_mmap_tab[i - 1].next = -1;
}

/* returns previous element (to facilitate deletion) */
static int
MT_mmap_find(void *base)
{
	/* maybe consider a hash table iso linked list?? */
	int i, prev = MT_MMAP_BUFSIZE;

	for (i = MT_mmap_first; i >= 0; i = MT_mmap_tab[i].next) {
		if (MT_mmap_tab[i].base <= (char *) base && (char *) base < MT_mmap_tab[i].base + MT_mmap_tab[i].len) {
			return prev;
		}
		prev = i;
	}
	return i;
}

static int
MT_mmap_idx(void *base, size_t len)
{
	if (len > MT_MMAP_TILE) {
		int i = MT_mmap_find(base);

		if (i >= 0) {
			if (i == MT_MMAP_BUFSIZE) {
				return MT_mmap_first;
			} else {
				return MT_mmap_tab[i].next;
			}
		}
	}
	return -1;
}

#ifndef NATIVE_WIN32
static int
MT_mmap_new(char *path, void *base, size_t len, int fd, int writable)
{
	(void) pthread_mutex_lock(&MT_mmap_lock);
	if (len > MT_MMAP_TILE && MT_mmap_free >= 0) {
		int i = MT_mmap_free;

		MT_mmap_free = MT_mmap_tab[i].next;
		MT_mmap_tab[i].next = MT_mmap_first;
		MT_mmap_first = i;
		if (MT_mmap_cur == -1)
			MT_mmap_cur = i;
#ifdef MMAP_DEBUG
		mnstr_printf(GDKstdout, "#MT_mmap_new: %s fd=%d\n", path, fd);
#endif
		strncpy(MT_mmap_tab[i].path, path, 128);
		MT_mmap_tab[i].base = base;
		MT_mmap_tab[i].len = len;
		MT_mmap_tab[i].save_tile = 1;
		MT_mmap_tab[i].last_tile = 0;
		MT_mmap_tab[i].wrap_tile = 0;
		MT_mmap_tab[i].first_tile = 0;
		MT_mmap_tab[i].unload_tile = 0;
		MT_mmap_tab[i].writable = writable;
		MT_mmap_tab[i].fd = fd;
		MT_mmap_tab[i].usecnt = 0;
		MT_mmap_tab[i].random = 0;
		fd = -fd;
	}
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	return fd;
}

static void
MT_mmap_del(void *base, size_t len)
{
	int relock = 0;
	while (len > MT_MMAP_TILE) {
		int prev;
		if (relock)
			(void) pthread_mutex_unlock(&MT_mmap_relock);
		(void) pthread_mutex_lock(&MT_mmap_lock);
		prev = MT_mmap_find(base);
		if (prev >= 0) {
			int ret, victim = (prev == MT_MMAP_BUFSIZE) ? MT_mmap_first : MT_mmap_tab[prev].next;
			if (relock == 0 && MT_mmap_busy == victim) {
				/* OOPS, the vmtrim thread is saving a tile of this heap... wait for it to finish with relock */
				(void) pthread_mutex_unlock(&MT_mmap_lock);
				relock = 1;
				continue;
			}
			if (prev == MT_MMAP_BUFSIZE) {
				MT_mmap_first = MT_mmap_tab[MT_mmap_first].next;
			} else {
				MT_mmap_tab[prev].next = MT_mmap_tab[victim].next;
			}
			if (MT_mmap_cur == victim) {
				MT_mmap_cur = MT_mmap_first;
			}
#ifdef HAVE_POSIX_FADVISE
			if (!do_not_use_posix_fadvise && MT_mmap_tab[victim].fd >= 0) {
				/* tell the OS quite clearly that you want to drop this */
				ret = posix_fadvise(MT_mmap_tab[victim].fd, 0LL, MT_mmap_tab[victim].len & ~(MT_pagesize() - 1), POSIX_FADV_DONTNEED);
#ifdef MMAP_DEBUG
				mnstr_printf(GDKstdout,
					      "#MT_mmap_del: posix_fadvise(%s,fd=%d,%uMB,POSIX_FADV_DONTNEED) = %d\n",
					      MT_mmap_tab[victim].path,
					      MT_mmap_tab[victim].fd,
					      (unsigned int) (MT_mmap_tab[victim].len >> 20),
					      ret);
#endif
			}
#endif
			ret = close(MT_mmap_tab[victim].fd);
#ifdef MMAP_DEBUG
			mnstr_printf(GDKstdout,
				      "#MT_mmap_del: close(%s fd=%d) = %d\n",
				      MT_mmap_tab[victim].path,
				      MT_mmap_tab[victim].fd,
				      ret);
#endif
			MT_mmap_tab[victim].next = MT_mmap_free;
			MT_mmap_empty(victim);
			MT_mmap_free = victim;
			(void) ret;
		}
		(void) pthread_mutex_unlock(&MT_mmap_lock);
		if (relock)
			(void) pthread_mutex_unlock(&MT_mmap_relock);
		break;
	}
}

static int
MT_fadvise(void *base, size_t len, int advice)
{
	int ret = 0;

#ifdef HAVE_POSIX_FADVISE
	if (!do_not_use_posix_fadvise) {
		int i;

		(void) pthread_mutex_lock(&MT_mmap_lock);
		i = MT_mmap_idx(base, len);
		if (i >= 0) {
			if (MT_mmap_tab[i].fd >= 0) {
				ret = posix_fadvise(MT_mmap_tab[i].fd, 0, len & ~(MT_pagesize() - 1), advice);
#ifdef MMAP_DEBUG
				mnstr_printf(GDKstdout,
					      "#MT_fadvise: posix_fadvise(%s,fd=%d,%uMB,%d) = %d\n",
					      MT_mmap_tab[i].path,
					      MT_mmap_tab[i].fd,
					      (unsigned int) (len >> 20),
					      advice, ret);
#endif
			}
		}
		(void) pthread_mutex_unlock(&MT_mmap_lock);
	}
#else
	(void) base;
	(void) len;
	(void) advice;
#endif
	return ret;
}

#endif /* NATIVE_WIN32 */


static void
MT_mmap_unload_tile(int i, size_t off, stream *err)
{
	size_t len = MIN((size_t) MT_MMAP_TILE, MT_mmap_tab[i].len - off);
	/* tell Linux to please stop caching this stuff */
	int ret = posix_madvise(MT_mmap_tab[i].base + off, len & ~(MT_pagesize() - 1), POSIX_MADV_DONTNEED);

	if (err) {
		mnstr_printf(err,
			      "#MT_mmap_unload_tile: posix_madvise(%s,off=%uMB,%uMB,fd=%d,POSIX_MADV_DONTNEED) = %d\n",
			      MT_mmap_tab[i].path,
			      (unsigned int) (off >> 20),
			      (unsigned int) (len >> 20),
			      MT_mmap_tab[i].fd,
			      ret);
	}
#ifdef HAVE_POSIX_FADVISE
	if (!do_not_use_posix_fadvise) {
		/* tell the OS quite clearly that you want to drop this */
		ret = posix_fadvise(MT_mmap_tab[i].fd, off, len & ~(MT_pagesize() - 1), POSIX_FADV_DONTNEED);
		if (err) {
			mnstr_printf(err,
				      "#MT_mmap_unload_tile: posix_fadvise(%s,off=%uMB,%uMB,fd=%d,POSIX_MADV_DONTNEED) = %d\n",
				      MT_mmap_tab[i].path,
				      (unsigned int) (off >> 20),
				      (unsigned int) (len >> 20),
				      MT_mmap_tab[i].fd,
				      ret);
		}
	}
#endif
}

static int
MT_mmap_save_tile(int i, size_t tile, stream *err)
{
	int t, ret;
	size_t len = MIN((size_t) MT_MMAP_TILE, MT_mmap_tab[i].len - tile);

	/* save to disk an 128MB tile, and observe how long this takes */
	if (err) {
		mnstr_printf(err,
			      "#MT_mmap_save_tile: msync(%s,off=%uM,%u,SYNC)...\n",
			      MT_mmap_tab[i].path,
			      (unsigned int) (tile >> 20),
			      (unsigned int) (len >> 20));
	}
	MT_mmap_busy = i;
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	t = GDKms();
	ret = MT_msync(MT_mmap_tab[i].base, tile, len, MMAP_SYNC);
	t = GDKms() - t;
	(void) pthread_mutex_lock(&MT_mmap_lock);
	MT_mmap_busy = -1;
	if (err) {
		mnstr_printf(err,
			      "#MT_mmap_save_tile: msync(%s,tile=%uM,%uM,SYNC) = %d (%dms)\n",
			      MT_mmap_tab[i].path,
			      (unsigned int) (tile >> 20),
			      (unsigned int) (len >> 20),
			      ret, t);
	}
	if (t > 200) {
		/* this took time; so we should report back on our
		   actions and await new orders */
		/* note that MT_mmap_lock is already locked by our parent */
		if (MT_mmap_tab[i].save_tile == 1) {
			MT_mmap_tab[i].first_tile = tile;
			/* leave first tile for later sequential use
			   pass (start unloading after it) */
			MT_mmap_tab[i].unload_tile = tile + MT_MMAP_TILE;
		}
		MT_mmap_tab[i].save_tile = tile + MT_MMAP_TILE;
		return 1;
	}
	return 0;
}

/* round-robin next. this is to ensure some fairness if multiple large
   results are produced simultaneously */
static int
MT_mmap_next(int i)
{
	if (i != -1) {
		i = MT_mmap_tab[i].next;
		if (i == -1)
			i = MT_mmap_first;
	}
	return i;
}

int
MT_mmap_trim(size_t target, void *fp)
{
	stream *err = (stream *) fp;
	size_t off, rss = MT_getrss();
	/* worry = 0   if rss < 17/20 target (target = .8RAM)
	 * ----- start mmap sync zone ------------------------
	 *       = 1   17/20target <= rss <18/20 target
	 *       = 2   18/20target <= rss <19/20 target
	 *       = 3   19/20target <= rss <target
	 * ----- start mmap posix_fadvise don'tneed zone -----
	 *       = 4   rss > target
	 */
	int i, worry = (int) MIN(MAX(16, rss * 20 / (size_t) MAX(1, target)) - 16, 4);

	(void) pthread_mutex_lock(&MT_mmap_relock);
	(void) pthread_mutex_lock(&MT_mmap_lock);
	if (err) {
		mnstr_printf(err, "#MT_mmap_trim(%u MB): rss = %u MB\n",
			      (unsigned int) ((target) >> 20),
			      (unsigned int) (rss >> 20));
	}
	assert((MT_mmap_cur == -1) == (MT_mmap_first == -1));	/* either both or neither is -1 */
	/* try to selectively unload pages from the writable regions */
	if (rss > target) {
		size_t delta = ((rss - target) + 4 * MT_MMAP_TILE - 1) & ~(MT_MMAP_TILE - 1);
		int curprio, keepprio, maxprio = 0;

		/* try to unload heap tiles, in order of precedence (usecnt) */
		for (keepprio = 0; keepprio <= maxprio; keepprio++) {
			for (i = MT_mmap_next(MT_mmap_cur); delta && i != MT_mmap_cur; i = MT_mmap_next(i)) {
				assert(i >= 0);

				curprio = MT_mmap_tab[i].usecnt + MT_mmap_tab[i].writable + 4 * MT_mmap_tab[i].random;
				if (maxprio < curprio)
					maxprio = curprio;

				if (MT_mmap_tab[i].fd >= 0 &&
				    curprio == keepprio) {
					size_t lim = (MT_mmap_tab[i].wrap_tile == 1) ? MT_mmap_tab[i].save_tile : MT_mmap_tab[i].len;
					if (MT_mmap_tab[i].unload_tile >= lim) {
						MT_mmap_tab[i].unload_tile = 0;
						MT_mmap_tab[i].wrap_tile++;
					}
					while (MT_mmap_tab[i].unload_tile < lim &&
					       MT_mmap_tab[i].unload_tile < MT_mmap_tab[i].len) {
						if (MT_mmap_tab[i].writable)
							MT_mmap_save_tile(i, MT_mmap_tab[i].unload_tile, err);
						MT_mmap_unload_tile(i, MT_mmap_tab[i].unload_tile, err);
						MT_mmap_tab[i].unload_tile += MT_MMAP_TILE;
						if ((delta -= MT_MMAP_TILE) == 0) {
							rss = MT_getrss();
							if (rss < target) {
								MT_mmap_cur = i;
								goto done;
							}
							delta = ((rss - target) + 4 * MT_MMAP_TILE - 1) & ~(MT_MMAP_TILE - 1);
						}
					}
				}
			}
		}
		rss = MT_getrss();
	}
done:
	if (worry > 1) {
		/* schedule background saves of tiles */
		for (i = MT_mmap_next(MT_mmap_cur); i != MT_mmap_cur; i = MT_mmap_next(i)) {
			assert(i >= 0);
			if (MT_mmap_tab[i].writable &&
			    MT_mmap_tab[i].last_tile <= 1 &&
			    (MT_mmap_tab[i].random == 0 ||
			     MT_mmap_tab[i].len > target)) {
				if (MT_mmap_tab[i].save_tile == 1) {
					/* first run, walk backwards
					   until we hit an unsaved
					   tile */
					for (off = MT_mmap_tab[i].len; off >= MT_MMAP_TILE; off -= MT_MMAP_TILE)
						if (MT_mmap_save_tile(i, off, err))
							goto bailout;
				} else {
					/* save the next tile */
					for (off = MT_mmap_tab[i].save_tile; off + MT_MMAP_TILE < MT_mmap_tab[i].len; off += MT_MMAP_TILE) {
						if (MT_mmap_save_tile(i, off, err))
							goto bailout;
					}
					/* we seem to have run through
					   all savable tiles */
					if (MT_mmap_tab[i].last_tile++ == 0) {
						MT_mmap_tab[i].save_tile = 0;	/* now start saving from the beginning */
					}
				}
			}
		}
	}
bailout:
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	(void) pthread_mutex_unlock(&MT_mmap_relock);
	return (worry);
}

/* a thread informs it is going to (preload==1) or stops
   using (preload==-1) a range of memory */
void
MT_mmap_inform(void *base, size_t len, int preload, int advice, int writable)
{
	int i, ret = 0;

	assert(advice == MMAP_NORMAL || advice == MMAP_RANDOM || advice == MMAP_SEQUENTIAL || advice == MMAP_WILLNEED || advice == MMAP_DONTNEED);

	(void) pthread_mutex_lock(&MT_mmap_lock);
	i = MT_mmap_idx(base, len);
	if (i >= 0) {
		if (writable)
			MT_mmap_tab[i].writable = (writable > 0);
		MT_mmap_tab[i].random += preload * (advice == MMAP_WILLNEED);	/* done as a counter to keep track of multiple threads */
		MT_mmap_tab[i].usecnt += preload;	/* active thread count */
		if ( advice == MMAP_DONTNEED){
			ret = posix_madvise(MT_mmap_tab[i].base, MT_mmap_tab[i].len & ~(MT_pagesize() - 1), MMAP_DONTNEED);
			MT_mmap_tab[i].usecnt = 0;
		} else
		if (MT_mmap_tab[i].usecnt == 0)
			ret = posix_madvise(MT_mmap_tab[i].base, MT_mmap_tab[i].len & ~(MT_pagesize() - 1), MMAP_SEQUENTIAL);
	}
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	if (ret) {
		mnstr_printf(GDKstdout,
			      "#MT_mmap_inform: posix_madvise(file=%s, fd=%d, base=" PTRFMT ", len=" SZFMT "MB, advice=MMAP_SEQUENTIAL) = %d (%s)\n",
			      (i >= 0 ? MT_mmap_tab[i].path : ""),
			      (i >= 0 ? MT_mmap_tab[i].fd : -1),
			      PTRFMTCAST base,
			      len >> 20,
			      errno, strerror(errno));
	}
}

void *
MT_mmap(char *path, int mode, off_t off, size_t len)
{
	MT_mmap_hdl hdl;
	void *ret = MT_mmap_open(&hdl, path, mode, off, len, 0);

	MT_mmap_close(&hdl);
	return ret;
}

#ifdef DEBUG_ALLOC
static unsigned char MT_alloc_map[65536] = { 0 };

static void
MT_alloc_init(void)
{
	char *p = NULL;
	int i;

	for (i = 0; i < 65536; i++, p += MT_VMUNITSIZE) {
		int mode = '.';

#ifdef WIN32
		if (!VirtualAlloc(p, MT_VMUNITSIZE, MEM_RESERVE, PAGE_NOACCESS)) {
			mode |= 128;
		} else {
			VirtualFree(p, 0, MEM_RELEASE);
		}
#else
		MMAP_OPEN_DEV_ZERO;
		void *q = (char *) mmap(p, MT_VMUNITSIZE, PROT_NONE, MMAP_FLAGS(MAP_NORESERVE), MMAP_FD, 0);

		MMAP_CLOSE_DEV_ZERO;
		if (q != p)
			mode |= 128;
		if (q != (char *) -1L)
			munmap(q, MT_VMUNITSIZE);
#endif
		MT_alloc_map[i] = mode;
	}
}
#endif

#ifndef NATIVE_WIN32
#ifdef HAVE_POSIX_FADVISE
#ifdef HAVE_UNAME
#include <sys/utsname.h>
#endif
#endif

void
MT_init_posix(int alloc_map)
{
#ifdef HAVE_POSIX_FADVISE
#ifdef HAVE_UNAME
	struct utsname ubuf;

	/* do not use posix_fadvise on Linux systems running a 2.4 or
	   older kernel */
	do_not_use_posix_fadvise = uname(&ubuf) == 0 && strcmp(ubuf.sysname, "Linux") == 0 && strncmp(ubuf.release, "2.4", 3) <= 0;
#endif
#endif
	MT_heapbase = (char *) sbrk(0);

#ifdef DEBUG_ALLOC
	if (alloc_map)
		MT_alloc_init();
#else
	(void) alloc_map;
#endif
	MT_mmap_init();
}

size_t
MT_getrss(void)
{
	static char MT_mmap_procfile[128] = { 0 };
	int fd;

#if defined(HAVE_PROCFS_H) && defined(__sun__)
	/* retrieve RSS the Solaris way (2.6+) */
	psinfo_t psbuff;
	if (MT_mmap_procfile[0] == 0) {
		/* getpid returns pid_t, cast to long to be sure */
		sprintf(MT_mmap_procfile, "/proc/%ld/psinfo", (long) getpid());
	}
	fd = open(MT_mmap_procfile, O_RDONLY);
	if (fd >= 0) {
		if (read(fd, &psbuff, sizeof(psbuff)) == sizeof(psbuff)) {
			close(fd);
			return psbuff.pr_rssize * 1024;
		}
		close(fd);
	}
#else
	/* get RSS  -- linux only for the moment */

	if (MT_mmap_procfile[0] == 0) {
		/* getpid returns pid_t, cast to long to be sure */
		sprintf(MT_mmap_procfile, "/proc/%ld/stat", (long) getpid());
	}
	fd = open(MT_mmap_procfile, O_RDONLY);
	if (fd >= 0) {
		char buf[1024], *r = buf;
		size_t i, sz = read(fd, buf, 1024);

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
MT_mmap_open(MT_mmap_hdl *hdl, char *path, int mode, off_t off, size_t len, size_t nremaps)
{
	int fd = open(path, O_CREAT | ((mode & MMAP_WRITE) ? O_RDWR : O_RDONLY), MONETDB_MODE);
	void *ret = (void *) -1L;

	(void) nremaps;
	if (fd > 1) {
		hdl->mode = mode;
		hdl->fixed = NULL;
		hdl->hdl = (void *) (ssize_t) fd;
		ret = MT_mmap_remap(hdl, off, len);
	}
	if (ret != (void *) -1L) {
		hdl->fixed = ret;
		hdl->hdl = (void *) (ssize_t) MT_mmap_new(path, ret, len, fd, (mode & MMAP_WRITABLE));
	}
	return ret;
}

void *
MT_mmap_remap(MT_mmap_hdl *hdl, off_t off, size_t len)
{
	int fd = (int) (ssize_t) hdl->hdl;
	void *ret = mmap(hdl->fixed,
			 len,
			 ((hdl->mode & MMAP_WRITABLE) ? PROT_WRITE : 0) | PROT_READ,
			 ((hdl->mode & MMAP_COPY) ? (MAP_PRIVATE | MAP_NORESERVE) : MAP_SHARED) | (hdl->fixed ? MAP_FIXED : 0),
			 (fd < 0) ? -fd : fd,
			 off);

	if (ret != (void *) -1L) {
		if (hdl->mode & MMAP_ADVISE) {
			(void) MT_madvise(ret, len & ~(MT_pagesize() - 1), hdl->mode & MMAP_ADVISE);
		}
		hdl->fixed = (void *) ((char *) ret + len);
	}
	return ret;
}

void
MT_mmap_close(MT_mmap_hdl *hdl)
{
	int fd = (int) (ssize_t) hdl->hdl;
	if (fd > 0)
		close(fd);
	hdl->hdl = NULL;
}

int
MT_munmap(void *p, size_t len)
{
	int ret = munmap(p, len);

#ifdef MMAP_DEBUG
	mnstr_printf(GDKstdout, "#munmap(" LLFMT "," LLFMT ",%d) = %d\n", (long long) p, (long long) len, ret);
#endif
	MT_mmap_del(p, len);
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

int
MT_madvise(void *p, size_t len, int advice)
{
	int ret = posix_madvise(p, len & ~(MT_pagesize() - 1), advice);

#ifdef MMAP_DEBUG
	mnstr_printf(GDKstdout, "#posix_madvise(" PTRFMT "," SZFMT ",%d) = %d\n",
		      PTRFMTCAST p, len, advice, ret);
#endif
	if (MT_fadvise(p, len, advice))
		ret = -1;
	return ret;
}

struct Mallinfo
MT_mallinfo(void)
{
	struct Mallinfo _ret;

#if defined(HAVE_USEFUL_MALLINFO) && 0
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
	if (_ret.uordblks + _ret.fordblks > _ret.arena) {
		MT_alloc_register(MT_heapbase, _ret.arena, 'H');
	}
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

#define MT_SMALLBLOCK 256

static LONG WINAPI
MT_ignore_exceptions(struct _EXCEPTION_POINTERS *ExceptionInfo)
{
	(void) ExceptionInfo;
	return EXCEPTION_EXECUTE_HANDLER;
}

void
MT_init_posix(int alloc_map)
{
	MT_heapbase = 0;
#ifdef DEBUG_ALLOC
	if (alloc_map)
		MT_alloc_init();
#else
	(void) alloc_map;
#endif
	MT_mmap_init();
	SetUnhandledExceptionFilter(MT_ignore_exceptions);
}

size_t
MT_getrss()
{
#if (_WIN32_WINNT >= 0x0500)
	MEMORYSTATUSEX state;

	state.dwLength = sizeof(state);
	GlobalMemoryStatusEx(&state);
	return (size_t) (state.ullTotalPhys - state.ullAvailPhys);
#else
	MEMORYSTATUS state;

	GlobalMemoryStatus(&state);
	return state.dwTotalPhys - state.dwAvailPhys;
#endif
}

char *
MT_heapcur(void)
{
	return (char *) 0;
}

/* Windows mmap keeps a global list of base addresses for complex
   (remapped) memory maps the reason is that each remapped segment
   needs to be unmapped separately in the end. */
typedef struct _remap_t {
	struct _remap_t *next;
	char *start;
	char *end;
	size_t cnt;
	void *bases[1];		/* EXTENDS (cnt-1) BEYOND THE END OF THE STRUCT */
} remap_t;

remap_t *remaps = NULL;

static remap_t *
remap_find(char *base, int delete)
{
	remap_t *map, *prev = NULL;

	(void) pthread_mutex_lock(&MT_mmap_lock);
	for (map = remaps; map; map = map->next) {
		if (base >= map->start && base < map->end) {
			if (delete) {
				if (prev)
					prev->next = map->next;
				else
					remaps = map->next;
				map->next = NULL;
			}
			break;
		}
	}
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	return map;
}


void *
MT_mmap_open(MT_mmap_hdl *hdl, char *path, int mode, off_t off, size_t len, size_t nremaps)
{
	void *ret = NULL;
	DWORD mode0 = FILE_READ_ATTRIBUTES | FILE_READ_DATA;
	DWORD mode1 = FILE_SHARE_READ | FILE_SHARE_WRITE;
	DWORD mode2 = mode & MMAP_ADVISE;
	DWORD mode3 = PAGE_READONLY;
	int mode4 = FILE_MAP_READ;
	SECURITY_ATTRIBUTES sa;
	HANDLE h1, h2;
	remap_t *map;

	memset(hdl, 0, sizeof(MT_mmap_hdl));
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

	h2 = CreateFileMapping(h1, &sa, mode3, (DWORD) ((((__int64) off + (__int64) len) >> 32) & LL_CONSTANT(0xFFFFFFFF)), (DWORD) ((off + len) & LL_CONSTANT(0xFFFFFFFF)), NULL);
	if (h2 == NULL) {
		GDKsyserror("MT_mmap: CreateFileMapping(" PTRFMT ", &sa, %lu, %lu, %lu, NULL) failed\n",
			    PTRFMTCAST h1, mode3,
			    (DWORD) ((((__int64) off + (__int64) len) >> 32) & LL_CONSTANT(0xFFFFFFFF)),
			    (DWORD) ((off + len) & LL_CONSTANT(0xFFFFFFFF)));
		CloseHandle(h1);
		return (void *) -1;
	}
	hdl->hdl = (void *) h2;
	hdl->mode = mode4;
	CloseHandle(h1);

	if (nremaps == 0) {
		return MT_mmap_remap(hdl, off, len);	/* normal mmap(). no further remaps */
	}
	/* for complex mmaps we now make it very likely that the
	   MapViewOfFileEx-es to a predetermined VM address will all
	   succeed */
	ret = VirtualAlloc(NULL, len, MEM_RESERVE, PAGE_READWRITE);
	if (ret == NULL)
		return (void *) -1;
	hdl->fixed = ret;	/* now we have a VM region that is large enough */

	/* ensure exclusive VM access to MapViewOfFile and
	   VirtualAlloc (almost.. malloc() may trigger it --
	   ignored) */
	(void) pthread_mutex_lock(&MT_mmap_lock);
	hdl->hasLock = 1;

	/* release the range, so we can MapViewOfFileEx into it later */
	VirtualFree(ret, 0, MEM_RELEASE);

	/* allocate a map record to administer all your bases (they
	   are belong to us!) */
	map = malloc(sizeof(remap_t) + sizeof(void *) * nremaps);
	if (map == NULL)
		return (void *) -1;

	hdl->map = (void *) map;
	map->cnt = 0;
	map->start = (char *) hdl->fixed;
	map->end = map->start + len;
	return ret;
}


void *
MT_mmap_remap(MT_mmap_hdl *hdl, off_t off, size_t len)
{
	remap_t *map = (remap_t *) hdl->map;
	void *ret;

	ret = MapViewOfFileEx((HANDLE) hdl->hdl, hdl->mode, (DWORD) ((__int64) off >> 32), (DWORD) off, len, (void *) ((char *) hdl->fixed));
	if (ret == NULL) {
		return (void *) -1;
	}
	if (map)
		map->bases[map->cnt++] = ret;	/* administer new base */
	hdl->fixed = (void *) ((char *) ret + len);
	return ret;
}

void
MT_mmap_close(MT_mmap_hdl *hdl)
{
	if (hdl->hasLock) {
		(void) pthread_mutex_unlock(&MT_mmap_lock);
	}
	if (hdl->hdl) {
		CloseHandle((HANDLE) hdl->hdl);
		hdl->hdl = NULL;
	}
}

int
MT_munmap(void *p, size_t dummy)
{
	remap_t *map = remap_find(p, TRUE);
	int ret = 0;

	(void) dummy;
	if (map) {
		/* remapped region; has multiple bases on which we
		   must invoke the Windows API */
		size_t i;

		for (i = 0; i < map->cnt; i++)
			if (UnmapViewOfFile(map->bases[i]) == 0)
				ret = -1;
		free(map);
	} else {
		/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
		 * while Unix's   munmap          returns success==0, error==-1. */
		if (UnmapViewOfFile(p) == 0)
			ret = -1;
	}
	return ret;
}

int
MT_msync(void *p, size_t off, size_t len, int mode)
{
	remap_t *map = remap_find(p, FALSE);
	int ret = 0;

	(void) mode;
	if (map) {
		/* remapped region; has multiple bases on which we
		   must invoke the Windows API */
		size_t i;
		for (i = 0; i < map->cnt; i++)	/* oops, have to flush all now.. */
			if (FlushViewOfFile(map->bases[i], 0) == 0)
				ret = -1;
	} else {
		/*       Windows' UnmapViewOfFile returns success!=0, error== 0,
		 * while Unix's   munmap          returns success==0, error==-1. */
		if (FlushViewOfFile(((char *) p) + off, len) == 0)
			ret = -1;
	}
	return ret;
}

int
MT_madvise(void *p, size_t len, int advice)
{
	(void) p;
	(void) len;
	(void) advice;
	return 0;		/* would -1 be better? */
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
				MT_alloc_register(hinfo._pentry, hinfo._size, 'H');
			} else {
				_ret.fsmblks += hinfo._size;
				MT_alloc_register(hinfo._pentry, hinfo._size, 'h');
			}
		} else {
			_ret.ordblks++;
			if (hinfo._useflag == _USEDENTRY) {
				_ret.uordblks += hinfo._size;
				MT_alloc_register(hinfo._pentry, hinfo._size, 'H');
			} else {
				_ret.fordblks += hinfo._size;
				MT_alloc_register(hinfo._pentry, hinfo._size, 'h');
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

	if (ret < 0) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG THRprintf(GDKout, "retry rmdir %s\n", pathname);
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
		/* Vista is paranoid: we cannot delete read-only files owned
		 * by ourselves. Vista somehow also sets these files to read-only.
		 */
		(void) SetFileAttributes(pathname, FILE_ATTRIBUTE_NORMAL);
		ret = _unlink(pathname);
	}
	if (ret < 0) {
		/* it could be the <expletive deleted> indexing
		 * service which prevents us from doing what we have a
		 * right to do, so try again (once) */
		IODEBUG THRprintf(GDKout, "retry unlink %s\n", pathname);
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

	if (ret < 0) {
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
	   from WIN32 error code */
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

#define MT_PAGESIZE(s)		((((s)-1)/MT_pagesize()+1)*MT_pagesize())

#if defined(MAP_ANON) && !defined(MAP_ANONYMOUS)
#define MAP_ANONYMOUS MAP_ANON
#endif
#if defined(MAP_ANONYMOUS)
#define MMAP_FLAGS(f)		f|MAP_ANONYMOUS
#define MMAP_FD			-1
#define MMAP_OPEN_DEV_ZERO	int fd = 1
#define MMAP_CLOSE_DEV_ZERO	(void)fd
#else
#define MMAP_FLAGS(f)		f
#define MMAP_FD			fd
#define MMAP_OPEN_DEV_ZERO	int fd = open("/dev/zero", O_RDWR, MONETDB_MODE)
#define MMAP_CLOSE_DEV_ZERO	close(fd)
#endif

void *
MT_vmalloc(size_t size, size_t *maxsize)
{
	MMAP_OPEN_DEV_ZERO;
	char *q, *r = (char *) -1L;

	if (fd < 0) {
		return NULL;
	}
	size = MT_PAGESIZE(size);
	*maxsize = MT_PAGESIZE(*maxsize);
	if (*maxsize > size) {
		r = (char *) mmap(NULL, *maxsize, PROT_NONE, MMAP_FLAGS(MAP_PRIVATE | MAP_NORESERVE), MMAP_FD, 0);
	}
	if (r == (char *) -1L) {
		*maxsize = size;
		q = (char *) mmap(NULL, size, PROT_READ | PROT_WRITE, MMAP_FLAGS(MAP_PRIVATE), MMAP_FD, 0);
	} else {
		q = (char *) mmap(r, size, PROT_READ | PROT_WRITE, MMAP_FLAGS(MAP_PRIVATE | MAP_FIXED), MMAP_FD, 0);
	}
	MMAP_CLOSE_DEV_ZERO;
	return (void *) ((q == (char *) -1L) ? NULL : q);
}

void
MT_vmfree(void *p, size_t size)
{
	size = MT_PAGESIZE(size);
	munmap(p, size);
}

void *
MT_vmrealloc(void *voidptr, size_t oldsize, size_t newsize, size_t oldmaxsize, size_t *newmaxsize)
{
	char *p = (char *) voidptr;
	char *q = (char *) -1L;

	/* sanitize sizes */
	oldsize = MT_PAGESIZE(oldsize);
	newsize = MT_PAGESIZE(newsize);
	oldmaxsize = MT_PAGESIZE(oldmaxsize);
	*newmaxsize = MT_PAGESIZE(*newmaxsize);
	if (*newmaxsize < newsize) {
		*newmaxsize = newsize;
	}

	if (oldsize > newsize) {
		munmap(p + oldsize, oldsize - newsize);
	} else if (oldsize < newsize) {
		if (newsize < oldmaxsize) {
			MMAP_OPEN_DEV_ZERO;
			if (fd >= 0) {
				q = (char *) mmap(p + oldsize, newsize - oldsize, PROT_READ | PROT_WRITE, MMAP_FLAGS(MAP_PRIVATE | MAP_FIXED), MMAP_FD, (off_t) oldsize);
				MMAP_CLOSE_DEV_ZERO;
			}
		}
		if (q == (char *) -1L) {
			q = (char *) MT_vmalloc(newsize, newmaxsize);
			if (q != NULL) {
				memcpy(q, p, oldsize);
				MT_vmfree(p, oldmaxsize);
				return q;
			}
		}
	}
	*newmaxsize = MAX(oldmaxsize, newsize);
	return p;
}

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

#define MT_PAGESIZE(s)		(((((s)-1) >> 12) + 1) << 12)
#define MT_SEGSIZE(s)		((((((s)-1) >> 16) & 65535) + 1) << 16)

#ifndef MEM_TOP_DOWN
#define MEM_TOP_DOWN 0
#endif

void *
MT_vmalloc(size_t size, size_t *maxsize)
{
	void *p, *a = NULL;
	int mode = 0;

	size = MT_PAGESIZE(size);
	if (*maxsize < size) {
		*maxsize = size;
	}
	*maxsize = MT_SEGSIZE(*maxsize);
	if (*maxsize < 1000000) {
		mode = MEM_TOP_DOWN;	/* help NT in keeping memory defragmented */
	}
	(void) pthread_mutex_lock(&MT_mmap_lock);
	if (*maxsize > size) {
		a = (void *) VirtualAlloc(NULL, *maxsize, MEM_RESERVE | mode, PAGE_NOACCESS);
		if (a == NULL) {
			*maxsize = size;
		}
	}
	p = (void *) VirtualAlloc(a, size, MEM_COMMIT | mode, PAGE_READWRITE);
	(void) pthread_mutex_unlock(&MT_mmap_lock);
	if (p == NULL) {
		mnstr_printf(GDKstdout, "#VirtualAlloc(" PTRFMT "," SZFMT ",MEM_COMMIT,PAGE_READWRITE): failed\n", PTRFMTCAST a, size);
	}
	return p;
}


void
MT_vmfree(void *p, size_t size)
{
	if (VirtualFree(p, size, MEM_DECOMMIT) == 0)
		mnstr_printf(GDKstdout, "#VirtualFree(" PTRFMT "," SZFMT ",MEM_DECOMMIT): failed\n", PTRFMTCAST p, size);
	if (VirtualFree(p, 0, MEM_RELEASE) == 0)
		mnstr_printf(GDKstdout, "#VirtualFree(" PTRFMT ",0,MEM_RELEASE): failed\n", PTRFMTCAST p);
}

void *
MT_vmrealloc(void *v, size_t oldsize, size_t newsize, size_t oldmaxsize, size_t *newmaxsize)
{
	char *p = (char *) v, *a = p;

	/* sanitize sizes */
	oldsize = MT_PAGESIZE(oldsize);
	newsize = MT_PAGESIZE(newsize);
	oldmaxsize = MT_PAGESIZE(oldmaxsize);
	*newmaxsize = MT_PAGESIZE(*newmaxsize);
	if (*newmaxsize < newsize) {
		*newmaxsize = newsize;
	}

	if (oldsize > newsize) {
		size_t ret = VirtualFree(p + newsize, oldsize - newsize, MEM_DECOMMIT);

		if (ret == 0)
			mnstr_printf(GDKstdout, "#VirtualFree(" PTRFMT "," SSZFMT ",MEM_DECOMMIT): failed\n", PTRFMTCAST(p + newsize), (ssize_t) (oldsize - newsize));
	} else if (oldsize < newsize) {
		(void) pthread_mutex_lock(&MT_mmap_lock);
		a = (char *) VirtualAlloc(p, newsize, MEM_COMMIT, PAGE_READWRITE);
		(void) pthread_mutex_unlock(&MT_mmap_lock);
		if (a != p) {
			char *q = a;

			if (a == NULL) {
				q = MT_vmalloc(newsize, newmaxsize);
			}
			if (q != NULL) {
				memcpy(q, p, oldsize);
				MT_vmfree(p, oldmaxsize);
			}
			if (a == NULL)
				return q;
		}
	}
	*newmaxsize = MAX(oldmaxsize, newsize);
	return a;
}

void
MT_sleep_ms(unsigned int ms)
{
	Sleep(ms);
}


/*
 * @-
 * cygnus1.1.X has a bug in the semaphore routines. we work around it by directly using the WIN32 primitives.
 */
#ifndef NATIVE_WIN32

int
sem_init(sem_t * sem, int pshared, unsigned int value)
{
	(void) pshared;
	*sem = (sem_t) CreateSemaphore(NULL, value, 128, NULL);
	return (*sem) ? 0 : -1;
}

int
sem_destroy(sem_t * sem)
{
	return CloseHandle((HANDLE) *sem) ? 0 : -1;
}

int
sem_wait(sem_t * sem)
{
	return (WaitForSingleObject((HANDLE) *sem, (unsigned int) INFINITE) != WAIT_FAILED) ? 0 : -1;
}

int
sem_post(sem_t * sem)
{
	return (ReleaseSemaphore((HANDLE) *sem, 1, NULL) == 0) ? -1 : 0;
}
#endif
#endif

/*
 * @+ Memory fragmentation monitoring
 * On 32-bits systems, MonetDB's aggressive use of virtual memory may bring it into
 * trouble as the limits of what is addressable in a 32-bits system are reached
 * (an 32-bits OS only allows 2 to 4GB of memory to be used). In order to aid debugging
 * situations where VM allocs fail (due to memory fragmentation), a monitoring
 * system was established. To this purpose, a map is made for the VM addresses
 * between 0 and 3GB, in tiles of MT_VMUNITSIZE (64KB). These tiles have a byte
 * value from the following domain:
 *
 * @table @samp
 * @item 0-9
 * thread stack space of thread <num>
 * @item H
 * in use for a large BAT heap.
 * @item h
 * free (last usage was B)
 * @item S
 * in use for a malloc block
 * @item s
 * free (last usage was S)
 * @item P
 * in use for the BBP array
 * @item p
 * free (last usage was P)
 * @item M
 * in use as memory mapped region
 * @item m
 * free (last usage was M)
 * @end table
 *
 * The MT_alloc_printmap condenses the map by printing a char for each MB,
 * hence combining info from 16 tiles. On NT, we can check in real-time which
 * tiles are actually in use (in case our own tile administration is out-of-sync
 * with reality, eg due to a memory leak). This real-life usage is printed in a
 * second line with encoding .=free, *=inuse, X=unusable. On Unix systems,
 * *=inuse is not testable (unless with complicated signal stuff). On 64-bits
 * systems, this administration is dysfunctional.
 */
#ifdef DEBUG_ALLOC
#define INUSEMODE(x) ((x >= '0' && x <= ('9'+4)) || (x >= 'A' && x <= 'Z'))

/* The memory table dump can also be produced in tuple format to enable
 * front-ends to analyze it more easily.
 */
struct {
	char tag;
	char *color;
	char *info;
} Encoding[] = {
	{
	'.', "0x00FFFDFE", "free"}, {
	'0', "0x000035FC", "thread stack space of thread 0"}, {
	'1', "0x000067FE", "thread stack space of thread 1"}, {
	'2', "0x000095FE", "thread stack space of thread 2"}, {
	'3', "0x0000BDFC", "thread stack space of thread 3"}, {
	'4', "0x0000DCF8", "thread stack space of thread 4"}, {
	'5', "0x002735FC", "thread stack space of thread 5"}, {
	'6', "0x002767FE", "thread stack space of thread 6"}, {
	'7', "0x002795FE", "thread stack space of thread 7"}, {
	'8', "0x0027BDFC", "thread stack space of thread 8"}, {
	'9', "0x0027DCF8", "thread stack space of thread 9"}, {
	'B', "0x0000672D", "in use for a large BAT heap."}, {
	'b', "0x004EF2A7", "free (last usage was B)"}, {
	'S', "0x00B4006E", "in use for a malloc block"}, {
	's', "0x00F2BDE0", "free (last usage was S)"}, {
	'P', "0x00F26716", "in use for the BBP array"}, {
	'p', "0x00F2BD16", "free (last usage was P)"}, {
	'M', "0x00959516", "in use as memory mapped region"}, {
	'm', "0x00CEDC16", "free (last usage was M)"}, {
	'c', "0x00FFFD2D", "free (last usage was M)"}, {
	0, "0x00FFFDFE", "free"}
};
#endif

int
MT_alloc_register(void *addr, size_t size, char mode)
{
#ifdef DEBUG_ALLOC
	if (MT_alloc_map[0]) {
		size_t p = (size_t) addr;

		if (size > 0) {
			size_t i, base = p >> 16;

			size = (size - 1) >> 16;
			assert(p && ((long long) p) + size < (LL_CONSTANT(1) << 32));
			for (i = 0; i <= size; i++)
				MT_alloc_map[base + i] = (MT_alloc_map[base + i] & 128) | mode;
		}
	}
#else
	(void) addr;
	(void) size;
	(void) mode;
#endif
	return 0;
}


int
MT_alloc_print(void)
{
#ifdef DEBUG_ALLOC
#ifdef WIN32
	char *p = NULL;
#endif
	int i, j, k;

	if (MT_alloc_map[0] == 0)
		return 0;

	for (i = 0; i < 40; i++) {
		mnstr_printf(GDKout, "%02d00MB ", i);
		for (j = 0; j < 100; j++) {
			int mode = '.';

			for (k = 0; k < 16; k++) {
				int m = MT_alloc_map[k + 16 * (j + 100 * i)] & 127;

				if (mode == '.' || INUSEMODE(m))
					mode = m;
			}
			mnstr_printf(GDKout, "%c", mode);
		}
#ifdef WIN32
		mnstr_printf(GDKout, "\n       ");
		for (j = 0; j < 100; j++) {
			int mode = '.';

			for (k = 0; k < 16; k++, p += 1 << 16)
				if (!IsBadReadPtr(p, 1)) {
					mode = '*';
				} else if (MT_alloc_map[k + 16 * (j + 100 * i)] & 128) {
					mode = 'X';
				}
			mnstr_printf(GDKout, "%c", mode);
		}
#endif
		mnstr_printf(GDKout, "\n");
	}
#endif
	return 0;
}

int
MT_alloc_table(void)
{
#ifdef DEBUG_ALLOC
#ifdef WIN32
	char *p = NULL;
#endif
	int i, j, k;

	if (MT_alloc_map[0] == 0)
		return 0;

	mnstr_printf(GDKout, "# addr\tX\tY\tcolor\tmode\tcomment\t# name\n");
	mnstr_printf(GDKout, "# str\tint\tint\tcolor\tstr\tstr\t# type\n");
	for (i = 0; i < 40; i++) {
		for (j = 0; j < 100; j++) {
			int mode = '.';

			for (k = 0; k < 16; k++) {
				int m = MT_alloc_map[k + 16 * (j + 100 * i)] & 127;

				if (mode == '.' || INUSEMODE(m))
					mode = m;
			}
			for (k = 0; k >= 0; k++)
				if (Encoding[k].tag == mode || Encoding[k].tag == 0) {
					if (mode == 0)
						mode = ' ';
					mnstr_printf(GDKout, "[ \"%d\",\t%d,\t%d,\t", k + 16 * (j + 100 * i), j, i);
					mnstr_printf(GDKout, "\"%s\",\t", Encoding[k].color);

					mnstr_printf(GDKout, "\"%c\",\t\"%s\"\t]\n", mode, Encoding[k].info);
					break;
				}
		}
#ifdef WIN32
		mnstr_printf(GDKout, "\n       ");
		for (j = 0; j < 100; j++) {
			int mode = '.';

			for (k = 0; k < 16; k++, p += 1 << 16)
				if (!IsBadReadPtr(p, 1)) {
					mode = '*';
				} else if (MT_alloc_map[k + 16 * (j + 100 * i)] & 128) {
					mode = 'X';
				}
			mnstr_printf(GDKout, "%c", mode);
		}
		mnstr_printf(GDKout, "\n");
#endif
	}
#endif
	return 0;
}

