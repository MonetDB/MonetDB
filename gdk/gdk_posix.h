/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef GDK_POSIX_H
#define GDK_POSIX_H

#include <sys/types.h>

#include <time.h>

#ifdef HAVE_FTIME
#include <sys/timeb.h>		/* ftime */
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>		/* gettimeofday */
#endif

#if defined(HAVE_WINSOCK_H) && defined(NATIVE_WIN32)
#include <winsock.h>		/* for timeval */
#endif

#include "gdk_system.h" /* gdk_export */

#ifdef NATIVE_WIN32
#include <io.h>
#include <direct.h>
#endif

/*
 * @- locking, sleep
 */

gdk_export void MT_sleep_ms(unsigned int ms);

/*
 * @- virtual memory
 */
#define MT_VMUNITLOG	16
#define MT_VMUNITSIZE	(1 << MT_VMUNITLOG)

/* make sure POSIX_MADV_* and posix_madvise() are defined somehow */
#ifdef HAVE_SYS_MMAN_H
# ifndef __USE_BSD
#  define __USE_BSD
# endif
# include <sys/mman.h>
#endif

#ifdef __linux__
/* on Linux, posix_madvise does not seem to work, fall back to classic
 * madvise */
#undef HAVE_POSIX_MADVISE
#undef HAVE_POSIX_FADVISE
#undef POSIX_MADV_NORMAL
#undef POSIX_MADV_RANDOM
#undef POSIX_MADV_SEQUENTIAL
#undef POSIX_MADV_WILLNEED
#undef POSIX_MADV_DONTNEED
#endif

#ifndef HAVE_POSIX_MADVISE
# ifdef HAVE_MADVISE
#  define posix_madvise madvise
#  define HAVE_POSIX_MADVISE 1
#  ifndef MADV_RANDOM
#   define MADV_RANDOM	0
#  endif
#  ifndef POSIX_MADV_NORMAL
#   define POSIX_MADV_NORMAL     MADV_NORMAL
#   define POSIX_MADV_RANDOM     MADV_RANDOM
#   define POSIX_MADV_SEQUENTIAL MADV_SEQUENTIAL
#   define POSIX_MADV_WILLNEED   MADV_WILLNEED
#   define POSIX_MADV_DONTNEED   MADV_DONTNEED
#  endif
# else
#  define posix_madvise(x,y,z)	0
#  ifndef POSIX_MADV_NORMAL
#   define POSIX_MADV_NORMAL     0
#   define POSIX_MADV_RANDOM     0
#   define POSIX_MADV_SEQUENTIAL 0
#   define POSIX_MADV_WILLNEED   0
#   define POSIX_MADV_DONTNEED   0
#  endif
# endif
#endif

/* in case they are still not defined, define these values as
 * something that doesn't do anything */
#ifndef POSIX_MADV_NORMAL
#define POSIX_MADV_NORMAL 0
#endif
#ifndef POSIX_MADV_RANDOM
#define POSIX_MADV_RANDOM 0
#endif
#ifndef POSIX_MADV_SEQUENTIAL
#define POSIX_MADV_SEQUENTIAL 0
#endif
#ifndef POSIX_MADV_WILLNEED
#define POSIX_MADV_WILLNEED 0
#endif
#ifndef POSIX_MADV_DONTNEED
#define POSIX_MADV_DONTNEED 0
#endif

/* the new mmap modes, mimic default MADV_* madvise POSIX constants */
#define MMAP_NORMAL	POSIX_MADV_NORMAL	/* no further special treatment */
#define MMAP_RANDOM	POSIX_MADV_RANDOM	/* expect random page references */
#define MMAP_SEQUENTIAL	POSIX_MADV_SEQUENTIAL	/* expect sequential page references */
#define MMAP_WILLNEED	POSIX_MADV_WILLNEED	/* will need these pages */
#define MMAP_DONTNEED	POSIX_MADV_DONTNEED	/* don't need these pages */

#define MMAP_READ		1024	/* region is readable (default if ommitted) */
#define MMAP_WRITE		2048	/* region may be written into */
#define MMAP_COPY		4096	/* writable, but changes never reach file */
#define MMAP_ASYNC		8192	/* asynchronous writes (default if ommitted) */
#define MMAP_SYNC		16384	/* writing is done synchronously */

#define MT_MMAP_LOG 27
#define MT_MMAP_TILE (1<<MT_MMAP_LOG)
#define MT_MMAP_BUFSIZE 4096

/* in order to be sure of madvise and msync modes, pass them to mmap()
 * call as well */

gdk_export size_t MT_getrss(void);

gdk_export void *MT_mmap(const char *path, int mode, size_t len);
gdk_export int MT_munmap(void *p, size_t len);

gdk_export int MT_path_absolute(const char *path);


/*
 * @+ Posix under WIN32
 * WIN32 actually supports many Posix functions directly.  Some it
 * does not, though.  For some functionality we move in Monet from
 * Posix calls to MT_*() calls, which translate easier to WIN32.
 * Examples are MT_mmap() , MT_sleep_ms() and MT_path_absolute(). Why?
 * In the case of mmap() it is much easier for WIN32 to get a filename
 * parameter rather than a file-descriptor.  That is the reason in the
 * case of mmap() to go for a MT_mmap() solution.
 *
 * For some other functionality, we do not need to abandon the Posix
 * interface, though. Two cases can be distinguished.  Missing
 * functions in WIN32 are directly implemented
 * (e.g. dlopen()/dlsym()/dlclose()).  Posix functions in WIN32 whose
 * functionality should be changed a bit. Examples are
 * stat()/rename()/mkdir()/rmdir() who under WIN32 do not work if the
 * path ends with a directory separator, but should work according to
 * Posix. We remap such functions using a define to an equivalent
 * win_*() function (which in its implementation calls through to the
 * WIN32 function).
 */
gdk_export void *mdlopen(const char *library, int mode);

#ifdef NATIVE_WIN32

#define RTLD_LAZY	1
#define RTLD_NOW	2
#define RTLD_GLOBAL	4

gdk_export void *dlopen(const char *file, int mode);
gdk_export int dlclose(void *handle);
gdk_export void *dlsym(void *handle, const char *name);
gdk_export char *dlerror(void);

#ifndef HAVE_GETTIMEOFDAY
gdk_export int gettimeofday(struct timeval *tv, int *ignore_zone);
#endif
gdk_export int win_stat(const char *, struct stat *);
gdk_export int win_rmdir(const char *);
gdk_export int win_rename(const char *, const char *);
gdk_export int win_unlink(const char *);
gdk_export int win_mkdir(const char *, const int mode);

#define _stat64(x,y)	win_stat(x,y)
#define mkdir		win_mkdir
#define rmdir		win_rmdir
#define rename		win_rename
#define remove		win_unlink

#endif	/* NATIVE_WIN32 */

#endif /* GDK_POSIX_H */
