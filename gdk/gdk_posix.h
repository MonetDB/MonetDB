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

#ifndef HAVE_SYS_SOCKET_H
#ifdef HAVE_WINSOCK_H
#include <winsock.h>		/* for timeval */
#endif
#endif

#ifdef NATIVE_WIN32
#include <io.h>
#include <direct.h>
#endif

/* make sure POSIX_MADV_* and posix_madvise() are defined somehow */
#ifdef HAVE_SYS_MMAN_H
# ifndef __USE_BSD
#  define __USE_BSD
# endif
# include <sys/mman.h>
#endif

#define MMAP_READ		1024	/* region is readable (default if omitted) */
#define MMAP_WRITE		2048	/* region may be written into */
#define MMAP_COPY		4096	/* writable, but changes never reach file */

/* in order to be sure of madvise and msync modes, pass them to mmap()
 * call as well */

gdk_export size_t MT_getrss(void);

gdk_export bool MT_path_absolute(const char *path);


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

#endif	/* NATIVE_WIN32 */

#ifndef HAVE_LOCALTIME_R
gdk_export struct tm *localtime_r(const time_t *restrict, struct tm *restrict);
#endif
#ifndef HAVE_GMTIME_R
gdk_export struct tm *gmtime_r(const time_t *restrict, struct tm *restrict);
#endif
#ifndef HAVE_ASCTIME_R
gdk_export char *asctime_r(const struct tm *restrict, char *restrict);
#endif
#ifndef HAVE_CTIME_R
gdk_export char *ctime_r(const time_t *restrict, char *restrict);
#endif
#if !defined(HAVE_STRERROR_R) && !defined(HAVE_STRERROR_S)
gdk_export int strerror_r(int errnum, char *buf, size_t buflen);
#endif

static inline const char *
GDKstrerror(int errnum, char *buf, size_t buflen)
{
#ifdef HAVE_STRERROR_S
	if (strerror_s(buf, buflen, errnum) == 0)
		return buf;
	snprintf(buf, buflen, "Unknown error %d", errnum);
	return buf;
#elif defined(STRERROR_R_CHARP)
	return strerror_r(errnum, buf, buflen);
#else
	if (strerror_r(errnum, buf, buflen) == 0)
		return buf;
	snprintf(buf, buflen, "Unknown error %d", errnum);
	return buf;
#endif
}

#endif /* GDK_POSIX_H */
