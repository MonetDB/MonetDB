/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef _GDK_SYSTEM_H_
#define _GDK_SYSTEM_H_

#ifdef WIN32
#ifndef LIBGDK
#define gdk_export extern __declspec(dllimport)
#else
#define gdk_export extern __declspec(dllexport)
#endif
#else
#define gdk_export extern
#endif

/* if __has_attribute is not known to the preprocessor, we ignore
 * attributes completely; if it is known, use it to find out whether
 * specific attributes that we use are known */
#ifndef __has_attribute
#ifndef __GNUC__
#define __has_attribute(attr)	0
#ifndef __attribute__
#define __attribute__(attr)	/* empty */
#endif
#else
/* older GCC does have attributes, but not __has_attribute and not all
 * attributes that we use are known */
#define __has_attribute__alloc_size__ 1
#define __has_attribute__cold__ 1
#define __has_attribute__format__ 1
#define __has_attribute__malloc__ 1
#define __has_attribute__noreturn__ 1
#define __has_attribute__returns_nonnull__ 0
#define __has_attribute__visibility__ 1
#define __has_attribute__warn_unused_result__ 1
#define __has_attribute(attr)	__has_attribute##attr
#endif
#endif
#if !__has_attribute(__warn_unused_result__)
#define __warn_unused_result__
#endif
#if !__has_attribute(__malloc__)
#define __malloc__
#endif
#if !__has_attribute(__alloc_size__)
#define __alloc_size__(a)
#endif
#if !__has_attribute(__format__)
#define __format__(a,b,c)
#endif
#if !__has_attribute(__noreturn__)
#define __noreturn__
#endif
/* these are used in some *private.h files */
#if !__has_attribute(__visibility__)
#define __visibility__(a)
#elif defined(__CYGWIN__)
#define __visibility__(a)
#endif
#if !__has_attribute(__cold__)
#define __cold__
#endif

/* also see gdk.h for these */
#define THRDMASK	(1)
#define THRDDEBUG	if (GDKdebug & THRDMASK)
#define TEMMASK		(1<<10)
#define TEMDEBUG	if (GDKdebug & TEMMASK)

/*
 * @- pthreads Includes and Definitions
 */
#ifdef HAVE_PTHREAD_H
/* don't re-include config.h; on Windows, don't redefine pid_t in an
 * incompatible way */
#undef HAVE_CONFIG_H
#ifdef pid_t
#undef pid_t
#endif
#include <sched.h>
#include <pthread.h>
#endif

#ifdef HAVE_SEMAPHORE_H
# include <semaphore.h>
#endif

#ifdef HAVE_DISPATCH_DISPATCH_H
#include <dispatch/dispatch.h>
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>	   /* prerequisite of sys/sysctl on OpenBSD */
#endif
#ifdef HAVE_SYS_SYSCTL_H
# include <sys/sysctl.h>
#endif

/* new pthread interface, where the thread id changed to a struct */
#ifdef PTW32_VERSION
#define PTW32 1
#endif

/* debug and errno integers */
gdk_export int GDKdebug;
gdk_export void GDKsetdebug(int debug);
gdk_export int GDKverbose;
gdk_export void GDKsetverbose(int verbosity);

gdk_export int GDKnr_threads;

/* API */

/*
 * @- sleep
 */

gdk_export void MT_sleep_ms(unsigned int ms);

/*
 * @- MT Thread Api
 */
typedef size_t MT_Id;		/* thread number. will not be zero */

enum MT_thr_detach { MT_THR_JOINABLE, MT_THR_DETACHED };

gdk_export bool MT_thread_init(void);
gdk_export int MT_create_thread(MT_Id *t, void (*function) (void *),
				void *arg, enum MT_thr_detach d,
				const char *threadname);
gdk_export const char *MT_thread_getname(void);
gdk_export void *MT_thread_getdata(void);
gdk_export void MT_thread_setdata(void *data);
gdk_export void MT_exiting_thread(void);
gdk_export MT_Id MT_getpid(void);
gdk_export int MT_join_thread(MT_Id t);

#if SIZEOF_VOID_P == 4
/* "limited" stack size on 32-bit systems */
/* to avoid address space fragmentation   */
#define THREAD_STACK_SIZE	((size_t)1024*1024)
#else
/* "increased" stack size on 64-bit systems    */
/* since some compilers seem to require this   */
/* for burg-generated code in pathfinder       */
/* and address space fragmentation is no issue */
#define THREAD_STACK_SIZE	((size_t)2*1024*1024)
#endif


/*
 * @- MT Lock API
 */
#include "gdk_atomic.h"

/* in non-debug builds, we don't keep lock statistics */
#ifndef NDEBUG
#define LOCK_STATS
#endif

/* define this if you want to use pthread (or Windows) locks instead
 * of atomic instructions for locking (latching) */
/* #define USE_PTHREAD_LOCKS */

#ifdef LOCK_STATS

#define _DBG_LOCK_COUNT_0(l)						\
	do {								\
		(void) ATOMIC_INC(GDKlockcnt, dummy);			\
		TEMDEBUG fprintf(stderr, "#%s: %s: locking %s...\n",	\
				 MT_thread_getname(), __func__, (l)->name); \
	} while (0)
#define _DBG_LOCK_LOCKER(l)				\
	do {						\
		(l)->locker = __func__;			\
		(l)->thread = MT_thread_getname();	\
	} while (0)
#define _DBG_LOCK_UNLOCKER(l)					\
	do {							\
		(l)->locker = __func__;				\
		(l)->thread = NULL;				\
		TEMDEBUG fprintf(stderr, "#%s: %s: unlocking %s\n",	\
				 MT_thread_getname(), __func__, (l)->name); \
	} while (0)
#define _DBG_LOCK_CONTENTION(l)						\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: %s: lock %s contention\n", \
				 MT_thread_getname(), __func__, (l)->name); \
		(void) ATOMIC_INC(GDKlockcontentioncnt, dummy);		\
		(void) ATOMIC_INC((l)->contention, dummy);		\
	} while (0)
#define _DBG_LOCK_SLEEP(l)						\
	do {								\
		if (_spincnt == 1024)					\
			(void) ATOMIC_INC(GDKlocksleepcnt, dummy);	\
		(void) ATOMIC_INC((l)->sleep, dummy);			\
	} while (0)
#define _DBG_LOCK_COUNT_2(l)						\
	do {								\
		(l)->count++;						\
		if ((l)->next == (struct MT_Lock *) -1) {		\
			while (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) \
				;					\
			(l)->next = GDKlocklist;			\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(GDKlocklistlock, dummy);		\
		}							\
		TEMDEBUG fprintf(stderr, "#%s: %s: locking %s complete\n", \
				 MT_thread_getname(), __func__, (l)->name); \
	} while (0)
#define _DBG_LOCK_INIT(l)						\
	do {								\
		(l)->count = (l)->contention = (l)->sleep = 0;		\
		(l)->locker = NULL;					\
		(l)->thread = NULL;					\
		/* if name starts with "sa_" don't link in GDKlocklist */ \
		/* since the lock is in memory that is governed by the */ \
		/* SQL storage allocator, and hence we have no control */ \
		/* over when the lock is destroyed and the memory freed */ \
		if (strncmp((l)->name, "sa_", 3) != 0) {		\
			MT_Lock * volatile _p;				\
			while (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) \
				;					\
			for (_p = GDKlocklist; _p; _p = _p->next)	\
				assert(_p != (l));			\
			(l)->next = GDKlocklist;			\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(GDKlocklistlock, dummy);		\
		} else {						\
			(l)->next = NULL;				\
		}							\
	} while (0)
#define _DBG_LOCK_DESTROY(l)						\
	do {								\
		/* if name starts with "sa_" don't link in GDKlocklist */ \
		/* since the lock is in memory that is governed by the */ \
		/* SQL storage allocator, and hence we have no control */ \
		/* over when the lock is destroyed and the memory freed */ \
		if (strncmp((l)->name, "sa_", 3) != 0) {		\
			MT_Lock * volatile *_p;				\
			while (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) \
				;					\
			for (_p = &GDKlocklist; *_p; _p = &(*_p)->next)	\
				if ((l) == *_p) {			\
					*_p = (l)->next;		\
					break;				\
				}					\
			ATOMIC_CLEAR(GDKlocklistlock, dummy);		\
		}							\
	} while (0)

#else

#define _DBG_LOCK_COUNT_0(l)		((void) 0)
#define _DBG_LOCK_CONTENTION(l)		((void) 0)
#define _DBG_LOCK_SLEEP(l)		((void) 0)
#define _DBG_LOCK_COUNT_2(l)		((void) 0)
#define _DBG_LOCK_INIT(l)		((void) 0)
#define _DBG_LOCK_DESTROY(l)		((void) 0)
#define _DBG_LOCK_LOCKER(l)		((void) 0)
#define _DBG_LOCK_UNLOCKER(l)		((void) 0)

#endif

#ifdef USE_PTHREAD_LOCKS

#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
typedef struct MT_Lock {
	HANDLE lock;
	char name[16];
#ifdef LOCK_STATS
	size_t count;
	ATOMIC_TYPE volatile contention;
	ATOMIC_TYPE volatile sleep;
	struct MT_Lock * volatile next;
	const char *locker;
	const char *thread;
#endif
} MT_Lock;

#define MT_lock_init(l, n)					\
	do {							\
		assert((l)->lock == NULL);			\
		(l)->lock = CreateMutex(NULL, 0, NULL);		\
		strncpy((l)->name, (n), sizeof((l)->name));	\
		(l)->name[sizeof((l)->name) - 1] = 0;		\
		_DBG_LOCK_INIT(l);				\
	} while (0)
#define MT_lock_set(l)							\
	do {								\
		assert((l)->lock);					\
		_DBG_LOCK_COUNT_0(l);					\
		if (WaitForSingleObject((l)->lock, 0) != WAIT_OBJECT_0) { \
			_DBG_LOCK_CONTENTION(l);			\
			MT_thread_setlockwait(l);			\
			while (WaitForSingleObject((l)->lock, INFINITE) != WAIT_OBJECT_0) \
				;					\
			MT_thread_setlockwait(NULL);			\
		}							\
		_DBG_LOCK_LOCKER(l);					\
		_DBG_LOCK_COUNT_2(l);					\
	} while (0)
#define MT_lock_unset(l)			\
	do {					\
		assert((l)->lock);					\
		_DBG_LOCK_UNLOCKER(l);		\
		ReleaseMutex((l)->lock);	\
	} while (0)
#define MT_lock_try(l)	(WaitForSingleObject((l)->lock, 0) == WAIT_OBJECT_0)
#define MT_lock_destroy(l)			\
	do {					\
		assert((l)->lock);		\
		_DBG_LOCK_DESTROY(l);		\
		CloseHandle((l)->lock);		\
		(l)->lock = NULL;		\
	} while (0)

#else
typedef struct MT_Lock {
	pthread_mutex_t lock;
	char name[16];
#ifdef LOCK_STATS
	size_t count;
	ATOMIC_TYPE volatile contention;
	ATOMIC_TYPE volatile sleep;
	struct MT_Lock * volatile next;
	const char *locker;
	const char *thread;
#endif
} MT_Lock;
#define MT_lock_init(l, n)					\
	do {							\
		pthread_mutex_init(&(l)->lock, 0);		\
		strncpy((l)->name, (n), sizeof((l)->name));	\
		(l)->name[sizeof((l)->name) - 1] = 0;		\
		_DBG_LOCK_INIT(l);				\
	} while (0)
#define MT_lock_set(l)							\
	do {								\
		_DBG_LOCK_COUNT_0(l);					\
		if (pthread_mutex_trylock(&(l)->lock) != 0) {		\
			_DBG_LOCK_CONTENTION(l);			\
			MT_thread_setlockwait(l);			\
			while (pthread_mutex_lock(&(l)->lock) != 0)	\
				;					\
			MT_thread_setlockwait(NULL);			\
		}							\
		_DBG_LOCK_LOCKER(l);					\
		_DBG_LOCK_COUNT_2(l);					\
	} while (0)
#define MT_lock_unset(l)						\
	do {								\
		_DBG_LOCK_UNLOCKER(l);		\
		pthread_mutex_unlock(&(l)->lock);			\
	} while (0)
#define MT_lock_try(l)		(pthread_mutex_trylock(&(l)->lock) == 0)
#define MT_lock_destroy(l)				\
	do {						\
		_DBG_LOCK_DESTROY(l);			\
		pthread_mutex_destroy(&(l)->lock);	\
	} while (0)

#ifdef PTHREAD_MUTEX_INITIALIZER
#ifdef LOCK_STATS
#define MT_LOCK_INITIALIZER(n)	= { .lock = PTHREAD_MUTEX_INITIALIZER, .next = (struct MT_Lock *) -1, .name = n, }
#else
#define MT_LOCK_INITIALIZER(n)	= { .lock = PTHREAD_MUTEX_INITIALIZER, .name = n, }
#endif
#endif
#endif

#else

/* if LOCK_STATS is set, we maintain a bunch of counters and maintain
 * a linked list of active locks */
typedef struct MT_Lock {
	ATOMIC_FLAG volatile lock;
	char name[16];
#ifdef LOCK_STATS
	size_t count;
	ATOMIC_TYPE volatile contention;
	ATOMIC_TYPE volatile sleep;
	struct MT_Lock * volatile next;
	const char *locker;
	const char *thread;
#endif
} MT_Lock;

#define MT_lock_set(l)							\
	do {								\
		_DBG_LOCK_COUNT_0(l);					\
		if (ATOMIC_TAS((l)->lock, dummy) != 0) {		\
			/* we didn't get the lock */			\
			int _spincnt = GDKnr_threads > 1 ? 0 : 1023;	\
			_DBG_LOCK_CONTENTION(l);			\
			MT_thread_setlockwait(l);			\
			do {						\
				if (++_spincnt >= 1024) {		\
					_DBG_LOCK_SLEEP(l);		\
					MT_sleep_ms(1);			\
				}					\
			} while (ATOMIC_TAS((l)->lock, dummy) != 0);	\
			MT_thread_setlockwait(NULL);			\
		}							\
		_DBG_LOCK_LOCKER(l);					\
		_DBG_LOCK_COUNT_2(l);					\
	} while (0)
#define MT_lock_try(l)	(ATOMIC_TAS((l)->lock, dummy) == 0)
#define MT_lock_init(l, n)					\
	do {							\
		ATOMIC_CLEAR((l)->lock, dummy);			\
		strncpy((l)->name, (n), sizeof((l)->name));	\
		(l)->name[sizeof((l)->name) - 1] = 0;		\
		_DBG_LOCK_INIT(l);				\
	} while (0)
#define MT_lock_unset(l)				\
		do {					\
			_DBG_LOCK_UNLOCKER(l);		\
			ATOMIC_CLEAR((l)->lock, dummy);	\
		} while (0)
#define MT_lock_destroy(l)	_DBG_LOCK_DESTROY(l)

#ifdef LOCK_STATS
#define MT_LOCK_INITIALIZER(n)	= { .next = (struct MT_Lock *) -1, .name = n, }
#else
#define MT_LOCK_INITIALIZER(n)	= { .name = n, }
#endif

#endif

#ifndef MT_LOCK_INITIALIZER
/* no static initialization possible, so we need dynamic initialization */
#define MT_LOCK_INITIALIZER(n)
#define NEED_MT_LOCK_INIT
#endif

#ifdef LOCK_STATS
gdk_export void GDKlockstatistics(int);
gdk_export MT_Lock * volatile GDKlocklist;
gdk_export ATOMIC_FLAG volatile GDKlocklistlock;
gdk_export ATOMIC_TYPE volatile GDKlockcnt;
gdk_export ATOMIC_TYPE volatile GDKlockcontentioncnt;
gdk_export ATOMIC_TYPE volatile GDKlocksleepcnt;
#endif

/*
 * @- MT Semaphore API
 */
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)

typedef struct {
	HANDLE sema;
	char name[16];
} MT_Sema;

#define MT_sema_init(s, nr, n)						\
	do {								\
		assert((s)->sema == NULL);				\
		strncpy((s)->name, (n), sizeof((s)->name));		\
		(s)->name[sizeof((s)->name) - 1] = 0;			\
		(s)->sema = CreateSemaphore(NULL, nr, 0x7fffffff, NULL); \
	} while (0)
#define MT_sema_destroy(s)			\
	do {					\
		assert((s)->sema != NULL);	\
		CloseHandle((s)->sema);		\
		(s)->sema = NULL;		\
	} while (0)
#define MT_sema_up(s)		ReleaseSemaphore((s)->sema, 1, NULL)
#define MT_sema_down(s)							\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down...\n",	\
				 MT_thread_getname(), __func__, (s)->name); \
		if (WaitForSingleObject((s)->sema, 0) != WAIT_OBJECT_0) { \
			MT_thread_setsemawait(s);			\
			while (WaitForSingleObject((s)->sema, INFINITE) != WAIT_OBJECT_0) \
				;					\
			MT_thread_setsemawait(NULL);			\
		}							\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down complete\n", \
				 MT_thread_getname(), __func__, (s)->name); \
	} while (0)

#elif defined(HAVE_DISPATCH_SEMAPHORE_CREATE)

/* MacOS X */
typedef struct {
	dispatch_semaphore_t sema;
	char name[16];
} MT_Sema;

#define MT_sema_init(s, nr, n)						\
	do {								\
		strncpy((s)->name, (n), sizeof((s)->name));		\
		(s)->name[sizeof((s)->name) - 1] = 0;			\
		(s)->sema = dispatch_semaphore_create((long) (nr));	\
	} while (0)
#define MT_sema_destroy(s)	dispatch_release((s)->sema)
#define MT_sema_up(s)		dispatch_semaphore_signal((s)->sema)
#define MT_sema_down(s)		dispatch_semaphore_wait((s)->sema, DISPATCH_TIME_FOREVER)

#elif defined(_AIX) || defined(__MACH__)

/* simulate semaphores using mutex and condition variable */

typedef struct {
	int cnt;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	char name[16];
} MT_Sema;

#define MT_sema_init(s, nr, n)					\
	do {							\
		strncpy((s)->name, (n), sizeof((s)->name));	\
		(s)->name[sizeof((s)->name) - 1] = 0;		\
		(s)->cnt = (nr);				\
		pthread_mutex_init(&(s)->mutex, 0);		\
		pthread_cond_init(&(s)->cond, 0);		\
	} while (0)
#define MT_sema_destroy(s)				\
	do {						\
		pthread_mutex_destroy(&(s)->mutex);	\
		pthread_cond_destroy(&(s)->cond);	\
	} while (0)
#define MT_sema_up(s)						\
	do {							\
		pthread_mutex_lock(&(s)->mutex);		\
		if ((s)->cnt++ < 0) {				\
			pthread_cond_signal(&(s)->cond);	\
		}						\
		pthread_mutex_unlock(&(s)->mutex);		\
	} while (0)
#define MT_sema_down(s)							\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down...\n",	\
				 MT_thread_getname(), __func__, (s)->name); \
		pthread_mutex_lock(&(s)->mutex);			\
		if (--(s)->cnt < 0) {					\
			MT_thread_setsemawait(s);			\
			do {						\
				pthread_cond_wait(&(s)->cond,		\
						  &(s)->mutex);		\
			} while ((s)->cnt < 0);				\
			MT_thread_setsemawait(NULL);			\
			pthread_mutex_unlock(&(s)->mutex);		\
		}							\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down complete\n", \
				 MT_thread_getname(), __func__, (s)->name); \
	} while (0)

#else

typedef struct {
	sem_t sema;
	char name[16];
} MT_Sema;

#define MT_sema_init(s, nr, n)					\
	do {							\
		strncpy((s)->name, (n), sizeof((s)->name));	\
		(s)->name[sizeof((s)->name) - 1] = 0;		\
		sem_init(&(s)->sema, 0, nr);			\
	} while (0)
#define MT_sema_destroy(s)	sem_destroy(&(s)->sema)
#define MT_sema_up(s)						\
	do {							\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s up\n",	\
				 MT_thread_getname(), __func__, (s)->name); \
		sem_post(&(s)->sema);				\
	} while (0)
#define MT_sema_down(s)							\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down...\n",	\
				 MT_thread_getname(), __func__, (s)->name); \
		if (sem_trywait(&(s)->sema) != 0) {			\
			MT_thread_setsemawait(s);			\
			while (sem_wait(&(s)->sema) != 0)		\
				;					\
			MT_thread_setsemawait(NULL);			\
		}							\
		TEMDEBUG fprintf(stderr, "#%s: %s: sema %s down complete\n", \
				 MT_thread_getname(), __func__, (s)->name); \
	} while (0)

#endif

gdk_export void MT_thread_setlockwait(MT_Lock *lock);
gdk_export void MT_thread_setsemawait(MT_Sema *sema);

gdk_export int MT_check_nr_cores(void);

#endif /*_GDK_SYSTEM_H_*/
