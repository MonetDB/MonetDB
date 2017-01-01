/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _GDK_SYSTEM_H_
#define _GDK_SYSTEM_H_

#ifdef NATIVE_WIN32
#ifndef LIBGDK
#define gdk_export extern __declspec(dllimport)
#else
#define gdk_export extern __declspec(dllexport)
#endif
#else
#define gdk_export extern
#endif

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
#ifndef WIN32
/* Linux gprof messes up on multithreaded programs */
#ifdef PROFILE
/* Linux gprof messes up on multithreaded programs */
gdk_export int gprof_pthread_create(pthread_t * __restrict,
				    __const pthread_attr_t * __restrict,
				    void *(*fcn) (void *),
				    void *__restrict);
#define pthread_create gprof_pthread_create
#endif
#endif
#endif

#ifdef HAVE_SEMAPHORE_H
# include <semaphore.h>
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

/* API */

/*
 * @- MT Thread Api
 */
typedef size_t MT_Id;		/* thread number. will not be zero */

enum MT_thr_detach { MT_THR_JOINABLE, MT_THR_DETACHED };

gdk_export int MT_create_thread(MT_Id *t, void (*function) (void *),
				void *arg, enum MT_thr_detach d);
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
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
typedef HANDLE pthread_mutex_t;
typedef void *pthread_mutexattr_t;
gdk_export void pthread_mutex_init(pthread_mutex_t *,
				   const pthread_mutexattr_t *);
gdk_export void pthread_mutex_destroy(pthread_mutex_t *);
gdk_export int pthread_mutex_lock(pthread_mutex_t *);
gdk_export int pthread_mutex_trylock(pthread_mutex_t *);
gdk_export int pthread_mutex_unlock(pthread_mutex_t *);
#endif

#include "gdk_atomic.h"

/* define this if you want to use pthread (or Windows) locks instead
 * of atomic instructions for locking (latching) */
/* #define USE_PTHREAD_LOCKS */

#ifdef USE_PTHREAD_LOCKS

typedef struct {
	pthread_mutex_t lock;
	const char *name;
} MT_Lock;

#define MT_lock_init(l, n)				\
	do {						\
		(l)->name = (n);			\
		pthread_mutex_init(&(l)->lock, 0);	\
	} while (0)
#define MT_lock_destroy(l)	pthread_mutex_destroy(&(l)->lock)
#define MT_lock_set(l)							\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: locking %s...\n",	\
				 __func__, (l)->name);			\
		pthread_mutex_lock(&(l)->lock);				\
		TEMDEBUG fprintf(stderr, "#%s: locking %s complete\n",	\
				 __func__, (l)->name);			\
	} while (0)
#define MT_lock_unset(l)						\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: unlocking %s\n",		\
				 __func__, (l)->name);			\
		pthread_mutex_unlock(&(l)->lock);			\
	} while (0)

#ifdef PTHREAD_MUTEX_INITIALIZER
#define MT_LOCK_INITIALIZER(name)	= { PTHREAD_MUTEX_INITIALIZER, name }
#else
/* no static initialization possible, so we need dynamic initialization */
#define MT_LOCK_INITIALIZER(name)
#define NEED_MT_LOCK_INIT
#endif

#else

/* if NDEBUG is not set, i.e., if assertions are enabled, we maintain
 * a bunch of counters and maintain a linked list of active locks */
typedef struct MT_Lock {
	ATOMIC_FLAG volatile lock;
#ifndef NDEBUG
	size_t count;
	size_t contention;
	size_t sleep;
	struct MT_Lock * volatile next;
	const char *name;
	const char *locker;
#endif
} MT_Lock;

#ifndef NDEBUG

#define MT_LOCK_INITIALIZER(name)	= {0, 0, 0, 0, (struct MT_Lock *) -1, name, NULL}

gdk_export void GDKlockstatistics(int);
gdk_export MT_Lock * volatile GDKlocklist;
gdk_export ATOMIC_FLAG volatile GDKlocklistlock;
gdk_export ATOMIC_TYPE volatile GDKlockcnt;
gdk_export ATOMIC_TYPE volatile GDKlockcontentioncnt;
gdk_export ATOMIC_TYPE volatile GDKlocksleepcnt;
#define _DBG_LOCK_COUNT_0(l, n)		(void) ATOMIC_INC(GDKlockcnt, dummy)
#define _DBG_LOCK_LOCKER(l, n)		((l)->locker = (n))
#define _DBG_LOCK_CONTENTION(l, n)					\
	do {								\
		TEMDEBUG fprintf(stderr, "#lock %s contention in %s\n", \
				 (l)->name, n);				\
		(void) ATOMIC_INC(GDKlockcontentioncnt, dummy);		\
		(l)->contention++;					\
	} while (0)
#define _DBG_LOCK_SLEEP(l, n)						\
	do {								\
		if (_spincnt == 1024)					\
			(void) ATOMIC_INC(GDKlocksleepcnt, dummy);	\
		(l)->sleep++;						\
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
	} while (0)
#define _DBG_LOCK_INIT(l, n)						\
	do {								\
		(l)->count = (l)->contention = (l)->sleep = 0;		\
		(l)->name = (n);					\
		_DBG_LOCK_LOCKER(l, NULL);				\
		/* if name starts with "sa_" don't link in GDKlocklist */ \
		/* since the lock is in memory that is governed by the */ \
		/* SQL storage allocator, and hence we have no control */ \
		/* over when the lock is destroyed and the memory freed */ \
		if (strncmp((n), "sa_", 3) != 0) {			\
			while (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) \
				;					\
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
			MT_Lock * volatile _p;				\
			/* save a copy for statistical purposes */	\
			_p = GDKmalloc(sizeof(MT_Lock));		\
			while (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) \
				;					\
			if (_p) {					\
				memcpy(_p, l, sizeof(MT_Lock));		\
				_p->next = GDKlocklist;			\
				GDKlocklist = _p;			\
			}						\
			for (_p = GDKlocklist; _p; _p = _p->next)	\
				if (_p->next == (l)) {			\
					_p->next = (l)->next;		\
					break;				\
				}					\
			ATOMIC_CLEAR(GDKlocklistlock, dummy);		\
		}							\
	} while (0)

#else

#define MT_LOCK_INITIALIZER(name)	= ATOMIC_FLAG_INIT

#define _DBG_LOCK_COUNT_0(l, n)		((void) (n))
#define _DBG_LOCK_CONTENTION(l, n)	((void) (n))
#define _DBG_LOCK_SLEEP(l, n)		((void) (n))
#define _DBG_LOCK_COUNT_2(l)		((void) 0)
#define _DBG_LOCK_INIT(l, n)		((void) (n))
#define _DBG_LOCK_DESTROY(l)		((void) 0)
#define _DBG_LOCK_LOCKER(l, n)		((void) (n))

#endif

#define MT_lock_set(l)							\
	do {								\
		_DBG_LOCK_COUNT_0(l, __func__);				\
		if (ATOMIC_TAS((l)->lock, dummy) != 0) {		\
			/* we didn't get the lock */			\
			int _spincnt = GDKnr_threads > 1 ? 0 : 1023;	\
			_DBG_LOCK_CONTENTION(l, __func__);		\
			do {						\
				if (++_spincnt >= 1024) {		\
					_DBG_LOCK_SLEEP(l, __func__);	\
					MT_sleep_ms(1);			\
				}					\
			} while (ATOMIC_TAS((l)->lock, dummy) != 0);	\
		}							\
		_DBG_LOCK_LOCKER(l, __func__);				\
		_DBG_LOCK_COUNT_2(l);					\
	} while (0)
#define MT_lock_init(l, n)			\
	do {					\
		ATOMIC_CLEAR((l)->lock, dummy);	\
		_DBG_LOCK_INIT(l, n);		\
	} while (0)
#define MT_lock_unset(l)				\
		do {					\
			_DBG_LOCK_LOCKER(l, __func__);	\
			ATOMIC_CLEAR((l)->lock, dummy);	\
		} while (0)
#define MT_lock_destroy(l)	_DBG_LOCK_DESTROY(l)

#endif

/*
 * @- MT Semaphore API
 */
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)

typedef HANDLE pthread_sema_t;
gdk_export void pthread_sema_init(pthread_sema_t *s, int flag, int nresources);
gdk_export void pthread_sema_destroy(pthread_sema_t *s);
gdk_export void pthread_sema_up(pthread_sema_t *s);
gdk_export void pthread_sema_down(pthread_sema_t *s);

#elif defined(_AIX) || defined(__MACH__)

typedef struct {
	int cnt;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} pthread_sema_t;

gdk_export void pthread_sema_init(pthread_sema_t *s, int flag, int nresources);
gdk_export void pthread_sema_destroy(pthread_sema_t *s);
gdk_export void pthread_sema_up(pthread_sema_t *s);
gdk_export void pthread_sema_down(pthread_sema_t *s);

#else

#define pthread_sema_t		sem_t
#define pthread_sema_init	sem_init
#define pthread_sema_destroy	sem_destroy
#define pthread_sema_up		sem_post
#define pthread_sema_down(x)	while(sem_wait(x))

#endif

typedef struct {
	pthread_sema_t sema;
	const char *name;
} MT_Sema;

#define MT_sema_init(s, nr, n)				\
	do {						\
		(s)->name = (n);			\
		pthread_sema_init(&(s)->sema, 0, nr);	\
	} while (0)
#define MT_sema_destroy(s)	pthread_sema_destroy(&(s)->sema)
#define MT_sema_up(s)						\
	do {							\
		TEMDEBUG fprintf(stderr, "#%s: sema %s up\n",	\
				 __func__, (s)->name);		\
		pthread_sema_up(&(s)->sema);			\
	} while (0)
#define MT_sema_down(s)							\
	do {								\
		TEMDEBUG fprintf(stderr, "#%s: sema %s down...\n",	\
				 __func__, (s)->name);			\
		pthread_sema_down(&(s)->sema);				\
		TEMDEBUG fprintf(stderr, "#%s: sema %s down complete\n", \
				 __func__, (s)->name);			\
	} while (0)

gdk_export int MT_check_nr_cores(void);

/*
 * @- Timers
 * The following relative timers are available for inspection.
 * Note that they may consume recognizable overhead.
 *
 */
gdk_export lng GDKusec(void);
gdk_export int GDKms(void);

#endif /*_GDK_SYSTEM_H_*/
