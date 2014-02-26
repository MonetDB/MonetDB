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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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

#define MT_geterrno()	errno
#define MT_seterrno(x)	errno=x

/* lock performance tracing */
/* #define MT_LOCK_TRACE 1 */
#ifdef MT_LOCK_TRACE
gdk_export MT_Id MT_locktrace;
gdk_export void MT_locktrace_start(void);
gdk_export void MT_locktrace_end(void);
gdk_export unsigned long long MT_locktrace_cnt[65536];
gdk_export char *MT_locktrace_nme[65536];
gdk_export unsigned long long MT_clock(void);

#define MT_locktrace_hash(_id)	((int) (((lng) ((size_t) (_id))) ^ (((lng) ((size_t) (_id))) >> 16)) & 65535)
#define MT_log_trace(_impl, _object, _action, _caller, _fp, _pat)	\
	do {								\
		unsigned long long _c=0;				\
		if (MT_locktrace)					\
			_c = MT_getpid() == MT_locktrace ? MT_clock() : 0; \
		MT_log(_impl, _object, _action, _caller, _fp);		\
		if (_c)						\
			MT_locktrace_cnt[MT_locktrace_hash(_pat)] += MT_clock() - _c; \
	} while(0)
#define MT_locktrace_set(s, n)						\
	do {								\
		int _i = MT_locktrace_hash(s);				\
		if (MT_locktrace_nme[_i] && MT_locktrace_nme[_i] != (n)) { \
			printf("MT_locktrace: name collision %s hides %s\n", \
			       MT_locktrace_nme[_i], (n));		\
		} else							\
			MT_locktrace_nme[_i] = (n);			\
	} while (0)
#else
#define MT_log_trace(_impl, _object, _action, _caller, _fp, _pat) MT_log(_impl, _object, _action, _caller, _fp)
#define MT_locktrace_set(s, n)
#endif

#define MT_log(_impl, _object, _action, _caller, _fp)			\
	do {								\
		TEMDEBUG {						\
			fprintf(_fp, "%s: " _action "(" PTRFMT ")\n",	\
				_caller, PTRFMTCAST(void*) _object);	\
			fflush(_fp);					\
		}							\
		_impl;							\
	} while (0)

/* API */

/*
 * @- MT Thread Api
 */
typedef size_t MT_Id;		/* thread number. will not be zero */

enum MT_thr_detach { MT_THR_JOINABLE, MT_THR_DETACHED };

gdk_export int MT_create_thread(MT_Id *t, void (*function) (void *),
				void *arg, enum MT_thr_detach d);
gdk_export void MT_exit_thread(int status)
	__attribute__((__noreturn__));
gdk_export void MT_exiting_thread(void);
gdk_export void MT_global_exit(int status)
	__attribute__((__noreturn__));
gdk_export MT_Id MT_getpid(void);
gdk_export int MT_join_thread(MT_Id t);
gdk_export int MT_kill_thread(MT_Id t);

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
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
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

#ifdef ATOMIC_LOCK

typedef pthread_mutex_t MT_Lock;

#define MT_lock_init(l, n)					\
	do {							\
		pthread_mutex_init((pthread_mutex_t*) (l), 0);	\
		MT_locktrace_set(l, n);				\
	} while (0)
#define MT_lock_destroy(l)	pthread_mutex_destroy((pthread_mutex_t*) (l))
#define MT_lock_set(l, n)	MT_log_trace(pthread_mutex_lock((pthread_mutex_t *) (l)), (l), "MT_set_lock", n, stderr, (l))
#define MT_lock_unset(l, n)	MT_log(pthread_mutex_unlock((pthread_mutex_t *) (l)), (l), "MT_unset_lock", n, stderr)
#define MT_lock_try(l)		pthread_mutex_trylock((pthread_mutex_t *) (l))
#define MT_lock_dump(l, fp, n)	MT_log(/*nothing*/, &(l), "MT_dump_lock", n, fp)

#ifdef PTHREAD_MUTEX_INITIALIZER
#define MT_LOCK_INITIALIZER(name)	= PTHREAD_MUTEX_INITIALIZER
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
#define _DBG_LOCK_COUNT_0(l, n)		ATOMIC_INC(GDKlockcnt, dummy, n)
#define _DBG_LOCK_LOCKER(l, n)		((l)->locker = (n))
#define _DBG_LOCK_CONTENTION(l, n)					\
	do {								\
		TEMDEBUG fprintf(stderr, "#lock %s contention in %s\n", (l)->name, n); \
		ATOMIC_INC(GDKlockcontentioncnt, dummy, n);		\
	} while (0)
#define _DBG_LOCK_SLEEP(l, n)					\
	do {							\
		if (_spincnt == 1024)				\
			ATOMIC_INC(GDKlocksleepcnt, dummy, n);	\
	} while (0)
#define _DBG_LOCK_COUNT_1(l)			\
	do {					\
		(l)->contention++;		\
		(l)->sleep += _spincnt >= 1024;	\
	} while (0)
#define _DBG_LOCK_COUNT_2(l)						\
	do {								\
		(l)->count++;						\
		if ((l)->next == (struct MT_Lock *) -1) {		\
			while (ATOMIC_TAS(GDKlocklistlock, dummy, "") != 0) \
				;					\
			(l)->next = GDKlocklist;			\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(GDKlocklistlock, dummy, "");	\
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
			while (ATOMIC_TAS(GDKlocklistlock, dummy, "") != 0) \
				;					\
			(l)->next = GDKlocklist;			\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(GDKlocklistlock, dummy, "");	\
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
			memcpy(_p, l, sizeof(MT_Lock));			\
			while (ATOMIC_TAS(GDKlocklistlock, dummy, "") != 0) \
				;					\
			_p->next = GDKlocklist;				\
			GDKlocklist = _p;				\
			for (_p = GDKlocklist; _p; _p = _p->next)	\
				if (_p->next == (l)) {			\
					_p->next = (l)->next;		\
					break;				\
				}					\
			ATOMIC_CLEAR(GDKlocklistlock, dummy, "");	\
		}							\
	} while (0)

#else

#define MT_LOCK_INITIALIZER(name)	= ATOMIC_FLAG_INIT

#define _DBG_LOCK_COUNT_0(l, n)		((void) (n))
#define _DBG_LOCK_CONTENTION(l, n)	((void) (n))
#define _DBG_LOCK_SLEEP(l, n)		((void) (n))
#define _DBG_LOCK_COUNT_1(l)		((void) 0)
#define _DBG_LOCK_COUNT_2(l)		((void) 0)
#define _DBG_LOCK_INIT(l, n)		((void) (n))
#define _DBG_LOCK_DESTROY(l)		((void) 0)
#define _DBG_LOCK_LOCKER(l, n)		((void) (n))

#endif

#define MT_lock_set(l, n)						\
	do {								\
		_DBG_LOCK_COUNT_0(l, n);				\
		if (ATOMIC_TAS((l)->lock, dummy, n) != 0) {		\
			/* we didn't get the lock */			\
			int _spincnt = GDKnr_threads > 1 ? 0 : 1023;	\
			_DBG_LOCK_CONTENTION(l, n);			\
			do {						\
				if (++_spincnt >= 1024) {		\
					_DBG_LOCK_SLEEP(l, n);		\
					MT_sleep_ms(_spincnt >> 10);	\
				}					\
			} while (ATOMIC_TAS((l)->lock, dummy, n) != 0); \
			_DBG_LOCK_COUNT_1(l);				\
		}							\
		_DBG_LOCK_LOCKER(l, n);					\
		_DBG_LOCK_COUNT_2(l);					\
	} while (0)
#define MT_lock_init(l, n)				\
	do {						\
		ATOMIC_CLEAR((l)->lock, dummy, n);	\
		_DBG_LOCK_INIT(l, n);			\
	} while (0)
#define MT_lock_unset(l, n)					\
		do {						\
			_DBG_LOCK_LOCKER(l, n);			\
			ATOMIC_CLEAR((l)->lock, dummy, n);	\
		} while (0)
#define MT_lock_destroy(l)	_DBG_LOCK_DESTROY(l)
/* return 0 on success, -1 on failure to get the lock */
#define MT_lock_try(l)		((ATOMIC_TAS((l)->lock, dummy, dummy) == 0) - 1)

#endif

/*
 * @- MT Semaphore API
 */
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)

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

typedef pthread_sema_t MT_Sema;

#define MT_sema_init(s, nr, n)			\
	do {					\
		pthread_sema_init((s), 0, nr);	\
		MT_locktrace_set((s), n);	\
	} while (0)
#define MT_sema_destroy(s)	pthread_sema_destroy(s)
#define MT_sema_up(s, n)	MT_log(pthread_sema_up(s), (s), "MT_up_sema", n, stderr)
#define MT_sema_down(s, n)	MT_log_trace(pthread_sema_down(s), (s), "MT_down_sema", n, stderr, s)

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
