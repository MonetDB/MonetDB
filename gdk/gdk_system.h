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
 * Copyright August 2008-2011 MonetDB B.V.
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
   incompatible way
 */
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
gdk_export int gprof_pthread_create(pthread_t * __restrict, __const pthread_attr_t * __restrict, void *(*fcn) (void *), void *__restrict);
#define pthread_create gprof_pthread_create
#endif
#endif
#endif

#ifdef HAVE_SEMAPHORE_H
# include <semaphore.h>
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>  /* prerequisite of sys/sysctl on OpenBSD */
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
gdk_export void MT_locktrace_start();
gdk_export void MT_locktrace_end();
gdk_export unsigned long long MT_locktrace_cnt[65536];
gdk_export char *MT_locktrace_nme[65536];
gdk_export unsigned long long MT_clock();

#define MT_locktrace_hash(_id)  ((int) (((lng) ((size_t) _id))^(((lng) ((size_t) _id))>>16))&65535)
#define MT_log_trace(_impl, _object, _action, _caller, _fp, _pat) do { unsigned long long _c=0; if (MT_locktrace) _c=(MT_getpid() == MT_locktrace)?MT_clock():0; MT_log(_impl, _object, _action, _caller, _fp); if (_c) { MT_locktrace_cnt[MT_locktrace_hash(_pat)] += MT_clock() - _c; } } while(0)
#define MT_locktrace_set(s,n) {int _i = MT_locktrace_hash(s); \
                               if (MT_locktrace_nme[_i] && MT_locktrace_nme[_i] != (n)) { \
                                  printf("MT_locktrace: name collision %s hides %s\n", MT_locktrace_nme[_i], (n)); \
                               } else MT_locktrace_nme[_i] = (n); }
#else
#define MT_log_trace(_impl, _object, _action, _caller, _fp, _pat) MT_log(_impl, _object, _action, _caller, _fp)
#define MT_locktrace_set(s,n)
#endif

#define MT_log(_impl, _object, _action, _caller, _fp) do { if (GDKdebug & 1024) { fprintf(_fp, "%s: " _action "(" PTRFMT ")\n", _caller, PTRFMTCAST(void*) _object); fflush(_fp); } _impl; } while (0)

/* API */

/*
 * @- MT Thread Api
 */
typedef size_t MT_Id;		/* thread number. will not be zero */

enum MT_thr_detach { MT_THR_JOINABLE, MT_THR_DETACHED };

gdk_export int MT_create_thread(MT_Id *t, void (*function) (void *), void *arg, enum MT_thr_detach d);
gdk_export void MT_exit_thread(int status)
	__attribute__((__noreturn__));
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
gdk_export void pthread_mutex_init(pthread_mutex_t *, const pthread_mutexattr_t *);
gdk_export void pthread_mutex_destroy(pthread_mutex_t *);
gdk_export int pthread_mutex_lock(pthread_mutex_t *);
gdk_export int pthread_mutex_trylock(pthread_mutex_t *);
gdk_export int pthread_mutex_unlock(pthread_mutex_t *);
#endif

typedef pthread_mutex_t MT_Lock;

#define MT_lock_init(l,n)    { pthread_mutex_init((pthread_mutex_t*) l, 0); MT_locktrace_set(l,n); }
#define MT_lock_destroy(l)   pthread_mutex_destroy((pthread_mutex_t*) l)
#define MT_lock_set(l,n)     MT_log_trace(pthread_mutex_lock((pthread_mutex_t *) l), l, "MT_set_lock", n, stderr, l)
#define MT_lock_unset(l,n)   MT_log(pthread_mutex_unlock((pthread_mutex_t *) l), l, "MT_unset_lock", n, stderr)
#define MT_lock_try(l)       pthread_mutex_trylock((pthread_mutex_t *) l)
#define MT_lock_dump(l,fp,n) MT_log(/*nothing*/, &l, "MT_dump_lock", n, fp)

gdk_export MT_Lock MT_system_lock;

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
#define pthread_sema_t       sem_t
#define pthread_sema_init    sem_init
#define pthread_sema_destroy sem_destroy
#define pthread_sema_up      sem_post
#define pthread_sema_down(x) while(sem_wait(x))
#endif

typedef pthread_sema_t MT_Sema;

#define MT_sema_init(s,nr,n) { pthread_sema_init(s,0,nr); MT_locktrace_set(s,n); }
#define MT_sema_destroy(s)   pthread_sema_destroy(s)
#define MT_sema_up(s,n)      MT_log(pthread_sema_up(s), s, "MT_up_sema", n, stderr)
#define MT_sema_down(s,n)    MT_log_trace(pthread_sema_down(s), s, "MT_down_sema", n, stderr, s)
#define MT_sema_dump(s,fp,n) MT_log(/*nothing*/, s, "MT_dump_sema", n, fp)

/*
 * @- MT Conditional Variable API
 */
#if !defined(HAVE_PTHREAD_H) && defined(_MSC_VER)
typedef struct {
	int waiters_count;	/* number of waiting threads */
	CRITICAL_SECTION waiters_count_lock; /* serialize access to waiters_count_ */
	HANDLE sema;	  /* queue up threads waiting for condition */
} pthread_cond_t;
typedef void *pthread_condattr_t;
gdk_export int pthread_cond_init(pthread_cond_t *, pthread_condattr_t *);
gdk_export int pthread_cond_destroy(pthread_cond_t *);
gdk_export int pthread_cond_signal(pthread_cond_t *);
gdk_export int pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
#endif
typedef pthread_cond_t MT_Cond;

#define MT_cond_init(c,n)    { pthread_cond_init((pthread_cond_t*) c, NULL); MT_locktrace_set(c,n); }
#define MT_cond_destroy(c)   pthread_cond_destroy((pthread_cond_t*) c)
#define MT_cond_signal(c,n)  MT_log(pthread_cond_signal((pthread_cond_t*) c), c, "MT_signal_cond", n, stderr)
#define MT_cond_wait(c,l,n)  MT_log_trace(pthread_cond_wait((pthread_cond_t*) c, (pthread_mutex_t *) l), c, "MT_wait_cond", n, stderr, c)

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
