/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _GDK_SYSTEM_H_
#define _GDK_SYSTEM_H_

/* if __has_attribute is not known to the preprocessor, we ignore
 * attributes completely; if it is known, use it to find out whether
 * specific attributes that we use are known */
#ifndef __has_attribute
#ifndef __GNUC__
/* we can define __has_attribute as 1 since we define __attribute__ as empty */
#define __has_attribute(attr)	1
#ifndef __attribute__
#define __attribute__(attr)	/* empty */
#endif
#else
/* older GCC does have attributes, but not __has_attribute and not all
 * attributes that we use are known */
#define __has_attribute__access__ 0
#define __has_attribute__alloc_size__ 1
#define __has_attribute__cold__ 1
#define __has_attribute__const__ 1
#define __has_attribute__constructor__ 1
#define __has_attribute__designated_init__ 0
#define __has_attribute__format__ 1
#define __has_attribute__malloc__ 1
#define __has_attribute__nonnull__ 1
#define __has_attribute__nonstring__ 0
#define __has_attribute__pure__ 1
#define __has_attribute__returns_nonnull__ 0
#define __has_attribute__visibility__ 1
#define __has_attribute__warn_unused_result__ 1
#define __has_attribute(attr)	__has_attribute##attr
#endif
#endif
#if !__has_attribute(__access__)
#define __access__(...)
#endif
#if !__has_attribute(__alloc_size__)
#define __alloc_size__(...)
#endif
#if !__has_attribute(__cold__)
#define __cold__
#endif
#if !__has_attribute(__const__)
#define __const__
#endif
#if !__has_attribute(__constructor__)
#define __constructor__
#endif
#if !__has_attribute(__designated_init__)
#define __designated_init__
#endif
#if !__has_attribute(__format__)
#define __format__(...)
#endif
/* attribute malloc with argument seems to have been introduced in gcc 13 */
#if !__has_attribute(__malloc__)
#define __malloc__
#define __malloc__(...)
#elif !defined(__GNUC__) || __GNUC__ < 13
#define __malloc__(...)
#endif
#if !__has_attribute(__nonnull__)
#define __nonnull__(...)
#endif
#if !__has_attribute(__nonstring__)
#define __nonstring__
#endif
#if !__has_attribute(__pure__)
#define __pure__
#endif
#if !__has_attribute(__returns_nonnull__)
#define __returns_nonnull__
#endif
#if !__has_attribute(__visibility__)
#define __visibility__(...)
#elif defined(__CYGWIN__)
#define __visibility__(...)
#endif
#if !__has_attribute(__warn_unused_result__)
#define __warn_unused_result__
#endif

/* unreachable code */
#ifdef __has_builtin
#if __has_builtin(__builtin_unreachable)
#define MT_UNREACHABLE()	do { assert(0); __builtin_unreachable(); } while (0)
#endif
#endif
#ifndef MT_UNREACHABLE
#if defined(_MSC_VER)
#define MT_UNREACHABLE()	do { assert(0); __assume(0); } while (0)
#else
#define MT_UNREACHABLE()	do { assert(0); GDKfatal("Unreachable C code path reached"); } while (0)
#endif
#endif

/* also see gdk.h for these */
#define THRDMASK	(1U)
#define TEMMASK		(1U<<10)

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

#if defined(__APPLE__) && defined(__GNUC__)
/* GCC-12 installed with Homebrew on MacOS has a bug which makes
 * including <dispatch/dispatch.h> impossible.  However we need that for
 * properly working semaphores, so we have this bit of code to work
 * around the bug. */
#define HAVE_DISPATCH_DISPATCH_H 1
#define HAVE_DISPATCH_SEMAPHORE_CREATE 1
#if __has_attribute(__swift_attr__)
#define OS_SWIFT_UNAVAILABLE_FROM_ASYNC(msg) \
       __attribute__((__swift_attr__("@_unavailableFromAsync(message: \"" msg "\")")))
#else
#define OS_SWIFT_UNAVAILABLE_FROM_ASYNC(msg)
#endif
#define OS_ASSUME_PTR_ABI_SINGLE_BEGIN __ASSUME_PTR_ABI_SINGLE_BEGIN
#define OS_ASSUME_PTR_ABI_SINGLE_END __ASSUME_PTR_ABI_SINGLE_END
#define OS_UNSAFE_INDEXABLE __unsafe_indexable
#define OS_HEADER_INDEXABLE __header_indexable
#define OS_COUNTED_BY(N) __counted_by(N)
#define OS_SIZED_BY(N) __sized_by(N)
#endif
#ifdef HAVE_DISPATCH_DISPATCH_H
#include <dispatch/dispatch.h>
#endif

#ifdef HAVE_SYS_PARAM_H
# include <sys/param.h>	   /* prerequisite of sys/sysctl on OpenBSD */
#endif
#ifdef BSD /* BSD macro is defined in sys/param.h */
# include <sys/sysctl.h>
#endif

/* new pthread interface, where the thread id changed to a struct */
#ifdef PTW32_VERSION
#define PTW32 1
#endif

#include "matomic.h"

/* debug and errno integers */
gdk_export ATOMIC_TYPE GDKdebug;
gdk_export void GDKsetdebug(unsigned debug);
gdk_export unsigned GDKgetdebug(void);

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
#define MT_NAME_LEN	32	/* length of thread/semaphore/etc. names */

#define UNKNOWN_THREAD "unknown thread"
typedef int64_t lng;

typedef struct QryCtx {
	lng starttime;
	lng endtime;
	struct bstream *bs;
	ATOMIC_TYPE datasize;
	ATOMIC_BASE_TYPE maxmem;
} QryCtx;

gdk_export bool THRhighwater(void);
gdk_export bool MT_thread_init(void);
gdk_export int MT_create_thread(MT_Id *t, void (*function) (void *),
				void *arg, enum MT_thr_detach d,
				const char *threadname);
gdk_export gdk_return MT_thread_init_add_callback(void (*init)(void *), void (*destroy)(void *), void *data);
gdk_export bool MT_thread_register(void);
gdk_export void MT_thread_deregister(void);
gdk_export const char *MT_thread_getname(void);
gdk_export void *MT_thread_getdata(void);
gdk_export void MT_thread_setdata(void *data);
gdk_export void MT_exiting_thread(void);
gdk_export MT_Id MT_getpid(void);
gdk_export int MT_join_thread(MT_Id t);
gdk_export QryCtx *MT_thread_get_qry_ctx(void);
gdk_export void MT_thread_set_qry_ctx(QryCtx *ctx);
gdk_export void GDKsetbuf(char *);
gdk_export char *GDKgetbuf(void);

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
#include "matomic.h"

/* define this to keep lock statistics (can be expensive) */
/* #define LOCK_STATS 1 */

/* define this to keep track of which locks a thread has acquired */
#ifndef NDEBUG			/* normally only in debug builds */
#ifndef __COVERITY__
#define LOCK_OWNER 1
#endif
#endif

#ifndef LOCK_OWNER
#define MT_thread_add_mylock(l) ((void) 0)
#define MT_thread_del_mylock(l) ((void) 0)
#endif

#ifdef LOCK_STATS
#include "gdk_tracer.h"

#define _DBG_LOCK_COUNT_0(l)					\
	do {							\
		ATOMIC_INC(&GDKlockcnt);			\
		TRC_DEBUG(TEM, "Locking %s...\n", (l)->name);	\
	} while (0)

#define _DBG_LOCK_LOCKER(l)				\
	(						\
		(l)->locker = __func__,			\
		(l)->thread = MT_thread_getname(),	\
		MT_thread_add_mylock(l)			\
	)

#define _DBG_LOCK_UNLOCKER(l)					\
	do {							\
		MT_thread_del_mylock(l);			\
		(l)->locker = __func__;				\
		(l)->thread = NULL;				\
		TRC_DEBUG(TEM, "Unlocking %s\n", (l)->name);	\
	} while (0)

#define _DBG_LOCK_CONTENTION(l)						\
	do {								\
		TRC_DEBUG(TEM, "Lock %s contention\n", (l)->name);	\
		ATOMIC_INC(&GDKlockcontentioncnt);			\
		ATOMIC_INC(&(l)->contention);				\
	} while (0)

#define _DBG_LOCK_SLEEP(l)	(ATOMIC_INC(&(l)->sleep))

#define _DBG_LOCK_COUNT_2(l)						\
	do {								\
		(l)->count++;						\
		if ((l)->next == (struct MT_Lock *) -1) {		\
			while (ATOMIC_TAS(&GDKlocklistlock) != 0)	\
				;					\
			(l)->next = GDKlocklist;			\
			(l)->prev = NULL;				\
			if (GDKlocklist)				\
				GDKlocklist->prev = (l);		\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(&GDKlocklistlock);			\
		}							\
		TRC_DEBUG(TEM, "Locking %s complete\n", (l)->name);	\
	} while (0)

#define _DBG_LOCK_INIT(l)					\
	do {							\
		(l)->count = 0;					\
		ATOMIC_INIT(&(l)->contention, 0);		\
		ATOMIC_INIT(&(l)->sleep, 0);			\
		(l)->locker = NULL;				\
		(l)->thread = NULL;				\
		while (ATOMIC_TAS(&GDKlocklistlock) != 0)	\
			;					\
		if (GDKlocklist)				\
			GDKlocklist->prev = (l);		\
		(l)->next = GDKlocklist;			\
		(l)->prev = NULL;				\
		GDKlocklist = (l);				\
		ATOMIC_CLEAR(&GDKlocklistlock);			\
	} while (0)

#define _DBG_LOCK_DESTROY(l)					\
	do {							\
		while (ATOMIC_TAS(&GDKlocklistlock) != 0)	\
			;					\
		if ((l)->next)					\
			(l)->next->prev = (l)->prev;		\
		if ((l)->prev)					\
			(l)->prev->next = (l)->next;		\
		else if (GDKlocklist == (l))			\
			GDKlocklist = (l)->next;		\
		ATOMIC_CLEAR(&GDKlocklistlock);			\
	} while (0)

#else

#ifdef LOCK_OWNER
#define _DBG_LOCK_LOCKER(l)				\
	(						\
		(l)->locker = __func__,			\
		(l)->thread = MT_thread_getname(),	\
		MT_thread_add_mylock(l)			\
	)

#define _DBG_LOCK_UNLOCKER(l)					\
	do {							\
		MT_thread_del_mylock(l);			\
		(l)->locker = __func__;				\
		(l)->thread = NULL;				\
	} while (0)
#else
#define _DBG_LOCK_LOCKER(l)		((void) 0)
#define _DBG_LOCK_UNLOCKER(l)		((void) 0)
#endif

#define _DBG_LOCK_COUNT_0(l)		((void) 0)
#define _DBG_LOCK_CONTENTION(l)		((void) 0)
#define _DBG_LOCK_SLEEP(l)		((void) 0)
#define _DBG_LOCK_COUNT_2(l)		((void) 0)
#define _DBG_LOCK_INIT(l)		((void) 0)
#define _DBG_LOCK_DESTROY(l)		((void) 0)

#endif

#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
typedef struct MT_Lock {
	CRITICAL_SECTION lock;
	char name[MT_NAME_LEN];
#ifdef LOCK_STATS
	size_t count;
	ATOMIC_TYPE contention;
	ATOMIC_TYPE sleep;
	struct MT_Lock *volatile next;
	struct MT_Lock *volatile prev;
#endif
#if defined(LOCK_STATS) || defined(LOCK_OWNER)
	const char *locker;
	const char *thread;
#endif
#ifdef LOCK_OWNER
	struct MT_Lock *nxt;
#endif
} MT_Lock;

/* Windows defines read as _read and adds a deprecation warning to read
 * if you were to still use that.  We need the token "read" here.  We
 * cannot simply #undef read, since that messes up the deprecation
 * stuff.  So we define _read as read to change the token back to "read"
 * where replacement stops (recursive definitions are allowed in C and
 * are handled well).  After our use, we remove the definition of _read
 * so everything reverts back to the way it was.  Bonus: this also works
 * if "read" was not defined. */
#define _read read
#pragma section(".CRT$XCU", read)
#undef _read
#ifdef _WIN64
#define _LOCK_PREF_ ""
#else
#define _LOCK_PREF_ "_"
#endif
#define MT_LOCK_INITIALIZER(n) { 0 };					\
static void wininit_##n(void)						\
{									\
	MT_lock_init(&n, #n);						\
}									\
__declspec(allocate(".CRT$XCU")) void (*wininit_##n##_)(void) = wininit_##n; \
__pragma(comment(linker, "/include:" _LOCK_PREF_ "wininit_" #n "_"))

#define MT_lock_init(l, n)					\
	do {							\
		InitializeCriticalSection(&(l)->lock);		\
		strcpy_len((l)->name, (n), sizeof((l)->name));	\
		_DBG_LOCK_INIT(l);				\
	} while (0)

#define MT_lock_try(l)	(TryEnterCriticalSection(&(l)->lock) && (_DBG_LOCK_LOCKER(l), true))

#define MT_lock_set(l)						\
	do {							\
		_DBG_LOCK_COUNT_0(l);				\
		if (!TryEnterCriticalSection(&(l)->lock)) {	\
			_DBG_LOCK_CONTENTION(l);		\
			MT_thread_setlockwait(l);		\
			EnterCriticalSection(&(l)->lock);	\
			MT_thread_setlockwait(NULL);		\
		}						\
		_DBG_LOCK_LOCKER(l);				\
		_DBG_LOCK_COUNT_2(l);				\
	} while (0)

#define MT_lock_unset(l)				\
	do {						\
		_DBG_LOCK_UNLOCKER(l);			\
		LeaveCriticalSection(&(l)->lock);	\
	} while (0)

#define MT_lock_destroy(l)				\
	do {						\
		_DBG_LOCK_DESTROY(l);			\
		DeleteCriticalSection(&(l)->lock);	\
	} while (0)

typedef struct MT_RWLock {
	SRWLOCK lock;
	char name[MT_NAME_LEN];
} MT_RWLock;

#define MT_RWLOCK_INITIALIZER(n)	{ .lock = SRWLOCK_INIT, .name = #n, }

#define MT_rwlock_init(l, n)					\
	do {							\
		InitializeSRWLock(&(l)->lock);			\
		strcpy_len((l)->name, (n), sizeof((l)->name));	\
	 } while (0)

#define MT_rwlock_destroy(l)	((void) 0)

#define MT_rwlock_rdlock(l)	AcquireSRWLockShared(&(l)->lock)
#define MT_rwlock_rdtry(l)	TryAcquireSRWLockShared(&(l)->lock)

#define MT_rwlock_rdunlock(l)	ReleaseSRWLockShared(&(l)->lock)

#define MT_rwlock_wrlock(l)	AcquireSRWLockExclusive(&(l)->lock)
#define MT_rwlock_wrtry(l)	TryAcquireSRWLockExclusive(&(l)->lock)

#define MT_rwlock_wrunlock(l)	ReleaseSRWLockExclusive(&(l)->lock)

typedef DWORD MT_TLS_t;

#else

typedef struct MT_Lock {
	pthread_mutex_t lock;
	char name[MT_NAME_LEN];
#ifdef LOCK_STATS
	size_t count;
	ATOMIC_TYPE contention;
	ATOMIC_TYPE sleep;
	struct MT_Lock *volatile next;
	struct MT_Lock *volatile prev;
#endif
#if defined(LOCK_STATS) || defined(LOCK_OWNER)
	const char *locker;
	const char *thread;
#endif
#ifdef LOCK_OWNER
	struct MT_Lock *nxt;
#endif
} MT_Lock;

#ifdef LOCK_STATS
#define MT_LOCK_INITIALIZER(n)	{ .lock = PTHREAD_MUTEX_INITIALIZER, .name = #n, .next = (struct MT_Lock *) -1, }
#else
#define MT_LOCK_INITIALIZER(n)	{ .lock = PTHREAD_MUTEX_INITIALIZER, .name = #n, }
#endif

#define MT_lock_init(l, n)					\
	do {							\
		pthread_mutex_init(&(l)->lock, 0);		\
		strcpy_len((l)->name, (n), sizeof((l)->name));	\
		_DBG_LOCK_INIT(l);				\
	} while (0)

#define MT_lock_try(l)		(pthread_mutex_trylock(&(l)->lock) == 0 && (_DBG_LOCK_LOCKER(l), true))

#if defined(__GNUC__) && defined(HAVE_PTHREAD_MUTEX_TIMEDLOCK) && defined(HAVE_CLOCK_GETTIME)
#define MT_lock_trytime(l, ms)						\
	({								\
		struct timespec ts;					\
		clock_gettime(CLOCK_REALTIME, &ts);			\
		ts.tv_nsec += (ms % 1000) * 1000000;			\
		if (ts.tv_nsec >= 1000000000) {				\
			ts.tv_nsec -= 1000000000;			\
			ts.tv_sec++;					\
		}							\
		ts.tv_sec += (ms / 1000);				\
		int ret = pthread_mutex_timedlock(&(l)->lock, &ts);	\
		if (ret == 0)						\
			_DBG_LOCK_LOCKER(l);				\
		ret == 0;						\
	})
#endif

#define MT_lock_set(l)						\
	do {							\
		_DBG_LOCK_COUNT_0(l);				\
		if (pthread_mutex_trylock(&(l)->lock)) {	\
			_DBG_LOCK_CONTENTION(l);		\
			MT_thread_setlockwait(l);		\
			pthread_mutex_lock(&(l)->lock);		\
			MT_thread_setlockwait(NULL);		\
		}						\
		_DBG_LOCK_LOCKER(l);				\
		_DBG_LOCK_COUNT_2(l);				\
	} while (0)

#define MT_lock_unset(l)				\
	do {						\
		_DBG_LOCK_UNLOCKER(l);			\
		pthread_mutex_unlock(&(l)->lock);	\
	} while (0)

#define MT_lock_destroy(l)				\
	do {						\
		_DBG_LOCK_DESTROY(l);			\
		pthread_mutex_destroy(&(l)->lock);	\
	} while (0)

#if !defined(__GLIBC__) || __GLIBC__ > 2 || (__GLIBC__ == 2 && defined(__GLIBC_MINOR__) && __GLIBC_MINOR__ >= 30)
/* this is the normal implementation of our pthreads-based read-write lock */
typedef struct MT_RWLock {
	pthread_rwlock_t lock;
	char name[MT_NAME_LEN];
} MT_RWLock;

#define MT_RWLOCK_INITIALIZER(n)				\
	{ .lock = PTHREAD_RWLOCK_INITIALIZER, .name = #n, }

#define MT_rwlock_init(l, n)					\
	do {							\
		pthread_rwlock_init(&(l)->lock, NULL);		\
		strcpy_len((l)->name, (n), sizeof((l)->name));	\
	 } while (0)

#define MT_rwlock_destroy(l)	pthread_rwlock_destroy(&(l)->lock)

#define MT_rwlock_rdlock(l)	pthread_rwlock_rdlock(&(l)->lock)
#define MT_rwlock_rdtry(l)	(pthread_rwlock_tryrdlock(&(l)->lock) == 0)

#define MT_rwlock_rdunlock(l)	pthread_rwlock_unlock(&(l)->lock)

#define MT_rwlock_wrlock(l)	pthread_rwlock_wrlock(&(l)->lock)
#define MT_rwlock_wrtry(l)	(pthread_rwlock_trywrlock(&(l)->lock) == 0)

#define MT_rwlock_wrunlock(l)	pthread_rwlock_unlock(&(l)->lock)

#else
/* in glibc before 2.30, there was a deadlock condition in the tryrdlock
 * and trywrlock functions, we work around that by not using the
 * implementation at all
 * see https://sourceware.org/bugzilla/show_bug.cgi?id=23844 for a
 * discussion and comment 14 for the analysis */
typedef struct MT_RWLock {
	pthread_mutex_t lock;
	ATOMIC_TYPE readers;
	char name[MT_NAME_LEN];
} MT_RWLock;

#define MT_RWLOCK_INITIALIZER(n)					\
	{ .lock = PTHREAD_MUTEX_INITIALIZER, .readers = ATOMIC_VAR_INIT(0), .name = #n, }

#define MT_rwlock_init(l, n)					\
	do {							\
		pthread_mutex_init(&(l)->lock, 0);		\
		ATOMIC_INIT(&(l)->readers, 0);			\
		strcpy_len((l)->name, (n), sizeof((l)->name));	\
	} while (0)

#define MT_rwlock_destroy(l)				\
	do {						\
		pthread_mutex_destroy(&(l)->lock);	\
	} while (0)

#define MT_rwlock_rdlock(l)				\
	do {						\
		pthread_mutex_lock(&(l)->lock);		\
		ATOMIC_INC(&(l)->readers);		\
		pthread_mutex_unlock(&(l)->lock);	\
	} while (0)

static inline bool
MT_rwlock_rdtry(MT_RWLock *l)
{
	if (pthread_mutex_trylock(&l->lock) != 0)
		return false;
	ATOMIC_INC(&(l)->readers);
	pthread_mutex_unlock(&l->lock);
	return true;
}

#define MT_rwlock_rdunlock(l)			\
	do {					\
		ATOMIC_DEC(&(l)->readers);	\
	} while (0)

#define MT_rwlock_wrlock(l)				\
	do {						\
		pthread_mutex_lock(&(l)->lock);		\
		while (ATOMIC_GET(&(l)->readers) > 0)	\
			MT_sleep_ms(1);			\
	} while (0)

static inline bool
MT_rwlock_wrtry(MT_RWLock *l)
{
	if (pthread_mutex_trylock(&l->lock) != 0)
		return false;
	if (ATOMIC_GET(&l->readers) > 0) {
		pthread_mutex_unlock(&l->lock);
		return false;
	}
	return true;
}

#define MT_rwlock_wrunlock(l)  pthread_mutex_unlock(&(l)->lock);

#endif

typedef pthread_key_t MT_TLS_t;

#endif

#ifndef MT_lock_trytime
/* simplistic way to try lock with timeout: just sleep */
#define MT_lock_trytime(l, ms) (MT_lock_try(l) || (MT_sleep_ms(ms), MT_lock_try(l)))
#endif

gdk_export gdk_return MT_alloc_tls(MT_TLS_t *newkey);
gdk_export void MT_tls_set(MT_TLS_t key, void *val);
gdk_export void *MT_tls_get(MT_TLS_t key);

#ifdef LOCK_STATS
gdk_export void GDKlockstatistics(int);
gdk_export MT_Lock * volatile GDKlocklist;
gdk_export ATOMIC_FLAG GDKlocklistlock;
gdk_export ATOMIC_TYPE GDKlockcnt;
gdk_export ATOMIC_TYPE GDKlockcontentioncnt;
gdk_export ATOMIC_TYPE GDKlocksleepcnt;
#endif

/*
 * @- MT Semaphore API
 */
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)

typedef struct {
	HANDLE sema;
	char name[MT_NAME_LEN];
} MT_Sema;

#define MT_sema_init(s, nr, n)						\
	do {								\
		assert((s)->sema == NULL);				\
		strcpy_len((s)->name, (n), sizeof((s)->name));		\
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
		TRC_DEBUG(TEM, "Sema %s down...\n", (s)->name);		\
		if (WaitForSingleObject((s)->sema, 0) != WAIT_OBJECT_0) { \
			MT_thread_setsemawait(s);			\
			while (WaitForSingleObject((s)->sema, INFINITE) != WAIT_OBJECT_0) \
				;					\
			MT_thread_setsemawait(NULL);			\
		}							\
		TRC_DEBUG(TEM, "Sema %s down complete\n", (s)->name);	\
	} while (0)

#elif defined(HAVE_DISPATCH_SEMAPHORE_CREATE)

/* MacOS X */
typedef struct {
	dispatch_semaphore_t sema;
	char name[MT_NAME_LEN];
} MT_Sema;

#define MT_sema_init(s, nr, n)						\
	do {								\
		strcpy_len((s)->name, (n), sizeof((s)->name));		\
		(s)->sema = dispatch_semaphore_create((long) (nr));	\
	} while (0)

#define MT_sema_destroy(s)	dispatch_release((s)->sema)
#define MT_sema_up(s)		dispatch_semaphore_signal((s)->sema)
#define MT_sema_down(s)		dispatch_semaphore_wait((s)->sema, DISPATCH_TIME_FOREVER)

#elif defined(_AIX) || defined(__MACH__)

/* simulate semaphores using mutex and condition variable */

typedef struct {
	int cnt, wakeups;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	char name[MT_NAME_LEN];
} MT_Sema;

#define MT_sema_init(s, nr, n)					\
	do {							\
		strcpy_len((s)->name, (n), sizeof((s)->name));	\
		(s)->cnt = (nr);				\
		(s)->wakeups = 0;				\
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
		if (++(s)->cnt <= 0) {				\
			(s)->wakeups++;				\
			pthread_cond_signal(&(s)->cond);	\
		}						\
		pthread_mutex_unlock(&(s)->mutex);		\
	} while (0)

#define MT_sema_down(s)							\
	do {								\
		TRC_DEBUG(TEM, "Sema %s down...\n", (s)->name);		\
		pthread_mutex_lock(&(s)->mutex);			\
		if (--(s)->cnt < 0) {					\
			MT_thread_setsemawait(s);			\
			do {						\
				pthread_cond_wait(&(s)->cond,		\
						  &(s)->mutex);		\
			} while ((s)->wakeups < 1);			\
			MT_thread_setsemawait(NULL);			\
			(s)->wakeups--;					\
			pthread_mutex_unlock(&(s)->mutex);		\
		}							\
		TRC_DEBUG(TEM, "Sema %s down complete\n", (s)->name);	\
	} while (0)

#else

typedef struct {
	sem_t sema;
	char name[MT_NAME_LEN];
} MT_Sema;

#define MT_sema_init(s, nr, n)					\
	do {							\
		strcpy_len((s)->name, (n), sizeof((s)->name));	\
		sem_init(&(s)->sema, 0, nr);			\
	} while (0)

#define MT_sema_destroy(s)	sem_destroy(&(s)->sema)

#define MT_sema_up(s)						\
	do {							\
		TRC_DEBUG(TEM, "Sema %s up\n", (s)->name);	\
		sem_post(&(s)->sema);				\
	} while (0)

#define MT_sema_down(s)							\
	do {								\
		TRC_DEBUG(TEM, "Sema %s down...\n", (s)->name);		\
		if (sem_trywait(&(s)->sema) != 0) {			\
			MT_thread_setsemawait(s);			\
			while (sem_wait(&(s)->sema) != 0)		\
				;					\
			MT_thread_setsemawait(NULL);			\
		}							\
		TRC_DEBUG(TEM, "Sema %s down complete\n", (s)->name);	\
	} while (0)

#endif

gdk_export void MT_thread_setlockwait(MT_Lock *lock);
gdk_export void MT_thread_setsemawait(MT_Sema *sema);
gdk_export void MT_thread_setworking(const char *work);
gdk_export void MT_thread_setalgorithm(const char *algo);
gdk_export const char *MT_thread_getalgorithm(void);
#ifdef LOCK_OWNER
#define hide_exp(a,b) a ## b	/* hide export from exports test */
hide_exp(gdk_ex,port) void MT_thread_add_mylock(MT_Lock *lock);
hide_exp(gdk_ex,port) void MT_thread_del_mylock(MT_Lock *lock);
#undef hide_exp
#endif

gdk_export int MT_check_nr_cores(void);

/*
 * @ Condition Variable API
 */

typedef struct MT_Cond {
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	CONDITION_VARIABLE cv;
#else
	pthread_cond_t cv;
#endif
	char name[MT_NAME_LEN];
} MT_Cond;

#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
#  define MT_COND_INITIALIZER(N) { .cv = CONDITION_VARIABLE_INIT, .name = #N }
#else
#  define MT_COND_INITIALIZER(N) { .cv = PTHREAD_COND_INITIALIZER, .name = #N }
#endif

gdk_export void MT_cond_init(MT_Cond *cond);
gdk_export void MT_cond_destroy(MT_Cond *cond);
gdk_export void MT_cond_wait(MT_Cond *cond, MT_Lock *lock);
gdk_export void MT_cond_signal(MT_Cond *cond);
gdk_export void MT_cond_broadcast(MT_Cond *cond);


#endif /*_GDK_SYSTEM_H_*/
