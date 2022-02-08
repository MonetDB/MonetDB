/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
/* we can define __has_attribute as 1 since we define __attribute__ as empty */
#define __has_attribute(attr)	1
#ifndef __attribute__
#define __attribute__(attr)	/* empty */
#endif
#else
/* older GCC does have attributes, but not __has_attribute and not all
 * attributes that we use are known */
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
#if !__has_attribute(__alloc_size__)
#define __alloc_size__(a)
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
#define __format__(a,b,c)
#endif
#if !__has_attribute(__malloc__)
#define __malloc__
#endif
#if !__has_attribute(__nonnull__)
#define __nonnull__(a)
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
#define __visibility__(a)
#elif defined(__CYGWIN__)
#define __visibility__(a)
#endif
#if !__has_attribute(__warn_unused_result__)
#define __warn_unused_result__
#endif

/* also see gdk.h for these */
#define THRDMASK	(1)
#define TEMMASK		(1<<10)

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
#ifdef BSD /* BSD macro is defined in sys/param.h */
# include <sys/sysctl.h>
#endif

/* new pthread interface, where the thread id changed to a struct */
#ifdef PTW32_VERSION
#define PTW32 1
#endif

/* debug and errno integers */
gdk_export int GDKdebug;
gdk_export void GDKsetdebug(int debug);
gdk_export int GDKgetdebug(void);

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
	const lng starttime;
	lng querytimeout;
} QryCtx;

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
gdk_export QryCtx *MT_thread_get_qry_ctx(void);
gdk_export void MT_thread_set_qry_ctx(QryCtx *ctx);

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

#ifdef LOCK_STATS
#include "gdk_tracer.h"

#define _DBG_LOCK_COUNT_0(l)					\
	do {							\
		(void) ATOMIC_INC(&GDKlockcnt);			\
		TRC_DEBUG(TEM, "Locking %s...\n", (l)->name); 	\
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
		TRC_DEBUG(TEM, "Unlocking %s\n", (l)->name);	\
	} while (0)

#define _DBG_LOCK_CONTENTION(l)						\
	do {								\
		TRC_DEBUG(TEM, "Lock %s contention\n", (l)->name); 	\
		(void) ATOMIC_INC(&GDKlockcontentioncnt);		\
		(void) ATOMIC_INC(&(l)->contention);			\
	} while (0)

#define _DBG_LOCK_SLEEP(l)	((void) ATOMIC_INC(&(l)->sleep))

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

#define _DBG_LOCK_INIT(l)						\
	do {								\
		(l)->count = 0;						\
		ATOMIC_INIT(&(l)->contention, 0);			\
		ATOMIC_INIT(&(l)->sleep, 0);				\
		(l)->locker = NULL;					\
		(l)->thread = NULL;					\
		/* if name starts with "sa_" don't link in GDKlocklist */ \
		/* since the lock is in memory that is governed by the */ \
		/* SQL storage allocator, and hence we have no control */ \
		/* over when the lock is destroyed and the memory freed */ \
		if (strncmp((l)->name, "sa_", 3) != 0) {		\
			while (ATOMIC_TAS(&GDKlocklistlock) != 0)	\
				;					\
			if (GDKlocklist)				\
				GDKlocklist->prev = (l);		\
			(l)->next = GDKlocklist;			\
			(l)->prev = NULL;				\
			GDKlocklist = (l);				\
			ATOMIC_CLEAR(&GDKlocklistlock);			\
		} else {						\
			(l)->next = NULL;				\
			(l)->prev = NULL;				\
		}							\
	} while (0)

#define _DBG_LOCK_DESTROY(l)						\
	do {								\
		/* if name starts with "sa_" don't link in GDKlocklist */ \
		/* since the lock is in memory that is governed by the */ \
		/* SQL storage allocator, and hence we have no control */ \
		/* over when the lock is destroyed and the memory freed */ \
		if (strncmp((l)->name, "sa_", 3) != 0) {		\
			while (ATOMIC_TAS(&GDKlocklistlock) != 0)	\
				;					\
			if ((l)->next)					\
				(l)->next->prev = (l)->prev;		\
			if ((l)->prev)					\
				(l)->prev->next = (l)->next;		\
			else if (GDKlocklist == (l))			\
				GDKlocklist = (l)->next;		\
			ATOMIC_CLEAR(&GDKlocklistlock);			\
			ATOMIC_DESTROY(&(l)->contention);		\
			ATOMIC_DESTROY(&(l)->sleep);			\
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
	const char *locker;
	const char *thread;
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

#define MT_lock_try(l)	TryEnterCriticalSection(&(l)->lock)

#define MT_lock_set(l)						\
	do {							\
		_DBG_LOCK_COUNT_0(l);				\
		if (!MT_lock_try(l)) {				\
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
	const char *locker;
	const char *thread;
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

#define MT_lock_try(l)		(pthread_mutex_trylock(&(l)->lock) == 0)

#ifdef LOCK_STATS
#define MT_lock_set(l)					\
	do {						\
		_DBG_LOCK_COUNT_0(l);			\
		if (!MT_lock_try(l)) {			\
			_DBG_LOCK_CONTENTION(l);	\
			MT_thread_setlockwait(l);	\
			pthread_mutex_lock(&(l)->lock);	\
			MT_thread_setlockwait(NULL);	\
		}					\
		_DBG_LOCK_LOCKER(l);			\
		_DBG_LOCK_COUNT_2(l);			\
	} while (0)
#else
#define MT_lock_set(l)		pthread_mutex_lock(&(l)->lock)
#endif

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

#define MT_RWLOCK_INITIALIZER(n)				\
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
		ATOMIC_DESTROY(&(l)->readers);		\
	} while (0)

#define MT_rwlock_rdlock(l)				\
	do {						\
		pthread_mutex_lock(&(l)->lock);		\
		(void) ATOMIC_INC(&(l)->readers);	\
		pthread_mutex_unlock(&(l)->lock);	\
	} while (0)

static inline bool
MT_rwlock_rdtry(MT_RWLock *l)
{
	if (pthread_mutex_trylock(&l->lock) != 0)
		return false;
	(void) ATOMIC_INC(&(l)->readers);
	pthread_mutex_unlock(&l->lock);
	return true;
}

#define MT_rwlock_rdunlock(l)				\
	do {						\
		(void) ATOMIC_DEC(&(l)->readers);	\
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

#endif

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
	int cnt;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	char name[MT_NAME_LEN];
} MT_Sema;

#define MT_sema_init(s, nr, n)					\
	do {							\
		strcpy_len((s)->name, (n), sizeof((s)->name));	\
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
		TRC_DEBUG(TEM, "Sema %s down...\n", (s)->name);		\
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

gdk_export int MT_check_nr_cores(void);

#endif /*_GDK_SYSTEM_H_*/
