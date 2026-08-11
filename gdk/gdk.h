/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * The name GDK comes from the original name: Goblin Database Kernel.
 * This kernel was originally written as a library by Martin L. Kersten,
 * Peter Boncz, and Niels Nes, and subsequently heavily modified by
 * Sjoerd Mullender.
 * For the old documentation on the library, dig down in the archives:
 * you can find the old comments in the repository from which this file
 * came.
 */

#ifndef _GDK_H_
#define _GDK_H_

#include "monetdb_config.h"

/* standard C-99 include files */
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* standard includes upon which all configure tests depend */
#ifdef HAVE_SYS_STAT_H
# include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#ifdef HAVE_SYS_FILE_H
# include <sys/file.h>
#endif

#ifdef HAVE_DIRENT_H
# include <dirent.h>
#endif

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

#include <sys/types.h>

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

#ifdef __APPLE__
/* the compiler on the Mac can't deal with including xxhash.h twice
 * because of identical redefinitions of types and we happen to know
 * that the xxhash version is high enough, so just define the magic
 * inline token and include the file only once */
#define XXH_INLINE_ALL
#endif

#include <xxhash.h>

#ifndef __APPLE__
#if XXH_VERSION_NUMBER >= 0*100*100 + 8*100 + 0   /* at least 0.8.0 */
/* in newer versions, we can define XXH_INLINE_ALL to inline all hash
 * functions before including xxhash.h again (we didn't need the first
 * include, except we need the version number to make the
 * distinction) */
#define XXH_INLINE_ALL
#include <xxhash.h>
#endif
#endif

#include "stream.h"
#include "mstring.h"
#include "matomic.h"

#ifndef PATH_MAX
#define PATH_MAX	1024
#endif

#ifdef WIN32
#ifndef LIBGDK
#define gdk_export extern __declspec(dllimport)
#else
#define gdk_export extern __declspec(dllexport)
#endif
#else
#define gdk_export extern
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

/* Only ever compare with GDK_SUCCEED, never with GDK_FAIL, and do not
 * use as a Boolean. */
typedef enum gdk_return { GDK_FAIL, GDK_SUCCEED } gdk_return;

/* basic GDK types */
typedef bool msk;
typedef int8_t bit;
typedef int8_t bte;
typedef int16_t sht;
typedef int64_t lng;
typedef uint64_t ulng;
#ifdef HAVE_HGE
typedef int128_t hge;
typedef uint128_t uhge;
#endif

typedef struct allocator allocator;
/* checkpoint or snapshot of allocator internal state we can use
 * to restore to a point in time */
typedef struct allocator_state {
	size_t nr;
	size_t used;
	size_t objects;
	size_t inuse;
	size_t tmp_used;
	allocator *ma;
} allocator_state;

/* // TODO: Complete it when documentation is accepted
 *
 * Tracer is the general logging system for the MonetDB stack modelled
 * after the well-known logging schemes (e.g: Python). It provides a
 * number of logging levels and options to increase or reduce the
 * verbosity either of individual code parts or of the codebase as a
 * whole. It allows users to focus on logging messages related to
 * certain steps of execution, which can be proved handy when it comes
 * to debugging. The behavior of Tracer can be controlled at runtime
 * using the SQL API described later on. Certain calls require an "id"
 * to operate which can be found on the list of each section below.
 *
 * Internally, the logger uses a buffer to capture log messages before
 * they are forwarded to the specific adapter.
 *
 * - Sets the minimum flush level that an event will trigger the
 *   logger to flush the buffer
 * - Produces messages to the output stream. It is also used as a
 *   fallback mechanism in case GDKtracer fails to log for whatever
 *   reason.
 * - Struct buffer with allocated space etc.
 * - Flush buffer sends the messages to the selected adapter
 * - Write about the log structure (e.g: MT_thread_get_name + datetime
 *   + blah blah)
 */

#define TRC_NAME(TOKEN)		TRC_##TOKEN

#define TRC_GENERATE_ENUM(ENUM) TRC_NAME(ENUM),


// ADAPTERS
#define TRC_FOREACH_ADPTR(ADPTR)		\
	ADPTR( BASIC )				\
	ADPTR( PROFILER )			\
	ADPTR( MBEDDED )			\
						\
	ADPTR( ADAPTERS_COUNT )

typedef enum {
	TRC_FOREACH_ADPTR(TRC_GENERATE_ENUM)
} adapter_t;



// LOG LEVELS
#define TRC_FOREACH_LEVEL(LEVEL)		\
	LEVEL( M_CRITICAL )			\
	LEVEL( M_ERROR )			\
	LEVEL( M_WARNING )			\
	LEVEL( M_INFO )				\
	LEVEL( M_DEBUG )			\
						\
	LEVEL( LOG_LEVELS_COUNT )

typedef enum {
	TRC_FOREACH_LEVEL(TRC_GENERATE_ENUM)
} log_level_t;


// LAYERS
#define TRC_FOREACH_LAYER(LAYER)		\
	LAYER( MDB_ALL )			\
	LAYER( SQL_ALL )			\
	LAYER( MAL_ALL )			\
	LAYER( GDK_ALL )			\
						\
	LAYER( LAYERS_COUNT )

typedef enum {
	TRC_FOREACH_LAYER(TRC_GENERATE_ENUM)
} layer_t;




// COMPONENTS
#define TRC_FOREACH_COMP(COMP)			\
	COMP( ACCELERATOR )			\
	COMP( ALGO )				\
	COMP( ALLOC )				\
	COMP( BAT )				\
	COMP( CHECK )				\
	COMP( DELTA )				\
	COMP( HEAP )				\
	COMP( IO )				\
	COMP( WAL )				\
	COMP( PAR )				\
	COMP( PERF )				\
	COMP( TEM )				\
	COMP( THRD )				\
	COMP( TM )				\
						\
	COMP( GEOM )				\
	COMP( FITS )				\
	COMP( SHP )				\
	COMP( PARQUET )				\
						\
	COMP( LOADER )				\
						\
	COMP( SQL_PARSER )			\
	COMP( SQL_TRANS )			\
	COMP( SQL_REWRITER )			\
	COMP( SQL_EXECUTION )			\
	COMP( SQL_STORE )			\
						\
	COMP( MAL_REMOTE )			\
	COMP( MAL_MAPI )			\
	COMP( MAL_SERVER )			\
	COMP( MAL_LOADER )			\
	COMP( MAL_INSTRUCTION )			\
						\
	COMP( MAL_OPTIMIZER )			\
						\
	COMP( GDK )				\
						\
	COMP( COMPONENTS_COUNT )

typedef enum {
	TRC_FOREACH_COMP(TRC_GENERATE_ENUM)
} component_t;

#undef TRC_GENERATE_ENUM


/*
 * Logging macros
 */
gdk_export ATOMIC_TYPE lvl_per_component[];

// If the LOG_LEVEL of the message is one of the following: CRITICAL,
// ERROR or WARNING it is logged no matter the component. In any other
// case the component is taken into account
#define GDK_TRACER_TEST(LOG_LEVEL, COMP)				\
	(TRC_NAME(LOG_LEVEL) <= TRC_NAME(M_WARNING)  ||			\
	 (log_level_t) ATOMIC_GET(&lvl_per_component[TRC_NAME(COMP)]) >= TRC_NAME(LOG_LEVEL))


#define GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP, ...)			\
	GDKtracer_log(__FILE__, __func__, __LINE__,			\
		      TRC_NAME(LOG_LEVEL), TRC_NAME(COMP), NULL, __VA_ARGS__)

#ifdef __COVERITY__
/* hide this for static code analysis: too many false positives */
#define GDK_TRACER_LOG(LOG_LEVEL, COMP, MSG, ...)	((void) 0)
#else
#define GDK_TRACER_LOG(LOG_LEVEL, COMP, ...)				\
	do {								\
		if (GDK_TRACER_TEST(LOG_LEVEL, COMP)) {			\
			GDK_TRACER_LOG_BODY(LOG_LEVEL, COMP,		\
					    __VA_ARGS__);		\
		}							\
	} while (0)
#endif


#define TRC_CRITICAL(COMP, ...)					\
	GDK_TRACER_LOG_BODY(M_CRITICAL, COMP, __VA_ARGS__)

#define TRC_ERROR(COMP, ...)					\
	GDK_TRACER_LOG_BODY(M_ERROR, COMP, __VA_ARGS__)

#define TRC_WARNING(COMP, ...)					\
	GDK_TRACER_LOG_BODY(M_WARNING, COMP, __VA_ARGS__)

#define TRC_INFO(COMP, ...)					\
	GDK_TRACER_LOG(M_INFO, COMP, __VA_ARGS__)

#define TRC_DEBUG(COMP, ...)					\
	GDK_TRACER_LOG(M_DEBUG, COMP, __VA_ARGS__)



// Conditional logging - Example usage
// NOTE: When using the macro with *_IF always use the macro with
// *_ENDIF for logging. Not doing that will result in checking
// the LOG_LEVEL of the the COMPONENT twice. Also NEVER use the
// *_ENDIF macro without before performing a check with *_IF
// macro. Such an action will have as a consequence logging everything
// without taking into account the LOG_LEVEL of the COMPONENT.
/*
    TRC_INFO_IF(SQL_STORE)
    {
	TRC_INFO_ENDIF(SQL_STORE, "Test\n")
    }
*/
#define TRC_CRITICAL_IF(COMP)			\
	/* if (GDK_TRACER_TEST(M_CRITICAL, COMP)) */

#define TRC_ERROR_IF(COMP)			\
	/* if (GDK_TRACER_TEST(M_ERROR, COMP)) */

#define TRC_WARNING_IF(COMP)			\
	/* if (GDK_TRACER_TEST(M_WARNING, COMP)) */

#define TRC_INFO_IF(COMP)			\
	if (GDK_TRACER_TEST(M_INFO, COMP))

#define TRC_DEBUG_IF(COMP)			\
	if (GDK_TRACER_TEST(M_DEBUG, COMP))


#define TRC_CRITICAL_ENDIF(COMP, ...)				\
	GDK_TRACER_LOG_BODY(M_CRITICAL, COMP, __VA_ARGS__)

#define TRC_ERROR_ENDIF(COMP, ...)				\
	GDK_TRACER_LOG_BODY(M_ERROR, COMP, __VA_ARGS__)

#define TRC_WARNING_ENDIF(COMP, ...)				\
	GDK_TRACER_LOG_BODY(M_WARNING, COMP, __VA_ARGS__)

#define TRC_INFO_ENDIF(COMP, ...)				\
	GDK_TRACER_LOG_BODY(M_INFO, COMP, __VA_ARGS__)

#define TRC_DEBUG_ENDIF(COMP, ...)				\
	GDK_TRACER_LOG_BODY(M_DEBUG, COMP, __VA_ARGS__)



/*
 * GDKtracer API
 * For the allowed log_levels, components and layers see the
 * LOG_LEVEL, COMPONENT and LAYER enum respectively.
 */
// Used for logrotate
gdk_export void GDKtracer_reinit_basic(int sig);

gdk_export gdk_return GDKtracer_set_tracefile(const char *tracefile);

gdk_export gdk_return GDKtracer_stop(void);

gdk_export gdk_return GDKtracer_set_component_level(const char *comp, const char *lvl);
gdk_export const char *GDKtracer_get_component_level(const char *comp);
gdk_export gdk_return GDKtracer_reset_component_level(const char *comp);

gdk_export gdk_return GDKtracer_set_layer_level(const char *layer, const char *lvl);
gdk_export gdk_return GDKtracer_reset_layer_level(const char *layer);

gdk_export gdk_return GDKtracer_set_flush_level(const char *lvl);
gdk_export gdk_return GDKtracer_reset_flush_level(void);

gdk_export gdk_return GDKtracer_set_adapter(const char *adapter);
gdk_export gdk_return GDKtracer_reset_adapter(void);

gdk_export void GDKtracer_log(const char *file, const char *func,
			      int lineno, log_level_t lvl,
			      component_t comp,
			      const char *syserr,
			      _In_z_ _Printf_format_string_ const char *format,
			      ...)
	__attribute__((__format__(__printf__, 7, 8)));

gdk_export gdk_return GDKtracer_flush_buffer(void);

/* debug and errno integers */
gdk_export ATOMIC_TYPE GDKdebug;
gdk_export void GDKsetdebug(unsigned debug);
gdk_export unsigned GDKgetdebug(void);

gdk_export int GDKnr_threads;

/* API */

gdk_export void MT_sleep_ms(unsigned int ms);

/*
 * @- MT Thread Api
 */
typedef size_t MT_Id;		/* thread number. will not be zero */

enum MT_thr_detach { MT_THR_JOINABLE, MT_THR_DETACHED };

#define MT_NAME_LEN	32	/* length of thread/semaphore/etc. names */

#define UNKNOWN_THREAD "unknown thread"

typedef struct QryCtx {
	lng starttime;
	lng endtime;
	struct bstream *bs;
	ATOMIC_TYPE datasize;
	ATOMIC_BASE_TYPE maxmem;
	allocator *errorallocator;
	bool oahash_enabled;
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
gdk_export allocator *MT_thread_getallocator(void);
gdk_export void MT_thread_setallocator(allocator *ma);
gdk_export void *MT_thread_getdata(void);
gdk_export void MT_thread_setdata(void *data);
gdk_export void MT_exiting_thread(void);
gdk_export MT_Id MT_getpid(void);
gdk_export int MT_join_thread(MT_Id t);
gdk_export QryCtx *MT_thread_get_qry_ctx(void);
gdk_export void MT_thread_set_qry_ctx(QryCtx *ctx);
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
		strtcpy((l)->name, (n), sizeof((l)->name));	\
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
		strtcpy((l)->name, (n), sizeof((l)->name));	\
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
		strtcpy((l)->name, (n), sizeof((l)->name));	\
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
		strtcpy((l)->name, (n), sizeof((l)->name));	\
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
		strtcpy((l)->name, (n), sizeof((l)->name));	\
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
		strtcpy((s)->name, (n), sizeof((s)->name));		\
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
		strtcpy((s)->name, (n), sizeof((s)->name));		\
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
		strtcpy((s)->name, (n), sizeof((s)->name));	\
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
		strtcpy((s)->name, (n), sizeof((s)->name));	\
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
gdk_export void MT_thread_setalgorithm(const char *algo, const char *func);
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

gdk_export void MT_cond_init(MT_Cond *cond, const char *name);
gdk_export void MT_cond_destroy(MT_Cond *cond);
gdk_export void MT_cond_wait(MT_Cond *cond, MT_Lock *lock);
gdk_export void MT_cond_signal(MT_Cond *cond);
gdk_export void MT_cond_broadcast(MT_Cond *cond);

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

gdk_export _Noreturn void GDKfatal(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));

#undef MIN
#undef MAX
#define MAX(A,B)	((A)<(B)?(B):(A))
#define MIN(A,B)	((A)>(B)?(B):(A))

/* defines from ctype with casts that allow passing char values */
#define GDKisspace(c)	isspace((unsigned char) (c))
#define GDKisalnum(c)	isalnum((unsigned char) (c))
#define GDKisdigit(c)	isdigit((unsigned char) (c))
#define GDKisxdigit(c)	isxdigit((unsigned char) (c))

#define BATDIR		"bat"
#define TEMPDIR_NAME	"TEMP_DATA"

#define DELDIR		BATDIR DIR_SEP_STR "DELETE_ME"
#define BAKDIR		BATDIR DIR_SEP_STR "BACKUP"
#define SUBDIR		BAKDIR DIR_SEP_STR "SUBCOMMIT" /* note K, not T */
#define LEFTDIR		BATDIR DIR_SEP_STR "LEFTOVERS"
#define TEMPDIR		BATDIR DIR_SEP_STR TEMPDIR_NAME

/*
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of the following debug options.
*/

#define THRDMASK	(1U)
#define CHECKMASK	(1U<<1)
#define CHECKDEBUG	if (ATOMIC_GET(&GDKdebug) & CHECKMASK)
#define IOMASK		(1U<<4)
#define BATMASK		(1U<<5)
#define PARMASK		(1U<<7)
#define TESTINGMASK	(1U<<8)
#define TMMASK		(1U<<9)
#define TEMMASK		(1U<<10)
#define PERFMASK	(1U<<12)
#define DELTAMASK	(1U<<13)
#define LOADMASK	(1U<<14)
#define PUSHCANDMASK	(1U<<15)	/* used in opt_pushselect.c */
#define TAILCHKMASK	(1U<<16)	/* check .tail file size during commit */
#define ACCELMASK	(1U<<20)
#define ALGOMASK	(1U<<21)

#define NOSYNCMASK	(1U<<24)

#define DEADBEEFMASK	(1U<<25)
#define DEADBEEFCHK	if (!(ATOMIC_GET(&GDKdebug) & DEADBEEFMASK))

#define ALLOCMASK	(1U<<26)

#define HEAPMASK	(1U<<28)

#define FORCEMITOMASK	(1U<<29)
#define FORCEMITODEBUG	if (ATOMIC_GET(&GDKdebug) & FORCEMITOMASK)

#ifndef TRUE
#define TRUE		true
#define FALSE		false
#endif

#define BATMARGIN	1.2	/* extra free margin for new heaps */
#define BATTINY_BITS	8
#define BATTINY		((BUN)1<<BATTINY_BITS)	/* minimum allocation buncnt for a BAT */

enum {
	TYPE_void = 0,
	TYPE_msk,		/* bit mask */
	TYPE_bit,		/* TRUE, FALSE, or nil */
	TYPE_bte,
	TYPE_sht,
	TYPE_int,
	TYPE_oid,
	TYPE_ptr,		/* C pointer! */
	TYPE_flt,
	TYPE_dbl,
	TYPE_lng,
#ifdef HAVE_HGE
	TYPE_hge,
#endif
	TYPE_date,
	TYPE_daytime,
	TYPE_timestamp,
	TYPE_uuid,
	TYPE_inet4,
	TYPE_inet6,
	TYPE_str,
	TYPE_blob,
	TYPE_any = 255,		/* limit types to <255! */
};

typedef union {
	uint32_t align;		/* force alignment, only used for equality */
	uint8_t quad[4] __attribute__((__nonstring__));
} inet4;
typedef union {
#ifdef HAVE_HGE
	hge align;		/* force alignment, only used for equality */
#else
	lng align[2];		/* force alignment, not otherwise used */
#endif
	uint8_t hex[16];
} inet6;

#define SIZEOF_OID	SIZEOF_SIZE_T
typedef size_t oid;
#define OIDFMT		"%zu"

typedef int bat;		/* Index into BBP */
typedef void *ptr;		/* Internal coding of types */

#define SIZEOF_PTR	SIZEOF_VOID_P
typedef float flt;
typedef double dbl;
typedef char *str;

#define UUID_SIZE	16	/* size of a UUID */
#define UUID_STRLEN	36	/* length of string representation */

typedef union {
#ifdef HAVE_HGE
	hge h;			/* force alignment, only used for equality */
#else
	lng l[2];		/* force alignment, not otherwise used */
#endif
	uint8_t u[UUID_SIZE] __attribute__((__nonstring__));
} uuid;

typedef struct {
	size_t nitems;
	uint8_t data[] __attribute__((__nonstring__))
		__attribute__((__counted_by__(nitems)));
} blob;
gdk_export size_t blobsize(size_t nitems) __attribute__((__const__));

#define SIZEOF_LNG		8
#define LL_CONSTANT(val)	INT64_C(val)
#define LLFMT			"%" PRId64
#define ULLFMT			"%" PRIu64
#define LLSCN			"%" SCNd64
#define ULLSCN			"%" SCNu64

#ifdef HAVE_HGE
#define SIZEOF_HGE		16
/* constants, printf format, scanf format: not supported for 128 bit
 * integers */
#endif

/* The types `oid`, `BUN` and `var_t` are all defined as `size_t`, but
 * they serve different purposes.  `oid` is the *logical* row number of
 * a BAT; `BUN` is the *physical* row number of a BAT and is therefore
 * also used for the total number of rows and the potential capacity of
 * a BAT; `var_t` is an *offset* into the var-size heap (vheap).  The
 * difference between the *logical* and *physical* row number is the
 * value of the head sequence base (`hseqbase`) of the BAT. */

typedef oid var_t;		/* type used for heap index of var-sized BAT */
#define SIZEOF_VAR_T	SIZEOF_OID
#define VARFMT		OIDFMT

#if SIZEOF_VAR_T == SIZEOF_INT
#define VAR_MAX		((var_t) INT_MAX)
#else
#define VAR_MAX		((var_t) INT64_MAX)
#endif

typedef oid BUN;		/* BUN position */
#define SIZEOF_BUN	SIZEOF_OID
#define BUNFMT		OIDFMT
/* alternatively:
typedef size_t BUN;
#define SIZEOF_BUN	SIZEOF_SIZE_T
#define BUNFMT		"%zu"
*/
#if SIZEOF_BUN == SIZEOF_INT
#define BUN_NONE ((BUN) INT_MAX)
#else
#define BUN_NONE ((BUN) INT64_MAX)
#endif
#define BUN_MAX (BUN_NONE - 1)	/* maximum allowed size of a BAT */

typedef enum {
	PERSISTENT = 0,
	TRANSIENT,
	SYSTRANS,
} role_t;

/* Heap storage modes */
typedef enum {
	STORE_INVALID = 0,	/* invalid value, used to indicate error */
	STORE_MEM,		/* load into GDKmalloced memory */
	STORE_MMAP,		/* mmap() into virtual memory */
	STORE_PRIV,		/* BAT copy of copy-on-write mmap */
	STORE_CMEM,		/* load into malloc (not GDKmalloc) memory*/
	STORE_NOWN,		/* memory not owned by the BAT */
	STORE_MMAPABS,		/* mmap() into virtual memory from an
				 * absolute path (not part of dbfarm) */
} storage_t;

typedef struct {
	size_t free;		/* index where free area starts. */
	size_t size;		/* size of the heap (bytes) */
	char *base;		/* base pointer in memory. */
#if SIZEOF_VOID_P == 4
	char filename[32];	/* file containing image of the heap */
#else
	char filename[40];	/* file containing image of the heap */
#endif

	ATOMIC_TYPE refs;	/* reference count for this heap */
	bte farmid;		/* id of farm where heap is located */
	bool cleanhash;		/* string heaps must clean hash */
	bool dirty;		/* specific heap dirty marker */
	bool remove;		/* remove storage file when freeing */
	bool wasempty;		/* heap was empty when last saved/created */
	bool hasfile;		/* .filename exists on disk */
	storage_t storage;	/* storage mode (mmap/malloc). */
	storage_t newstorage;	/* new desired storage mode at re-allocation. */
	bat parentid;		/* cache id of VIEW parent bat */
} Heap;

typedef struct Hash Hash;
typedef struct Strimps Strimps;

#ifdef HAVE_RTREE
typedef struct RTree RTree;
#endif

typedef struct {
	union {			/* storage is first in the record */
		int ival;
		oid oval;
		sht shval;
		bte btval;
		msk mval;
		flt fval;
		ptr pval;
		bat bval;
		str sval;
		dbl dval;
		lng lval;
#ifdef HAVE_HGE
		hge hval;
#endif
		uuid uval;
		inet4 ip4val;
		inet6 ip6val;
	} val;
	size_t len;
	short vtype;
	bool allocated;
	bool bat;
} *ValPtr, ValRecord;

/* interface definitions */
gdk_export void *VALconvert(allocator *ma, int typ, ValPtr t);
gdk_export char *VALformat(allocator *ma, const ValRecord *res)
	__attribute__((__warn_unused_result__));
gdk_export void VALempty(ValPtr v)
	__attribute__((__access__(write_only, 1)));
gdk_export void VALclear(ValPtr v);
gdk_export ValPtr VALset(ValPtr v, int t, void *p);
gdk_export void *VALget(ValPtr v);
gdk_export int VALcmp(const ValRecord *p, const ValRecord *q);
gdk_export bool VALisnil(const ValRecord *v);

typedef struct PROPrec PROPrec;

typedef void  (*pipeline_io_destroy)  (void *pl_io);
typedef int   (*pipeline_io_done)     (void *pl_io, int wid, int nr_workers, bool redo);
typedef int   (*pipeline_io_next)     (void *pl_io, int wid);
typedef void *(*pipeline_io_next_bat) (void *pl_io, int wid);

typedef struct pipeline_io {
	pipeline_io_destroy destroy;
	pipeline_io_done done;
	pipeline_io_next next;         /* counter incrementing sources */
	pipeline_io_next_bat next_bat; /* bat generating sources */
	int type;                      /* sink/source type */
	char *error;
} pipeline_source, pipeline_sink;

#define TSKdestroy(b) if (b->pl_io && b->pl_io->destroy) { b->pl_io->destroy(b->pl_io); b->pl_io = NULL; }
#define TSKfree(b)    TSKdestroy(b)

#define ORDERIDXOFF		3

#define HLL_BITS 6
#define BITS_MASK   ((1ULL << HLL_BITS) - 1)
#define MAX_CLZ     (64 - HLL_BITS)
#define BUCKETS     (1U << HLL_BITS)
#define CLZ_BUCKETS (MAX_CLZ + 1)
#define HLLSEED     0xadc83b19ULL

/* assert that atom width is power of 2, i.e., width == 1<<shift */
#define assert_shift_width(shift,width) assert(((shift) == 0 && (width) == 0) || ((unsigned)1<<(shift)) == (unsigned)(width))

#define GDKLIBRARY_HASHASH	061044U /* first in Jul2021: hashash bit in string heaps */
#define GDKLIBRARY_HSIZE	061045U /* first in Jan2022: heap "size" values */
#define GDKLIBRARY_JSON 	061046U /* first in Sep2022: json storage changes*/
#define GDKLIBRARY_STATUS	061047U /* first in Dec2023: no status/filename columns */
#define GDKLIBRARY_USTR		061050U /* first in Aug2024: no ustr */
#define GDKLIBRARY		061051U /* first after Dec2025 */

/* The batRestricted field indicates whether a BAT is readonly.
 * we have modes: BAT_WRITE  = all permitted
 *                BAT_APPEND = append-only
 *                BAT_READ   = read-only
 * VIEW bats are always mapped read-only.
 */
typedef enum {
	BAT_WRITE,		  /* all kinds of access allowed */
	BAT_READ,		  /* only read-access allowed */
	BAT_APPEND,		  /* only reads and appends allowed */
} restrict_t;

/* theaplock: this lock should be held when reading or writing any of
 * the fields that are saved in the BBP.dir file (plus any, if any, that
 * share bitfields with any of the fields), i.e. hseqbase,
 * batRestricted, batTransient, batCount, and the theap properties tkey,
 * tseqbase, tsorted, trevsorted, twidth, tshift, tnonil, tnil, tnokey,
 * tnosorted, tnorevsorted, tminpos, tmaxpos, and tunique_est, also when
 * BBP_logical(bid) is changed, and also when reading or writing any of
 * the following fields: theap, tvheap, batInserted, batCapacity.  There
 * is no need for the lock if the bat cannot possibly be modified
 * concurrently, e.g. when it is new and not yet returned to the
 * interpreter or during system initialization.
 * If multiple bats need to be locked at the same time by the same
 * thread, first lock the view, then the view's parent(s). */
typedef struct BAT {
	/* static bat properties */
	oid hseqbase;		/* head seq base */
	MT_Id creator_tid;	/* which thread created it */
	bat batCacheid;		/* index into BBP */
	role_t batRole;		/* role of the bat */

	/* dynamic bat properties */
	restrict_t batRestricted:2; /* access privileges */
	bool batTransient:1;	/* should the BAT persist on disk? */
	bool batCopiedtodisk:1;	/* once written */
	uint16_t selcnt;	/* how often used in equi select without hash */
	uint16_t unused;	/* value=0 for now (sneakily used by mat.c) */

	/* delta status administration */
	BUN batInserted;	/* start of inserted elements */
	BUN batCount;		/* tuple count */
	BUN batCapacity;	/* tuple capacity */

	/* dynamic column properties */
	uint16_t twidth;	/* byte-width of the atom array */
	int8_t ttype;		/* type id. */
	uint8_t tshift;		/* log2 of bun width */
	/* see also comment near BATassertProps() for more information
	 * about the properties */
	bool tkey:1;		/* no duplicate values present */
	bool tvkey:1;		/* no duplicate values in tvheap */
	bool tnonil:1;		/* there are no nils in the column */
	bool tnil:1;		/* there is a nil in the column */
	bool tsorted:1;		/* column is sorted in ascending order */
	bool trevsorted:1;	/* column is sorted in descending order */
	bool tascii:1;		/* string column is fully ASCII (7 bit) */
	bat ustr;		/* use ustr bat */
	BUN tnokey[2];		/* positions that prove key==FALSE */
	BUN tnosorted;		/* position that proves sorted==FALSE */
	BUN tnorevsorted;	/* position that proves revsorted==FALSE */
	BUN tminpos, tmaxpos;	/* location of min/max value */
	oid tmaxval;
	double tunique_est;	/* estimated number of unique values */
	oid tseqbase;		/* start of dense sequence */
	bool tprivate_bat;	/* used by single worker thread only */

	Heap *theap;		/* space for the column. */
	BUN tbaseoff;		/* offset in heap->base (in whole items) */
	Heap *tvheap;		/* space for the varsized data. */
	Hash *thash;		/* hash table */
#ifdef HAVE_RTREE
	RTree *trtree;		/* rtree geometric index */
#endif
	Heap *torderidx;	/* order oid index */
	Strimps *tstrimps;	/* string imprint index  */
	PROPrec *tprops;	/* list of dynamic properties stored in the bat descriptor */

	struct pipeline_io *pl_io;

	MT_Lock theaplock;	/* lock protecting heap reference changes */
	MT_RWLock thashlock;	/* lock specifically for hash management */
	MT_Lock batIdxLock;	/* lock to manipulate other indexes/properties */
	Heap *oldtail;		/* old tail heap, to be destroyed after commit */
	QryCtx *qc;		/* query context of owner if transient */
} BAT;

/* some access functions for the bitmask type */
static inline void
mskSet(const BAT *b, BUN p)
{
	((uint32_t *) b->theap->base)[p / 32] |= 1U << (p % 32);
}

static inline void
mskClr(const BAT *b, BUN p)
{
	((uint32_t *) b->theap->base)[p / 32] &= ~(1U << (p % 32));
}

static inline void
mskSetVal(const BAT *b, BUN p, msk v)
{
	if (v)
		mskSet(b, p);
	else
		mskClr(b, p);
}

__attribute__((__pure__))
static inline msk
mskGetVal(const BAT *b, BUN p)
{
	return ((uint32_t *) b->theap->base)[p / 32] & (1U << (p % 32));
}

gdk_export gdk_return HEAPextend(Heap *h, size_t size, bool mayshare)
	__attribute__((__warn_unused_result__));
gdk_export size_t HEAPvmsize(const Heap *h)
	__attribute__((__pure__));
gdk_export size_t HEAPmemsize(const Heap *h)
	__attribute__((__pure__));
gdk_export void HEAPdecref(Heap *h, bool remove);
gdk_export void HEAPincref(Heap *h);
gdk_export gdk_return HEAPalloc(Heap *h, size_t nitems, size_t itemsize)
	__attribute__((__warn_unused_result__));
	//__attribute__((__visibility__("hidden")));

__attribute__((__pure__))
static inline bat
VIEWtparent(const BAT *b)
{
	return b->theap == NULL || b->theap->parentid == b->batCacheid ? 0 : b->theap->parentid;
}

__attribute__((__pure__))
static inline bat
VIEWvtparent(const BAT *b)
{
	return b->tvheap == NULL || b->tvheap->parentid == b->batCacheid ? 0 : b->tvheap->parentid;
}

__attribute__((__pure__))
static inline bool
isVIEW(const BAT *b)
{
	return VIEWtparent(b) != 0 || (!b->ustr && VIEWvtparent(b) != 0);
}

typedef struct {
	char *logical;		/* logical name (may point at bak) */
	char bak[16];		/* logical name backup (tmp_%o) */
	BAT descr;		/* the BAT descriptor */
	char *options;		/* A string list of options */
#if SIZEOF_VOID_P == 4
	char physical[20];	/* dir + basename for storage */
#else
	char physical[24];	/* dir + basename for storage */
#endif
	bat next;		/* next BBP slot in linked list */
	int refs;		/* in-memory references on which the loaded status of a BAT relies */
	int lrefs;		/* logical references on which the existence of a BAT relies */
	ATOMIC_TYPE status;	/* status mask used for spin locking */
	MT_Id pid;		/* creator of this bat while "private" */
} BBPrec;

gdk_export bat BBPlimit;
#if SIZEOF_VOID_P == 4
#define N_BBPINIT	1000
#define BBPINITLOG	11
#else
#define N_BBPINIT	10000
#define BBPINITLOG	14
#endif
#define BBPINIT		(1 << BBPINITLOG)
/* absolute maximum number of BATs is N_BBPINIT * BBPINIT
 * this also gives the longest possible "physical" name and "bak" name
 * of a BAT: the "bak" name is "tmp_%o", so at most 14 + \0 bytes on 64
 * bit architecture and 11 + \0 on 32 bit architecture; the physical
 * name is a bit more complicated, but the longest possible name is 22 +
 * \0 bytes (16 + \0 on 32 bits), the longest possible extension adds
 * another 17 bytes (.thsh(grp|uni)(l|b)%08x) */
gdk_export BBPrec *BBP[N_BBPINIT];

/* fast defines without checks; internal use only  */
#define BBP_record(i)	BBP[(i)>>BBPINITLOG][(i)&(BBPINIT-1)]
#define BBP_logical(i)	BBP_record(i).logical
#define BBP_bak(i)	BBP_record(i).bak
#define BBP_next(i)	BBP_record(i).next
#define BBP_physical(i)	BBP_record(i).physical
#define BBP_options(i)	BBP_record(i).options
#define BBP_desc(i)	(&BBP_record(i).descr)
#define BBP_refs(i)	BBP_record(i).refs
#define BBP_lrefs(i)	BBP_record(i).lrefs
#define BBP_status(i)	((unsigned) ATOMIC_GET(&BBP_record(i).status))
#define BBP_pid(i)	BBP_record(i).pid
#define BATgetId(b)	BBP_logical((b)->batCacheid)
#define BBPvalid(i)	(BBP_logical(i) != NULL)

#define BBPRENAME_ALREADY	(-1)
#define BBPRENAME_ILLEGAL	(-2)
#define BBPRENAME_LONG		(-3)
#define BBPRENAME_MEMORY	(-4)

gdk_export void BBPlock(void);
gdk_export void BBPunlock(void);
gdk_export void BBPtmlock(void);
gdk_export void BBPtmunlock(void);

gdk_export BAT *BBPquickdesc(bat b);

#define GDK_VARALIGN SIZEOF_VAR_T

/* atomFromStr returns the number of bytes of the input string that
 * were processed.  atomToStr returns the length of the string
 * produced.  Both functions return -1 on (any kind of) failure.  If
 * *dst is not NULL, *len specifies the available space.  If there is
 * not enough space, or if *dst is NULL, *dst will be freed (if not
 * NULL) and a new buffer will be allocated and returned in *dst.
 * *len will be set to reflect the actual size allocated.  If
 * allocation fails, *dst will be NULL on return and *len is
 * undefined.  In any case, if the function returns, *buf is either
 * NULL or a valid pointer and then *len is the size of the area *buf
 * points to.
 *
 * atomCmp returns a value less than zero/equal to zero/greater than
 * zero if the first argument points to a values which is deemed
 * smaller/equal to/larger than the value pointed to by the second
 * argument.
 *
 * atomHash calculates a hash function for the value pointed to by the
 * argument.
 *
 * atomRead reads cnt values from stream s and returns them in dst.  The
 * available space in dst is given in *dstlen.  If dst is too small (or
 * NULL), a new buffer is allocated and *dstlen is filled in with the
 * allocated size.  atomRread returns a pointer to the buffer where the
 * data was written.  On any kind of failure (usually either allocation
 * or reading), the function returns NULL and dst and *dstlen are
 * unaffected (i.e. similar to realloc).
 */

#define IDLENGTH	64	/* maximum BAT id length */

typedef struct {
	/* simple attributes */
	char name[IDLENGTH];
	uint8_t storage;	/* stored as another type? */
	bool linear;		/* atom can be ordered linearly */
	uint16_t size;		/* fixed size of atom */

	/* automatically generated fields */
	const void *atomNull;	/* global nil value */

	/* generic (fixed + varsized atom) ADT functions */
	ssize_t (*atomFromStr) (allocator *ma, const char *src, size_t *len, void **dst, bool external);
	ssize_t (*atomToStr) (allocator *ma, char **dst, size_t *len, const void *src, bool external);
	void *(*atomRead) (allocator *ma, void *dst, size_t *dstlen, stream *s, size_t cnt);
	gdk_return (*atomWrite) (const void *src, stream *s, size_t cnt);
	int (*atomCmp) (const void *v1, const void *v2);
	bool (*atomEqual) (const void *v1, const void *v2);
	BUN (*atomHash) (const void *v);

	/* varsized atom-only ADT functions */
	var_t (*atomPut) (BAT *, var_t *off, const void *src);
	void (*atomDel) (Heap *, var_t *atom);
	size_t (*atomLen) (const void *atom);
	gdk_return (*atomHeap) (Heap *, size_t);
} atomDesc;

#define MAXATOMS	128

gdk_export atomDesc BATatoms[MAXATOMS];
gdk_export int GDKatomcnt;

gdk_export int ATOMallocate(const char *nme);
gdk_export int ATOMindex(const char *nme);

gdk_export const char *ATOMname(int id);
gdk_export size_t ATOMlen(int id, const void *v);
gdk_export int ATOMprint(int id, const void *val, stream *fd);
gdk_export char *ATOMformat(allocator *ma, int id, const void *val)
	__attribute__((__warn_unused_result__));

/*
 * @- maximum atomic string lengths
 */
#define bitStrlen	8
#define bteStrlen	8
#define shtStrlen	12
#define intStrlen	24
#if SIZEOF_OID == SIZEOF_INT
#define oidStrlen	24
#else
#define oidStrlen	48
#endif
#if SIZEOF_PTR == SIZEOF_INT
#define ptrStrlen	24
#else
#define ptrStrlen	48
#endif
#define lngStrlen	48
#ifdef HAVE_HGE
#define hgeStrlen	96
#endif
#define fltStrlen	48
#define dblStrlen	96

/*
 * The system comes with the traditional atomic types: int (4 bytes),
 * bool(1 byte) and str (variable). In addition, we support the notion
 * of an OID type, which ensures uniqueness of its members.  This
 * leads to the following type descriptor table.
 */

#ifdef HAVE_HGE
gdk_export ssize_t hgeFromStr(allocator *ma, const char *src, size_t *len, hge **dst, bool external);
gdk_export ssize_t hgeToStr(allocator *ma, str *dst, size_t *len, const hge *src, bool external);
#endif
gdk_export ssize_t lngFromStr(allocator *ma, const char *src, size_t *len, lng **dst, bool external);
gdk_export ssize_t lngToStr(allocator *ma, str *dst, size_t *len, const lng *src, bool external);
gdk_export ssize_t intFromStr(allocator *ma, const char *src, size_t *len, int **dst, bool external);
gdk_export ssize_t intToStr(allocator *ma, str *dst, size_t *len, const int *src, bool external);
gdk_export ssize_t ptrFromStr(allocator *ma, const char *src, size_t *len, ptr **dst, bool external);
gdk_export ssize_t ptrToStr(allocator *ma, str *dst, size_t *len, const ptr *src, bool external);
gdk_export ssize_t bitFromStr(allocator *ma, const char *src, size_t *len, bit **dst, bool external);
gdk_export ssize_t bitToStr(allocator *ma, str *dst, size_t *len, const bit *src, bool external);
gdk_export ssize_t OIDfromStr(allocator *ma, const char *src, size_t *len, oid **dst, bool external);
gdk_export ssize_t OIDtoStr(allocator *ma, str *dst, size_t *len, const oid *src, bool external);
gdk_export ssize_t shtFromStr(allocator *ma, const char *src, size_t *len, sht **dst, bool external);
gdk_export ssize_t shtToStr(allocator *ma, str *dst, size_t *len, const sht *src, bool external);
gdk_export ssize_t bteFromStr(allocator *ma, const char *src, size_t *len, bte **dst, bool external);
gdk_export ssize_t bteToStr(allocator *ma, str *dst, size_t *len, const bte *src, bool external);
gdk_export ssize_t fltFromStr(allocator *ma, const char *src, size_t *len, flt **dst, bool external);
gdk_export ssize_t fltToStr(allocator *ma, str *dst, size_t *len, const flt *src, bool external);
gdk_export ssize_t dblFromStr(allocator *ma, const char *src, size_t *len, dbl **dst, bool external);
gdk_export ssize_t dblToStr(allocator *ma, str *dst, size_t *len, const dbl *src, bool external);
gdk_export ssize_t GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len, char quote);
gdk_export ssize_t strFromStr(allocator *ma, const char *restrict src, size_t *restrict len, str *restrict dst, bool external);
gdk_export size_t escapedStrlen(const char *restrict src, const char *sep1, const char *sep2, int quote);
gdk_export size_t escapedStr(char *restrict dst, const char *restrict src, size_t dstlen, const char *sep1, const char *sep2, int quote);
/*
 * @- nil values
 * All types have a single value designated as a NIL value. It
 * designates a missing value and it is ignored (forbidden) in several
 * primitives.  The current policy is to use the smallest value in any
 * ordered domain.  The routine atomnil returns a pointer to the nil
 * value representation.
 */
#define GDK_bit_max ((bit) 1)
#define GDK_bit_min ((bit) 0)
#define GDK_bte_max ((bte) INT8_MAX)
#define GDK_bte_min ((bte) INT8_MIN+1)
#define GDK_sht_max ((sht) INT16_MAX)
#define GDK_sht_min ((sht) INT16_MIN+1)
#define GDK_int_max ((int) INT32_MAX)
#define GDK_int_min ((int) INT32_MIN+1)
#define GDK_lng_max ((lng) INT64_MAX)
#define GDK_lng_min ((lng) INT64_MIN+1)
#ifdef HAVE_HGE
#define GDK_hge_max ((((hge) 1) << 126) - 1 + (((hge) 1) << 126))
#define GDK_hge_min (-GDK_hge_max)
#endif
#define GDK_flt_max ((flt) FLT_MAX)
#define GDK_flt_min ((flt) -FLT_MAX)
#define GDK_dbl_max ((dbl) DBL_MAX)
#define GDK_dbl_min ((dbl) -DBL_MAX)
#define GDK_oid_max (((oid) 1 << ((8 * SIZEOF_OID) - 1)) - 1)
#define GDK_oid_min ((oid) 0)
/* representation of the nil */
gdk_export const bte bte_nil;
gdk_export const sht sht_nil;
gdk_export const int int_nil;
#ifdef NAN_CANNOT_BE_USED_AS_INITIALIZER
/* Definition of NAN is seriously broken on Intel compiler (at least
 * in some versions), so we work around it. */
union _flt_nil_t {
	uint32_t l;
	flt f;
};
gdk_export const union _flt_nil_t _flt_nil_;
#define flt_nil (_flt_nil_.f)
union _dbl_nil_t {
	uint64_t l;
	dbl d;
};
gdk_export const union _dbl_nil_t _dbl_nil_;
#define dbl_nil (_dbl_nil_.d)
#else
gdk_export const flt flt_nil;
gdk_export const dbl dbl_nil;
#endif
gdk_export const lng lng_nil;
#ifdef HAVE_HGE
gdk_export const hge hge_nil;
#endif
gdk_export const oid oid_nil;
gdk_export const char str_nil[2];
gdk_export const ptr ptr_nil;
gdk_export const uuid uuid_nil;
gdk_export const inet4 inet4_nil;
gdk_export const inet6 inet6_nil;

/* derived NIL values - OIDDEPEND */
#define bit_nil	((bit) bte_nil)
#define bat_nil	((bat) int_nil)

#define void_nil	oid_nil

#define is_bit_nil(v)	((v) == GDK_bte_min-1)
#define is_bte_nil(v)	((v) == GDK_bte_min-1)
#define is_sht_nil(v)	((v) == GDK_sht_min-1)
#define is_int_nil(v)	((v) == GDK_int_min-1)
#define is_lng_nil(v)	((v) == GDK_lng_min-1)
#ifdef HAVE_HGE
#define is_hge_nil(v)	((v) == GDK_hge_min-1)
#endif
#define is_oid_nil(v)	((v) == ((oid) 1 << ((8 * SIZEOF_OID) - 1)))
#define is_flt_nil(v)	isnan(v)
#define is_dbl_nil(v)	isnan(v)
#define is_bat_nil(v)	(((v) & 0x7FFFFFFF) == 0) /* v == bat_nil || v == 0 */

#if defined(_MSC_VER) && !defined(__INTEL_COMPILER) && _MSC_VER < 1800
#define isnan(x)	_isnan(x)
#define isinf(x)	(_fpclass(x) & (_FPCLASS_NINF | _FPCLASS_PINF))
#define isfinite(x)	_finite(x)
#endif

#ifdef HAVE_HGE
#define is_uuid_nil(x)	((x).h == 0)
#define is_inet6_nil(x)	((x).align == 0)
#else
#define is_uuid_nil(x)	(memcmp((x).u, uuid_nil.u, UUID_SIZE) == 0)
#define is_inet6_nil(x)	(memcmp((x).hex, inet6_nil.hex, 16) == 0)
#endif
#define is_inet4_nil(x)	((x).align == 0)

#define is_blob_nil(x)	((x)->nitems == ~(size_t)0)

/*
 * @- Derived types
 * In all algorithms across GDK, you will find switches on the types
 * (bte, sht, int, flt, dbl, lng, hge, str). They respectively
 * represent an octet, a 16-bit int, a 32-bit int, a 32-bit float, a
 * 64-bit double, a 64-bit int, a 128-bit int, and a pointer-sized location
 * of a char-buffer (ended by a zero char).
 *
 * In contrast, the types (bit, ptr, bat, oid) are derived types. They
 * do not occur in the switches. The ATOMstorage macro maps them
 * respectively onto a @code{ bte}, @code{ int} (pointers are 32-bit),
 * @code{ int}, and @code{ int}. OIDs are 32-bit.
 *
 * This approach makes it tractable to switch to 64-bits OIDs, or to a
 * fully 64-bits OS easily. One only has to map the @code{ oid} and
 * @code{ ptr} types to @code{ lng} instead of @code{ int}.
 *
 * Derived types mimic their fathers in many ways. They inherit the
 * @code{ size}, @code{ linear}, and @code{ null}
 * properties of their father.  The same goes for the
 * ADT functions HASH, CMP, PUT, NULL, DEL, LEN, and HEAP. So, a
 * derived type differs in only two ways from its father:
 * @table @code
 * @item [string representation]
 * the only two ADT operations specific for a derived type are FROMSTR
 * and TOSTR.
 * @item [identity]
 * (a @code{ bit} is really of a different type than @code{ bte}). The
 * set of operations on derived type values or BATs of such types may
 * differ from the sets of operations on the father type.
 * @end table
 */
/* use "do ... while(0)" so that lhs can safely be used in if statements */
#define ATOMstorage(t)		BATatoms[t].storage
#define ATOMsize(t)		BATatoms[t].size
#define ATOMfromstr(ma,t,s,l,src,ext)	BATatoms[t].atomFromStr(ma,src,l,s,ext)
#define ATOMnilptr(t)		BATatoms[t].atomNull
#define ATOMcompare(t)		BATatoms[t].atomCmp
#define ATOMcmp(t,l,r)		((*ATOMcompare(t))(l, r))
#define ATOMequal(t)		BATatoms[t].atomEqual
#define ATOMeq(t,l,r)		(*ATOMequal(t))(l, r)
#define ATOMhash(t,src)		BATatoms[t].atomHash(src)
#define ATOMdel(t,hp,src)	do if (BATatoms[t].atomDel) BATatoms[t].atomDel(hp,src); while (0)
#define ATOMvarsized(t)		(BATatoms[t].atomPut != NULL)
#define ATOMlinear(t)		BATatoms[t].linear
#define ATOMtype(t)		((t) == TYPE_void ? TYPE_oid : (t))
#define ATOMextern(t)		(ATOMstorage(t) >= TYPE_str)

/* The base type is the storage type if the comparison function, the
 * hash function, and the nil value are the same as those of the
 * storage type; otherwise it is the type itself. */
#define ATOMbasetype(t)	((t) != ATOMstorage(t) &&			\
			 ATOMnilptr(t) == ATOMnilptr(ATOMstorage(t)) && \
			 ATOMcompare(t) == ATOMcompare(ATOMstorage(t)) && \
			 BATatoms[t].atomHash == BATatoms[ATOMstorage(t)].atomHash ? \
			 ATOMstorage(t) : (t))

/*
 * In case that atoms are added to a bat, their logical reference
 * count should be incremented (and decremented if deleted). Notice
 * that BATs with atomic types that have logical references (e.g. BATs
 * of BATs but also BATs of ODMG odSet) can never be persistent, as
 * this would make the commit tremendously complicated.
 */

__attribute__((__warn_unused_result__))
static inline gdk_return
ATOMputVAR(BAT *b, var_t *dst, const void *src)
{
	assert(BATatoms[b->ttype].atomPut != NULL);
	if ((*BATatoms[b->ttype].atomPut)(b, dst, src) == (var_t) -1)
		return GDK_FAIL;
	return GDK_SUCCEED;
}


__attribute__((__warn_unused_result__))
static inline gdk_return
ATOMputFIX(int type, void *dst, const void *src)
{
	assert(BATatoms[type].atomPut == NULL);
	switch (ATOMsize(type)) {
	case 0:		/* void */
		break;
	case 1:
		* (bte *) dst = * (bte *) src;
		break;
	case 2:
		* (sht *) dst = * (sht *) src;
		break;
	case 4:
		* (int *) dst = * (int *) src;
		break;
	case 8:
		* (lng *) dst = * (lng *) src;
		break;
	case 16:
		/* any 16 byte atom will do here */
#ifdef HAVE_HGE
		* (hge *) dst = * (hge *) src;
#else
		* (uuid *) dst = * (uuid *) src;
#endif
		break;
	default:
		memcpy(dst, src, ATOMsize(type));
		break;
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
ATOMreplaceVAR(BAT *b, var_t *dst, const void *src)
{
	var_t loc = *dst;
	int type = b->ttype;

	assert(BATatoms[type].atomPut != NULL);
	if ((*BATatoms[type].atomPut)(b, &loc, src) == (var_t) -1)
		return GDK_FAIL;
	ATOMdel(type, b->tvheap, dst);
	*dst = loc;
	return GDK_SUCCEED;
}

/* string heaps:
 * - strings are 8 byte aligned
 * - start with a 1024 bucket hash table
 * - heaps < 64KiB are fully duplicate eliminated with this hash tables
 * - heaps >= 64KiB are opportunistically (imperfect) duplicate
 *   eliminated as only the last 128KiB chunk is considered and there
 *   is no linked list
 * - buckets and next pointers are unsigned short "indices"
 * - indices should be multiplied by 8 and takes from ELIMBASE to get
 *   an offset
 * Note that a 64KiB chunk of the heap contains at most 8K 8-byte
 * aligned strings. The 1K bucket list means that in worst load, the
 * list length is 8 (OK).
 */
#define GDK_STRHASHTABLE	(1<<10)	/* 1024 */
#define GDK_STRHASHMASK		(GDK_STRHASHTABLE-1)
#define GDK_STRHASHSIZE		(GDK_STRHASHTABLE * sizeof(var_t))
#define GDK_ELIMPOWER		16	/* 64KiB is the threshold */
#define GDK_ELIMLIMIT		(1<<GDK_ELIMPOWER)	/* equivalently: ELIMBASE == 0 */
#define GDK_ELIMDOUBLES(h)	((h)->free < GDK_ELIMLIMIT)
#define GDK_ELIMBASE(x)		(((x) >> GDK_ELIMPOWER) << GDK_ELIMPOWER)
#define GDK_VAROFFSET		((var_t) GDK_STRHASHSIZE)

/*
 * @- String Comparison, NILs and UTF-8
 *
 * Using the char* type for strings is handy as this is the type of
 * any constant strings in a C/C++ program. Therefore, MonetDB uses
 * this definition for str.  However, different compilers and
 * platforms use either signed or unsigned characters for the char
 * type.  It is required that string ordering in MonetDB is consistent
 * over platforms though.
 *
 * As for the choice how strings should be ordered, our support for
 * UTF-8 actually imposes that it should follow 'unsigned char'
 * doctrine (like in the AIX native compiler). In this semantics,
 * though we have to take corrective action to ensure that str(nil) is
 * the smallest value of the domain.
 */
__attribute__((__pure__))
static inline bool
strEQ(const char *l, const char *r)
{
	return strcmp(l, r) == 0;
}

__attribute__((__pure__))
static inline bool
strNil(const char *s)
{
	return s == NULL || (s[0] == '\200' && s[1] == '\0');
}

__attribute__((__pure__))
static inline size_t
strLen(const char *s)
{
	return strNil(s) ? 2 : strlen(s) + 1;
}

__attribute__((__pure__))
static inline int
strCmp(const char *l, const char *r)
{
	return l == r
		? 0
		: strNil(r)
		? !strNil(l)
		: strNil(l) ? -1 : strcmp(l, r);
}

__attribute__((__pure__))
static inline bool
strEq(const char *l, const char *r)
{
	return l == r
		? true
		: strNil(r)
		? strNil(l)
		: strNil(l) ? false : strcmp(l, r) == 0;
}

__attribute__((__pure__))
static inline var_t
VarHeapVal(const void *b, BUN p, int w)
{
	size_t off;
	switch (w) {
	case 1:
		off = (size_t) ((const uint8_t *) b)[p];
		return off == 0 ? 0 : off + GDK_VAROFFSET;
	case 2:
		off = (size_t) ((const uint16_t *) b)[p];
		return off == 0 ? 0 : off + GDK_VAROFFSET;
	case 4:
		return (size_t) ((const uint32_t *) b)[p];
#if SIZEOF_VAR_T == 8
	case 8:
		return (size_t) ((const uint64_t *) b)[p];
#endif
	default:
		MT_UNREACHABLE();
	}
}

/* BAT iterator, also protects use of BAT heaps with reference counts.
 *
 * A BAT iterator has to be used with caution, but it does have to be
 * used in many place.
 *
 * An iterator is initialized by assigning it the result of a call to
 * either bat_iterator or bat_iterator_nolock.  The former must be
 * accompanied by a call to bat_iterator_end to release resources.
 *
 * bat_iterator should be used for BATs that could possibly be modified
 * in another thread while we're reading the contents of the BAT.
 * Alternatively, but only for very quick access, the theaplock can be
 * taken, the data read, and the lock released.  For longer duration
 * accesses, it is better to use the iterator, even without the BUNt*
 * functions, since the theaplock is only held very briefly.
 *
 * Note, bat_iterator must only be used for read-only access.
 *
 * If BATs are to be modified, higher level code must assure that no
 * other thread is going to modify the same BAT at the same time.  A
 * to-be-modified BAT should not use bat_iterator.  It can use
 * bat_iterator_nolock, but be aware that this creates a copy of the
 * heap pointer(s) (i.e. theap and tvheap) and if the heaps get
 * extended, the pointers in the BAT structure may be modified, but that
 * does not modify the pointers in the iterator.  This means that after
 * operations that may grow a heap, the iterator should be
 * reinitialized.
 *
 * The BAT iterator provides a number of fields that can (and often
 * should) be used to access information about the BAT.  For string
 * BATs, if a parallel threads adds values, the offset heap (theap) may
 * get replaced by one that is wider.  This involves changing the twidth
 * and tshift values in the BAT structure.  These changed values should
 * not be used to access the data in the iterator.  Instead, use the
 * width and shift values in the iterator itself.
 */
typedef struct BATiter {
	BAT *b;
	Heap *h;
	void *base;
	Heap *vh;
	BUN count;
	BUN baseoff;
	oid tseq;
	BUN hfree, vhfree;
	BUN nokey[2];
	BUN nosorted, norevsorted;
	BUN minpos, maxpos;
	double unique_est;
	uint16_t width;
	uint8_t shift;
	int8_t type;
	bool key:1,
		vkey:1,
		nonil:1,
		nil:1,
		sorted:1,
		revsorted:1,
		hdirty:1,
		vhdirty:1,
		copiedtodisk:1,
		transient:1,
		ascii:1;
	bat ustr;
	restrict_t restricted:2;
#ifndef NDEBUG
	bool locked:1;
#endif
	union {
		oid tvid;
		bool tmsk;
	};
} BATiter;

static inline BATiter
bat_iterator_nolock(BAT *b)
{
	/* does not get matched by bat_iterator_end */
	if (b) {
		const bool isview = VIEWtparent(b) != 0;
		return (BATiter) {
			.b = b,
			.h = b->theap,
			.base = b->theap->base ? b->theap->base + (b->tbaseoff << b->tshift) : NULL,
			.baseoff = b->tbaseoff,
			.vh = b->tvheap,
			.count = b->batCount,
			.width = b->twidth,
			.shift = b->tshift,
			.type = b->ttype,
			.tseq = b->tseqbase,
			/* don't use b->theap->free in case b is a slice */
			.hfree = b->ttype ?
				  b->ttype == TYPE_msk ?
				   (((size_t) b->batCount + 31) / 32) * 4 :
				  (size_t) b->batCount << b->tshift :
				 0,
			.vhfree = b->tvheap ? b->tvheap->free : 0,
			.nokey[0] = b->tnokey[0],
			.nokey[1] = b->tnokey[1],
			.nosorted = b->tnosorted,
			.norevsorted = b->tnorevsorted,
			.minpos = isview ? BUN_NONE : b->tminpos,
			.maxpos = isview ? BUN_NONE : b->tmaxpos,
			.unique_est = b->tunique_est,
			.key = b->tkey,
			.vkey = (b->tvheap &&
				 (b->tvkey ||
				  BBP_desc(b->tvheap->parentid)->tvkey ||
				  (b->ttype > 0 &&
				   ATOMstorage(b->ttype) == TYPE_str &&
				   GDK_ELIMDOUBLES(b->tvheap)))),
			.nonil = b->tnonil,
			.nil = b->tnil,
			.sorted = b->tsorted,
			.revsorted = b->trevsorted,
			.ascii = b->tascii,
			.ustr = b->ustr,
			/* only look at heap dirty flag if we own it */
			.hdirty = b->theap->parentid == b->batCacheid && b->theap->dirty,
			/* also, if there is no vheap, it's not dirty */
			.vhdirty = b->tvheap && b->tvheap->parentid == b->batCacheid && b->tvheap->dirty,
			.copiedtodisk = b->batCopiedtodisk,
			.transient = b->batTransient,
			.restricted = b->batRestricted,
#ifndef NDEBUG
			.locked = false,
#endif
		};
	}
	return (BATiter) {0};
}

static inline void
bat_iterator_incref(BATiter *bi)
{
#ifndef NDEBUG
	bi->locked = true;
#endif
	HEAPincref(bi->h);
	if (bi->vh)
		HEAPincref(bi->vh);
}

static inline BATiter
bat_iterator(BAT *b)
{
	/* needs matching bat_iterator_end */
	BATiter bi;
	if (b) {
		BAT *pb = NULL, *pvb = NULL;
		/* for a view, always first lock the view and then the
		 * parent(s)
		 * note that a varsized bat can have two different
		 * parents and that the parent for the tail can itself
		 * have a parent for its vheap (which would have to be
		 * our own vheap parent), so lock the vheap after the
		 * tail */
		MT_lock_set(&b->theaplock);
		if (b->theap->parentid != b->batCacheid) {
			pb = BBP_desc(b->theap->parentid);
			MT_lock_set(&pb->theaplock);
		}
		if (b->tvheap &&
		    b->tvheap->parentid != b->batCacheid &&
		    b->tvheap->parentid != b->theap->parentid) {
			pvb = BBP_desc(b->tvheap->parentid);
			MT_lock_set(&pvb->theaplock);
		}
		bi = bat_iterator_nolock(b);
		bat_iterator_incref(&bi);
		if (pvb)
			MT_lock_unset(&pvb->theaplock);
		if (pb)
			MT_lock_unset(&pb->theaplock);
		MT_lock_unset(&b->theaplock);
	} else {
		bi = (BATiter) {
			.b = NULL,
#ifndef NDEBUG
			.locked = true,
#endif
		};
	}
	return bi;
}

/* return a copy of a BATiter instance; needs to be released with
 * bat_iterator_end */
static inline BATiter
bat_iterator_copy(const BATiter *bip)
{
	assert(bip);
	assert(bip->locked);
	if (bip->h)
		HEAPincref(bip->h);
	if (bip->vh)
		HEAPincref(bip->vh);
	return *bip;
}

static inline void
bat_iterator_end(BATiter *bip)
{
	/* matches bat_iterator */
	assert(bip);
	assert(bip->locked);
	if (bip->h)
		HEAPdecref(bip->h, false);
	if (bip->vh)
		HEAPdecref(bip->vh, false);
	*bip = (BATiter) {0};
}

gdk_export gdk_return HEAP_initialize(
	Heap *heap,		/* nbytes -- Initial size of the heap. */
	size_t nbytes,		/* alignment -- for objects on the heap. */
	size_t nprivate,	/* nprivate -- Size of private space */
	int alignment		/* alignment restriction for allocated chunks */
	);

gdk_export var_t HEAP_malloc(BAT *b, size_t nbytes);
gdk_export void HEAP_free(Heap *heap, var_t block);

gdk_export BAT *COLnew(oid hseq, int tltype, BUN capacity, role_t role)
	__attribute__((__warn_unused_result__));
gdk_export BAT *COLnew2(oid hseq, int tt, BUN cap, role_t role, uint16_t width)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATdense(oid hseq, oid tseq, BUN cnt)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATextend(BAT *b, BUN newcap)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATconvert2ustr(BAT *b, BAT *bu)
	__attribute__((__warn_unused_result__));

/* internal */
gdk_export uint8_t ATOMelmshift(int sz)
	__attribute__((__const__));
gdk_export gdk_return ATOMheap(int id, Heap *hp, size_t cap)
	__attribute__((__warn_unused_result__));
gdk_export const char *BATtailname(const BAT *b);

gdk_export gdk_return GDKupgradevarheap(BAT *b, var_t v, BUN cap, BUN ncopy)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNappend(BAT *b, const void *right, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNappendmulti(BAT *b, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATappend(BAT *b, BAT *n, BAT *s, bool force)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BUNreplace(BAT *b, oid left, const void *right, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNreplacemulti(BAT *b, const oid *positions, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BUNreplacemultiincr(BAT *b, oid position, const void *values, BUN count, bool force)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BUNdelete(BAT *b, oid o)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATdel(BAT *b, BAT *d)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATreplace(BAT *b, BAT *p, BAT *n, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATupdate(BAT *b, BAT *p, BAT *n, bool force)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATupdatepos(BAT *b, const oid *positions, BAT *n, bool autoincr, bool force)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return unshare_varsized_heap(BAT *b)
	__attribute__((__warn_unused_result__));
/* Functions to perform a binary search on a sorted BAT.
 * See gdk_search.c for details. */
gdk_export BUN SORTfnd(BAT *b, const void *v);
gdk_export BUN SORTfndfirst(BAT *b, const void *v);
gdk_export BUN SORTfndlast(BAT *b, const void *v);

gdk_export BUN ORDERfnd(BAT *b, Heap *oidxh, const void *v);
gdk_export BUN ORDERfndfirst(BAT *b, Heap *oidxh, const void *v);
gdk_export BUN ORDERfndlast(BAT *b, Heap *oidxh, const void *v);

gdk_export BUN BUNfnd(BAT *b, const void *right);

/* candidates by design are ordered oid lists, besides native oid bats
 * there are
 *	void bats for dense oid lists,
 *	negative oid lists
 *	masked oid lists
 */

#define CAND_NEGOID 0
#define CAND_MSK 1

typedef struct {
	uint64_t
		type:1,
//		mask:1,
		firstbit:48;
} ccand_t;

#define CCAND(b)	((ccand_t *) (b)->tvheap->base)
#define complex_cand(b)	((b)->ttype == TYPE_void && (b)->tvheap != NULL)
#define negoid_cand(b)	(complex_cand(b) && CCAND(b)->type == CAND_NEGOID)
#define mask_cand(b)	(complex_cand(b) && CCAND(b)->type == CAND_MSK)
#define ccand_first(b)	((b)->tvheap->base + sizeof(ccand_t))
#define ccand_free(b)	((b)->tvheap->free - sizeof(ccand_t))

enum cand_type {
	cand_dense,	/* simple dense BAT, i.e. no look ups */
	cand_materialized, /* simple materialized OID list */
	cand_except,	/* list of exceptions in vheap */
	cand_mask,	/* bitmask (TYPE_msk) bat as candidate list */
};

struct canditer {
	BAT *s;			/* candidate BAT the iterator is based on */
	union {
		struct {	/* for all except cand_mask */
			const oid *oids; /* candidate or exceptions for non-dense */
			BUN offset;	/* how much of candidate list BAT we skipped */
			oid add;	/* value to add because of exceptions seen */
		};
		struct {	/* only for cand_mask */
			const uint32_t *mask; /* bitmask */
			BUN nextmsk;
			oid mskoff;
			uint8_t nextbit;
			uint8_t firstbit;
			uint8_t lastbit;
		};
	};
	oid seq;		/* first candidate */
	oid hseq;		/* hseqbase from s/b for first candidate */
	BUN nvals;		/* number of values in .oids/.mask */
	BUN ncand;		/* number of candidates */
	BUN next;		/* next BUN to return value for */
	enum cand_type tpe;
};

/* iterate CI->ncand times using an anonymous index variable, and
 * evaluating the loop count only once */
#define CAND_LOOP(CI)	for (BUN CCTR = 0, CREPS = (CI)->ncand; CCTR < CREPS; CCTR++)
/* iterate CI->ncand times using the given index variable, and
 * evaluating the loop count only once */
#define CAND_LOOP_IDX(CI, IDX)	for (BUN CREPS = (IDX = 0, (CI)->ncand); IDX < CREPS; IDX++)

/* returns the position of the lowest order bit in x, i.e. the
 * smallest n such that (x & (1<<n)) != 0; must not be called with 0 */
__attribute__((__const__))
static inline int
candmask_lobit(uint32_t x)
{
	assert(x != 0);
#ifdef __has_builtin
#if __has_builtin(__builtin_ctz)
	return __builtin_ctz(x) /* ffs(x) - 1 */;
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
	unsigned long idx;
	if (_BitScanForward(&idx, x))
		return (int) idx;
	return -1;
#else
	/* use binary search for the lowest set bit */
	int n = 1;
	if ((x & 0x0000FFFF) == 0) { n += 16; x >>= 16; }
	if ((x & 0x000000FF) == 0) { n +=  8; x >>=  8; }
	if ((x & 0x0000000F) == 0) { n +=  4; x >>=  4; }
	if ((x & 0x00000003) == 0) { n +=  2; x >>=  2; }
	return n - (x & 1);
#endif
#endif
#undef BUILTIN_USED
}

/* population count: count number of 1 bits in a value */
__attribute__((__const__))
static inline uint32_t
candmask_pop(uint32_t x)
{
#ifdef __has_builtin
#if __has_builtin(__builtin_popcount)
	return (uint32_t) __builtin_popcount(x);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
	return (uint32_t) __popcnt((unsigned int) (x));
#else
	/* divide and conquer implementation (the two versions are
	 * essentially equivalent, but the first version is written a
	 * bit smarter) */
#if 1
	x -= (x >> 1) & ~0U/3 /* 0x55555555 */; /* 3-1=2; 2-1=1; 1-0=1; 0-0=0 */
	x = (x & ~0U/5) + ((x >> 2) & ~0U/5) /* 0x33333333 */;
	x = (x + (x >> 4)) & ~0UL/0x11 /* 0x0F0F0F0F */;
	x = (x + (x >> 8)) & ~0UL/0x101 /* 0x00FF00FF */;
	x = (x + (x >> 16)) & 0xFFFF /* ~0UL/0x10001 */;
#else
	x = (x & 0x55555555) + ((x >>  1) & 0x55555555);
	x = (x & 0x33333333) + ((x >>  2) & 0x33333333);
	x = (x & 0x0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F);
	x = (x & 0x00FF00FF) + ((x >>  8) & 0x00FF00FF);
	x = (x & 0x0000FFFF) + ((x >> 16) & 0x0000FFFF);
#endif
	return x;
#endif
#endif
#undef BUILTIN_USED
}

static inline oid
canditer_next_dense(struct canditer *ci)
{
	return ci->seq + ci->next++;
}

static inline oid
canditer_next(struct canditer *ci)
{
	oid o;
	if (ci->next == ci->ncand)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return canditer_next_dense(ci);
	case cand_materialized:
		assert(ci->next < ci->nvals);
		return ci->oids[ci->next++];
	case cand_except:
		o = ci->seq + ci->add + ci->next++;
		while (ci->add < ci->nvals && o == ci->oids[ci->add]) {
			ci->add++;
			o++;
		}
		return o;
	case cand_mask:
		while ((ci->mask[ci->nextmsk] >> ci->nextbit) == 0) {
			ci->nextmsk++;
			ci->nextbit = 0;
		}
		ci->nextbit += candmask_lobit(ci->mask[ci->nextmsk] >> ci->nextbit);
		o = ci->mskoff + ci->nextmsk * 32 + ci->nextbit;
		if (++ci->nextbit == 32) {
			ci->nextbit = 0;
			ci->nextmsk++;
		}
		ci->next++;
		return o;
	default:
		MT_UNREACHABLE();
	}
}

gdk_export void canditer_init(struct canditer *ci, BAT *b, BAT *s)
	__attribute__((__access__(write_only, 1)));
gdk_export oid canditer_peek(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_last(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_prev(struct canditer *ci);
gdk_export oid canditer_peekprev(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_idx(const struct canditer *ci, BUN p)
	__attribute__((__pure__));

__attribute__((__pure__))
static inline oid
canditer_idx_dense(const struct canditer *ci, BUN p)
{
	return p >= ci->ncand ? oid_nil : ci->seq + p;
}

gdk_export void canditer_setidx(struct canditer *ci, BUN p);
gdk_export void canditer_reset(struct canditer *ci);

__attribute__((__pure__))
static inline BUN
canditer_search_dense(const struct canditer *ci, oid o, bool next)
{
	if (o < ci->seq)
		return next ? 0 : BUN_NONE;
	else if (o >= ci->seq + ci->ncand)
		return next ? ci->ncand : BUN_NONE;
	else
		return o - ci->seq;
}

gdk_export BUN canditer_search(const struct canditer *ci, oid o, bool next)
	__attribute__((__pure__));

__attribute__((__pure__))
static inline bool
canditer_contains(const struct canditer *ci, oid o)
{
	if (ci->tpe == cand_mask) {
		if (o < ci->mskoff)
			return false;
		o -= ci->mskoff;
		BUN p = o / 32;
		if (p >= ci->nvals)
			return false;
		o %= 32;
		if (p == ci->nvals - 1 && o >= ci->lastbit)
			return false;
		return ci->mask[p] & (1U << o);
	}
	return canditer_search(ci, o, false) != BUN_NONE;
}

gdk_export oid canditer_mask_next(const struct canditer *ci, oid o, bool next)
	__attribute__((__pure__));

gdk_export BAT *canditer_slice(const struct canditer *ci, BUN lo, BUN hi);
gdk_export BAT *canditer_sliceval(const struct canditer *ci, oid lo, oid hi);
gdk_export BAT *canditer_slice2(const struct canditer *ci, BUN lo1, BUN hi1, BUN lo2, BUN hi2);
gdk_export BAT *canditer_slice2val(const struct canditer *ci, oid lo1, oid hi1, oid lo2, oid hi2);

gdk_export BAT *BATnegcands(oid tseq, BUN nr, BAT *odels);
gdk_export BAT *BATmaskedcands(oid hseq, BUN nr, BAT *masked, bool selected);
gdk_export BAT *BATunmask(BAT *b);

gdk_export BAT *BATmergecand(BAT *a, BAT *b);
gdk_export BAT *BATintersectcand(BAT *a, BAT *b);
gdk_export BAT *BATdiffcand(BAT *a, BAT *b);

__attribute__((__pure__))
static inline BUN
BUNfndVOID(const BAT *b, const oid *v)
{
	if ((is_oid_nil(*v) ^ is_oid_nil(b->tseqbase)) ||
	    *v < b->tseqbase ||
	    *v >= b->tseqbase + b->batCount)
		return BUN_NONE;
	return (BUN) (*v - b->tseqbase);
}

/* return required heap size in bytes for `cnt` elements */
__attribute__((__pure__))
static inline size_t
tailsize(const BAT *b, BUN cnt)
{
	if (b->ttype != TYPE_void) {
		if (ATOMstorage(b->ttype) == TYPE_msk)
			return (size_t) (((cnt + 31) / 32) * 4);
		return (size_t) cnt << b->tshift;
	}
	return 0;
}

__attribute__((__pure__))
static inline void *		/* not const! */
Tloc(const BAT *b, BUN p)
{
	return b->theap->base + ((p + b->tbaseoff) << b->tshift);
}

__attribute__((__pure__))
static inline BUN
BATcount(const BAT *b)
{
	return b->batCount;
}

__attribute__((__pure__))
static inline bool
Tmskval(const BATiter *bi, BUN p)
{
	assert(ATOMstorage(bi->type) == TYPE_msk);
	return ((const uint32_t *) bi->base)[p / 32] & (1U << (p % 32));
}

__attribute__((__pure__))
static inline const void *
BUNtmsk(BATiter *bi, BUN p)
{
	bi->tmsk = Tmskval(bi, p);
	return &bi->tmsk;
}

__attribute__((__pure__))
static inline const void *
BUNtloc(const BATiter *bi, BUN p)
{
	assert(bi->type != TYPE_msk);
	return (const void *) ((char *) bi->base + (p << bi->shift));
}

/* too large: not inline */
gdk_export const void *BUNtpos(BATiter *bi, BUN p)
	__attribute__((__pure__));

__attribute__((__pure__))
static inline const void *
BUNtvar(const BATiter *bi, BUN p)
{
	assert(bi->type && bi->vh);
	var_t off = VarHeapVal(bi->base, p, bi->width);
	return off == 0 ? ATOMnilptr(bi->type) : bi->vh->base + off;
}

__attribute__((__pure__))
static inline const void *
BUNtail(BATiter *bi, BUN p)
{
	if (bi->type) {
		if (bi->vh) {
			return BUNtvar(bi, p);
		} else if (bi->type == TYPE_msk) {
			return BUNtmsk(bi, p);
		} else {
			return BUNtloc(bi, p);
		}
	} else {
		return BUNtpos(bi, p);
	}
}

/* return the oid value at BUN position p from the (v)oid bat b
 * works with any TYPE_void or TYPE_oid bat */
__attribute__((__pure__))
static inline oid
BUNtoid(BAT *b, BUN p)
{
	assert(ATOMtype(b->ttype) == TYPE_oid);
	/* BATcount is the number of valid entries, so with
	 * exceptions, the last value can well be larger than
	 * b->tseqbase + BATcount(b) */
	assert(p < BATcount(b));
	assert(b->ttype == TYPE_void || b->tvheap == NULL);
	if (is_oid_nil(b->tseqbase)) {
		if (b->ttype == TYPE_void)
			return oid_nil;
		MT_lock_set(&b->theaplock);
		oid o = ((const oid *) b->theap->base)[p + b->tbaseoff];
		MT_lock_unset(&b->theaplock);
		return o;
	}
	if (b->ttype == TYPE_oid || b->tvheap == NULL) {
		return b->tseqbase + p;
	}
	/* b->tvheap != NULL, so we know there will be no parallel
	 * modifications (so no locking) */
	BATiter bi = bat_iterator_nolock(b);
	return * (const oid *) BUNtpos(&bi, p);
}

gdk_export BUN BATcount_no_nil(BAT *b, BAT *s);
gdk_export void BATsetcapacity(BAT *b, BUN cnt);
gdk_export void BATsetcount(BAT *b, BUN cnt);
gdk_export BUN BATgrows(BAT *b);
gdk_export gdk_return BATkey(BAT *b, bool onoff);
gdk_export gdk_return BATmode(BAT *b, bool transient);
gdk_export void BAThseqbase(BAT *b, oid o);
gdk_export void BATtseqbase(BAT *b, oid o);

gdk_export BAT *BATsetaccess(BAT *b, restrict_t mode)
	__attribute__((__warn_unused_result__));
gdk_export restrict_t BATgetaccess(BAT *b);


__attribute__((__pure__))
static inline bool
BATdirty(const BAT *b)
{
	return !b->batCopiedtodisk ||
		b->theap->dirty ||
		(b->tvheap != NULL && b->tvheap->dirty);
}

#define BATdirtybi(bi)	(!(bi).copiedtodisk || (bi).hdirty || (bi).vhdirty)

__attribute__((__pure__))
static inline BUN
BATcapacity(const BAT *b)
{
	return b->batCapacity;
}

gdk_export gdk_return BATclear(BAT *b, bool force);
gdk_export BAT *COLcopy(BAT *b, int tt, bool writable, role_t role);
gdk_export BAT *COLcopy2(BAT *b, int tt, bool writable, bool mayshare, role_t role);

gdk_export gdk_return BATgroup(BAT **groups, BAT **extents, BAT **histo, BAT *b, BAT *s, BAT *g, BAT *e, BAT *h)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATsave(BAT *b)
	__attribute__((__warn_unused_result__));

#define NOFARM (-1) /* indicate to GDKfilepath to create relative path */
#define MAXPATH	1024		/* maximum supported file path */

gdk_export gdk_return GDKfilepath(char *buf, size_t bufsize, int farmid, const char *dir, const char *nme, const char *ext)
	__attribute__((__access__(write_only, 1, 2)));
gdk_export bool GDKinmemory(int farmid);
gdk_export bool GDKembedded(void);
gdk_export gdk_return GDKcreatedir(const char *nme);

gdk_export void OIDXdestroy(BAT *b);

gdk_export gdk_return BATprintcolumns(stream *s, int argc, BAT *argv[]);
gdk_export gdk_return BATprint(stream *s, BAT *b);

gdk_export bool BATordered(BAT *b);
gdk_export bool BATordered_rev(BAT *b);
gdk_export gdk_return BATsort(BAT **sorted, BAT **order, BAT **groups, BAT *b, BAT *o, BAT *g, bool reverse, bool nilslast, bool stable)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));


gdk_export void GDKqsort(void *restrict h, void *restrict t, const void *restrict base, size_t n, int hs, int ts, int tpe, bool reverse, bool nilslast);

/* BAT is dense (i.e., BATtvoid() is true and tseqbase is not NIL) */
__attribute__((__pure__))
static inline bool
BATtdense(const BAT *b)
{
	return !is_oid_nil(b->tseqbase) &&
		(b->tvheap == NULL || b->tvheap->free == 0);
}
__attribute__((__pure__))
static inline bool
BATtdensebi(const BATiter *bi)
{
	return !is_oid_nil(bi->tseq) &&
		(bi->vh == NULL || bi->vhfree == 0);
}

/* BATtvoid: BAT can be (or actually is) represented by TYPE_void */
__attribute__((__pure__))
static inline bool
BATtvoid(const BAT *b)
{
	return b->ttype == TYPE_void || BATtdense(b);
}

__attribute__((__pure__))
static inline bool
BATtkey(const BAT *b)
{
	return b->tkey || BATtdense(b);
}

__attribute__((__pure__))
static inline int
BATttype(const BAT *b)
{
	if (BATtdense(b))
		return TYPE_oid;
	return b->ttype;
}

/* set some properties that are trivial to deduce; called with theaplock
 * held */
static inline void
BATsettrivprop(BAT *b)
{
	assert(!is_oid_nil(b->hseqbase));
	assert(is_oid_nil(b->tseqbase) || ATOMtype(b->ttype) == TYPE_oid);
	if (b->ttype == TYPE_void) {
		if (is_oid_nil(b->tseqbase)) {
			b->tnonil = b->batCount == 0;
			b->tnil = !b->tnonil;
			b->trevsorted = true;
			b->tkey = b->batCount <= 1;
			b->tunique_est = b->batCount == 0 ? 0.0 : 1.0;
		} else {
			b->tnonil = true;
			b->tnil = false;
			b->tkey = true;
			b->trevsorted = b->batCount <= 1;
			b->tunique_est = (double) b->batCount;
		}
		b->tsorted = true;
	} else if (b->batCount <= 1) {
		b->tnosorted = b->tnorevsorted = 0;
		b->tnokey[0] = b->tnokey[1] = 0;
		b->tunique_est = (double) b->batCount;
		b->tkey = true;
		if (ATOMlinear(b->ttype)) {
			b->tsorted = true;
			b->trevsorted = true;
			if (b->batCount == 0) {
				b->tminpos = BUN_NONE;
				b->tmaxpos = BUN_NONE;
				b->tnonil = true;
				b->tnil = false;
				if (b->ttype == TYPE_oid) {
					b->tseqbase = 0;
				}
			} else if (b->ttype == TYPE_oid) {
				oid sqbs = ((const oid *) b->theap->base)[b->tbaseoff];
				if (is_oid_nil(sqbs)) {
					b->tnonil = false;
					b->tnil = true;
					b->tminpos = BUN_NONE;
					b->tmaxpos = BUN_NONE;
				} else {
					b->tnonil = true;
					b->tnil = false;
					b->tminpos = 0;
					b->tmaxpos = 0;
				}
				b->tseqbase = sqbs;
			} else {
				var_t off;
				if (b->tvheap
				    ? ((off = VarHeapVal(Tloc(b, 0), 0, b->twidth)) == 0 ||
				       ATOMeq(b->ttype,
					      b->tvheap->base + off,
					      ATOMnilptr(b->ttype)))
				    : ATOMeq(b->ttype, Tloc(b, 0),
					     ATOMnilptr(b->ttype))) {
					/* the only value is NIL */
					b->tminpos = BUN_NONE;
					b->tmaxpos = BUN_NONE;
					b->tnil = true;
					b->tnonil = false;
				} else {
					/* the only value is both min and max */
					b->tminpos = 0;
					b->tmaxpos = 0;
					b->tnonil = true;
					b->tnil = false;
				}
			}
		} else {
			b->tsorted = false;
			b->trevsorted = false;
			b->tminpos = BUN_NONE;
			b->tmaxpos = BUN_NONE;
		}
	} else if (b->batCount == 2 && ATOMlinear(b->ttype)) {
		int c;
		if (b->tvheap) {
			var_t off0 = VarHeapVal(Tloc(b, 0), 0, b->twidth);
			var_t off1 = VarHeapVal(Tloc(b, 0), 1, b->twidth);
			if (off0 == off1)
				c = 0;
			else if (off0 == 0)
				c = ATOMeq(b->ttype,
					   b->tvheap->base + off1,
					   ATOMnilptr(b->ttype)) - 1;
			else if (off1 == 0)
				c = !ATOMeq(b->ttype,
					    b->tvheap->base + off0,
					    ATOMnilptr(b->ttype));
			else
				c = ATOMcmp(b->ttype,
					    b->tvheap->base + off0,
					    b->tvheap->base + off1);
		} else
			c = ATOMcmp(b->ttype, Tloc(b, 0), Tloc(b, 1));
		b->tsorted = c <= 0;
		b->tnosorted = !b->tsorted;
		b->trevsorted = c >= 0;
		b->tnorevsorted = !b->trevsorted;
		b->tkey = c != 0;
		b->tnokey[0] = 0;
		b->tnokey[1] = !b->tkey;
		b->tunique_est = (double) (1 + b->tkey);
	} else {
		if (!ATOMlinear(b->ttype)) {
			b->tsorted = false;
			b->trevsorted = false;
			b->tminpos = BUN_NONE;
			b->tmaxpos = BUN_NONE;
		}
		if (b->tkey)
			b->tunique_est = (double) b->batCount;
	}
}

static inline void
BATnegateprops(BAT *b)
{
	/* disable all properties here */
	b->tnonil = false;
	b->tnil = false;
	if (b->ttype) {
		b->tsorted = false;
		b->trevsorted = false;
		b->tnosorted = 0;
		b->tnorevsorted = 0;
	}
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	b->tmaxpos = b->tminpos = BUN_NONE;
}

#define GDKMAXERRLEN	5120
#define GDKERROR	"!ERROR: "
#define GDKFATAL	"!FATAL: "

gdk_export gdk_return GDKtracer_fill_comp_info(BAT *id, BAT *component, BAT *log_level);

#define GDKerror(...)		TRC_ERROR(GDK, __VA_ARGS__)
#define GDKsyserr(errno, ...)						\
	GDKtracer_log(__FILE__, __func__, __LINE__, TRC_NAME(M_ERROR),	\
		      TRC_NAME(GDK), GDKstrerror(errno, (char[64]){0}, 64), \
		      __VA_ARGS__)
#define GDKsyserror(...)	GDKsyserr(errno, __VA_ARGS__)

gdk_export void GDKclrerr(void);


/* tfastins* family: update a value at a particular location in the bat
 * bunfastapp* family: append a value to the bat
 * *_nocheck: do not check whether the capacity is large enough
 * * (without _nocheck): check bat capacity and possibly extend
 *
 * This means, for tfastins* it is the caller's responsibility to set
 * the batCount and theap->free values correctly (e.g. by calling
 * BATsetcount(), and for *_nocheck to make sure there is enough space
 * allocated in the theap (tvheap for variable-sized types is still
 * extended if needed, making that these functions can fail).
 */
__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nochecknolockVAR(BAT *b, BUN p, const void *v)
{
	var_t d;
	gdk_return rc;
	assert(b->tbaseoff == 0);
	assert(b->theap->parentid == b->batCacheid);
	rc = ATOMputVAR(b, &d, v);
	if (rc != GDK_SUCCEED) {
		return rc;
	}
	if (d != 0 &&
	    b->twidth < SIZEOF_VAR_T &&
	    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 << b->tshift))) {
		/* doesn't fit in current heap, upgrade it */
		rc = GDKupgradevarheap(b, d, 0, MAX(p, b->batCount));
		if (rc != GDK_SUCCEED) {
			return rc;
		}
	}
	switch (b->twidth) {
	case 1:
		if (d != 0)
			d -= GDK_VAROFFSET;
		((uint8_t *) b->theap->base)[p] = (uint8_t) d;
		break;
	case 2:
		if (d != 0)
			d -= GDK_VAROFFSET;
		((uint16_t *) b->theap->base)[p] = (uint16_t) d;
		break;
	case 4:
		((uint32_t *) b->theap->base)[p] = (uint32_t) d;
		break;
#if SIZEOF_VAR_T == 8
	case 8:
		((uint64_t *) b->theap->base)[p] = (uint64_t) d;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheckVAR(BAT *b, BUN p, const void *v)
{
	MT_lock_set(&b->theaplock);
	gdk_return rc = tfastins_nochecknolockVAR(b, p, v);
	MT_lock_unset(&b->theaplock);
	return rc;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheckFIX(BAT *b, BUN p, const void *v)
{
	return ATOMputFIX(b->ttype, Tloc(b, p), v);
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins_nocheck(BAT *b, BUN p, const void *v)
{
	assert(b->theap->parentid == b->batCacheid);
	assert(b->tbaseoff == 0);
	if (b->ttype == TYPE_void) {
		;
	} else if (ATOMstorage(b->ttype) == TYPE_msk) {
		mskSetVal(b, p, * (msk *) v);
	} else if (b->tvheap) {
		return tfastins_nocheckVAR(b, p, v);
	} else {
		return tfastins_nocheckFIX(b, p, v);
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
tfastins(BAT *b, BUN p, const void *v)
{
	if (p >= BATcapacity(b)) {
		if (p >= BUN_MAX) {
			GDKerror("tfastins: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		BUN sz = BATgrows(b);
		if (sz <= p)
			sz = p + BATTINY;
		gdk_return rc = BATextend(b, sz);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return tfastins_nocheck(b, p, v);
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp_nocheck(BAT *b, const void *v)
{
	BUN p = b->batCount;
	if (ATOMstorage(b->ttype) == TYPE_msk && p % 32 == 0)
		((uint32_t *) b->theap->base)[p / 32] = 0;
	gdk_return rc = tfastins_nocheck(b, p, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			if (p % 32 == 0)
				b->theap->free += 4;
		} else
			b->theap->free += b->twidth;
	}
	return rc;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp(BAT *b, const void *v)
{
	BUN p = b->batCount;
	if (ATOMstorage(b->ttype) == TYPE_msk && p % 32 == 0)
		((uint32_t *) b->theap->base)[p / 32] = 0;
	gdk_return rc = tfastins(b, p, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		if (ATOMstorage(b->ttype) == TYPE_msk) {
			if (p % 32 == 0)
				b->theap->free += 4;
		} else
			b->theap->free += b->twidth;
	}
	return rc;
}

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastappOID(BAT *b, oid o)
{
	BUN p = b->batCount;
	if (p >= BATcapacity(b)) {
		if (p >= BUN_MAX) {
			GDKerror("tfastins: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX);
			return GDK_FAIL;
		}
		gdk_return rc = BATextend(b, BATgrows(b));
		if (rc != GDK_SUCCEED)
			return rc;
	}
	((oid *) b->theap->base)[b->batCount++] = o;
	b->theap->free += sizeof(oid);
	return GDK_SUCCEED;
}

#define bunfastappTYPE(TYPE, b, v)					\
	(BATcount(b) >= BATcapacity(b) &&				\
	 ((BATcount(b) == BUN_MAX &&					\
	   (GDKerror("bunfastapp: too many elements to accommodate (" BUNFMT ")\n", BUN_MAX), \
	    true)) ||							\
	  BATextend((b), BATgrows(b)) != GDK_SUCCEED) ?			\
	 GDK_FAIL :							\
	 (assert((b)->theap->parentid == (b)->batCacheid),		\
	  (b)->theap->free += sizeof(TYPE),				\
	  ((TYPE *) (b)->theap->base)[(b)->batCount++] = * (const TYPE *) (v), \
	  GDK_SUCCEED))

__attribute__((__warn_unused_result__))
static inline gdk_return
bunfastapp_nocheckVAR(BAT *b, const void *v)
{
	gdk_return rc;
	rc = tfastins_nocheckVAR(b, b->batCount, v);
	if (rc == GDK_SUCCEED) {
		b->batCount++;
		b->theap->free += b->twidth;
	}
	return rc;
}

/* Strimps exported functions */
gdk_export gdk_return STRMPcreate(BAT *b, BAT *s);
gdk_export BAT *STRMPfilter(BAT *b, BAT *s, const char *q, const bool keep_nils);
gdk_export void STRMPdestroy(BAT *b);
gdk_export bool BAThasstrimps(BAT *b);
gdk_export gdk_return BATsetstrimps(BAT *b);

gdk_export int sketch_populate(BAT* n, BATiter *ni, struct canditer *nci, uint8_t cnting_sketch[BUCKETS][CLZ_BUCKETS]);
/* gdk_export void sketch_merge(BAT* b, BAT* n); */
gdk_export double sketch_estimate(uint8_t cnt_sketch[BUCKETS][CLZ_BUCKETS]);
gdk_export double bat_guess_uniques(BAT *b, BATiter *bi, struct canditer *bci);

/* Rtree structure functions */
#ifdef HAVE_RTREE
gdk_export bool RTREEexists(BAT *b);
gdk_export bool RTREEexists_bid(bat bid);
gdk_export gdk_return BATrtree(BAT *wkb, BAT* mbr);
/* inMBR is really a struct mbr * from geom module, but that is not
 * available here */
gdk_export BUN* RTREEsearch(allocator *ma, BAT *b, const void *inMBR, int result_limit);
#endif

gdk_export void RTREEdestroy(BAT *b);
gdk_export void RTREEfree(BAT *b);

/* The ordered index structure */

gdk_export gdk_return BATorderidx(BAT *b, bool stable);
gdk_export gdk_return GDKmergeidx(BAT *b, BAT**a, int n_ar);
gdk_export bool BATcheckorderidx(BAT *b);

#define DELTAdirty(b)	((b)->batInserted < BATcount(b))

struct Hash {
	int type;		/* type of index entity */
	uint8_t width;		/* width of hash entries */
	bool offsets;		/* hash on offsets */
	BUN mask1;		/* .mask1 < .nbucket <= .mask2 */
	BUN mask2;		/* ... both are power-of-two minus one */
	BUN nbucket;		/* number of valid hash buckets */
	BUN nunique;		/* number of unique values */
	BUN nheads;		/* number of chain heads */
	void *Bckt;		/* hash buckets, points into .heapbckt */
	void *Link;		/* collision list, points into .heaplink */
	Heap heaplink;		/* heap where the hash links are stored */
	Heap heapbckt;		/* heap where the hash buckets are stored */
};

static inline BUN
HASHbucket(const Hash *h, BUN v)
{
	return (v &= h->mask2) < h->nbucket ? v : v & h->mask1;
}

gdk_export gdk_return BAThash(BAT *b);
gdk_export void HASHdestroy(BAT *b);
gdk_export BUN HASHprobe(const Hash *h, const void *v);
gdk_export BUN HASHlist(Hash *h, BUN i);
gdk_export size_t HASHsize(BAT *b);

#define BUN2 2
#define BUN4 4
#if SIZEOF_BUN == 8
#define BUN8 8
#endif
#ifdef BUN2
typedef uint16_t BUN2type;
#endif
typedef uint32_t BUN4type;
#if SIZEOF_BUN > 4
typedef uint64_t BUN8type;
#endif
#ifdef BUN2
#define BUN2_NONE ((BUN2type) UINT16_C(0xFFFF))
#endif
#define BUN4_NONE ((BUN4type) UINT32_C(0xFFFFFFFF))
#ifdef BUN8
#define BUN8_NONE ((BUN8type) UINT64_C(0xFFFFFFFFFFFFFFFF))
#endif

/* play around with h->Bckt[i] and h->Link[j] */

static inline void
HASHput(Hash *h, BUN i, BUN v)
{
	/* if v == BUN_NONE, assigning the value to a BUN2type
	 * etc. automatically converts to BUN2_NONE etc. */
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		((BUN2type *) h->Bckt)[i] = (BUN2type) v;
		break;
#endif
	case BUN4:
		((BUN4type *) h->Bckt)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		((BUN8type *) h->Bckt)[i] = (BUN8type) v;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
}

static inline void
HASHputlink(Hash *h, BUN i, BUN v)
{
	/* if v == BUN_NONE, assigning the value to a BUN2type
	 * etc. automatically converts to BUN2_NONE etc. */
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		assert(v == BUN_NONE || v == BUN2_NONE || v < i);
		((BUN2type *) h->Link)[i] = (BUN2type) v;
		break;
#endif
	case BUN4:
		assert(v == BUN_NONE || v == BUN4_NONE || v < i);
		((BUN4type *) h->Link)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		assert(v == BUN_NONE || v == BUN8_NONE || v < i);
		((BUN8type *) h->Link)[i] = (BUN8type) v;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
}

__attribute__((__pure__))
static inline BUN
HASHget(const Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		i = (BUN) ((BUN2type *) h->Bckt)[i];
		return i == BUN2_NONE ? BUN_NONE : i;
#endif
	case BUN4:
		i = (BUN) ((BUN4type *) h->Bckt)[i];
		return i == BUN4_NONE ? BUN_NONE : i;
#ifdef BUN8
	case BUN8:
		i = (BUN) ((BUN8type *) h->Bckt)[i];
		return i == BUN8_NONE ? BUN_NONE : i;
#endif
	default:
		MT_UNREACHABLE();
	}
}

__attribute__((__pure__))
static inline BUN
HASHgetlink(const Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		i = (BUN) ((BUN2type *) h->Link)[i];
		return i == BUN2_NONE ? BUN_NONE : i;
#endif
	case BUN4:
		i = (BUN) ((BUN4type *) h->Link)[i];
		return i == BUN4_NONE ? BUN_NONE : i;
#ifdef BUN8
	case BUN8:
		i = (BUN) ((BUN8type *) h->Link)[i];
		return i == BUN8_NONE ? BUN_NONE : i;
#endif
	default:
		MT_UNREACHABLE();
	}
}

#if SIZEOF_BUN == 4
#define XXHASHFUNC	XXH32
#else
#define XXHASHFUNC	XXH64
#endif

__attribute__((__pure__))
static inline BUN
bteHash(const void *x)
{
	return (BUN) *(uint8_t *) x;
}

__attribute__((__pure__))
static inline BUN
shtHash(const void *x)
{
	return (BUN) *(uint16_t *) x;
}

__attribute__((__pure__))
static inline BUN
intHash(const void *x)
{
	return (BUN) XXH32(x, sizeof(int), 0);
}

__attribute__((__pure__))
static inline BUN
lngHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(lng), 0);
}

#ifdef HAVE_HGE
__attribute__((__pure__))
static inline BUN
hgeHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(hge), 0);
}
#endif

__attribute__((__pure__))
static inline BUN
fltHash(const void *x)
{
	if (is_flt_nil(*(const flt *)x)) /* any NaN */
		return intHash(&(uint32_t){UINT32_C(0x7FC00000)});
	if (*(const flt *)x == 0) /* +0 or -0 */
		return (BUN) intHash(&(uint32_t){0});
	return intHash(x);
}

__attribute__((__pure__))
static inline BUN
dblHash(const void *x)
{
	if (is_dbl_nil(*(const dbl *)x)) /* any NaN */
		return lngHash(&(uint64_t){UINT64_C(0x7FF8000000000000)});
	if (*(const dbl *)x == 0) /* +0 or -0 */
		return lngHash(&(uint64_t){0});
	return lngHash(x);
}

/* if you happen to already know the string length, pass it along */
__attribute__((__pure__))
static inline BUN
strHashLen(const void *x, size_t len)
{
	return (BUN) XXHASHFUNC(x, len, 0);
}

__attribute__((__pure__))
static inline BUN
strHash(const void *x)
{
	return strHashLen(x, strlen(x));
}

__attribute__((__pure__))
static inline BUN
uuidHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(uuid), 0);
}

__attribute__((__pure__))
static inline BUN
inet4Hash(const void *x)
{
	return (BUN) XXH32(x, sizeof(inet4), 0);
}

__attribute__((__pure__))
static inline BUN
inet6Hash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(inet6), 0);
}

__attribute__((__pure__))
static inline BUN
blobHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, blobsize(((const blob *) x)->nitems), 0);
}

#define hash_loc(H,V)	hash_any(H,V)
#define hash_var(H,V)	hash_any(H,V)
#define hash_any(H,V)	HASHbucket(H, ATOMhash((H)->type, (V)))
#define hash_bte(H,V)	(assert((H)->nbucket >= 256), bteHash(V))
#define hash_sht(H,V)	(assert((H)->nbucket >= 65536), shtHash(V))
#define hash_int(H,V)	HASHbucket(H, intHash(V))
/* XXX return size_t-sized value for 8-byte oid? */
#define hash_lng(H,V)	HASHbucket(H, lngHash(V))
#ifdef HAVE_HGE
#define hash_hge(H,V)	HASHbucket(H, hgeHash(V))
#endif
#if SIZEOF_OID == SIZEOF_INT
#define hash_oid(H,V)	hash_int(H,V)
#else
#define hash_oid(H,V)	hash_lng(H,V)
#endif
#define hash_inet4(H,V)	HASHbucket(H, inet4Hash(V))

#define hash_flt(H,V)	HASHbucket(H, ATOMhash(TYPE_flt, (V)))
#define hash_dbl(H,V)	HASHbucket(H, ATOMhash(TYPE_dbl, (V)))

#define hash_uuid(H,V)	HASHbucket(H, uuidHash(V))

#define hash_inet6(H,V)	HASHbucket(H, inet6Hash(V))

/*
 * @- hash-table supported loop over BUNs The first parameter `bi' is
 * a BAT iterator, the second (`h') should point to the Hash
 * structure, and `v' a pointer to an atomic value (corresponding to
 * the head column of `b'). The 'hb' is an BUN index, pointing out the
 * `hb'-th BUN.
 */
#define HASHloop(bi, h, hb, v)					\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if ((h)->offsets ?					\
		    *(var_t*)(v) == VarHeapVal((bi)->base, hb, (bi)->width) : \
		    ATOMeq(h->type, v, BUNtail(bi, hb)))
#define HASHloop_str(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHbucket(h, strHash(v)));	\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (strEQ(v, BUNtvar(bi, hb)))
#define HASHloop_var_t(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (*(var_t*)(v) == VarHeapVal((bi)->base, hb, (bi)->width))

#define HASHlooploc(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMeq(h->type, v, BUNtloc(bi, hb)))
#define HASHloopvar(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMeq(h->type, v, BUNtvar(bi, hb)))

#define HASHloop_TYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (* (const TYPE *) (v) == * (const TYPE *) BUNtloc(bi, hb))

/* need to take special care comparing nil floating point values */
#define HASHloop_fTYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (is_##TYPE##_nil(* (const TYPE *) (v))		\
		    ? is_##TYPE##_nil(* (const TYPE *) BUNtloc(bi, hb)) \
		    : * (const TYPE *) (v) == * (const TYPE *) BUNtloc(bi, hb))

#define HASHloop_bte(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, bte)
#define HASHloop_sht(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, sht)
#define HASHloop_int(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, int)
#define HASHloop_lng(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, lng)
#ifdef HAVE_HGE
#define HASHloop_hge(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, hge)
#endif
#define HASHloop_flt(bi, h, hb, v)	HASHloop_fTYPE(bi, h, hb, v, flt)
#define HASHloop_dbl(bi, h, hb, v)	HASHloop_fTYPE(bi, h, hb, v, dbl)
#define HASHloop_inet4(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_inet4(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const inet4 *) (v))->align == ((const inet4 *) BUNtloc(bi, hb))->align)
#ifdef HAVE_HGE
#define HASHloop_uuid(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_uuid(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const uuid *) (v))->h == ((const uuid *) BUNtloc(bi, hb))->h)
#define HASHloop_inet6(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_inet6(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const inet6 *) (v))->align == ((const inet6 *) BUNtloc(bi, hb))->align)
#else
#define HASHloop_uuid(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_uuid(h, v));				\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (memcmp((const uuid *) (v), (const uuid *) BUNtloc(bi, hb), 16) == 0)
//		if (((const uuid *) (v))->l[0] == ((const uuid *) BUNtloc(bi, hb))->l[0] && ((const uuid *) (v))->l[1] == ((const uuid *) BUNtloc(bi, hb))->l[1])
#define HASHloop_inet6(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_inet6(h, v));				\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (memcmp((const inet6 *) (v), (const inet6 *) BUNtloc(bi, hb), 16) == 0)
//		if (((const inet6 *) (v))->align[0] == ((const inet6 *) BUNtloc(bi, hb))->align[0] && ((const inet6 *) (v))->align[1] == ((const inet6 *) BUNtloc(bi, hb))->align[1])
#endif

#define BBPLOADED	1	/* set if bat in memory */
#define BBPSWAPPED	2	/* set if dirty bat is not in memory */
#define BBPTMP		4	/* set if non-persistent bat has image on disk */

/* These 4 symbols indicate what the persistence state is of a bat.
 * - If the bat was persistent at the last commit (or at startup
 *   before the first commit), BBPEXISTING or BBPDELETED is set.
 * - If the bat is to be persistent after the next commit, BBPEXISTING
 *   or BBPNEW is set (i.e. (status&BBPPERSISTENT) != 0).
 * - If the bat was transient at the last commit (or didn't exist),
 *   BBPNEW is set, or none of these flag values is set.
 * - If the bat is to be transient at the next commit, BBPDELETED is
 *   set, or none of these flag values is set.
 * BATmode() switches between BBPDELETED and BBPEXISTING (bat was
 * persistent at last commit), or between BBPNEW and 0 (bat was
 * transient or didn't exist at last commit).
 * Committing a bat switches from BBPNEW to BBPEXISTING, or turns off
 * BBPDELETED.
 * In any case, only at most one of BBPDELETED, BBPEXISTING, and
 * BBPNEW may be set at any one time.
 *
 * In short,
 * BBPEXISTING -- bat was and should remain persistent;
 * BBPDELETED -- bat was persistent at last commit and should be transient;
 * BBPNEW -- bat was transient at last commit and should be persistent;
 * none of the above -- bat was and should remain transient.
 */
#define BBPDELETED	16	/* set if bat persistent at last commit is now transient */
#define BBPEXISTING	32	/* set if bat was already persistent at end of last commit */
#define BBPNEW		64	/* set if bat has become persistent since last commit */
#define BBPPERSISTENT	(BBPEXISTING|BBPNEW)	/* mask for currently persistent bats */

#define BBPSTATUS	127

#define BBPUNLOADING	128	/* set while we are unloading */
#define BBPLOADING	256	/* set while we are loading */
#define BBPSAVING       512	/* set while we are saving */
#define BBPRENAMED	1024	/* set when bat is renamed in this transaction */
#define BBPDELETING	2048	/* set while we are deleting (special case in module unload) */
#define BBPHOT		4096	/* bat is "hot", i.e. is still in active use */
#define BBPSYNCING	8192	/* bat between creating backup and saving */

#define BBPUNSTABLE	(BBPUNLOADING|BBPDELETING)	/* set while we are unloading */
#define BBPWAITING      (BBPUNLOADING|BBPLOADING|BBPSAVING|BBPDELETING|BBPSYNCING)

gdk_export bat getBBPsize(void); /* current occupied size of BBP array */
gdk_export unsigned BBPheader(FILE *fp, int *lineno, bat *bbpsize, lng *logno, bool allow_hge_upgrade);
gdk_export int BBPreadBBPline(FILE *fp, unsigned bbpversion, int *lineno, BAT *bn,
#ifdef GDKLIBRARY_HASHASH
			      int *hashash,
#endif
			      char *batname, char *filename, char **options);

/* global calls */
gdk_export gdk_return BBPaddfarm(const char *dirname, uint32_t rolemask, bool logerror);

/* update interface */
gdk_export gdk_return BBPsave(BAT *b);
gdk_export int BBPrename(BAT *b, const char *nme);

/* query interface */
gdk_export bat BBPindex(const char *nme);

/* swapping interface */
gdk_export int BBPfix(bat b);
gdk_export int BBPunfix(bat b);
static inline void
BBPreclaim(BAT *b)
{
	if (b != NULL)
		BBPunfix(b->batCacheid);
}
gdk_export int BBPretain(bat b);
gdk_export int BBPrelease(bat b);
gdk_export void BBPkeepref(BAT *b)
	__attribute__((__nonnull__(1)));
gdk_export void BBPcold(bat i);
gdk_export void BBPrelinquishbats(void);
#ifdef GDKLIBRARY_JSON
typedef gdk_return ((*json_storage_conversion)(char **, const char **));
gdk_export gdk_return BBPjson_upgrade(json_storage_conversion);
#endif
#define BBP_status_set(bid, mode)			\
	ATOMIC_SET(&BBP_record(bid).status, mode)

#define BBP_status_on(bid, flags)			\
	ATOMIC_OR(&BBP_record(bid).status, flags)

#define BBP_status_off(bid, flags)			\
	ATOMIC_AND(&BBP_record(bid).status, ~(flags))

#define BBPswappable(b) ((b) && (b)->batCacheid && BBP_refs((b)->batCacheid) == 0)
#define BBPtrimmable(b) (BBPswappable(b) && isVIEW(b) == 0 && (BBP_status((b)->batCacheid)&BBPWAITING) == 0)

/* low level support for patching BBP.dir */
gdk_export gdk_return BBPdir_first(bool subcommit, lng logno, FILE **obbpfp, FILE **nbbpfp);
gdk_export bat BBPdir_step(bat bid, BUN size, int n, char *buf, size_t bufsize, FILE **obbpfp, FILE *nbbpf, BATiter *bi, int *nbatp);
gdk_export gdk_return BBPdir_last(int n, char *buf, size_t bufsize, FILE *obbpf, FILE *nbbpf);

gdk_export BUN GDKL3_size;

gdk_export void GDKprintinforegister(void (*func)(void));
gdk_export void GDKprintinfo(void);

gdk_export const char *GDKgetenv(const char *name);

gdk_export bool GDKgetenv_istext(const char *name, const char* text);
gdk_export bool GDKgetenv_isyes(const char *name);
gdk_export bool GDKgetenv_istrue(const char *name);

gdk_export int GDKgetenv_int(const char *name, int def);

gdk_export gdk_return GDKsetenv(const char *name, const char *value);
gdk_export gdk_return GDKcopyenv(BAT **key, BAT **val, bool writable);

/*
 * @+ Memory management
 * Memory management in GDK mostly relies on the facilities offered by
 * the underlying OS.  The below routines monitor the available memory
 * resources which consist of physical swap space and logical vm
 * space.  There are three kinds of memory, that affect these two
 * resources in different ways:
 *
 * - memory mapping
 *   which ask for a logical region of virtual memory space.  In
 *   principle, no physical memory is needed to keep the system afloat
 *   here, as the memory mapped file is swapped onto a disk object
 *   that already exists.
 *
 *   Actually, there are two kings of memory mapping used in GDK,
 *   namely read-only direct mapped and writable copy-on write. For
 *   the dirty pages, the latter actually also consumes physical
 *   memory resources, but that is ignored here for simplicity.
 *
 * - anonymous virtual memory
 *   This is virtual memory that is mapped on the swap file. Hence,
 *   this consumes both logical VM space resources and physical memory
 *   space.
 *
 * - malloced memory
 *   comes from the heap and directly consumes physical memory
 *   resources.
 *
 * The malloc routine checks the memory consumption every 1000 calls,
 * or for calls larger that 50000 bytes. Consequently, at least every
 * 50MB increase, alloc memory is checked. The VM calls always check
 * the memory consumption.
 */
/* default setting to administer everything */
#define GDK_MEM_NULLALLOWED

#if SIZEOF_VOID_P==8
#define GDK_VM_MAXSIZE	LL_CONSTANT(4398046511104)	/* :-) a 64-bit OS: 4TB */
#else
#define GDK_VM_MAXSIZE	LL_CONSTANT(1610612736)	/* :-| a 32-bit OS: 1.5GB */
#endif
/* virtual memory defines */
gdk_export size_t _MT_npages;
gdk_export size_t _MT_pagesize;

#define MT_pagesize()	_MT_pagesize
#define MT_npages()	_MT_npages

gdk_export size_t GDK_mem_maxsize;	/* max allowed size of committed memory */
gdk_export size_t GDK_vm_maxsize;	/* max allowed size of reserved vm */

gdk_export void *GDKmmap(const char *path, int mode, size_t len)
	__attribute__((__warn_unused_result__));
gdk_export gdk_return GDKmunmap(void *addr, int mode, size_t len);

gdk_export size_t GDKmem_cursize(void);	/* RAM/swapmem that MonetDB has claimed from OS */
gdk_export size_t GDKvm_cursize(void);	/* current MonetDB VM address space usage */

gdk_export void GDKfree(void *blk);
gdk_export void *GDKmalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__malloc__(GDKfree, 1)))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKzalloc(size_t size)
	__attribute__((__malloc__))
	__attribute__((__malloc__(GDKfree, 1)))
	__attribute__((__alloc_size__(1)))
	__attribute__((__warn_unused_result__));
gdk_export void *GDKrealloc(void *pold, size_t size)
	__attribute__((__alloc_size__(2)))
	__attribute__((__warn_unused_result__));
gdk_export char *GDKstrdup(const char *s)
	__attribute__((__malloc__))
	__attribute__((__malloc__(GDKfree, 1)))
	__attribute__((__warn_unused_result__));
gdk_export char *GDKstrndup(const char *s, size_t n)
	__attribute__((__malloc__))
	__attribute__((__malloc__(GDKfree, 1)))
	__attribute__((__warn_unused_result__));
gdk_export char *humansize(size_t val, char *buf, size_t buflen)
	__attribute__((__access__(write_only, 2, 3)));

gdk_export void MT_init(void);	/*  init the package. */
struct opt;
gdk_export gdk_return GDKinit(struct opt *set, int setlen, bool embedded, const char *caller_revision);

/*
 * Upon closing the session, all persistent BATs should be saved and
 * the transient BATs should be removed.  The buffer pool manager
 * takes care of this.
 */
gdk_export bool GDKexiting(void);

gdk_export void GDKprepareExit(void);
gdk_export void GDKreset(int status);
/* global version number */
gdk_export const char *GDKversion(bool full)
	__attribute__((__const__));
/* ABI version of GDK library */
gdk_export const char *GDKlibversion(void)
	__attribute__((__const__));

// these are used in embedded mode to jump out of GDKfatal
gdk_export jmp_buf GDKfataljump;
gdk_export char *GDKfatalmsg;
gdk_export bool GDKfataljumpenable;

/* Timers
 * The following relative timers are available for inspection.
 * Note that they may consume recognizable overhead.
 *
 */
gdk_export lng GDKusec(void);
gdk_export int GDKms(void);


#if !defined(NDEBUG) && !defined(__COVERITY__) && !defined(_CLANGD)
/* In debugging mode, replace GDKmalloc and other functions with a
 * version that optionally prints calling information.
 *
 * We have two versions of this code: one using a GNU C extension, and
 * one using traditional C.  The GNU C version also prints the name of
 * the calling function.
 */
#ifdef __GNUC__
#define GDKmalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKmalloc(_size);			\
		TRC_DEBUG(ALLOC, "GDKmalloc(%zu) -> %p\n",	\
			  _size, _res);				\
		_res;						\
	})
#define GDKzalloc(s)						\
	({							\
		size_t _size = (s);				\
		void *_res = GDKzalloc(_size);			\
		TRC_DEBUG(ALLOC, "GDKzalloc(%zu) -> %p\n",	\
			  _size, _res);				\
		_res;						\
	})
#define GDKrealloc(p, s)					\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		char _buf[2*sizeof(void*)+3];			\
		snprintf(_buf, sizeof(_buf), "%p", _ptr);	\
		void *_res = GDKrealloc(_ptr, _size);		\
		TRC_DEBUG(ALLOC, "GDKrealloc(%s,%zu) -> %p\n",	\
			  _buf, _size, _res);			\
		_res;						\
	 })
#define GDKfree(p)							\
	({								\
		void *_ptr = (p);					\
		if (_ptr)						\
			TRC_DEBUG(ALLOC, "GDKfree(%p)\n", _ptr);	\
		GDKfree(_ptr);						\
	})
#define GDKstrdup(s)						\
	({							\
		const char *_str = (s);				\
		void *_res = GDKstrdup(_str);			\
		TRC_DEBUG(ALLOC, "GDKstrdup(len=%zu) -> %p\n",	\
			  _str ? strlen(_str) : 0, _res);	\
		_res;						\
	})
#define GDKstrndup(s, n)					\
	({							\
		const char *_str = (s);				\
		size_t _n = (n);				\
		void *_res = GDKstrndup(_str, _n);		\
		TRC_DEBUG(ALLOC, "GDKstrndup(len=%zu) -> %p\n", \
			  _n,	_res);				\
		_res;						\
	})
#define GDKmmap(p, m, l)						\
	({								\
		const char *_path = (p);				\
		int _mode = (m);					\
		size_t _len = (l);					\
		void *_res = GDKmmap(_path, _mode, _len);		\
		TRC_DEBUG(ALLOC, "GDKmmap(%s,0x%x,%zu) -> %p\n",	\
			  _path ? _path : "NULL",			\
			  (unsigned) _mode, _len,			\
			  _res);					\
		_res;							\
	 })
#define GDKmunmap(p, m, l)					\
	({							\
		void *_ptr = (p);				\
		int _mode = (m);				\
		size_t _len = (l);				\
		gdk_return _res = GDKmunmap(_ptr, _mode, _len);	\
		TRC_DEBUG(ALLOC,				\
			  "GDKmunmap(%p,0x%x,%zu) -> %u\n",	\
			  _ptr, (unsigned) _mode, _len, _res);	\
		_res;						\
	})
#define malloc(s)					\
	({						\
		size_t _size = (s);			\
		void *_res = malloc(_size);		\
		TRC_DEBUG(ALLOC, "malloc(%zu) -> %p\n", \
			  _size, _res);			\
		_res;					\
	})
#define calloc(n, s)						\
	({							\
		size_t _nmemb = (n);				\
		size_t _size = (s);				\
		void *_res = calloc(_nmemb,_size);		\
		TRC_DEBUG(ALLOC, "calloc(%zu,%zu) -> %p\n",	\
			  _nmemb, _size, _res);			\
		_res;						\
	})
#define realloc(p, s)						\
	({							\
		void *_ptr = (p);				\
		size_t _size = (s);				\
		char _buf[12];					\
		snprintf(_buf, sizeof(_buf), "%p", _ptr);	\
		void *_res = realloc(_ptr, _size);		\
		TRC_DEBUG(ALLOC, "realloc(%s,%zu) -> %p\n",	\
			  _buf, _size, _res);			\
		_res;						\
	 })
#define free(p)						\
	({						\
		void *_ptr = (p);			\
		TRC_DEBUG(ALLOC, "free(%p)\n", _ptr);	\
		free(_ptr);				\
	})
#else
static inline void *
GDKmalloc_debug(size_t size)
{
	void *res = GDKmalloc(size);
	TRC_DEBUG(ALLOC, "GDKmalloc(%zu) -> %p\n", size, res);
	return res;
}
#define GDKmalloc(s)	GDKmalloc_debug((s))
static inline void *
GDKzalloc_debug(size_t size)
{
	void *res = GDKzalloc(size);
	TRC_DEBUG(ALLOC, "GDKzalloc(%zu) -> %p\n", size, res);
	return res;
}
#define GDKzalloc(s)	GDKzalloc_debug((s))
static inline void *
GDKrealloc_debug(void *ptr, size_t size)
{
	void *res = GDKrealloc(ptr, size);
	TRC_DEBUG(ALLOC, "GDKrealloc(%p,%zu) -> %p\n", ptr, size, res);
	return res;
}
#define GDKrealloc(p, s)	GDKrealloc_debug((p), (s))
static inline void
GDKfree_debug(void *ptr)
{
	TRC_DEBUG(ALLOC, "GDKfree(%p)\n", ptr);
	GDKfree(ptr);
}
#define GDKfree(p)	GDKfree_debug((p))
static inline char *
GDKstrdup_debug(const char *str)
{
	void *res = GDKstrdup(str);
	TRC_DEBUG(ALLOC, "GDKstrdup(len=%zu) -> %p\n",
		  str ? strlen(str) : 0, res);
	return res;
}
#define GDKstrdup(s)	GDKstrdup_debug((s))
static inline char *
GDKstrndup_debug(const char *str, size_t n)
{
	void *res = GDKstrndup(str, n);
	TRC_DEBUG(ALLOC, "GDKstrndup(len=%zu) -> %p\n", n, res);
	return res;
}
#define GDKstrndup(s, n)	GDKstrndup_debug((s), (n))
static inline void *
GDKmmap_debug(const char *path, int mode, size_t len)
{
	void *res = GDKmmap(path, mode, len);
	TRC_DEBUG(ALLOC, "GDKmmap(%s,0x%x,%zu) -> %p\n",
		  path ? path : "NULL", (unsigned) mode, len, res);
	return res;
}
#define GDKmmap(p, m, l)	GDKmmap_debug((p), (m), (l))
static inline gdk_return
GDKmunmap_debug(void *ptr, int mode, size_t len)
{
	gdk_return res = GDKmunmap(ptr, mode, len);
	TRC_DEBUG(ALLOC, "GDKmunmap(%p,0x%x%zu) -> %d\n",
		  ptr, mode, len, (int) res);
	return res;
}
#define GDKmunmap(p, m, l)	GDKmunmap_debug((p), (m), (l))
static inline void *
malloc_debug(size_t size)
{
	void *res = malloc(size);
	TRC_DEBUG(ALLOC, "malloc(%zu) -> %p\n", size, res);
	return res;
}
#define malloc(s)	malloc_debug((s))
static inline void *
calloc_debug(size_t nmemb, size_t size)
{
	void *res = calloc(nmemb, size);
	TRC_DEBUG(ALLOC, "calloc(%zu,%zu) -> %p\n", nmemb, size, res);
	return res;
}
#define calloc(n, s)	calloc_debug((n), (s))
static inline void *
realloc_debug(void *ptr, size_t size)
{
	void *res = realloc(ptr, size);
	TRC_DEBUG(ALLOC, "realloc(%p,%zu) -> %p \n", ptr, size, res);
	return res;
}
#define realloc(p, s)	realloc_debug((p), (s))
static inline void
free_debug(void *ptr)
{
	TRC_DEBUG(ALLOC, "free(%p)\n", ptr);
	free(ptr);
}
#define free(p)	free_debug((p))
#endif
#endif

/* functions defined in gdk_bat.c */
gdk_export gdk_return void_inplace(BAT *b, oid id, const void *val, bool force)
	__attribute__((__warn_unused_result__));

#ifdef NATIVE_WIN32
#ifdef _MSC_VER
#define fileno _fileno
#endif
#define fdopen _fdopen
#define putenv _putenv
#endif

/* Return a pointer to the value contained in V.  Also see VALget
 * which returns a void *. */
__attribute__((__pure__))
static inline const void *
VALptr(const ValRecord *v)
{
	switch (ATOMstorage(v->vtype)) {
	case TYPE_void: return (const void *) &v->val.oval;
	case TYPE_msk: return (const void *) &v->val.mval;
	case TYPE_bte: return (const void *) &v->val.btval;
	case TYPE_sht: return (const void *) &v->val.shval;
	case TYPE_int: return (const void *) &v->val.ival;
	case TYPE_flt: return (const void *) &v->val.fval;
	case TYPE_dbl: return (const void *) &v->val.dval;
	case TYPE_lng: return (const void *) &v->val.lval;
#ifdef HAVE_HGE
	case TYPE_hge: return (const void *) &v->val.hval;
#endif
	case TYPE_uuid: return (const void *) &v->val.uval;
	case TYPE_inet4: return (const void *) &v->val.ip4val;
	case TYPE_inet6: return (const void *) &v->val.ip6val;
	case TYPE_ptr: return (const void *) &v->val.pval;
	case TYPE_str: return (const void *) v->val.sval;
	default:       return (const void *) v->val.pval;
	}
}

#define THREADS		1024	/* maximum value for gdk_nr_threads */

gdk_export stream *GDKstdout;
gdk_export stream *GDKstdin;

#define GDKerrbuf	(GDKgetbuf())

static inline bat
BBPcheck(bat x)
{
	if (!is_bat_nil(x)) {
		assert(x > 0);

		if (x < 0 || x >= getBBPsize() || BBP_logical(x) == NULL) {
			TRC_DEBUG(CHECK, "range error %d\n", (int) x);
		} else {
			/* No longer guaranteed in pipeline, hence disable it.
			 * TODO: find a proper way for this check
			 */
			//assert(BBP_pid(x) == 0 || BBP_pid(x) == MT_getpid());
			return x;
		}
	}
	return 0;
}

gdk_export BAT *BATdescriptor(bat i);

gdk_export gdk_return TMsubcommit_list(bat *restrict subcommit, BUN *restrict sizes, int cnt, lng logno)
	__attribute__((__warn_unused_result__));

gdk_export void BATcommit(BAT *b, BUN size);

gdk_export int ALIGNsynced(BAT *b1, BAT *b2);

gdk_export void BATassertProps(BAT *b);

gdk_export BAT *VIEWcreate(oid seq, BAT *b, BUN l, BUN h);
gdk_export void VIEWbounds(BAT *b, BAT *view, BUN l, BUN h);

#define ALIGNapp(x, f, e)						\
	do {								\
		if (!(f)) {						\
			MT_lock_set(&(x)->theaplock);			\
			if ((x)->batRestricted == BAT_READ ||		\
			   ((ATOMIC_GET(&(x)->theap->refs) & HEAPREFS) > 1)) { \
				GDKerror("access denied to %s, aborting.\n", BATgetId(x)); \
				MT_lock_unset(&(x)->theaplock);		\
				return (e);				\
			}						\
			MT_lock_unset(&(x)->theaplock);			\
		}							\
	} while (false)

#define BATloop(bi, p, q)				\
	for (q = (bi)->count, p = 0; p < q; p++)

enum prop_t {
	GDK_MIN_BOUND, /* MINimum allowed value for range partitions [min, max> */
	GDK_MAX_BOUND, /* MAXimum of the range partitions [min, max>, ie. excluding this max value */
	GDK_NOT_NULL,  /* bat bound to be not null */
	/* CURRENTLY_NO_PROPERTIES_DEFINED, */
};

gdk_export ValPtr BATgetprop(BAT *b, enum prop_t idx);
gdk_export ValPtr BATgetprop_nolock(BAT *b, enum prop_t idx);
gdk_export void BATrmprop(BAT *b, enum prop_t idx);
gdk_export void BATrmprop_nolock(BAT *b, enum prop_t idx);
gdk_export ValPtr BATsetprop(BAT *b, enum prop_t idx, int type, const void *v);
gdk_export ValPtr BATsetprop_nolock(BAT *b, enum prop_t idx, int type, const void *v);

#define JOIN_EQ		0
#define JOIN_LT		(-1)
#define JOIN_LE		(-2)
#define JOIN_GT		1
#define JOIN_GE		2
#define JOIN_BAND	3
#define JOIN_NE		(-3)

gdk_export BAT *BATselect(BAT *b, BAT *s, const void *tl, const void *th, bool li, bool hi, bool anti, bool nil_matches);
gdk_export BAT *BATthetaselect(BAT *b, BAT *s, const void *val, const char *op);

gdk_export BAT *BATconstant(oid hseq, int tt, const void *val, BUN cnt, role_t role);
gdk_export gdk_return BATsubcross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BAToutercross(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool max_one)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));

gdk_export gdk_return BATleftjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATmarkjoin(BAT **r1p, BAT **r2p, BAT **r3p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__access__(write_only, 3)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATouterjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool match_one, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATthetajoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, int op, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATsemijoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATintersect(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool max_one, BUN estimate);
gdk_export BAT *BATdiff(BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, bool not_in, BUN estimate);
gdk_export gdk_return BATjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, bool nil_matches, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BUN BATguess_uniques(BAT *b, struct canditer *ci);
gdk_export gdk_return BATbandjoin(BAT **r1p, BAT **r2p, BAT *l, BAT *r, BAT *sl, BAT *sr, const void *c1, const void *c2, bool li, bool hi, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export gdk_return BATrangejoin(BAT **r1p, BAT **r2p, BAT *l, BAT *rl, BAT *rh, BAT *sl, BAT *sr, bool li, bool hi, bool anti, bool symmetric, BUN estimate)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATproject(BAT *restrict l, BAT *restrict r);
gdk_export BAT *BATproject2(BAT *restrict l, BAT *restrict r1, BAT *restrict r2);
gdk_export BAT *BATprojectchain(BAT **bats);

gdk_export BAT *BATslice(BAT *b, BUN low, BUN high);

gdk_export BAT *BATunique(BAT *b, BAT *s);

gdk_export gdk_return BATfirstn(BAT **topn, BAT **gids, BAT *b, BAT *cands, BAT *grps, BUN n, bool asc, bool nilslast, bool distinct)
	__attribute__((__access__(write_only, 1)))
	__attribute__((__access__(write_only, 2)))
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATfirstn_offset(BAT *b, BAT *s, BAT *g, BUN n, BUN o, bool asc, bool nilslast, bool distinct)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATgroupedfirstn(BUN n, BAT *s, BAT *g, int nbats, BAT **bats, bool *asc, bool *nilslast)
	__attribute__((__warn_unused_result__));
gdk_export BAT *BATgroupedfirstn_offset(BUN n, BUN o, BAT *s, BAT *g, int nbats, BAT **bats, bool *asc, bool *nilslast)
	__attribute__((__warn_unused_result__));

gdk_export gdk_return GDKtoupper(allocator *ma, char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 2)))
	__attribute__((__access__(read_write, 3)));
gdk_export gdk_return GDKtolower(allocator *ma, char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 2)))
	__attribute__((__access__(read_write, 3)));
gdk_export gdk_return GDKcasefold(allocator *ma, char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 2)))
	__attribute__((__access__(read_write, 3)));
gdk_export int GDKstrncasecmp(const char *str1, const char *str2, size_t l1, size_t l2);
gdk_export int GDKstrcasecmp(const char *s1, const char *s2);
gdk_export char *GDKstrcasestr(const char *haystack, const char *needle);
gdk_export BAT *BATtoupper(BAT *b, BAT *s);
gdk_export BAT *BATtolower(BAT *b, BAT *s);
gdk_export BAT *BATcasefold(BAT *b, BAT *s);
gdk_export gdk_return GDKasciify(allocator *ma, char **restrict buf, size_t *restrict buflen, const char *restrict s)
	__attribute__((__access__(read_write, 2)))
	__attribute__((__access__(read_write, 3)));
gdk_export BAT *BATasciify(BAT *b, BAT *s);
#ifdef HAVE_OPENSSL
gdk_export gdk_return BATaggrdigest(allocator *ma, BAT **bnp, char **shap, const char *digest, BAT *b, BAT *g, BAT *e, BAT *s, bool skip_nils);
#endif

gdk_export BAT *BATsample(BAT *b, BUN n);
gdk_export BAT *BATsample_with_seed(BAT *b, BUN n, uint64_t seed);

#define MAXPARAMS	32

#define CHECK_QRY_TIMEOUT_SHIFT	14
#define CHECK_QRY_TIMEOUT_STEP	(1 << CHECK_QRY_TIMEOUT_SHIFT)
#define CHECK_QRY_TIMEOUT_MASK	(CHECK_QRY_TIMEOUT_STEP - 1)

#define TIMEOUT_MSG "Timeout was reached!"
#define INTERRUPT_MSG "Query interrupted!"
#define DISCONNECT_MSG "Client is disconnected!"
#define EXITING_MSG "Server is exiting!"

#define QRY_TIMEOUT (-1)	/* query timed out */
#define QRY_INTERRUPT (-2)	/* client indicated interrupt */
#define QRY_DISCONNECT (-3)	/* client disconnected */

static const char *
TIMEOUT_MESSAGE(const QryCtx *qc)
{
	if (GDKexiting())
		return EXITING_MSG;
	if (qc) {
		switch (qc->endtime) {
		case QRY_TIMEOUT:
			return TIMEOUT_MSG;
		case QRY_INTERRUPT:
			return INTERRUPT_MSG;
		case QRY_DISCONNECT:
			return DISCONNECT_MSG;
		default:
			MT_UNREACHABLE();
		}
	}
	return NULL;
}

static inline void
TIMEOUT_ERROR(const QryCtx *qc, const char *file, const char *func, int lineno)
{
	const char *e = TIMEOUT_MESSAGE(qc);
	if (e) {
		GDKtracer_log(file, func, lineno, TRC_NAME(M_ERROR),
			      TRC_NAME(GDK), NULL, "%s\n", e);
	}
}

#define TIMEOUT_HANDLER(rtpe, qc)					\
	do {								\
		TIMEOUT_ERROR(qc, __FILE__, __func__, __LINE__);	\
		return rtpe;						\
	} while(0)

static inline bool
TIMEOUT_TEST(QryCtx *qc)
{
	if (qc == NULL)
		return false;
	if (qc->endtime < 0)
		return true;
	if (qc->endtime && GDKusec() > qc->endtime) {
		qc->endtime = QRY_TIMEOUT;
		return true;
	}
	switch (bstream_getoob(qc->bs)) {
	case -1:
		qc->endtime = QRY_DISCONNECT;
		return true;
	case 0:
		return false;
	default:
		qc->endtime = QRY_INTERRUPT;
		return true;
	}
}

#define GOTO_LABEL_TIMEOUT_HANDLER(label, qc)				\
	do {								\
		TIMEOUT_ERROR(qc, __FILE__, __func__, __LINE__);	\
		goto label;						\
	} while(0)

#define GDK_CHECK_TIMEOUT_BODY(qc, callback)		\
	do {						\
		if (GDKexiting() || TIMEOUT_TEST(qc)) {	\
			callback;			\
		}					\
	} while (0)

#define GDK_CHECK_TIMEOUT(qc, counter, callback)		\
	do {							\
		if (counter > CHECK_QRY_TIMEOUT_STEP) {		\
			GDK_CHECK_TIMEOUT_BODY(qc, callback);	\
			counter = 0;				\
		} else {					\
			counter++;				\
		}						\
	} while (0)

/* here are some useful constructs to iterate a number of times (the
 * REPEATS argument--only evaluated once) and checking for a timeout
 * every once in a while; the QC->endtime value is a variable of type lng
 * which is either 0 or the GDKusec() compatible time after which the
 * loop should terminate; check for this condition after the loop using
 * the TIMEOUT_CHECK macro; in order to break out of any of these loops,
 * use TIMEOUT_LOOP_BREAK since plain break won't do it; it is perfectly
 * ok to use continue inside the body */

/* use IDX as a loop variable (already declared), initializing it to 0
 * and incrementing it on each iteration */
#define TIMEOUT_LOOP_IDX(IDX, REPEATS, QC)				\
	for (BUN REPS = (IDX = 0, (REPEATS)); REPS > 0; REPS = 0) /* "loops" at most once */ \
		for (BUN CTR1 = 0, END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
			if (CTR1 > 0 && TIMEOUT_TEST(QC)) {		\
				break;					\
			} else						\
				for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++, IDX++)

/* declare and use IDX as a loop variable, initializing it to 0 and
 * incrementing it on each iteration */
#define TIMEOUT_LOOP_IDX_DECL(IDX, REPEATS, QC)				\
	for (BUN IDX = 0, REPS = (REPEATS); REPS > 0; REPS = 0) /* "loops" at most once */ \
		for (BUN CTR1 = 0, END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
			if (CTR1 > 0 && TIMEOUT_TEST(QC)) {		\
				break;					\
			} else						\
				for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++, IDX++)

/* there is no user-visible loop variable */
#define TIMEOUT_LOOP(REPEATS, QC)					\
	for (BUN CTR1 = 0, REPS = (REPEATS), END1 = (REPS + CHECK_QRY_TIMEOUT_STEP) >> CHECK_QRY_TIMEOUT_SHIFT; CTR1 < END1 && !GDKexiting() && ((QC) == NULL || (QC)->endtime >= 0); CTR1++) \
		if (CTR1 > 0 && TIMEOUT_TEST(QC)) {			\
			break;						\
		} else							\
			for (BUN CTR2 = 0, END2 = CTR1 == END1 - 1 ? REPS & CHECK_QRY_TIMEOUT_MASK : CHECK_QRY_TIMEOUT_STEP; CTR2 < END2; CTR2++)

/* break out of the loop (cannot use do/while trick here) */
#define TIMEOUT_LOOP_BREAK			\
	{					\
		END1 = END2 = 0;		\
		break;				\
	}

/* check whether a timeout occurred, and execute the CALLBACK argument
 * if it did */
#define TIMEOUT_CHECK(QC, CALLBACK)					\
	do {								\
		if (GDKexiting() || ((QC) && (QC)->endtime < 0))	\
			CALLBACK;					\
	} while (0)

typedef gdk_return gdk_callback_func(int argc, void *argv[]);

gdk_export gdk_return gdk_add_callback(const char *name, gdk_callback_func *f,
				       int argc, void *argv[], int interval);
gdk_export gdk_return gdk_remove_callback(const char *, gdk_callback_func *f);

gdk_export void GDKusr1triggerCB(void (*func)(void));

#define SQLSTATE(sqlstate)	#sqlstate "!"
#define MAL_MALLOC_FAIL	"Could not allocate memory"

typedef struct exception_buffer {
#ifdef HAVE_SIGLONGJMP
	sigjmp_buf state;
#else
	jmp_buf state;
#endif
	int code;
	const char *msg;
	int enabled;
} exception_buffer;

gdk_export exception_buffer *eb_init(exception_buffer *eb)
	__attribute__((__access__(write_only, 1)));

/* != 0 on when we return to the savepoint */
#ifdef HAVE_SIGLONGJMP
#define eb_savepoint(eb) ((eb)->enabled = 1, sigsetjmp((eb)->state, 0))
#else
#define eb_savepoint(eb) ((eb)->enabled = 1, setjmp((eb)->state))
#endif
gdk_export _Noreturn void eb_error(exception_buffer *eb, const char *msg, int val);

gdk_export BAT *BATcalcnegate(BAT *b, BAT *s);
gdk_export BAT *BATcalcabsolute(BAT *b, BAT *s);
gdk_export BAT *BATcalcincr(BAT *b, BAT *s);
gdk_export BAT *BATcalcdecr(BAT *b, BAT *s);
gdk_export BAT *BATcalciszero(BAT *b, BAT *s);
gdk_export BAT *BATcalcsign(BAT *b, BAT *s);
gdk_export BAT *BATcalcisnil(BAT *b, BAT *s);
gdk_export BAT *BATcalcisnotnil(BAT *b, BAT *s);
gdk_export BAT *BATcalcnot(BAT *b, BAT *s);
gdk_export BAT *BATcalcmin(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcmin_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcmincst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalcmincst_no_nil(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstmin(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalccstmin_no_nil(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcmax(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcmax_no_nil(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcmaxcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalcmaxcst_no_nil(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstmax(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalccstmax_no_nil(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcadd(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp);
gdk_export BAT *BATcalcaddcst(BAT *b, const ValRecord *v, BAT *s, int tp);
gdk_export BAT *BATcalccstadd(const ValRecord *v, BAT *b, BAT *s, int tp);
gdk_export BAT *BATcalcsub(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp);
gdk_export BAT *BATcalcsubcst(BAT *b, const ValRecord *v, BAT *s, int tp);
gdk_export BAT *BATcalccstsub(const ValRecord *v, BAT *b, BAT *s, int tp);
gdk_export BAT *BATcalcmul(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp);
gdk_export BAT *BATcalcmulcst(BAT *b, const ValRecord *v, BAT *s, int tp);
gdk_export BAT *BATcalccstmul(const ValRecord *v, BAT *b, BAT *s, int tp);
gdk_export BAT *BATcalcdiv(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp);
gdk_export BAT *BATcalcdivcst(BAT *b, const ValRecord *v, BAT *s, int tp);
gdk_export BAT *BATcalccstdiv(const ValRecord *v, BAT *b, BAT *s, int tp);
gdk_export BAT *BATcalcmod(BAT *b1, BAT *b2, BAT *s1, BAT *s2, int tp);
gdk_export BAT *BATcalcmodcst(BAT *b, const ValRecord *v, BAT *s, int tp);
gdk_export BAT *BATcalccstmod(const ValRecord *v, BAT *b, BAT *s, int tp);
gdk_export BAT *BATcalcxor(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcxorcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstxor(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcor(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcorcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstor(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcand(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcandcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstand(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalclsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalclshcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstlsh(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcrsh(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcrshcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstrsh(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalclt(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcltcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstlt(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcle(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalclecst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstle(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcgt(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcgtcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstgt(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcge(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalcgecst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstge(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalceq(BAT *b1, BAT *b2, BAT *s1, BAT *s2, bool nil_matches);
gdk_export BAT *BATcalceqcst(BAT *b, const ValRecord *v, BAT *s, bool nil_matches);
gdk_export BAT *BATcalccsteq(const ValRecord *v, BAT *b, BAT *s, bool nil_matches);
gdk_export BAT *BATcalcne(BAT *b1, BAT *b2, BAT *s1, BAT *s2, bool nil_matches);
gdk_export BAT *BATcalcnecst(BAT *b, const ValRecord *v, BAT *s, bool nil_matches);
gdk_export BAT *BATcalccstne(const ValRecord *v, BAT *b, BAT *s, bool nil_matches);
gdk_export BAT *BATcalccmp(BAT *b1, BAT *b2, BAT *s1, BAT *s2);
gdk_export BAT *BATcalccmpcst(BAT *b, const ValRecord *v, BAT *s);
gdk_export BAT *BATcalccstcmp(const ValRecord *v, BAT *b, BAT *s);
gdk_export BAT *BATcalcbetween(BAT *b, BAT *lo, BAT *hi, BAT *s, BAT *slo, BAT *shi, bool symmetric, bool linc, bool hinc, bool nils_false, bool anti);
gdk_export BAT *BATcalcbetweencstcst(BAT *b, const ValRecord *lo, const ValRecord *hi, BAT *s, bool symmetric, bool linc, bool hinc, bool nils_false, bool anti);
gdk_export BAT *BATcalcbetweenbatcst(BAT *b, BAT *lo, const ValRecord *hi, BAT *s, BAT *slo, bool symmetric, bool linc, bool hinc, bool nils_false, bool anti);
gdk_export BAT *BATcalcbetweencstbat(BAT *b, const ValRecord *lo, BAT *hi, BAT *s, BAT *shi, bool symmetric, bool linc, bool hinc, bool nils_false, bool anti);
gdk_export gdk_return VARcalcbetween(ValPtr ret, const ValRecord *v, const ValRecord *lo, const ValRecord *hi, bool symmetric, bool linc, bool hinc, bool nils_false, bool anti);
gdk_export BAT *BATcalcifthenelse(BAT *b, BAT *b1, BAT *b2);
gdk_export BAT *BATcalcifthenelsecst(BAT *b, BAT *b1, const ValRecord *c2);
gdk_export BAT *BATcalcifthencstelse(BAT *b, const ValRecord *c1, BAT *b2);
gdk_export BAT *BATcalcifthencstelsecst(BAT *b, const ValRecord *c1, const ValRecord *c2);

gdk_export gdk_return VARcalcnot(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcnegate(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcabsolute(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcincr(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcdecr(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalciszero(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcsign(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcisnil(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcisnotnil(ValPtr ret, const ValRecord *v);
gdk_export gdk_return VARcalcadd(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcsub(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcmul(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcdiv(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcmod(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcxor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcor(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcand(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalclsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcrsh(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalclt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcgt(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcle(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalcge(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export gdk_return VARcalceq(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, bool nil_matches);
gdk_export gdk_return VARcalcne(ValPtr ret, const ValRecord *lft, const ValRecord *rgt, bool nil_matches);
gdk_export gdk_return VARcalccmp(ValPtr ret, const ValRecord *lft, const ValRecord *rgt);
gdk_export BAT *BATconvert(BAT *b, BAT *s, int tp, uint8_t scale1, uint8_t scale2, uint8_t precision);
gdk_export gdk_return VARconvert(allocator *ma, ValPtr ret, const ValRecord *v, uint8_t scale1, uint8_t scale2, uint8_t precision);
gdk_export gdk_return BATcalcavg(BAT *b, BAT *s, dbl *avg, BUN *vals, int scale, bool inout);

gdk_export BAT *BATgroupsum(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupprod(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export gdk_return BATgroupavg(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils, int scale);
gdk_export gdk_return BATgroupavg2(BAT **bnp, BAT **cntsp, BAT *b, BAT *g, BAT *e, BAT *s, int tp, BUN ngrp, bool skip_nils, int scale);
gdk_export gdk_return BATgroupavg3(BAT **avgp, BAT **remp, BAT **cntp, BAT *b, BAT *g, BAT *e, BAT *s, bool skip_nils, bool inout);
gdk_export BAT *BATgroupavg3combine(BAT *avg, BAT *rem, BAT *cnt, BAT *g, BAT *e, bool skip_nils);
gdk_export BAT *BATgroupcount(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupmin(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupmax(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupmedian(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupquantile(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile, bool skip_nils);
gdk_export BAT *BATgroupmedian_avg(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupquantile_avg(BAT *b, BAT *g, BAT *e, BAT *s, int tp, double quantile, bool skip_nils);

/* helper function for grouped aggregates */
gdk_export const char *BATgroupaggrinit(
	BAT *b, BAT *g, BAT *e, BAT *s,
	/* outputs: */
	oid *minp, oid *maxp, BUN *ngrpp,
	struct canditer *ci);

gdk_export gdk_return BATsum(void *res, int tp, BAT *b, BAT *s, bool skip_nils, bool nil_if_empty, bool inout);
gdk_export gdk_return BATprod(void *res, int tp, BAT *b, BAT *s, bool skip_nils, bool nil_if_empty, bool inout);
gdk_export void *BATmax(BAT *b, void *aggr);
gdk_export void *BATmin(BAT *b, void *aggr);
gdk_export void *BATmax_skipnil(allocator *alloc, BAT *b, void *aggr, bit skipnil, bool inout);
gdk_export void *BATmin_skipnil(allocator *alloc, BAT *b, void *aggr, bit skipnil, bool inout);

gdk_export dbl BATcalcstdev_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcstdev_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupstdev_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupstdev_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export dbl BATcalcvariance_population(dbl *avgp, BAT *b);
gdk_export dbl BATcalcvariance_sample(dbl *avgp, BAT *b);
gdk_export BAT *BATgroupvariance_sample(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupvariance_population(BAT *b, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export dbl BATcalccovariance_sample(BAT *b1, BAT *b2);
gdk_export dbl BATcalccovariance_population(BAT *b1, BAT *b2);
gdk_export dbl BATcalccorrelation(BAT *b1, BAT *b2);
gdk_export BAT *BATgroupcovariance_sample(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupcovariance_population(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);
gdk_export BAT *BATgroupcorrelation(BAT *b1, BAT *b2, BAT *g, BAT *e, BAT *s, int tp, bool skip_nils);

gdk_export BAT *BATgroupstr_group_concat(BAT *b, BAT *g, BAT *e, BAT *s, BAT *sep, bool skip_nils, const char *restrict separator);
gdk_export gdk_return BATstr_group_concat(allocator *ma, ValPtr res, BAT *b, BAT *s, BAT *sep, bool skip_nils, bool nil_if_empty, const char *restrict separator);
gdk_export BAT *GDKanalytical_str_group_concat(BAT *b, BAT *p, BAT *o, BAT *sep, BAT *s, BAT *e, const char *restrict separator, int frame_type);

gdk_export ValPtr VALcopy(allocator *va, ValPtr dst, const ValRecord *src)
	__attribute__((__access__(write_only, 2)));
gdk_export ValPtr VALinit(allocator *va, ValPtr d, int tpe, const void *s)
	__attribute__((__access__(write_only, 2)));

gdk_export allocator *create_allocator(const char *, bool use_lock);
gdk_export bool ma_tmp_active(const allocator *sa);
gdk_export void ma_reset(allocator *sa);
gdk_export void *ma_alloc(allocator *sa,  size_t sz);
gdk_export void *ma_zalloc(allocator *sa,  size_t sz);
gdk_export void *ma_realloc(allocator *sa,  void *ptr, size_t sz, size_t osz);
gdk_export void ma_destroy(allocator *sa);
gdk_export char *ma_strndup(allocator *sa, const char *s, size_t l);
gdk_export char *ma_strdup(allocator *sa, const char *s);
gdk_export char *ma_strconcat(allocator *sa, const char *s1, const char *s2);
gdk_export const char *ma_name(allocator *sa);
gdk_export allocator_state ma_open(allocator *sa);  /* open new frame of tempory allocations */
gdk_export void ma_close(const allocator_state *); /* close temporary frame, reset to old state */
gdk_export void ma_free(allocator *sa, void *);
gdk_export exception_buffer *ma_get_eb(allocator *sa)
       __attribute__((__pure__));
gdk_export char *ma_copy(allocator *sa, char *s, size_t l);

gdk_export int ma_info(allocator *sa, char *buf, size_t buflen, const char *pref);

#define MA_NEW( sa, type )				((type*)ma_alloc( sa, sizeof(type)))
#define MA_ZNEW( sa, type )				((type*)ma_zalloc( sa, sizeof(type)))
#define MA_NEW_ARRAY( sa, type, size )			(type*)ma_alloc( sa, ((size)*sizeof(type)))
#define MA_ZNEW_ARRAY( sa, type, size )			(type*)ma_zalloc( sa, ((size)*sizeof(type)))
#define MA_RENEW_ARRAY( sa, type, ptr, sz, osz )	(type*)ma_realloc( sa, ptr, ((sz)*sizeof(type)), ((osz)*sizeof(type)))


#if !defined(NDEBUG) && !defined(__COVERITY__) && defined(__GNUC__) && !defined(_CLANGD)
#define ma_alloc(sa, sz)					\
	({							\
		allocator *_sa = (sa);				\
		size_t _sz = (sz);				\
		void *_res = ma_alloc(_sa, _sz);		\
		TRC_DEBUG(ALLOC,				\
			  "ma_alloc(%p(%s),%zu) -> %p\n",	\
			  _sa, ma_name(_sa), _sz, _res);	\
		_res;						\
	})
#define ma_zalloc(sa, sz)					\
	({							\
		allocator *_sa = (sa);				\
		size_t _sz = (sz);				\
		void *_res = ma_zalloc(_sa, _sz);		\
		TRC_DEBUG(ALLOC,				\
			  "ma_zalloc(%p(%s),%zu) -> %p\n",	\
			  _sa, ma_name(_sa), _sz, _res);	\
		_res;						\
	})
#define ma_realloc(sa, ptr, sz, osz)					\
	({								\
		allocator *_sa = (sa);					\
		void *_ptr = (ptr);					\
		size_t _sz = (sz);					\
		size_t _osz = (osz);					\
		void *_res = ma_realloc(_sa, _ptr, _sz, _osz);		\
		TRC_DEBUG(ALLOC,					\
			  "ma_realloc(%p(%s),%p,%zu,%zu) -> %p\n",	\
			  _sa, ma_name(_sa), _ptr, _sz, _osz, _res);	\
		_res;							\
	})
#define ma_free(sa, p)					\
	({						\
		allocator *_sa = (sa);			\
		void *_p = (p);				\
		TRC_DEBUG(ALLOC,			\
			  "ma_free(%p(%s),%p)\n",	\
			  _sa, ma_name(_sa), _p);	\
		ma_free(_sa, _p);			\
	})
#define ma_strdup(sa, s)						\
	({								\
		allocator *_sa = (sa);					\
		const char *_s = (s);					\
		char *_res = ma_strdup(_sa, _s);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_strdup(%p(%s),len=%zu) -> %p\n",		\
			  _sa, ma_name(_sa), strlen(_s), _res);		\
		_res;							\
	})
#define ma_strndup(sa, s, l)						\
	({								\
		allocator *_sa = (sa);					\
		const char *_s = (s);					\
		size_t _l = (l);					\
		char *_res = ma_strndup(_sa, _s, _l);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_strndup(%p(%s),len=%zu) -> %p\n",		\
			  _sa, ma_name(_sa), _l, _res);			\
		_res;							\
	})
#define ma_strconcat(sa, s1, s2)					\
	({								\
		allocator *_sa = (sa);					\
		const char *_s1 = (s1);					\
		const char *_s2 = (s2);					\
		char *_res = ma_strconcat(_sa, _s1, _s2);		\
		TRC_DEBUG(ALLOC,					\
			  "ma_strconcat(%p(%s),len1=%zu,len2=%zu) -> %p\n", \
			  _sa, ma_name(_sa), strlen(_s1), strlen(_s2), _res); \
		_res;							\
	})
#define create_allocator(nm, lk)				\
	({							\
		const char *_nm = (nm);				\
		bool _lk = (lk);				\
		allocator *_res = create_allocator(_nm, _lk);	\
		TRC_DEBUG(ALLOC,				\
			  "create_allocator() -> %p(%s)\n",	\
			  _res, ma_name(_res));			\
		_res;						\
	})
#define ma_open(sa)							\
	({								\
		allocator *_sa = (sa);					\
		allocator_state _as = ma_open(_sa);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_open(%p(%s)) -> tmp_used = %zu\n",	\
			  _sa, ma_name(_sa), _as.tmp_used);		\
		_as;							\
	})
#define ma_close(as)							\
	({								\
		const allocator_state *_as = (as);			\
		TRC_DEBUG(ALLOC,					\
			  "ma_close(%p(%s), tmp_used = %zu)\n",		\
			  _as->ma, ma_name(_as->ma), _as->tmp_used);	\
		ma_close(_as);						\
	})
#define ma_reset(sa)							\
	({								\
		allocator *_sa = (sa);					\
		ma_reset(_sa);						\
		TRC_DEBUG(ALLOC,					\
			  "ma_reset(%p(%s))\n",	_sa, ma_name(_sa));	\
	})
#define ma_destroy(sa)							\
	({								\
		allocator *_sa = (sa);					\
		TRC_DEBUG(ALLOC,					\
			  "ma_destroy(%p(%s))\n", _sa, ma_name(_sa));	\
		ma_destroy(_sa);					\
	})
#endif

#endif /* _GDK_H_ */
