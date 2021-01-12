/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @a Niels Nes, Peter Boncz
 * @+ Threads
 * This file contains a wrapper layer for threading, hence the
 * underscore convention MT_x (Multi-Threading).  As all platforms
 * that MonetDB runs on now support POSIX Threads (pthreads), this
 * wrapping layer has become rather thin.
 *
 * In the late 1990s when multi-threading support was introduced in
 * MonetDB, pthreads was just emerging as a standard API and not
 * widely adopted yet.  The earliest MT implementation focused on SGI
 * Unix and provided multi- threading using multiple processses, and
 * shared memory.
 *
 * One of the relics of this model, namely the need to pre-allocate
 * locks and semaphores, and consequently a maximum number of them,
 * has been removed in the latest iteration of this layer.
 *
 */
/*
 * @- Mthreads Routine implementations
 */
#include "monetdb_config.h"
#include "mstring.h"
#include "gdk.h"
#include "gdk_system.h"
#include "gdk_system_private.h"

#include <time.h>

#ifdef HAVE_FTIME
#include <sys/timeb.h>		/* ftime */
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>		/* gettimeofday */
#endif

#include <signal.h>
#include <string.h>		/* for strerror */
#include <unistd.h>		/* for sysconf symbols */

#ifdef LOCK_STATS

ATOMIC_TYPE GDKlockcnt = ATOMIC_VAR_INIT(0);
ATOMIC_TYPE GDKlockcontentioncnt = ATOMIC_VAR_INIT(0);
ATOMIC_TYPE GDKlocksleepcnt = ATOMIC_VAR_INIT(0);
MT_Lock *volatile GDKlocklist = 0;
ATOMIC_FLAG GDKlocklistlock = ATOMIC_FLAG_INIT;

/* merge sort of linked list */
static MT_Lock *
sortlocklist(MT_Lock *l)
{
	MT_Lock *r, *t, *ll = NULL;

	if (l == NULL || l->next == NULL) {
		/* list is trivially sorted (0 or 1 element) */
		return l;
	}
	/* break list into two (almost) equal pieces:
	* l is start of "left" list, r of "right" list, ll last
	* element of "left" list */
	for (t = r = l; t && t->next; t = t->next->next) {
		ll = r;
		r = r->next;
	}
	ll->next = NULL;	/* break list into two */
	/* recursively sort both sublists */
	l = sortlocklist(l);
	r = sortlocklist(r);
	/* merge
	 * t is new list, ll is last element of new list, l and r are
	 * start of unprocessed part of left and right lists */
	t = ll = NULL;
	while (l && r) {
		if (ATOMIC_GET(&l->sleep) < ATOMIC_GET(&r->sleep) ||
		    (ATOMIC_GET(&l->sleep) == ATOMIC_GET(&r->sleep) &&
		     (ATOMIC_GET(&l->contention) < ATOMIC_GET(&r->contention) ||
		      (ATOMIC_GET(&l->contention) == ATOMIC_GET(&r->contention) &&
		       l->count <= r->count)))) {
			/* l is smaller */
			if (ll == NULL) {
				assert(t == NULL);
				t = ll = l;
			} else {
				ll->next = l;
				ll = ll->next;
			}
			l = l->next;
		} else {
			/* r is smaller */
			if (ll == NULL) {
				assert(t == NULL);
				t = ll = r;
			} else {
				ll->next = r;
				ll = ll->next;
			}
			r = r->next;
		}
	}
	/* append rest of remaining list */
	ll->next = l ? l : r;
	return t;
}

static inline bool
lock_isset(MT_Lock *l)
{
	if (MT_lock_try(l)) {
		MT_lock_unset(l);
		return false;
	}
	return true;
}

/* function used for debugging */
void
GDKlockstatistics(int what)
{
	MT_Lock *l;
	int n = 0;

	if (ATOMIC_TAS(&GDKlocklistlock) != 0) {
		fprintf(stderr, "GDKlocklistlock is set, so cannot access lock list\n");
		return;
	}
	if (what == -1) {
		for (l = GDKlocklist; l; l = l->next) {
			l->count = 0;
			ATOMIC_SET(&l->contention, 0);
			ATOMIC_SET(&l->sleep, 0);
		}
		ATOMIC_CLEAR(&GDKlocklistlock);
		return;
	}
	GDKlocklist = sortlocklist(GDKlocklist);
	fprintf(stderr, "lock name\tcount\tcontention\tsleep\tlocked\t(un)locker\tthread\n");
	for (l = GDKlocklist; l; l = l->next) {
		n++;
		if (what == 0 ||
		    (what == 1 && l->count) ||
		    (what == 2 && ATOMIC_GET(&l->contention)) ||
		    (what == 3 && lock_isset(l)))
			fprintf(stderr, "%-18s\t%zu\t%zu\t%zu\t%s\t%s\t%s\n",
				l->name, l->count,
				(size_t) ATOMIC_GET(&l->contention),
				(size_t) ATOMIC_GET(&l->sleep),
				lock_isset(l) ? "locked" : "",
				l->locker ? l->locker : "",
				l->thread ? l->thread : "");
	}
	fprintf(stderr, "Number of locks: %d\n", n);
	fprintf(stderr, "Total lock count: %zu\n", (size_t) ATOMIC_GET(&GDKlockcnt));
	fprintf(stderr, "Lock contention:  %zu\n", (size_t) ATOMIC_GET(&GDKlockcontentioncnt));
	fprintf(stderr, "Lock sleep count: %zu\n", (size_t) ATOMIC_GET(&GDKlocksleepcnt));
	ATOMIC_CLEAR(&GDKlocklistlock);
}

#endif	/* LOCK_STATS */

#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
static struct winthread {
	struct winthread *next;
	HANDLE hdl;
	DWORD tid;
	void (*func) (void *);
	void *data;
	MT_Lock *lockwait;	/* lock we're waiting for */
	MT_Sema *semawait;	/* semaphore we're waiting for */
	struct winthread *joinwait; /* process we are joining with */
	const char *working;	/* what we're currently doing */
	char algorithm[512];	/* the algorithm used in the last operation */
	size_t algolen;		/* length of string in .algorithm */
	ATOMIC_TYPE exited;
	bool detached:1, waiting:1;
	char threadname[MT_NAME_LEN];
} *winthreads = NULL;
static struct winthread mainthread = {
	.threadname = "main thread",
	.exited = ATOMIC_VAR_INIT(0),
};

static CRITICAL_SECTION winthread_cs;
static DWORD threadslot = TLS_OUT_OF_INDEXES;

void
dump_threads(void)
{
	TRC_DEBUG_IF(THRD) {
		EnterCriticalSection(&winthread_cs);
		for (struct winthread *w = winthreads; w; w = w->next) {
			TRC_DEBUG_ENDIF(THRD, "%s, waiting for %s, working on %.200s\n",
					w->threadname,
					w->lockwait ? w->lockwait->name :
					w->semawait ? w->semawait->name :
					w->joinwait ? w->joinwait->threadname :
					"nothing",
					ATOMIC_GET(&w->exited) ? "exiting" :
					w->working ? w->working : "nothing");
		}
		LeaveCriticalSection(&winthread_cs);
	}
}

bool
MT_thread_init(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES) {
		threadslot = TlsAlloc();
		if (threadslot == TLS_OUT_OF_INDEXES) {
			GDKwinerror("Creating thread-local slot for thread failed");
			return false;
		}
		mainthread.tid = GetCurrentThreadId();
		if (TlsSetValue(threadslot, &mainthread) == 0) {
			GDKwinerror("Setting thread-local value failed");
			TlsFree(threadslot);
			threadslot = TLS_OUT_OF_INDEXES;
			return false;
		}
		InitializeCriticalSection(&winthread_cs);
	}
	return true;
}

static struct winthread *
find_winthread(DWORD tid)
{
	struct winthread *w;

	EnterCriticalSection(&winthread_cs);
	for (w = winthreads; w && w->tid != tid; w = w->next)
		;
	LeaveCriticalSection(&winthread_cs);
	return w;
}

const char *
MT_thread_getname(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return mainthread.threadname;
	struct winthread *w = TlsGetValue(threadslot);
	return w ? w->threadname : UNKNOWN_THREAD;
}

void
MT_thread_setdata(void *data)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;
	struct winthread *w = TlsGetValue(threadslot);

	if (w)
		w->data = data;
}

void
MT_thread_setlockwait(MT_Lock *lock)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;
	struct winthread *w = TlsGetValue(threadslot);

	if (w)
		w->lockwait = lock;
}

void
MT_thread_setsemawait(MT_Sema *sema)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;
	struct winthread *w = TlsGetValue(threadslot);

	if (w)
		w->semawait = sema;
}

void
MT_thread_setworking(const char *work)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;
	struct winthread *w = TlsGetValue(threadslot);

	if (w)
		w->working = work;
}

void
MT_thread_setalgorithm(const char *algo)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;
	struct winthread *w = TlsGetValue(threadslot);

	if (w) {
		if (algo) {
			if (w->algolen > 0) {
				if (w->algolen < sizeof(w->algorithm))
					w->algolen += strconcat_len(w->algorithm + w->algolen, sizeof(w->algorithm) - w->algolen, "; ", algo, NULL);
			} else
				w->algolen = strcpy_len(w->algorithm, algo, sizeof(w->algorithm));
		} else {
			w->algorithm[0] = 0;
			w->algolen = 0;
		}
	}
}

const char *
MT_thread_getalgorithm(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return NULL;
	struct winthread *w = TlsGetValue(threadslot);

	return w && w->algorithm[0] ? w->algorithm : NULL;
}

bool
MT_thread_override_limits(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return false;
	struct winthread *w = TlsGetValue(threadslot);

	return w && w->working && strcmp(w->working, "store locked") == 0;
}

void *
MT_thread_getdata(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return NULL;
	struct winthread *w = TlsGetValue(threadslot);

	return w ? w->data : NULL;
}

static void
rm_winthread(struct winthread *w)
{
	struct winthread **wp;

	EnterCriticalSection(&winthread_cs);
	for (wp = &winthreads; *wp && *wp != w; wp = &(*wp)->next)
		;
	if (*wp)
		*wp = w->next;
	LeaveCriticalSection(&winthread_cs);
	ATOMIC_DESTROY(&w->exited);
	free(w);
}

static DWORD WINAPI
thread_starter(LPVOID arg)
{
	struct winthread *w = (struct winthread *) arg;
	void *data = w->data;

	w->data = NULL;
	TlsSetValue(threadslot, w);
	(*w->func)(data);
	ATOMIC_SET(&w->exited, 1);
	TRC_DEBUG(THRD, "Exit: \"%s\"\n", w->threadname);
	return 0;
}

static void
join_threads(void)
{
	bool waited;

	struct winthread *self = TlsGetValue(threadslot);
	EnterCriticalSection(&winthread_cs);
	do {
		waited = false;
		for (struct winthread *w = winthreads; w; w = w->next) {
			if (w->detached && !w->waiting && ATOMIC_GET(&w->exited)) {
				w->waiting = true;
				LeaveCriticalSection(&winthread_cs);
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", w->threadname);
				self->joinwait = w;
				WaitForSingleObject(w->hdl, INFINITE);
				self->joinwait = NULL;
				CloseHandle(w->hdl);
				rm_winthread(w);
				waited = true;
				EnterCriticalSection(&winthread_cs);
				break;
			}
		}
	} while (waited);
	LeaveCriticalSection(&winthread_cs);
}

void
join_detached_threads(void)
{
	bool waited;

	struct winthread *self = TlsGetValue(threadslot);
	EnterCriticalSection(&winthread_cs);
	do {
		waited = false;
		for (struct winthread *w = winthreads; w; w = w->next) {
			if (w->detached && !w->waiting) {
				w->waiting = true;
				LeaveCriticalSection(&winthread_cs);
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", w->threadname);
				self->joinwait = w;
				WaitForSingleObject(w->hdl, INFINITE);
				self->joinwait = NULL;
				CloseHandle(w->hdl);
				rm_winthread(w);
				waited = true;
				EnterCriticalSection(&winthread_cs);
				break;
			}
		}
	} while (waited);
	LeaveCriticalSection(&winthread_cs);
}

int
MT_create_thread(MT_Id *t, void (*f) (void *), void *arg, enum MT_thr_detach d, const char *threadname)
{
	struct winthread *w;

	join_threads();
	if (threadname == NULL) {
		TRC_CRITICAL(GDK, "Thread must have a name\n");
		return -1;
	}
	if (strlen(threadname) >= sizeof(w->threadname)) {
		TRC_CRITICAL(GDK, "Thread's name is too large\n");
		return -1;
	}

	w = malloc(sizeof(*w));
	if (w == NULL) {
		GDKsyserror("Cannot allocate memory\n");
		return -1;
	}

	*w = (struct winthread) {
		.func = f,
		.data = arg,
		.waiting = false,
		.detached = (d == MT_THR_DETACHED),
	};
	ATOMIC_INIT(&w->exited, 0);
	strcpy_len(w->threadname, threadname, sizeof(w->threadname));
	TRC_DEBUG(THRD, "Create thread \"%s\"\n", threadname);
	EnterCriticalSection(&winthread_cs);
	w->hdl = CreateThread(NULL, THREAD_STACK_SIZE, thread_starter, w,
			      0, &w->tid);
	if (w->hdl == NULL) {
		GDKwinerror("Failed to create thread");
		LeaveCriticalSection(&winthread_cs);
		free(w);
		return -1;
	}
	/* must not fail after this: the thread has been started */
	w->next = winthreads;
	winthreads = w;
	LeaveCriticalSection(&winthread_cs);
	*t = (MT_Id) w->tid;
	return 0;
}

MT_Id
MT_getpid(void)
{
	return (MT_Id) GetCurrentThreadId();
}

void
MT_exiting_thread(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES)
		return;

	struct winthread *w = TlsGetValue(threadslot);

	if (w) {
		ATOMIC_SET(&w->exited, 1);
		w->working = NULL;
	}
}

int
MT_join_thread(MT_Id t)
{
	struct winthread *w;

	assert(t != mainthread.tid);
	join_threads();
	w = find_winthread((DWORD) t);
	if (w == NULL || w->hdl == NULL)
		return -1;
	TRC_DEBUG(THRD, "Join thread \"%s\"\n", w->threadname);
	struct winthread *self = TlsGetValue(threadslot);
	self->joinwait = w;
	DWORD ret = WaitForSingleObject(w->hdl, INFINITE);
	self->joinwait = NULL;
	if (ret == WAIT_OBJECT_0 && CloseHandle(w->hdl)) {
		rm_winthread(w);
		return 0;
	}
	return -1;
}

int
MT_kill_thread(MT_Id t)
{
	struct winthread *w;

	assert(t != mainthread.tid);
	join_threads();
	w = find_winthread((DWORD) t);
	if (w == NULL)
		return -1;
	if (w->hdl == NULL) {
		/* detached thread */
		HANDLE h;
		int ret = 0;
		h = OpenThread(THREAD_ALL_ACCESS, 0, (DWORD) t);
		if (h == NULL)
			return -1;
		if (TerminateThread(h, -1))
			ret = -1;
		CloseHandle(h);
		return ret;
	}
	if (TerminateThread(w->hdl, -1))
		return 0;
	return -1;
}

#else  /* !defined(HAVE_PTHREAD_H) && defined(_MSC_VER) */

static struct posthread {
	struct posthread *next;
	void (*func)(void *);
	void *data;
	MT_Lock *lockwait;	/* lock we're waiting for */
	MT_Sema *semawait;	/* semaphore we're waiting for */
	struct posthread *joinwait; /* process we are joining with */
	const char *working;	/* what we're currently doing */
	char algorithm[512];	/* the algorithm used in the last operation */
	size_t algolen;		/* length of string in .algorithm */
	char threadname[MT_NAME_LEN];
	pthread_t tid;
	MT_Id mtid;
	ATOMIC_TYPE exited;
	bool detached:1, waiting:1;
} *posthreads = NULL;
static struct posthread mainthread = {
	.threadname = "main thread",
	.mtid = 1,
	.exited = ATOMIC_VAR_INIT(0),
};
static pthread_mutex_t posthread_lock = PTHREAD_MUTEX_INITIALIZER;
static MT_Id MT_thread_id = 1;

static pthread_key_t threadkey;
static bool thread_initialized = false;

void
dump_threads(void)
{
	TRC_DEBUG_IF(THRD) {
		pthread_mutex_lock(&posthread_lock);
		for (struct posthread *p = posthreads; p; p = p->next) {
			TRC_DEBUG_ENDIF(THRD, "%s, waiting for %s, working on %.200s\n",
					p->threadname,
					p->lockwait ? p->lockwait->name :
					p->semawait ? p->semawait->name :
					p->joinwait ? p->joinwait->threadname :
					"nothing",
					ATOMIC_GET(&p->exited) ? "exiting" :
					p->working ? p->working : "nothing");
		}
		pthread_mutex_unlock(&posthread_lock);
	}
}

bool
MT_thread_init(void)
{
	int ret;

	if ((ret = pthread_key_create(&threadkey, NULL)) != 0) {
		GDKsyserr(ret, "Creating specific key for thread failed");
		return false;
	}
	thread_initialized = true;
	mainthread.tid = pthread_self();
	if ((ret = pthread_setspecific(threadkey, &mainthread)) != 0) {
		GDKsyserr(ret, "Setting specific value failed");
		return false;
	}
	return true;
}

static struct posthread *
find_posthread(MT_Id tid)
{
	struct posthread *p;

	pthread_mutex_lock(&posthread_lock);
	for (p = posthreads; p && p->mtid != tid; p = p->next)
		;
	pthread_mutex_unlock(&posthread_lock);
	return p;
}

const char *
MT_thread_getname(void)
{
	struct posthread *p;

	if (!thread_initialized)
		return mainthread.threadname;
	p = pthread_getspecific(threadkey);
	return p ? p->threadname : UNKNOWN_THREAD;
}

void
MT_thread_setdata(void *data)
{
	if (!thread_initialized)
		return;
	struct posthread *p = pthread_getspecific(threadkey);

	if (p)
		p->data = data;
}

void *
MT_thread_getdata(void)
{
	if (!thread_initialized)
		return NULL;
	struct posthread *p = pthread_getspecific(threadkey);

	return p ? p->data : NULL;
}

void
MT_thread_setlockwait(MT_Lock *lock)
{
	if (!thread_initialized)
		return;
	struct posthread *p = pthread_getspecific(threadkey);

	if (p)
		p->lockwait = lock;
}

void
MT_thread_setsemawait(MT_Sema *sema)
{
	if (!thread_initialized)
		return;
	struct posthread *p = pthread_getspecific(threadkey);

	if (p)
		p->semawait = sema;
}

void
MT_thread_setworking(const char *work)
{
	if (!thread_initialized)
		return;
	struct posthread *p = pthread_getspecific(threadkey);

	if (p)
		p->working = work;
}

void
MT_thread_setalgorithm(const char *algo)
{
	if (!thread_initialized)
		return;
	struct posthread *p = pthread_getspecific(threadkey);

	if (p) {
		if (algo) {
			if (p->algolen > 0) {
				if (p->algolen < sizeof(p->algorithm))
					p->algolen += strconcat_len(p->algorithm + p->algolen, sizeof(p->algorithm) - p->algolen, "; ", algo, NULL);
			} else
				p->algolen = strcpy_len(p->algorithm, algo, sizeof(p->algorithm));
		} else {
			p->algorithm[0] = 0;
			p->algolen = 0;
		}
	}
}

const char *
MT_thread_getalgorithm(void)
{
	if (!thread_initialized)
		return NULL;
	struct posthread *p = pthread_getspecific(threadkey);

	return p && p->algorithm[0] ? p->algorithm : NULL;
}

bool
MT_thread_override_limits(void)
{
	if (!thread_initialized)
		return false;
	struct posthread *p = pthread_getspecific(threadkey);

	return p && p->working && strcmp(p->working, "store locked") == 0;
}

#ifdef HAVE_PTHREAD_SIGMASK
static void
MT_thread_sigmask(sigset_t *new_mask, sigset_t *orig_mask)
{
	/* do not check for errors! */
	sigdelset(new_mask, SIGQUIT);
	sigdelset(new_mask, SIGPROF);
	pthread_sigmask(SIG_SETMASK, new_mask, orig_mask);
}
#endif

static void
rm_posthread_locked(struct posthread *p)
{
	struct posthread **pp;

	for (pp = &posthreads; *pp && *pp != p; pp = &(*pp)->next)
		;
	if (*pp)
		*pp = p->next;
	ATOMIC_DESTROY(&p->exited);
	free(p);
}

static void
rm_posthread(struct posthread *p)
{
	pthread_mutex_lock(&posthread_lock);
	rm_posthread_locked(p);
	pthread_mutex_unlock(&posthread_lock);
}

static void *
thread_starter(void *arg)
{
	struct posthread *p = (struct posthread *) arg;
	void *data = p->data;

	p->data = NULL;
	pthread_setspecific(threadkey, p);
	(*p->func)(data);
	ATOMIC_SET(&p->exited, 1);
	TRC_DEBUG(THRD, "Exit thread \"%s\"\n", p->threadname);
	return NULL;
}

static void
join_threads(void)
{
	bool waited;

	struct posthread *self = pthread_getspecific(threadkey);
	pthread_mutex_lock(&posthread_lock);
	do {
		waited = false;
		for (struct posthread *p = posthreads; p; p = p->next) {
			if (p->detached && !p->waiting && ATOMIC_GET(&p->exited)) {
				p->waiting = true;
				pthread_mutex_unlock(&posthread_lock);
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", p->threadname);
				if (self) self->joinwait = p;
				pthread_join(p->tid, NULL);
				if (self) self->joinwait = NULL;
				rm_posthread(p);
				waited = true;
				pthread_mutex_lock(&posthread_lock);
				break;
			}
		}
	} while (waited);
	pthread_mutex_unlock(&posthread_lock);
}

void
join_detached_threads(void)
{
	bool waited;

	struct posthread *self = pthread_getspecific(threadkey);
	pthread_mutex_lock(&posthread_lock);
	do {
		waited = false;
		for (struct posthread *p = posthreads; p; p = p->next) {
			if (p->detached && !p->waiting) {
				p->waiting = true;
				pthread_mutex_unlock(&posthread_lock);
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", p->threadname);
				if (self) self->joinwait = p;
				pthread_join(p->tid, NULL);
				if (self) self->joinwait = NULL;
				rm_posthread(p);
				waited = true;
				pthread_mutex_lock(&posthread_lock);
				break;
			}
		}
	} while (waited);
	pthread_mutex_unlock(&posthread_lock);
}

int
MT_create_thread(MT_Id *t, void (*f) (void *), void *arg, enum MT_thr_detach d, const char *threadname)
{
	pthread_attr_t attr;
	int ret;
	struct posthread *p;

	join_threads();
	if (threadname == NULL) {
		TRC_CRITICAL(GDK, "Thread must have a name\n");
		return -1;
	}
	if (strlen(threadname) >= sizeof(p->threadname)) {
		TRC_CRITICAL(GDK, "Thread's name is too large\n");
		return -1;
	}
	if ((ret = pthread_attr_init(&attr)) != 0) {
		GDKsyserr(ret, "Cannot init pthread attr");
		return -1;
	}
	if ((ret = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE)) != 0) {
		GDKsyserr(ret, "Cannot set stack size");
		pthread_attr_destroy(&attr);
		return -1;
	}
	p = malloc(sizeof(struct posthread));
	if (p == NULL) {
		GDKsyserror("Cannot allocate memory\n");
		pthread_attr_destroy(&attr);
		return -1;
	}
	*p = (struct posthread) {
		.func = f,
		.data = arg,
		.waiting = false,
		.detached = (d == MT_THR_DETACHED),
	};
	ATOMIC_INIT(&p->exited, 0);

	strcpy_len(p->threadname, threadname, sizeof(p->threadname));
#ifdef HAVE_PTHREAD_SIGMASK
	sigset_t new_mask, orig_mask;
	(void) sigfillset(&new_mask);
	MT_thread_sigmask(&new_mask, &orig_mask);
#endif
	TRC_DEBUG(THRD, "Create thread \"%s\"\n", threadname);
	/* protect posthreads during thread creation and only add to
	 * it after the thread was created successfully */
	pthread_mutex_lock(&posthread_lock);
	*t = p->mtid = ++MT_thread_id;
	ret = pthread_create(&p->tid, &attr, thread_starter, p);
	if (ret != 0) {
		pthread_mutex_unlock(&posthread_lock);
		GDKsyserr(ret, "Cannot start thread");
		free(p);
		ret = -1;
	} else {
		/* must not fail after this: the thread has been started */
		p->next = posthreads;
		posthreads = p;
		pthread_mutex_unlock(&posthread_lock);
	}
	(void) pthread_attr_destroy(&attr); /* not interested in errors */
#ifdef HAVE_PTHREAD_SIGMASK
	MT_thread_sigmask(&orig_mask, NULL);
#endif
	return ret;
}

MT_Id
MT_getpid(void)
{
	struct posthread *p;

	if (!thread_initialized)
		return mainthread.mtid;
	p = pthread_getspecific(threadkey);
	return p ? p->mtid : 0;
}

void
MT_exiting_thread(void)
{
	struct posthread *p;

	if (!thread_initialized)
		return;
	p = pthread_getspecific(threadkey);
	if (p) {
		ATOMIC_SET(&p->exited, 1);
		p->working = NULL;
	}
}

int
MT_join_thread(MT_Id t)
{
	struct posthread *p;
	int ret;

	assert(t > 1);
	join_threads();
	p = find_posthread(t);
	if (p == NULL)
		return -1;
	TRC_DEBUG(THRD, "Join thread \"%s\"\n", p->threadname);
	struct posthread *self = pthread_getspecific(threadkey);
	if (self) self->joinwait = p;
	ret = pthread_join(p->tid, NULL);
	if (self) self->joinwait = NULL;
	if (ret != 0) {
		GDKsyserr(ret, "Joining thread failed");
		return -1;
	}
	rm_posthread(p);
	return 0;
}

int
MT_kill_thread(MT_Id t)
{
	assert(t > 1);
#ifdef HAVE_PTHREAD_KILL
	struct posthread *p;

	join_threads();
	p = find_posthread(t);
	if (p)
		return pthread_kill(p->tid, SIGHUP);
#else
	(void) t;
	join_threads();
#endif
	return -1;
}
#endif

int
MT_check_nr_cores(void)
{
	int ncpus = -1;

#if defined(HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
	/* this works on Linux, Solaris and AIX */
	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(HW_NCPU)   /* BSD */
	size_t len = sizeof(int);
	int mib[3];

	/* Everyone should have permission to make this call,
	 * if we get a failure something is really wrong. */
	mib[0] = CTL_HW;
	mib[1] = HW_NCPU;
	mib[2] = -1;
	sysctl(mib, 3, &ncpus, &len, NULL, 0);
#elif defined(WIN32)
	SYSTEM_INFO sysinfo;

	GetSystemInfo(&sysinfo);
	ncpus = sysinfo.dwNumberOfProcessors;
#endif

	/* if we ever need HPUX or OSF/1 (hope not), see
	 * http://ndevilla.free.fr/threads/ */

	if (ncpus <= 0)
		ncpus = 1;
#if SIZEOF_SIZE_T == SIZEOF_INT
	/* On 32-bits systems with large numbers of cpus/cores, we
	 * quickly run out of space due to the number of threads in
	 * use.  Since it is questionable whether many cores on a
	 * 32-bits system are going to be beneficial due to this, we
	 * simply limit the auto-detected cores to 16 on 32-bits
	 * systems.  The user can always override this via
	 * gdk_nr_threads. */
	if (ncpus > 16)
		ncpus = 16;
#endif

#ifndef WIN32
	/* get the number of allocated cpus from the cgroup settings */
	FILE *f = fopen("/sys/fs/cgroup/cpuset/cpuset.cpus", "r");
	if (f != NULL) {
		char buf[512];
		char *p = fgets(buf, 512, f);
		fclose(f);
		if (p != NULL) {
			/* syntax is: ranges of CPU numbers separated
			 * by comma; a range is either a single CPU
			 * id, or two IDs separated by a minus; any
			 * deviation causes the file to be ignored */
			int ncpu = 0;
			for (;;) {
				char *q;
				unsigned fst = strtoul(p, &q, 10);
				if (q == p)
					return ncpus;
				ncpu++;
				if (*q == '-') {
					p = q + 1;
					unsigned lst = strtoul(p, &q, 10);
					if (q == p || lst <= fst)
						return ncpus;
					ncpu += lst - fst;
				}
				if (*q == '\n')
					break;
				if (*q != ',')
					return ncpus;
				p = q + 1;
			}
			if (ncpu < ncpus)
				return ncpu;
		}
	}
#endif

	return ncpus;
}
