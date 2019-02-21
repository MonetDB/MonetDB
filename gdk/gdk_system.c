/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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

MT_Lock MT_system_lock MT_LOCK_INITIALIZER("MT_system_lock");

#if !defined(USE_PTHREAD_LOCKS) && !defined(NDEBUG)
ATOMIC_TYPE volatile GDKlockcnt;
ATOMIC_TYPE volatile GDKlockcontentioncnt;
ATOMIC_TYPE volatile GDKlocksleepcnt;
MT_Lock * volatile GDKlocklist = 0;
ATOMIC_FLAG volatile GDKlocklistlock = ATOMIC_FLAG_INIT;

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
		if (l->sleep < r->sleep ||
		    (l->sleep == r->sleep &&
		     (l->contention < r->contention ||
		      (l->contention == r->contention &&
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

void
GDKlockstatistics(int what)
{
	MT_Lock *l;
	int n = 0;

	if (ATOMIC_TAS(GDKlocklistlock, dummy) != 0) {
		fprintf(stderr, "#WARNING: GDKlocklistlock is set, so cannot access lock list\n");
		return;
	}
	if (what == -1) {
		for (l = GDKlocklist; l; l = l->next) {
			l->count = 0;
			l->contention = 0;
			l->sleep = 0;
		}
		ATOMIC_CLEAR(GDKlocklistlock, dummy);
		return;
	}
	GDKlocklist = sortlocklist(GDKlocklist);
	fprintf(stderr, "# lock name\tcount\tcontention\tsleep\tlocked\t(un)locker\tthread\n");
	for (l = GDKlocklist; l; l = l->next) {
		n++;
		if (what == 0 ||
		    (what == 1 && l->count) ||
		    (what == 2 && l->contention) ||
		    (what == 3 && l->lock))
			fprintf(stderr, "# %-18s\t%zu\t%zu\t%zu\t%s\t%s\t%s\n",
				l->name ? l->name : "unknown",
				l->count, l->contention, l->sleep,
				l->lock ? "locked" : "",
				l->locker ? l->locker : "",
				l->thread ? l->thread : "");
	}
	fprintf(stderr, "#number of locks  %d\n", n);
	fprintf(stderr, "#total lock count %zu\n", (size_t) GDKlockcnt);
	fprintf(stderr, "#lock contention  %zu\n", (size_t) GDKlockcontentioncnt);
	fprintf(stderr, "#lock sleep count %zu\n", (size_t) GDKlocksleepcnt);
	ATOMIC_CLEAR(GDKlocklistlock, dummy);
}
#endif

#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
static struct winthread {
	struct winthread *next;
	HANDLE hdl;
	DWORD tid;
	void (*func) (void *);
	void *data;
	bool exited:1, detached:1, waiting:1;
	const char *threadname;
} *winthreads = NULL;
static struct winthread mainthread = {
	.threadname = "main thread",
};

static CRITICAL_SECTION winthread_cs;
static DWORD threadslot = TLS_OUT_OF_INDEXES;

bool
MT_thread_init(void)
{
	if (threadslot == TLS_OUT_OF_INDEXES) {
		threadslot = TlsAlloc();
		if (threadslot == TLS_OUT_OF_INDEXES)
			return false;
		mainthread.tid = GetCurrentThreadId();
		if (TlsSetValue(threadslot, &mainthread) == 0) {
			TlsFree(threadslot);
			threadslot = TLS_OUT_OF_INDEXES;
			return false;
		}
		InitializeCriticalSection(&winthread_cs);
	}
	return true;
}

static inline struct winthread *
find_winthread_locked(DWORD tid)
{
	for (struct winthread *w = winthreads; w; w = w->next)
		if (w->tid == tid)
			return w;
	return NULL;
}

static struct winthread *
find_winthread(DWORD tid)
{
	struct winthread *w;

	EnterCriticalSection(&winthread_cs);
	w = find_winthread_locked(tid);
	LeaveCriticalSection(&winthread_cs);
	return w;
}

const char *
MT_thread_getname(void)
{
	struct winthread *w = TlsGetValue(threadslot);
	return w && w->threadname ? w->threadname : "unknown thread";
}

void
MT_thread_setname(const char *name)
{
	struct winthread *w = TlsGetValue(threadslot);

	if (w) {
		EnterCriticalSection(&winthread_cs);
		w->threadname = name;
		LeaveCriticalSection(&winthread_cs);
	}
}

void
MT_thread_setdata(void *data)
{
	struct winthread *w = TlsGetValue(threadslot);

	if (w)
		w->data = data;
}

void *
MT_thread_getdata(void)
{
	struct winthread *w = TlsGetValue(threadslot);

	return w ? w->data : NULL;
}

void
gdk_system_reset(void)
{
	assert(threadslot != TLS_OUT_OF_INDEXES);
	TlsFree(threadslot);
	threadslot = TLS_OUT_OF_INDEXES;
	DeleteCriticalSection(&winthread_cs);
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
	EnterCriticalSection(&winthread_cs);
	w->exited = true;
	LeaveCriticalSection(&winthread_cs);
	ExitThread(0);
	return TRUE;
}

static void
join_threads(void)
{
	bool waited;

	EnterCriticalSection(&winthread_cs);
	do {
		waited = false;
		for (struct winthread *w = winthreads; w; w = w->next) {
			if (w->exited && w->detached && !w->waiting) {
				w->waiting = true;
				LeaveCriticalSection(&winthread_cs);
				WaitForSingleObject(w->hdl, INFINITE);
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

	EnterCriticalSection(&winthread_cs);
	do {
		waited = false;
		for (struct winthread *w = winthreads; w; w = w->next) {
			if (w->detached && !w->waiting) {
				w->waiting = true;
				LeaveCriticalSection(&winthread_cs);
				WaitForSingleObject(w->hdl, INFINITE);
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
	struct winthread *w = malloc(sizeof(*w));

	if (w == NULL)
		return -1;

	join_threads();
	w->func = f;
	w->hdl = NULL;
	w->tid = 0;
	w->data = arg;
	w->exited = false;
	w->waiting = false;
	w->detached = (d == MT_THR_DETACHED);
	w->threadname = threadname;
	EnterCriticalSection(&winthread_cs);
	w->next = winthreads;
	winthreads = w;
	LeaveCriticalSection(&winthread_cs);
	w->hdl = CreateThread(NULL, THREAD_STACK_SIZE, thread_starter, w,
			      0, &w->tid);
	if (w->hdl == NULL) {
		rm_winthread(w);
		return -1;
	}
	/* must not fail after this: the thread has been started */
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
	struct winthread *w = TlsGetValue(threadslot);

	if (w) {
		EnterCriticalSection(&winthread_cs);
		w->exited = true;
		LeaveCriticalSection(&winthread_cs);
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
	if (WaitForSingleObject(w->hdl, INFINITE) == WAIT_OBJECT_0 &&
	    CloseHandle(w->hdl)) {
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

#ifdef USE_PTHREAD_LOCKS

void
pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *mutexattr)
{
	(void) mutexattr;
	*mutex = CreateMutex(NULL, 0, NULL);
}

void
pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	CloseHandle(*mutex);
}

int
pthread_mutex_lock(pthread_mutex_t *mutex)
{
	return WaitForSingleObject(*mutex, INFINITE) == WAIT_OBJECT_0 ? 0 : -1;
}

int
pthread_mutex_trylock(pthread_mutex_t *mutex)
{
	return WaitForSingleObject(*mutex, 0) == WAIT_OBJECT_0 ? 0 : -1;
}

int
pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	return ReleaseMutex(*mutex) ? 0 : -1;
}

#endif

void
pthread_sema_init(pthread_sema_t *s, int flag, int nresources)
{
	(void) flag;
	*s = CreateSemaphore(NULL, nresources, 0x7fffffff, NULL);
}

void
pthread_sema_destroy(pthread_sema_t *s)
{
	CloseHandle(*s);
}

void
pthread_sema_up(pthread_sema_t *s)
{
	ReleaseSemaphore(*s, 1, NULL);
}

void
pthread_sema_down(pthread_sema_t *s)
{
	WaitForSingleObject(*s, INFINITE);
}

#else  /* !defined(HAVE_PTHREAD_H) && defined(_MSC_VER) */

static struct posthread {
	struct posthread *next;
	void (*func)(void *);
	void *data;
	const char *threadname;
	pthread_t tid;
	MT_Id mtid;
	bool exited:1, detached:1, waiting:1;
} *posthreads = NULL;
static struct posthread mainthread = {
	.threadname = "main thread",
	.mtid = 1,
};
static pthread_mutex_t posthread_lock = PTHREAD_MUTEX_INITIALIZER;
static MT_Id MT_thread_id = 1;

static pthread_key_t threadkey;

bool
MT_thread_init(void)
{
	int ret;

	if ((ret = pthread_key_create(&threadkey, NULL)) != 0) {
		fprintf(stderr,
			"#MT_thread_init: creating specific key for thread "
			"failed: %s\n", strerror(ret));
		return false;
	}
	mainthread.tid = pthread_self();
	if ((ret = pthread_setspecific(threadkey, &mainthread)) != 0) {
		fprintf(stderr,
			"#MT_thread_init: setting specific value failed: %s\n",
			strerror(ret));
	}
	return true;
}

static struct posthread *
find_posthread(MT_Id tid)
{
	struct posthread *p;

	pthread_mutex_lock(&posthread_lock);
	for (p = posthreads; p; p = p->next)
		if (p->mtid == tid)
			break;
	pthread_mutex_unlock(&posthread_lock);
	return p;
}

void
MT_thread_setname(const char *name)
{
	struct posthread *p;

	p = pthread_getspecific(threadkey);
	if (p)
		p->threadname = name;
}

const char *
MT_thread_getname(void)
{
	struct posthread *p;

	p = pthread_getspecific(threadkey);
	return p && p->threadname ? p->threadname : "unknown thread";
}

void
MT_thread_setdata(void *data)
{
	struct posthread *p = pthread_getspecific(threadkey);

	if (p)
		p->data = data;
}

void *
MT_thread_getdata(void)
{
	struct posthread *p = pthread_getspecific(threadkey);

	return p ? p->data : NULL;
}

#ifdef HAVE_PTHREAD_SIGMASK
static void
MT_thread_sigmask(sigset_t *new_mask, sigset_t *orig_mask)
{
	/* do not check for errors! */
	sigdelset(new_mask, SIGQUIT);
	sigdelset(new_mask, SIGALRM);
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
	pthread_mutex_lock(&posthread_lock);
	p->exited = true;
	pthread_mutex_unlock(&posthread_lock);
	return NULL;
}

static void
join_threads(void)
{
	bool waited;

	pthread_mutex_lock(&posthread_lock);
	do {
		waited = false;
		for (struct posthread *p = posthreads; p; p = p->next) {
			if (p->exited && p->detached && !p->waiting) {
				p->waiting = true;
				pthread_mutex_unlock(&posthread_lock);
				pthread_join(p->tid, NULL);
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

	pthread_mutex_lock(&posthread_lock);
	do {
		waited = false;
		for (struct posthread *p = posthreads; p; p = p->next) {
			if (p->detached && !p->waiting) {
				p->waiting = true;
				pthread_mutex_unlock(&posthread_lock);
				pthread_join(p->tid, NULL);
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
	if ((ret = pthread_attr_init(&attr)) != 0) {
		fprintf(stderr,
			"#MT_create_thread: cannot init pthread attr: %s\n",
			strerror(ret));
		return -1;
	}
	if ((ret = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE)) != 0) {
		fprintf(stderr,
			"#MT_create_thread: cannot set stack size: %s\n",
			strerror(ret));
		pthread_attr_destroy(&attr);
		return -1;
	}
	p = malloc(sizeof(struct posthread));
	if (p == NULL) {
		fprintf(stderr,
			"#MT_create_thread: cannot allocate memory: %s\n",
			strerror(errno));
		pthread_attr_destroy(&attr);
		return -1;
	}
	p->tid = 0;
	p->func = f;
	p->data = arg;
	p->exited = false;
	p->waiting = false;
	p->detached = (d == MT_THR_DETACHED);
	p->threadname = threadname;
	pthread_mutex_lock(&posthread_lock);
	p->next = posthreads;
	posthreads = p;
	*t = p->mtid = ++MT_thread_id;
	pthread_mutex_unlock(&posthread_lock);
#ifdef HAVE_PTHREAD_SIGMASK
	sigset_t new_mask, orig_mask;
	(void) sigfillset(&new_mask);
	MT_thread_sigmask(&new_mask, &orig_mask);
#endif
	ret = pthread_create(&p->tid, &attr, thread_starter, p);
	if (ret != 0) {
		fprintf(stderr,
			"#MT_create_thread: cannot start thread: %s\n",
			strerror(ret));
		rm_posthread(p);
		ret = -1;
	} else {
		/* must not fail after this: the thread has been started */
	}
#ifdef HAVE_PTHREAD_SIGMASK
	MT_thread_sigmask(&orig_mask, NULL);
#endif
	return ret;
}

MT_Id
MT_getpid(void)
{
	struct posthread *p;

	p = pthread_getspecific(threadkey);
	return p ? p->mtid : 0;
}

void
MT_exiting_thread(void)
{
	struct posthread *p;

	p = pthread_getspecific(threadkey);
	if (p) {
		pthread_mutex_lock(&posthread_lock);
		p->exited = true;
		pthread_mutex_unlock(&posthread_lock);
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
	if ((ret = pthread_join(p->tid, NULL)) != 0) {
		fprintf(stderr, "#MT_join_thread: joining thread failed: %s\n",
			strerror(ret));
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

#if defined(_AIX) || defined(__MACH__)
void
pthread_sema_init(pthread_sema_t *s, int flag, int nresources)
{
	(void) flag;
	s->cnt = nresources;
	pthread_mutex_init(&(s->mutex), 0);
	pthread_cond_init(&(s->cond), 0);
}

void
pthread_sema_destroy(pthread_sema_t *s)
{
	pthread_mutex_destroy(&(s->mutex));
	pthread_cond_destroy(&(s->cond));
}

void
pthread_sema_up(pthread_sema_t *s)
{
	(void)pthread_mutex_lock(&(s->mutex));

	if (s->cnt++ < 0) {
		/* wake up sleeping thread */
		(void)pthread_cond_signal(&(s->cond));
	}
	(void)pthread_mutex_unlock(&(s->mutex));
}

void
pthread_sema_down(pthread_sema_t *s)
{
	(void)pthread_mutex_lock(&(s->mutex));

	if (--s->cnt < 0) {
		/* thread goes to sleep */
		(void)pthread_cond_wait(&(s->cond), &(s->mutex));
	}
	(void)pthread_mutex_unlock(&(s->mutex));
}
#endif
#endif

int
MT_check_nr_cores(void)
{
	int ncpus = -1;

#if defined(HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
	/* this works on Linux, Solaris and AIX */
	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(HAVE_SYS_SYSCTL_H) && defined(HW_NCPU)   /* BSD */
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
	/* On 32-bits systems with large amounts of cpus/cores, we quickly
	 * run out of space due to the amount of threads in use.  Since it
	 * is questionable whether many cores on a 32-bits system are going
	 * to beneficial due to this, we simply limit the auto-detected
	 * cores to 16 on 32-bits systems.  The user can always override
	 * this via gdk_nr_threads. */
	if (ncpus > 16)
		ncpus = 16;
#endif

	return ncpus;
}
