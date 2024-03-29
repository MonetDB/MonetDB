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

#include "mutils.h"

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
	r->prev = NULL;
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
				l->prev = ll;
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
				r->prev = ll;
				ll = ll->next;
			}
			r = r->next;
		}
	}
	/* append rest of remaining list */
	if (l) {
		ll->next = l;
		l->prev = ll;
	} else {
		ll->next = r;
		r->prev = ll;
	}
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
		printf("GDKlocklistlock is set, so cannot access lock list\n");
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
	printf("%-18s\t%s\t%s\t%s\t%s\t%s\t%s\n",
	       "lock name", "count", "content", "sleep",
	       "locked", "locker", "thread");
	for (l = GDKlocklist; l; l = l->next) {
		n++;
		if (what == 0 ||
		    (what == 1 && l->count) ||
		    (what == 2 && ATOMIC_GET(&l->contention)) ||
		    (what == 3 && lock_isset(l)))
			printf("%-18s\t%zu\t%zu\t%zu\t%s\t%s\t%s\n",
			       l->name, l->count,
			       (size_t) ATOMIC_GET(&l->contention),
			       (size_t) ATOMIC_GET(&l->sleep),
			       lock_isset(l) ? "locked" : "",
			       l->locker ? l->locker : "",
			       l->thread ? l->thread : "");
	}
	printf("Number of locks: %d\n", n);
	printf("Total lock count: %zu\n", (size_t) ATOMIC_GET(&GDKlockcnt));
	printf("Lock contention:  %zu\n", (size_t) ATOMIC_GET(&GDKlockcontentioncnt));
	printf("Lock sleep count: %zu\n", (size_t) ATOMIC_GET(&GDKlocksleepcnt));
	fflush(stdout);
	ATOMIC_CLEAR(&GDKlocklistlock);
}

#endif	/* LOCK_STATS */

struct thread_funcs {
	void (*init)(void *);
	void (*destroy)(void *);
	void *data;
};

static struct mtthread {
	struct mtthread *next;
	void (*func) (void *);	/* function to be called */
	void *data;		/* and its data */
	struct thread_funcs *thread_funcs; /* callback funcs */
	int nthread_funcs;
	MT_Lock *lockwait;	/* lock we're waiting for */
	MT_Sema *semawait;	/* semaphore we're waiting for */
	MT_Cond *condwait;	/* condition variable we're waiting for */
#ifdef LOCK_OWNER
	MT_Lock *mylocks;	/* locks we're holding */
#endif
	struct mtthread *joinwait; /* process we are joining with */
	const char *working;	/* what we're currently doing */
	char algorithm[512];	/* the algorithm used in the last operation */
	size_t algolen;		/* length of string in .algorithm */
	ATOMIC_TYPE exited;
	bool detached:1, waiting:1;
	unsigned int refs:20;
	bool limit_override;	/* not in bit field because of data races */
	char threadname[MT_NAME_LEN];
	QryCtx *qry_ctx;
#ifdef HAVE_PTHREAD_H
	pthread_t hdl;
#else
	HANDLE hdl;
	DWORD wtid;
#endif
	MT_Id tid;
	uintptr_t sp;
	char *errbuf;
	struct freebats freebats;
} *mtthreads = NULL;
struct mtthread mainthread = {
	.threadname = "main thread",
	.exited = ATOMIC_VAR_INIT(0),
	.refs = 1,
};
#ifdef HAVE_PTHREAD_H
static pthread_mutex_t posthread_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t threadkey;
#define thread_lock()		pthread_mutex_lock(&posthread_lock)
#define thread_unlock()		pthread_mutex_unlock(&posthread_lock)
#define thread_self()		pthread_getspecific(threadkey)
#define thread_setself(self)	pthread_setspecific(threadkey, self)
#else
static CRITICAL_SECTION winthread_cs;
static DWORD threadkey = TLS_OUT_OF_INDEXES;
#define thread_lock()		EnterCriticalSection(&winthread_cs)
#define thread_unlock()		LeaveCriticalSection(&winthread_cs)
#define thread_self()		TlsGetValue(threadkey)
#define thread_setself(self)	TlsSetValue(threadkey, self)
#endif
static bool thread_initialized = false;

#if defined(_MSC_VER) && _MSC_VER >= 1900
#pragma warning(disable : 4172)
#endif
static inline uintptr_t
THRsp(void)
{
#if defined(__GNUC__) || defined(__clang__)
	return (uintptr_t) __builtin_frame_address(0);
#else
	int l = 0;
	uintptr_t sp = (uintptr_t) (&l);

	return sp;
#endif
}

bool
THRhighwater(void)
{
	struct mtthread *s = thread_self();
	if (s != NULL && s->sp != 0) {
		uintptr_t c = THRsp();
		size_t diff = c < s->sp ? s->sp - c : c - s->sp;
		if (diff > THREAD_STACK_SIZE - 80 * 1024)
			return true;
	}
	return false;
}

void
dump_threads(void)
{
	char buf[1024];
	thread_lock();
	for (struct mtthread *t = mtthreads; t; t = t->next) {
		MT_Lock *lk = t->lockwait;
		MT_Sema *sm = t->semawait;
		MT_Cond *cn = t->condwait;
		struct mtthread *jn = t->joinwait;
		int pos = snprintf(buf, sizeof(buf),
				   "%s, tid %zu, %"PRIu32" free bats, waiting for %s%s, working on %.200s",
				   t->threadname,
				   t->tid,
				   t->freebats.nfreebats,
				   lk ? "lock " : sm ? "semaphore " : cn ? "condvar " : jn ? "thread " : "",
				   lk ? lk->name : sm ? sm->name : cn ? cn->name : jn ? jn->threadname : "nothing",
				   ATOMIC_GET(&t->exited) ? "exiting" :
				   t->working ? t->working : "nothing");
#ifdef LOCK_OWNER
		const char *sep = ", locked: ";
		for (MT_Lock *l = t->mylocks; l && pos < (int) sizeof(buf); l = l->nxt) {
			pos += snprintf(buf + pos, sizeof(buf) - pos,
					"%s%s(%s)", sep, l->name, l->locker);
			sep = ", ";
		}
#endif
		TRC_DEBUG_IF(THRD)
			TRC_DEBUG_ENDIF(THRD, "%s%s\n", buf, pos >= (int) sizeof(buf) ? "..." : "");
		else
			printf("%s%s\n", buf, pos >= (int) sizeof(buf) ? "..." : "");
	}
	thread_unlock();
}

static void
rm_mtthread(struct mtthread *t)
{
	struct mtthread **pt;

	assert(t != &mainthread);
	BBPrelinquish(&t->freebats);
	thread_lock();
	for (pt = &mtthreads; *pt && *pt != t; pt = &(*pt)->next)
		;
	if (*pt)
		*pt = t->next;
	ATOMIC_DESTROY(&t->exited);
	free(t);
	thread_unlock();
}

bool
MT_thread_init(void)
{
	if (thread_initialized)
		return true;
#ifdef HAVE_PTHREAD_H
	int ret;

	if ((ret = pthread_key_create(&threadkey, NULL)) != 0) {
		GDKsyserr(ret, "Creating specific key for thread failed");
		return false;
	}
	mainthread.hdl = pthread_self();
	if ((ret = thread_setself(&mainthread)) != 0) {
		GDKsyserr(ret, "Setting specific value failed");
		return false;
	}
#else
	threadkey = TlsAlloc();
	if (threadkey == TLS_OUT_OF_INDEXES) {
		GDKwinerror("Creating thread-local slot for thread failed");
		return false;
	}
	mainthread.wtid = GetCurrentThreadId();
	if (thread_setself(&mainthread) == 0) {
		GDKwinerror("Setting thread-local value failed");
		TlsFree(threadkey);
		threadkey = TLS_OUT_OF_INDEXES;
		return false;
	}
	InitializeCriticalSection(&winthread_cs);
#endif
	mainthread.tid = (MT_Id) &mainthread;
	mainthread.next = NULL;
	mtthreads = &mainthread;
	thread_initialized = true;
	return true;
}
bool
MT_thread_register(void)
{
	MT_Id mtid;

	assert(thread_initialized);
	if (!thread_initialized)
		return false;

	struct mtthread *self;

	if ((self = thread_self()) != NULL) {
		if (self->refs == 1000000) {
			/* there are limits... */
			return false;
		}
		self->refs++;
		return true;
	}

	self = malloc(sizeof(*self));
	if (self == NULL)
		return false;

	mtid = (MT_Id) self;
	*self = (struct mtthread) {
		.detached = false,
#ifdef HAVE_PTHREAD_H
		.hdl = pthread_self(),
#else
		.wtid = GetCurrentThreadId(),
#endif
		.refs = 1,
		.tid = mtid,
	};
	snprintf(self->threadname, sizeof(self->threadname), "foreign %zu", self->tid);
	ATOMIC_INIT(&self->exited, 0);
	thread_setself(self);
	thread_lock();
	self->next = mtthreads;
	mtthreads = self;
	thread_unlock();
	return true;
}

void
MT_thread_deregister(void)
{
	struct mtthread *self;

	if ((self = thread_self()) == NULL)
		return;

	if (--self->refs == 0) {
		rm_mtthread(self);
		thread_setself(NULL);
	}
}

static struct mtthread *
find_mtthread(MT_Id tid)
{
	struct mtthread *t;

	thread_lock();
	for (t = mtthreads; t && t->tid != tid; t = t->next)
		;
	thread_unlock();
	return t;
}

gdk_return
MT_alloc_tls(MT_TLS_t *newkey)
{
#ifdef HAVE_PTHREAD_H
	int ret;
	if ((ret = pthread_key_create(newkey, NULL)) != 0) {
		GDKsyserr(ret, "Creating TLS key for thread failed");
		return GDK_FAIL;
	}
#else
	if ((*newkey = TlsAlloc()) == TLS_OUT_OF_INDEXES) {
		GDKwinerror("Creating TLS key for thread failed");
		return GDK_FAIL;
	}
#endif
	return GDK_SUCCEED;
}

void
MT_tls_set(MT_TLS_t key, void *val)
{
#ifdef HAVE_PTHREAD_H
	pthread_setspecific(key, val);
#else
	assert(key != TLS_OUT_OF_INDEXES);
	TlsSetValue(key, val);
#endif
}

void *
MT_tls_get(MT_TLS_t key)
{
#ifdef HAVE_PTHREAD_H
	return pthread_getspecific(key);
#else
	assert(key != TLS_OUT_OF_INDEXES);
	return TlsGetValue(key);
#endif
}

const char *
MT_thread_getname(void)
{
	struct mtthread *self;

	if (!thread_initialized)
		return mainthread.threadname;
	self = thread_self();
	return self ? self->threadname : UNKNOWN_THREAD;
}

void
GDKsetbuf(char *errbuf)
{
	struct mtthread *self;

	self = thread_self();
	if (self == NULL)
		self = &mainthread;
	assert(errbuf == NULL || self->errbuf == NULL);
	self->errbuf = errbuf;
	if (errbuf)
		*errbuf = 0;		/* start clean */
}

char *
GDKgetbuf(void)
{
	struct mtthread *self;

	self = thread_self();
	if (self == NULL)
		self = &mainthread;
	return self->errbuf;
}

struct freebats *
MT_thread_getfreebats(void)
{
	struct mtthread *self;

	self = thread_self();
	if (self == NULL)
		self = &mainthread;
	return &self->freebats;
}

void
MT_thread_setdata(void *data)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self)
		self->data = data;
}

void *
MT_thread_getdata(void)
{
	if (!thread_initialized)
		return NULL;
	struct mtthread *self = thread_self();

	return self ? self->data : NULL;
}

void
MT_thread_set_qry_ctx(QryCtx *ctx)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self)
		self->qry_ctx = ctx;
}

QryCtx *
MT_thread_get_qry_ctx(void)
{
	if (!thread_initialized)
		return NULL;
	struct mtthread *self = thread_self();

	return self ? self->qry_ctx : NULL;
}

void
MT_thread_setlockwait(MT_Lock *lock)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self)
		self->lockwait = lock;
}

void
MT_thread_setsemawait(MT_Sema *sema)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self)
		self->semawait = sema;
}

static void
MT_thread_setcondwait(MT_Cond *cond)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self)
		self->condwait = cond;
}

#ifdef LOCK_OWNER
void
MT_thread_add_mylock(MT_Lock *lock)
{
	struct mtthread *self;
	if (!thread_initialized)
		self = &mainthread;
	else
		self = thread_self();

	if (self) {
		lock->nxt = self->mylocks;
		self->mylocks = lock;
	}
}

void
MT_thread_del_mylock(MT_Lock *lock)
{
	struct mtthread *self;
	if (!thread_initialized)
		self = &mainthread;
	else
		self = thread_self();

	if (self) {
		if (self->mylocks == lock) {
			self->mylocks = lock->nxt;
		} else {
			for (MT_Lock *l = self->mylocks; l; l = l->nxt) {
				if (l->nxt == lock) {
					l->nxt = lock->nxt;
					break;
				}
			}
		}
	}
}
#endif

void
MT_thread_setworking(const char *work)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self) {
		if (work == NULL)
			self->working = NULL;
		else if (strcmp(work, "store locked") == 0)
			self->limit_override = true;
		else if (strcmp(work, "store unlocked") == 0)
			self->limit_override = false;
		else
			self->working = work;
	}
}

void
MT_thread_setalgorithm(const char *algo)
{
	if (!thread_initialized)
		return;
	struct mtthread *self = thread_self();

	if (self) {
		if (algo) {
			if (self->algolen > 0) {
				if (self->algolen < sizeof(self->algorithm))
					self->algolen += strconcat_len(self->algorithm + self->algolen, sizeof(self->algorithm) - self->algolen, "; ", algo, NULL);
			} else
				self->algolen = strcpy_len(self->algorithm, algo, sizeof(self->algorithm));
		} else {
			self->algorithm[0] = 0;
			self->algolen = 0;
		}
	}
}

const char *
MT_thread_getalgorithm(void)
{
	if (!thread_initialized)
		return NULL;
	struct mtthread *self = thread_self();

	return self && self->algorithm[0] ? self->algorithm : NULL;
}

bool
MT_thread_override_limits(void)
{
	if (!thread_initialized)
		return false;
	struct mtthread *self = thread_self();

	return self && self->limit_override;
}

static struct thread_init_cb {
	struct thread_init_cb *next;
	void (*init)(void *);
	void (*destroy)(void *);
	void *data;
} *init_cb;
static MT_Lock thread_init_lock = MT_LOCK_INITIALIZER(thread_init_lock);

gdk_return
MT_thread_init_add_callback(void (*init)(void *), void (*destroy)(void *), void *data)
{
	struct thread_init_cb *p = GDKmalloc(sizeof(struct thread_init_cb));

	if (p == NULL)
		return GDK_FAIL;
	*p = (struct thread_init_cb) {
		.init = init,
		.destroy = destroy,
		.next = NULL,
		.data = data,
	};
	MT_lock_set(&thread_init_lock);
	struct thread_init_cb **pp = &init_cb;
	while (*pp)
		pp = &(*pp)->next;
	*pp = p;
	MT_lock_unset(&thread_init_lock);
	return GDK_SUCCEED;
}

#ifdef HAVE_PTHREAD_H
static void *
#else
static DWORD WINAPI
#endif
thread_starter(void *arg)
{
	struct mtthread *self = (struct mtthread *) arg;
	void *data = self->data;

#ifdef HAVE_PTHREAD_H
#ifdef HAVE_PTHREAD_SETNAME_NP
	/* name can be at most 16 chars including \0 */
	char name[16];
	(void) strcpy_len(name, self->threadname, sizeof(name));
	pthread_setname_np(
#ifndef __APPLE__
		pthread_self(),
#endif
		name);
#endif
#else
#ifdef HAVE_SETTHREADDESCRIPTION
	wchar_t *wname = utf8towchar(self->threadname);
	if (wname != NULL) {
		SetThreadDescription(GetCurrentThread(), wname);
		free(wname);
	}
#endif
#endif
	self->data = NULL;
	self->sp = THRsp();
	thread_setself(self);
	for (int i = 0; i < self->nthread_funcs; i++) {
		if (self->thread_funcs[i].init)
			(*self->thread_funcs[i].init)(self->thread_funcs[i].data);
	}
	(*self->func)(data);
	for (int i = 0; i < self->nthread_funcs; i++) {
		if (self->thread_funcs[i].destroy)
			(*self->thread_funcs[i].destroy)(self->thread_funcs[i].data);
	}
	free(self->thread_funcs);
	ATOMIC_SET(&self->exited, 1);
	TRC_DEBUG(THRD, "Exit thread \"%s\"\n", self->threadname);
	return 0;		/* NULL for pthreads, 0 for Windows */
}

static void
join_threads(void)
{
	bool waited;

	struct mtthread *self = thread_self();
	if (!self)
		return;
	thread_lock();
	do {
		waited = false;
		for (struct mtthread *t = mtthreads; t; t = t->next) {
			if (ATOMIC_GET(&t->exited) && t->detached && !t->waiting) {
				t->waiting = true;
				thread_unlock();
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", t->threadname);
				self->joinwait = t;
#ifdef HAVE_PTHREAD_H
				pthread_join(t->hdl, NULL);
#else
				WaitForSingleObject(t->hdl, INFINITE);
#endif
				self->joinwait = NULL;
#ifndef HAVE_PTHREAD_H
				CloseHandle(t->hdl);
#endif
				rm_mtthread(t);
				waited = true;
				thread_lock();
				break;
			}
		}
	} while (waited);
	thread_unlock();
}

void
join_detached_threads(void)
{
	bool waited;

	struct mtthread *self = thread_self();
	thread_lock();
	do {
		waited = false;
		for (struct mtthread *t = mtthreads; t; t = t->next) {
			if (t->detached && !t->waiting) {
				t->waiting = true;
				thread_unlock();
				TRC_DEBUG(THRD, "Join thread \"%s\"\n", t->threadname);
				self->joinwait = t;
#ifdef HAVE_PTHREAD_H
				pthread_join(t->hdl, NULL);
#else
				WaitForSingleObject(t->hdl, INFINITE);
#endif
				self->joinwait = NULL;
#ifndef HAVE_PTHREAD_H
				CloseHandle(t->hdl);
#endif
				rm_mtthread(t);
				waited = true;
				thread_lock();
				break;
			}
		}
	} while (waited);
	thread_unlock();
}

int
MT_create_thread(MT_Id *t, void (*f) (void *), void *arg, enum MT_thr_detach d, const char *threadname)
{
	struct mtthread *self;
	MT_Id mtid;

	assert(thread_initialized);
	join_threads();
	if (threadname == NULL) {
		TRC_CRITICAL(GDK, "Thread must have a name\n");
		return -1;
	}
	if (strlen(threadname) >= sizeof(self->threadname)) {
		TRC_CRITICAL(GDK, "Thread's name is too large\n");
		return -1;
	}

#ifdef HAVE_PTHREAD_H
	pthread_attr_t attr;
	int ret;
	if ((ret = pthread_attr_init(&attr)) != 0) {
		GDKsyserr(ret, "Cannot init pthread attr");
		return -1;
	}
	if ((ret = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE)) != 0) {
		GDKsyserr(ret, "Cannot set stack size");
		pthread_attr_destroy(&attr);
		return -1;
	}
#endif
	self = malloc(sizeof(*self));
	if (self == NULL) {
		GDKsyserror("Cannot allocate memory\n");
#ifdef HAVE_PTHREAD_H
		pthread_attr_destroy(&attr);
#endif
		return -1;
	}
	mtid = (MT_Id) self;

	*self = (struct mtthread) {
		.func = f,
		.data = arg,
		.waiting = false,
		.detached = (d == MT_THR_DETACHED),
		.refs = 1,
		.tid = mtid,
	};
	MT_lock_set(&thread_init_lock);
	/* remember the list of callback functions we need to call for
	 * this thread (i.e. anything registered so far) */
	for (struct thread_init_cb *p = init_cb; p; p = p->next)
		self->nthread_funcs++;
	if (self->nthread_funcs > 0) {
		self->thread_funcs = malloc(self->nthread_funcs * sizeof(*self->thread_funcs));
		if (self->thread_funcs == NULL) {
			GDKsyserror("Cannot allocate memory\n");
			MT_lock_unset(&thread_init_lock);
			free(self);
#ifdef HAVE_PTHREAD_H
			pthread_attr_destroy(&attr);
#endif
			return -1;
		}
		int n = 0;
		for (struct thread_init_cb *p = init_cb; p; p = p->next) {
			self->thread_funcs[n++] = (struct thread_funcs) {
				.init = p->init,
				.destroy = p->destroy,
				.data = p->data,
			};
		}
	}
	MT_lock_unset(&thread_init_lock);

	ATOMIC_INIT(&self->exited, 0);
	strcpy_len(self->threadname, threadname, sizeof(self->threadname));
	char *p;
	if ((p = strstr(self->threadname, "XXXX")) != NULL) {
		/* overwrite XXXX with thread ID; bottom three bits are
		 * likely 0, so skip those */
		char buf[5];
		snprintf(buf, 5, "%04zu", (mtid >> 3) % 9999);
		memcpy(p, buf, 4);
	}
	TRC_DEBUG(THRD, "Create thread \"%s\"\n", self->threadname);
#ifdef HAVE_PTHREAD_H
#ifdef HAVE_PTHREAD_SIGMASK
	sigset_t new_mask, orig_mask;
	(void) sigfillset(&new_mask);
	sigdelset(&new_mask, SIGQUIT);
	sigdelset(&new_mask, SIGPROF);
	pthread_sigmask(SIG_SETMASK, &new_mask, &orig_mask);
#endif
	ret = pthread_create(&self->hdl, &attr, thread_starter, self);
	pthread_attr_destroy(&attr);
#ifdef HAVE_PTHREAD_SIGMASK
	pthread_sigmask(SIG_SETMASK, &orig_mask, NULL);
#endif
	if (ret != 0) {
		GDKsyserr(ret, "Cannot start thread");
		free(self->thread_funcs);
		free(self);
		return -1;
	}
#else
	self->hdl = CreateThread(NULL, THREAD_STACK_SIZE, thread_starter, self,
			      0, &self->wtid);
	if (self->hdl == NULL) {
		GDKwinerror("Failed to create thread");
		free(self->thread_funcs);
		free(self);
		return -1;
	}
#endif
	/* must not fail after this: the thread has been started */
	*t = mtid;
	thread_lock();
	self->next = mtthreads;
	mtthreads = self;
	thread_unlock();
	return 0;
}

MT_Id
MT_getpid(void)
{
	struct mtthread *self;

	if (!thread_initialized)
		self = &mainthread;
	else
		self = thread_self();
	return self->tid;
}

void
MT_exiting_thread(void)
{
	struct mtthread *self;

	if (!thread_initialized)
		return;
	self = thread_self();
	if (self) {
		ATOMIC_SET(&self->exited, 1);
		self->working = NULL;
	}
}

int
MT_join_thread(MT_Id tid)
{
	struct mtthread *t;

	assert(tid != mainthread.tid);
	join_threads();
	t = find_mtthread(tid);
	if (t == NULL
#ifndef HAVE_PTHREAD_H
	    || t->hdl == NULL
#endif
		)
		return -1;
	TRC_DEBUG(THRD, "Join thread \"%s\"\n", t->threadname);
	struct mtthread *self = thread_self();
	self->joinwait = t;
#ifdef HAVE_PTHREAD_H
	int ret = pthread_join(t->hdl, NULL);
#else
	DWORD ret = WaitForSingleObject(t->hdl, INFINITE);
#endif
	self->joinwait = NULL;
	if (
#ifdef HAVE_PTHREAD_H
		ret == 0
#else
		ret == WAIT_OBJECT_0 && CloseHandle(t->hdl)
#endif
		) {
		rm_mtthread(t);
		return 0;
	}
	return -1;
}

static bool
MT_kill_thread(struct mtthread *t)
{
	assert(t != thread_self());
#ifdef HAVE_PTHREAD_H
#ifdef HAVE_PTHREAD_KILL
	if (pthread_kill(t->hdl, SIGHUP) == 0)
		return true;
#endif
#else
	if (t->hdl == NULL) {
		/* detached thread */
		HANDLE h;
		bool ret = false;
		h = OpenThread(THREAD_ALL_ACCESS, 0, t->wtid);
		if (h == NULL)
			return false;
		if (TerminateThread(h, -1))
			ret = true;
		CloseHandle(h);
		return ret;
	}
	if (TerminateThread(t->hdl, -1))
		return true;
#endif
	return false;
}

bool
MT_kill_threads(void)
{
	struct mtthread *self = thread_self();
	bool killed = false;

	assert(self == &mainthread);
	join_threads();
	thread_lock();
	for (struct mtthread *t = mtthreads; t; t = t->next) {
		if (t == self)
			continue;
		TRC_INFO(GDK, "Killing thread %s\n", t->threadname);
		killed |= MT_kill_thread(t);
	}
	thread_unlock();
	return killed;
}

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


void
MT_cond_init(MT_Cond *cond)
{
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	InitializeConditionVariable(&cond->cv);
#else
	pthread_cond_init(&cond->cv, NULL);
#endif
}


void
MT_cond_destroy(MT_Cond *cond)
{
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	/* no need */
#else
	pthread_cond_destroy(&cond->cv);
#endif
}

void
MT_cond_wait(MT_Cond *cond, MT_Lock *lock)
{
	MT_thread_setcondwait(cond);
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	SleepConditionVariableCS(&cond->cv, &lock->lock, INFINITE);
#else
	pthread_cond_wait(&cond->cv, &lock->lock);
#endif
	MT_thread_setcondwait(NULL);
}

void
MT_cond_signal(MT_Cond *cond)
{
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	WakeConditionVariable(&cond->cv);
#else
	pthread_cond_signal(&cond->cv);
#endif
}

void
MT_cond_broadcast(MT_Cond *cond)
{
#if !defined(HAVE_PTHREAD_H) && defined(WIN32)
	WakeAllConditionVariable(&cond->cv);
#else
	pthread_cond_broadcast(&cond->cv);
#endif
}
