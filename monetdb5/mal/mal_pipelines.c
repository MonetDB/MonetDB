/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_pipelines.h"
#include "mal_exception.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_runtime.h"
#include "mal_resource.h"
#include "mal_function.h"

#include "gdk_system.h"
#include "gdk_posix.h"

typedef struct queue {
	int size;	/* size of queue */
	int last;	/* last element in the queue */
	Pipelines **data;
	MT_Lock l;	/* it's a shared resource, ie we need locks */
	MT_Sema s;	/* threads wait on empty queues */
} Queue;

static struct worker {
	MT_Id id;
	enum {IDLE, WAITING, RUNNING, FREE, EXITED } flag;
	ATOMIC_PTR_TYPE cntxt;  /* client we do work for (NULL -> any) */
	char *errbuf;		    /* GDKerrbuf so that we can allocate before fork */
	Queue *q;				/* pipeline tasks to execute */
	int self;
} workers[THREADS];
static int pipelines_initialized = 0;

static ATOMIC_TYPE exiting = ATOMIC_VAR_INIT(0);
static MT_Lock pipelineLock = MT_LOCK_INITIALIZER(pipelineLock);
static void stopMALpipelines(void);


static Queue*
q_create(int sz, const char *name)
{
	Queue *q = (Queue*)GDKmalloc(sizeof(Queue));

	if (q == NULL)
		return NULL;
	*q = (Queue) {
		.size = ((sz << 1) >> 1), /* we want a multiple of 2 */
	};
	q->data = (Pipelines**) GDKmalloc(sizeof(Pipelines*) * q->size);
	if (q->data == NULL) {
		GDKfree(q);
		return NULL;
	}

	MT_lock_init(&q->l, name);
	MT_sema_init(&q->s, 0, name);
	return q;
}

static void
q_destroy(Queue *q)
{
	assert(q);
	MT_lock_destroy(&q->l);
	MT_sema_destroy(&q->s);
	GDKfree(q->data);
	GDKfree(q);
}

static void
q_enqueue_(Queue *q, Pipelines *d)
{
	assert(q);
	if (q->last == q->size) {
		q->size <<= 1;
		q->data = (Pipelines**) GDKrealloc(q->data, sizeof(Pipelines*) * q->size);
		assert(q->data);
	}
	q->data[q->last++] = d;
}
static void
q_enqueue(Queue *q, Pipelines *d)
{
	assert(q);
	MT_lock_set(&q->l);
	q_enqueue_(q, d);
	MT_lock_unset(&q->l);
	MT_sema_up(&q->s);
}

static Pipelines *
q_dequeue(Queue *q)
{
	Pipelines * r = NULL;

	assert(q);
	MT_sema_down(&q->s);
	if (ATOMIC_GET(&exiting))
		return NULL;
	MT_lock_set(&q->l);
	{
		int i = q->last - 1;

		r = q->data[i];
		assert (i >= 0);
		q->last--;
		memmove(q->data + i, q->data + i + 1, (q->last - i) * sizeof(q->data[0]));
	}
	MT_lock_unset(&q->l);
	return r;
}

void
mal_pipelines_reset(void)
{
	stopMALpipelines();
	for (int i = 0; i < THREADS; i++) {
		if (workers[i].q)
			q_destroy(workers[i].q);
	}
	memset((char*) workers, 0,  sizeof(workers));
	ATOMIC_SET(&exiting, 0);
}

static MalStkPtr
stack_copy(MalStkPtr stk, int start)
{
	MalStkPtr n = newGlobalStack(stk->stktop);
	ValPtr lhs, rhs;

	n->stktop = stk->stktop;
	n->blk = stk->blk;
	n->workers = 0;
	n->memory = 0;

	for (int i = 0; i < stk->stktop; i++) {
		lhs = &n->stk[i];
		if (isVarConstant(stk->blk, i) > 0) {
			if (!isVarDisabled(stk->blk, i)) {
				rhs = &getVarConstant(stk->blk, i);
				if(VALcopy(lhs, rhs) == NULL)
					break;
				if (rhs->vtype == TYPE_bat && rhs->val.bval)
					BBPretain(rhs->val.bval);
			}
		} else {
			rhs = &stk->stk[i];
			if ((getVarDeclared(stk->blk, i) <= start && getVarEolife(stk->blk, i) > start) || !rhs->vtype) {
				if(VALcopy(lhs, rhs) == NULL)
					break;
			} else {
				VALinit(lhs, rhs->vtype, ATOMnilptr(rhs->vtype));
			}
			if (rhs->vtype == TYPE_bat && rhs->val.bval)
				BBPretain(rhs->val.bval);
		}
	}
	return n;
}

// only call this while holding the lock
void
PIPELINEwait(Pipeline *p)
{
	Pipelines *pp = p->p;
	// we MUST be holding the lock at this point!
#ifdef HAVE_PTHREAD_H
	pthread_cond_wait(&pp->cond, &pp->l.lock);
#else
	MT_lock_unset(&pp->l);
	MT_sleep_ms(1);
	MT_lock_set(&pp->l);
#endif
	fprintf(stderr, "Iteration %d woke up\n", pp->counters[p->wid]);
}

// only call this while holding the lock
void
PIPELINEnotify(Pipeline *p, const char *msg)
{
	Pipelines *pp = p->p;

	fprintf(stderr, "iteration %d notifying receivers: %s\n", p->p->counters[p->wid], msg);
#ifdef HAVE_PTHREAD_H
	pthread_cond_broadcast(&pp->cond);
#endif
}

static void
PIPELINEworker(void *T)
{
	struct worker *t = (struct worker *) T;
#ifdef _MSC_VER
	srand((unsigned int) GDKusec());
#endif
	assert(t->errbuf != NULL);
	t->errbuf[0] = 0;
	GDKsetbuf(t->errbuf);		/* where to leave errors */
	t->errbuf = NULL;

	for (;;) {
		/* wait until we are allowed to start working */
		Pipelines *s = q_dequeue(t->q);
		Pipeline *p = (Pipeline*)GDKmalloc(sizeof(Pipeline));

		if (!s || !p || GDKexiting() || ATOMIC_GET(&exiting)) {
			if (p)
				GDKfree(p);
			break;
		}
		t->flag = RUNNING;

		MalStkPtr stk = stack_copy(s->stk, s->start);

		p->p = s;
		p->wid = (int) ATOMIC_INC(&s->workers);
		p->wls = NULL;
		stk->stk[s->mb->stmt[s->start]->argv[1]].val.ival = PIPELINEnext_counter(p);
		stk->stk[s->mb->stmt[s->start]->argv[2]].val.pval = p;
		/* the maxparts (arg 3) is generated ie constant value on the stack */
		str error = runMALsequence(s->cntxt, s->mb, s->start, s->stop, stk, 0, 0);
		PIPELINEclear_counter(p);
		if (error) {
			void *null = NULL;
			/* only collect one error (from one thread, needed for stable testing) */
			if (!ATOMIC_PTR_CAS(&s->error, &null, error))
				freeException(error);
			GDKerrbuf[0] = 0;
		}
		freeStack(stk);
		if (p->wls)
			GDKfree(p->wls);
		GDKfree(p);
		MT_sema_up(&s->s);
	}
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
}

static int
PIPELINESinitialize(void)
{
	int i;
	int created = 0;
	static bool first = true;

	MT_lock_set(&mal_contextLock);
	MT_lock_set(&pipelineLock);
	if (pipelines_initialized) {
		/* somebody else beat us to it */
		MT_lock_unset(&pipelineLock);
		MT_lock_unset(&mal_contextLock);
		return 0;
	}
	for (i = 0; i < GDKnr_threads; i++) {
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "PIPELINEsema%d", i);
		workers[i].flag = IDLE;
		workers[i].self = i;
		workers[i].q = q_create(256, name);
		if (first)				/* only initialize once */
			ATOMIC_PTR_INIT(&workers[i].cntxt, NULL);
		workers[i].errbuf = GDKmalloc(GDKMAXERRLEN);
		if (workers[i].errbuf == NULL) {
			TRC_CRITICAL(MAL_SERVER, "cannot allocate error buffer for worker");
			break;
		}
		snprintf(name, sizeof(name), "PIPELINEworker%d", i);
		if ((workers[i].id = THRcreate(PIPELINEworker, (void *) &workers[i], MT_THR_JOINABLE, name)) == 0) {
			GDKfree(workers[i].errbuf);
			workers[i].errbuf = NULL;
			workers[i].flag = IDLE;
		} else {
			created++;
		}
	}
	if (created)
		pipelines_initialized = 1;
	MT_lock_unset(&pipelineLock);
	MT_lock_unset(&mal_contextLock);
	if (created == 0) /* no threads created */
		return -1;
	return 0;
}

str
runMALpipelines(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, int maxparts, MalStkPtr stk)
{
	if (!pipelines_initialized)
		PIPELINESinitialize();
	Pipelines *s = GDKmalloc(sizeof(Pipelines));
	MalBlkPtr nmb = copyMalBlk(mb);
	if (!s || !nmb) {
		if (s)
			GDKfree(s);
		throw(MAL, "pipelines", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	s->mb = nmb;
	s->cntxt = cntxt;
	s->start = startpc;
	s->stop = stoppc;
	s->stk = stk;
	s->maxparts = maxparts;
	s->master_counter = 0;
	s->nr_workers = GDKnr_threads;
	ATOMIC_INIT(&s->workers, -1);
	ATOMIC_PTR_INIT(&s->error, NULL);

	s->mb = nmb;
	/* fix endless call of runMALpipelines but use as loop for parts */
	nmb->stmt[startpc]->fcn = NULL;
	nmb->stmt[startpc]->token = ASSIGNsymbol;
	getModuleId(nmb->stmt[startpc]) = NULL;
	getFunctionId(nmb->stmt[startpc]) = NULL;

	char name[MT_NAME_LEN];
	snprintf(name, sizeof(name), "PIPELINE%d", cntxt->idx);
	MT_sema_init(&s->s, 0, name);
	MT_lock_init(&s->l, name);
	for (size_t i = 0; i < sizeof(s->counters) / sizeof(s->counters[0]); i++)
		s->counters[i] = -1;
	if (pthread_cond_init(&s->cond, NULL) != 0)
		throw(MAL, "language.pipelines", "could not initialize condvar: %s",
			strerror_r(errno, (char[200]){0}, 200)
		);
	/* somehow get number of workers from statement/barrier */
	for (int i = 0; i < s->nr_workers; i++)
		q_enqueue(workers[i].q, s);

	//stk->stk[nmb->stmt[startpc]->argv[0]].val.btval = FALSE; /* end barrier */
	/* wait for result */
	for (int i = 0; i < s->nr_workers; i++)
		MT_sema_down(&s->s);
	MT_sema_destroy(&s->s);
	MT_lock_destroy(&s->l);
	str err = ATOMIC_PTR_GET(&s->error);
	freeMalBlk(s->mb);
	ATOMIC_DESTROY(&s->counter);
	ATOMIC_DESTROY(&s->workers);
	ATOMIC_PTR_DESTROY(&s->error);
#ifdef HAVE_PTHREAD_H
	pthread_cond_destroy(&s->cond);
#endif
	GDKfree(s);
	return err;
}

static void
stopMALpipelines(void)
{
	int i;

	ATOMIC_SET(&exiting, 1);
	MT_lock_set(&pipelineLock);

	/* first wake up all running threads */
	for (i = 0; i < THREADS; i++) {
		if (workers[i].flag == RUNNING)
			q_enqueue(workers[i].q, NULL);
	}
	for (i = 0; i < THREADS; i++) {
		if (workers[i].flag != IDLE) {
			MT_lock_unset(&pipelineLock);
			MT_join_thread(workers[i].id);
			MT_lock_set(&pipelineLock);
			workers[i].flag = IDLE;
		}
	}
	MT_lock_unset(&pipelineLock);
}

int
PIPELINEnext_counter(Pipeline *p)
{
	MT_lock_set(&p->p->l);
	PIPELINEnotify(p, "next counter");
	int n = (int) p->p->master_counter++;
	p->p->counters[p->wid] = n;
	MT_lock_unset(&p->p->l);
	return n;
}

void
PIPELINEclear_counter(Pipeline *p)
{
	MT_lock_set(&p->p->l);
	PIPELINEnotify(p, "clear counter");
	p->p->counters[p->wid] = -1;
	MT_lock_unset(&p->p->l);
}
