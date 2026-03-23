/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
stack_copy(allocator *ma, MalStkPtr stk, int start)
{
	MalStkPtr n = newGlobalStack(ma, stk->stktop);
	ValPtr lhs, rhs;

	n->stktop = stk->stktop;
	n->blk = stk->blk;
	n->tag = stk->tag;
	n->memory = 0;

	for (int i = 0; i < stk->stktop; i++) {
		lhs = &n->stk[i];
		if (isVarConstant(stk->blk, i) > 0) {
			if (!isVarDisabled(stk->blk, i)) {
				rhs = &getVarConstant(stk->blk, i);
				if(VALcopy(ma, lhs, rhs) == NULL)
					break;
				if (rhs->bat && rhs->val.bval)
					BBPretain(rhs->val.bval);
			}
		} else {
			rhs = &stk->stk[i];
			if (/*getVarScope(stk->blk, i) < stk->calldepth ||*/ ((getVarDeclared(stk->blk, i) <= start && getVarEolife(stk->blk, i) > start)) || !rhs->vtype) {
				if(VALcopy(ma, lhs, rhs) == NULL)
					break;
			} else if (rhs->bat) {
                lhs->bat = rhs->bat;
                lhs->vtype = rhs->vtype;
                lhs->len = 0;
                lhs->val.bval = bat_nil;
			} else {
				VALinit(ma, lhs, rhs->vtype, ATOMnilptr(rhs->vtype));
			}
			if (lhs->bat && lhs->val.bval)
				BBPretain(lhs->val.bval);
		}
	}
	return n;
}

#ifdef CPU_ZERO
static void
thread_runoncpu(int cpu)
{
        cpu_set_t cpuset;

//printf("run on %d\n", cpu);
        CPU_ZERO(&cpuset);
        CPU_SET(cpu , &cpuset);
        sched_setaffinity(0, sizeof(cpuset), &cpuset);
}
#endif

static void
PIPELINEworker(void *T)
{
	struct worker *t = (struct worker *) T;
#ifdef _MSC_VER
	srand((unsigned int) GDKusec());
#endif
	Pipeline *p = (Pipeline*)GDKmalloc(sizeof(Pipeline));
#ifdef CPU_ZERO
	thread_runoncpu(t->self);
#endif
	if (p != NULL) {
		for (;;) {
			/* wait until we are allowed to start working */
			Pipelines *s = q_dequeue(t->q);

			if (!s || GDKexiting() || ATOMIC_GET(&exiting)) {
				break;
			}
			t->flag = RUNNING;

			allocator *ma = MT_thread_getallocator();
			allocator_state ma_state = ma_open(ma);
			MalStkPtr stk = stack_copy(ma, s->stk, s->start);

			MT_thread_set_qry_ctx(&s->cntxt->qryctx);

			*p = (Pipeline) {
				.p = s,
				.wid = (int) ATOMIC_INC(&s->workers),
				.seqnr = -1,
				.wls = NULL,
			};
			QryCtx *qc = MT_thread_get_qry_ctx();
			qc->wid = p->wid;
			stk->stk[s->mb->stmt[s->start]->argv[1]].val.ival = PIPELINEnext_counter(p);
			stk->stk[s->mb->stmt[s->start]->argv[2]].val.pval = p;
			/* the maxparts (arg 3) is generated ie constant value on the stack */
			str error = runMALsequence(s->cntxt, s->mb, s->start+1, s->stop, stk, 0, 0);
			PIPELINEclear_counter(p);
			if (error) {
				void *null = NULL;
				/* only collect one error (from one thread, needed for stable testing) */
				if (ATOMIC_PTR_CAS(&s->error, &null, error)) {
					strcpy(s->errbuf, error);
					ATOMIC_PTR_CAS(&s->error, (void**)&error, s->errbuf);
				}
			}
			freeStack(stk);
			ma_close(&ma_state);
			if (p->wls)
				GDKfree(p->wls);
			MT_sema_up(&s->s);
		}
		GDKfree(p);
	}
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
		snprintf(name, sizeof(name), "PIPELINEworker%d", i);
		if (MT_create_thread(&workers[i].id, PIPELINEworker, (void *) &workers[i], MT_THR_JOINABLE, name) < 0) {
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
runMALpipelines(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, int maxparts, bat sink, MalStkPtr stk)
{
	int restart = 0;
	if (!pipelines_initialized)
		PIPELINESinitialize();
	Pipelines *s = GDKmalloc(sizeof(Pipelines));
	if (!s)
		throw(MAL, "pipelines", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	bool profiler = cntxt->sqlprofiler;
	s->mb = mb;
	s->cntxt = cntxt;
	s->start = startpc;
	s->stop = stoppc;
	s->stk = stk;
	s->maxparts = maxparts;
	s->sink = sink;
	s->master_counter = 0;
	s->nr_workers = GDKnr_threads;
	s->status = 0;
	if (maxparts > 0)
		s->nr_workers = MIN(maxparts, GDKnr_threads);
	if (s->nr_workers > 1)
		cntxt->sqlprofiler = false;
	/* initialize with direct increment of all threads at once */
	ATOMIC_INIT(&s->workers, -1);
	ATOMIC_PTR_INIT(&s->error, NULL);
	s->errbuf = GDKgetbuf();

	char name[MT_NAME_LEN];
	snprintf(name, sizeof(name), "PIPELINE%d", cntxt->idx);
	MT_sema_init(&s->s, 0, name);
	MT_lock_init(&s->l, name);
	for (size_t i = 0; i < sizeof(s->counters) / sizeof(s->counters[0]); i++)
		s->counters[i] = -1;
	MT_cond_init(&s->cond, "pipeline-workers");
	/* somehow get number of workers from statement/barrier */
	for (int i = 0; i < s->nr_workers; i++)
		q_enqueue(workers[i].q, s);

	/* wait for result */
	for (int i = 0; i < s->nr_workers; i++)
		MT_sema_down(&s->s);
	MT_sema_destroy(&s->s);
	MT_lock_destroy(&s->l);
	bool has_sink = (s->sink != 0);
	str err = ATOMIC_PTR_GET(&s->error);
	if (err) {
		QryCtx *qc = MT_thread_get_qry_ctx();
		allocator *ma = qc->errorallocator;
		char *nerr = ma_copy(ma, err, strlen(err)+1);
		err[0] = 0;
		err = nerr;
		if (has_sink) {
			BAT *sb = BATdescriptor(s->sink);

			if (!sb) {
				err = createException(SQL, "language.pipeline", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			} else {
				Sink *sink = sb->tsink;
				sink->error = err;
				BBPreclaim(sb);
			}
		}
	}
	MT_cond_destroy(&s->cond);
	restart = (!err && s->status);
	if (!restart && profiler && s->nr_workers > 1) {
		lng clk = GDKusec();

		for(int i = s->start; i<s->stop; i++) {
			InstrPtr pci = mb->stmt[i];
			sqlProfilerEvent(cntxt, mb, stk, pci, clk, pci->ticks);
		}
	}
	GDKfree(s);
	cntxt->sqlprofiler = profiler;
	if (restart) /* TODO move into new loop around pipeline */
		return runMALpipelines(cntxt, mb, startpc, stoppc, maxparts, sink, stk);
	return has_sink ? NULL : err;
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
	int n = (int) p->p->master_counter++;
	p->p->counters[p->wid] = n;
	MT_cond_broadcast(&p->p->cond);
	MT_lock_unset(&p->p->l);
	return n;
}

void
PIPELINEclear_counter(Pipeline *p)
{
	MT_lock_set(&p->p->l);
	p->p->counters[p->wid] = -1;
	MT_cond_broadcast(&p->p->cond);
	MT_lock_unset(&p->p->l);
}
