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
 * (author) M Kersten, S Mullender
 * Dataflow processing only works on a code
 * sequence that does not include additional (implicit) flow of control
 * statements and, ideally, consist of expensive BAT operations.
 * The dataflow portion is identified as a guarded block,
 * whose entry is controlled by the function language.dataflow();
 *
 * The dataflow worker tries to follow the sequence of actions
 * as layed out in the plan, but abandon this track when it hits
 * a blocking operator, or an instruction for which not all arguments
 * are available or resources become scarce.
 *
 * The flow graphs is organized such that parallel threads can
 * access it mostly without expensive locking and dependent
 * variables are easy to find..
 */
#include "monetdb_config.h"
#include "mal_dataflow.h"
#include "mal_exception.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_runtime.h"
#include "mal_resource.h"
#include "mal_function.h"

#define DFLOWpending 0			/* runnable */
#define DFLOWrunning 1			/* currently in progress */
#define DFLOWwrapup  2			/* done! */
#define DFLOWretry   3			/* reschedule */
#define DFLOWskipped 4			/* due to errors */

/* The per instruction status of execution */
typedef struct FLOWEVENT {
	struct DATAFLOW *flow;		/* execution context */
	int pc;						/* pc in underlying malblock */
	int blocks;					/* awaiting for variables */
	sht state;					/* of execution */
	lng clk;
	sht cost;
	lng hotclaim;				/* memory foot print of result variables */
	lng argclaim;				/* memory foot print of arguments */
	lng maxclaim;				/* memory foot print of largest argument, could be used to indicate result size */
	struct FLOWEVENT *next;		/* linked list for queues */
} *FlowEvent, FlowEventRec;

typedef struct queue {
	int exitcount;				/* how many threads should exit */
	FlowEvent first, last;		/* first and last element of the queue */
	MT_Lock l;					/* it's a shared resource, ie we need locks */
	MT_Sema s;					/* threads wait on empty queues */
} Queue;

/*
 * The dataflow dependency is administered in a graph list structure.
 * For each instruction we keep the list of instructions that
 * should be checked for eligibility once we are finished with it.
 */
typedef struct DATAFLOW {
	Client cntxt;				/* for debugging and client resolution */
	MalBlkPtr mb;				/* carry the context */
	MalStkPtr stk;
	int start, stop;			/* guarded block under consideration */
	FlowEvent status;			/* status of each instruction */
	ATOMIC_PTR_TYPE error;		/* error encountered */
	int *nodes;					/* dependency graph nodes */
	int *edges;					/* dependency graph */
	MT_Lock flowlock;			/* lock to protect the above */
	Queue *done;				/* instructions handled */
	bool set_qry_ctx;
} *DataFlow, DataFlowRec;

struct worker {
	MT_Id id;
	enum { WAITING, RUNNING, FREE, EXITED, FINISHING } flag;
	ATOMIC_PTR_TYPE cntxt;		/* client we do work for (NULL -> any) */
	MT_Sema s;
	struct worker *next;
	char errbuf[GDKMAXERRLEN];	/* GDKerrbuf so that we can allocate before fork */
};
/* heads of three mutually exclusive linked lists, all using the .next
 * field in the worker struct */
static struct worker *workers;		  /* "working" workers */
static struct worker *exited_workers; /* to be joined threads (.flag==EXITED) */
static struct worker *free_workers;	/* free workers (.flag==FREE) */
static int free_count = 0;		/* number of free threads */
static int free_max = 0;		/* max number of spare free threads */

static Queue *todo = 0;			/* pending instructions */

static ATOMIC_TYPE exiting = ATOMIC_VAR_INIT(0);
static MT_Lock dataflowLock = MT_LOCK_INITIALIZER(dataflowLock);

/*
 * Calculate the size of the dataflow dependency graph.
 */
static int
DFLOWgraphSize(MalBlkPtr mb, int start, int stop)
{
	int cnt = 0;

	for (int i = start; i < stop; i++)
		cnt += getInstrPtr(mb, i)->argc;
	return cnt;
}

/*
 * The dataflow execution is confined to a barrier block.
 * Within the block there are multiple flows, which, in principle,
 * can be executed in parallel.
 */

static Queue *
q_create(const char *name)
{
	Queue *q = GDKzalloc(sizeof(Queue));

	if (q == NULL)
		return NULL;
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
	GDKfree(q);
}

/* keep a simple LIFO queue. It won't be a large one, so shuffles of requeue is possible */
/* we might actually sort it for better scheduling behavior */
static void
q_enqueue(Queue *q, FlowEvent d)
{
	assert(q);
	assert(d);
	MT_lock_set(&q->l);
	if (q->first == NULL) {
		assert(q->last == NULL);
		q->first = q->last = d;
	} else {
		assert(q->last != NULL);
		q->last->next = d;
		q->last = d;
	}
	d->next = NULL;
	MT_lock_unset(&q->l);
	MT_sema_up(&q->s);
}

/*
 * A priority queue over the hot claims of memory may
 * be more effective. It priorizes those instructions
 * that want to use a big recent result
 */

static void
q_requeue(Queue *q, FlowEvent d)
{
	assert(q);
	assert(d);
	MT_lock_set(&q->l);
	if (q->first == NULL) {
		assert(q->last == NULL);
		q->first = q->last = d;
		d->next = NULL;
	} else {
		assert(q->last != NULL);
		d->next = q->first;
		q->first = d;
	}
	MT_lock_unset(&q->l);
	MT_sema_up(&q->s);
}

static FlowEvent
q_dequeue(Queue *q, Client cntxt)
{
	assert(q);
	MT_sema_down(&q->s);
	if (ATOMIC_GET(&exiting))
		return NULL;
	MT_lock_set(&q->l);
	if (cntxt == NULL && q->exitcount > 0) {
		q->exitcount--;
		MT_lock_unset(&q->l);
		return NULL;
	}

	FlowEvent *dp = &q->first;
	FlowEvent pd = NULL;
	/* if cntxt == NULL, return the first event, if cntxt != NULL, find
	 * the first event in the queue with matching cntxt value and return
	 * that */
	if (cntxt != NULL) {
		while (*dp && (*dp)->flow->cntxt != cntxt) {
			pd = *dp;
			dp = &pd->next;
		}
	}
	FlowEvent d = *dp;
	if (d) {
		*dp = d->next;
		d->next = NULL;
		if (*dp == NULL)
			q->last = pd;
	}
	MT_lock_unset(&q->l);
	return d;
}

/*
 * We simply move an instruction into the front of the queue.
 * Beware, we assume that variables are assigned a value once, otherwise
 * the order may really create errors.
 * The order of the instructions should be retained as long as possible.
 * Delay processing when we run out of memory.  Push the instruction back
 * on the end of queue, waiting for another attempt. Problem might become
 * that all threads but one are cycling through the queue, each time
 * finding an eligible instruction, but without enough space.
 * Therefore, we wait for a few milliseconds as an initial punishment.
 *
 * The process could be refined by checking for cheap operations,
 * i.e. those that would require no memory at all (aggr.count)
 * This, however, would lead to a dependency to the upper layers,
 * because in the kernel we don't know what routines are available
 * with this property. Nor do we maintain such properties.
 */

static void
DFLOWworker(void *T)
{
	struct worker *t = (struct worker *) T;
	bool locked = false;
#ifdef _MSC_VER
	srand((unsigned int) GDKusec());
#endif
	GDKsetbuf(t->errbuf);		/* where to leave errors */
	snprintf(t->s.name, sizeof(t->s.name), "DFLOWsema%04zu", MT_getpid());

	for (;;) {
		DataFlow flow;
		FlowEvent fe = 0, fnxt = 0;
		str error = 0;
		int i;
		lng claim;
		Client cntxt;
		InstrPtr p;

		GDKclrerr();

		if (t->flag == WAITING) {
			/* wait until we are allowed to start working */
			MT_sema_down(&t->s);
			t->flag = RUNNING;
			if (ATOMIC_GET(&exiting)) {
				break;
			}
		}
		assert(t->flag == RUNNING);
		cntxt = ATOMIC_PTR_GET(&t->cntxt);
		while (1) {
			MT_thread_set_qry_ctx(NULL);
			if (fnxt == 0) {
				MT_thread_setworking("waiting for work");
				cntxt = ATOMIC_PTR_GET(&t->cntxt);
				fe = q_dequeue(todo, cntxt);
				if (fe == NULL) {
					if (cntxt) {
						/* we're not done yet with work for the current
						 * client (as far as we know), so give up the CPU
						 * and let the scheduler enter some more work, but
						 * first compensate for the down we did in
						 * dequeue */
						MT_sema_up(&todo->s);
						MT_sleep_ms(1);
						continue;
					}
					/* no more work to be done: exit */
					break;
				}
				if (fe->flow->cntxt && fe->flow->cntxt->mythread)
					MT_thread_setworking(fe->flow->cntxt->mythread);
			} else
				fe = fnxt;
			if (ATOMIC_GET(&exiting)) {
				break;
			}
			fnxt = 0;
			assert(fe);
			flow = fe->flow;
			assert(flow);
			MT_thread_set_qry_ctx(flow->set_qry_ctx ? &flow->cntxt->
								  qryctx : NULL);

			/* whenever we have a (concurrent) error, skip it */
			if (ATOMIC_PTR_GET(&flow->error)) {
				q_enqueue(flow->done, fe);
				continue;
			}

			p = getInstrPtr(flow->mb, fe->pc);
			claim = fe->argclaim;
			if (p->fcn != (MALfcn) deblockdataflow &&	/* never block on deblockdataflow() */
				!MALadmission_claim(flow->cntxt, flow->mb, flow->stk, p, claim)) {
				fe->hotclaim = 0;	/* don't assume priority anymore */
				fe->maxclaim = 0;
				MT_lock_set(&todo->l);
				FlowEvent last = todo->last;
				MT_lock_unset(&todo->l);
				if (last == NULL)
					MT_sleep_ms(DELAYUNIT);
				q_requeue(todo, fe);
				continue;
			}
			ATOMIC_BASE_TYPE wrks = ATOMIC_INC(&flow->cntxt->workers);
			ATOMIC_BASE_TYPE mwrks = ATOMIC_GET(&flow->mb->workers);
			while (wrks > mwrks) {
				if (ATOMIC_CAS(&flow->mb->workers, &mwrks, wrks))
					break;
			}
			error = runMALsequence(flow->cntxt, flow->mb, fe->pc, fe->pc + 1,
								   flow->stk, 0, 0);
			ATOMIC_DEC(&flow->cntxt->workers);
			/* release the memory claim */
			MALadmission_release(flow->cntxt, flow->mb, flow->stk, p, claim);

			MT_lock_set(&flow->flowlock);
			fe->state = DFLOWwrapup;
			MT_lock_unset(&flow->flowlock);
			if (error) {
				void *null = NULL;
				/* only collect one error (from one thread, needed for stable testing) */
				if (!ATOMIC_PTR_CAS(&flow->error, &null, error))
					freeException(error);
				/* after an error we skip the rest of the block */
				q_enqueue(flow->done, fe);
				continue;
			}

			/* see if you can find an eligible instruction that uses the
			 * result just produced. Then we can continue with it right away.
			 * We are just looking forward for the last block, which means we
			 * are safe from concurrent actions. No other thread can steal it,
			 * because we hold the logical lock.
			 * All eligible instructions are queued
			 */
			p = getInstrPtr(flow->mb, fe->pc);
			assert(p);
			fe->hotclaim = 0;
			fe->maxclaim = 0;

			for (i = 0; i < p->retc; i++) {
				lng footprint;
				footprint = getMemoryClaim(flow->mb, flow->stk, p, i, FALSE);
				fe->hotclaim += footprint;
				if (footprint > fe->maxclaim)
					fe->maxclaim = footprint;
			}

/* Try to get rid of the hot potato or locate an alternative to proceed.
 */
#define HOTPOTATO
#ifdef HOTPOTATO
			/* HOT potato choice */
			int last = 0, nxt = -1;
			lng nxtclaim = -1;

			MT_lock_set(&flow->flowlock);
			for (last = fe->pc - flow->start;
				 last >= 0 && (i = flow->nodes[last]) > 0;
				 last = flow->edges[last]) {
				if (flow->status[i].state == DFLOWpending
					&& flow->status[i].blocks == 1) {
					/* find the one with the largest footprint */
					if (nxt == -1 || flow->status[i].argclaim > nxtclaim) {
						nxt = i;
						nxtclaim = flow->status[i].argclaim;
					}
				}
			}
			/* hot potato can not be removed, use alternative to proceed */
			if (nxt >= 0) {
				flow->status[nxt].state = DFLOWrunning;
				flow->status[nxt].blocks = 0;
				flow->status[nxt].hotclaim = fe->hotclaim;
				flow->status[nxt].argclaim += fe->hotclaim;
				if (flow->status[nxt].maxclaim < fe->maxclaim)
					flow->status[nxt].maxclaim = fe->maxclaim;
				fnxt = flow->status + nxt;
			}
			MT_lock_unset(&flow->flowlock);
#endif

			q_enqueue(flow->done, fe);
			if (fnxt == 0 && profilerStatus) {
				profilerHeartbeatEvent("wait");
			}
		}
		MT_lock_set(&dataflowLock);
		if (GDKexiting() || ATOMIC_GET(&exiting) || free_count >= free_max) {
			locked = true;
			break;
		}
		free_count++;
		struct worker **tp = &workers;
		while (*tp && *tp != t)
			tp = &(*tp)->next;
		assert(*tp && *tp == t);
		*tp = t->next;
		t->flag = FREE;
		t->next = free_workers;
		free_workers = t;
		MT_lock_unset(&dataflowLock);
		MT_thread_setworking("idle, waiting for new client");
		MT_sema_down(&t->s);
		if (GDKexiting() || ATOMIC_GET(&exiting))
			break;
		assert(t->flag == WAITING);
	}
	if (!locked)
		MT_lock_set(&dataflowLock);
	if (t->flag != FINISHING) {
		struct worker **tp = t->flag == FREE ? &free_workers : &workers;
		while (*tp && *tp != t)
			tp = &(*tp)->next;
		assert(*tp && *tp == t);
		*tp = t->next;
		t->flag = EXITED;
		t->next = exited_workers;
		exited_workers = t;
	}
	MT_lock_unset(&dataflowLock);
	GDKsetbuf(NULL);
}

/*
 * Create an interpreter pool.
 * One worker will adaptively be available for each client.
 * The remainder are taken from the GDKnr_threads argument and
 * typically is equal to the number of cores
 * The workers are assembled in a local table to enable debugging.
 */
static int
DFLOWinitialize(void)
{
	int limit;
	int created = 0;

	MT_lock_set(&mal_contextLock);
	MT_lock_set(&dataflowLock);
	if (todo) {
		/* somebody else beat us to it */
		MT_lock_unset(&dataflowLock);
		MT_lock_unset(&mal_contextLock);
		return 0;
	}
	free_max = GDKgetenv_int("dataflow_max_free",
							 GDKnr_threads < 4 ? 4 : GDKnr_threads);
	todo = q_create("todo");
	if (todo == NULL) {
		MT_lock_unset(&dataflowLock);
		MT_lock_unset(&mal_contextLock);
		return -1;
	}
	limit = GDKnr_threads ? GDKnr_threads - 1 : 0;
	while (limit > 0) {
		limit--;
		struct worker *t = GDKmalloc(sizeof(*t));
		if (t == NULL) {
			TRC_CRITICAL(MAL_SERVER, "cannot allocate structure for worker");
			continue;
		}
		*t = (struct worker) {
			.flag = RUNNING,
		};
		ATOMIC_PTR_INIT(&t->cntxt, NULL);
		MT_sema_init(&t->s, 0, "DFLOWsema"); /* placeholder name */
		if (MT_create_thread(&t->id, DFLOWworker, t,
							 MT_THR_JOINABLE, "DFLOWworkerXXXX") < 0) {
			ATOMIC_PTR_DESTROY(&t->cntxt);
			MT_sema_destroy(&t->s);
			GDKfree(t);
		} else {
			t->next = workers;
			workers = t;
			created++;
		}
	}
	if (created == 0) {
		/* no threads created */
		q_destroy(todo);
		todo = NULL;
		MT_lock_unset(&dataflowLock);
		MT_lock_unset(&mal_contextLock);
		return -1;
	}
	MT_lock_unset(&dataflowLock);
	MT_lock_unset(&mal_contextLock);
	return 0;
}

/*
 * The dataflow administration is based on administration of
 * how many variables are still missing before it can be executed.
 * For each instruction we keep a list of instructions whose
 * blocking counter should be decremented upon finishing it.
 */
static str
DFLOWinitBlk(DataFlow flow, MalBlkPtr mb, int size)
{
	int pc, i, j, k, l, n, etop = 0;
	int *assign;
	InstrPtr p;

	if (flow == NULL)
		throw(MAL, "dataflow", "DFLOWinitBlk(): Called with flow == NULL");
	if (mb == NULL)
		throw(MAL, "dataflow", "DFLOWinitBlk(): Called with mb == NULL");
	assign = (int *) GDKzalloc(mb->vtop * sizeof(int));
	if (assign == NULL)
		throw(MAL, "dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	etop = flow->stop - flow->start;
	for (n = 0, pc = flow->start; pc < flow->stop; pc++, n++) {
		p = getInstrPtr(mb, pc);
		if (p == NULL) {
			GDKfree(assign);
			throw(MAL, "dataflow",
				  "DFLOWinitBlk(): getInstrPtr() returned NULL");
		}

		/* initial state, ie everything can run */
		flow->status[n].flow = flow;
		flow->status[n].pc = pc;
		flow->status[n].state = DFLOWpending;
		flow->status[n].cost = -1;
		ATOMIC_PTR_SET(&flow->status[n].flow->error, NULL);

		/* administer flow dependencies */
		for (j = p->retc; j < p->argc; j++) {
			/* list of instructions that wake n-th instruction up */
			if (!isVarConstant(mb, getArg(p, j)) && (k = assign[getArg(p, j)])) {
				assert(k < pc);	/* only dependencies on earlier instructions */
				/* add edge to the target instruction for wakeup call */
				k -= flow->start;
				if (flow->nodes[k]) {
					/* add wakeup to tail of list */
					for (i = k; flow->edges[i] > 0; i = flow->edges[i])
						;
					flow->nodes[etop] = n;
					flow->edges[etop] = -1;
					flow->edges[i] = etop;
					etop++;
					(void) size;
					if (etop == size) {
						int *tmp;
						/* in case of realloc failure, the original
						 * pointers will be freed by the caller */
						tmp = (int *) GDKrealloc(flow->nodes,
												 sizeof(int) * 2 * size);
						if (tmp == NULL) {
							GDKfree(assign);
							throw(MAL, "dataflow",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						}
						flow->nodes = tmp;
						tmp = (int *) GDKrealloc(flow->edges,
												 sizeof(int) * 2 * size);
						if (tmp == NULL) {
							GDKfree(assign);
							throw(MAL, "dataflow",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						}
						flow->edges = tmp;
						size *= 2;
					}
				} else {
					flow->nodes[k] = n;
					flow->edges[k] = -1;
				}

				flow->status[n].blocks++;
			}

			/* list of instructions to be woken up explicitly */
			if (!isVarConstant(mb, getArg(p, j))) {
				/* be careful, watch out for garbage collection interference */
				/* those should be scheduled after all its other uses */
				l = getEndScope(mb, getArg(p, j));
				if (l != pc && l < flow->stop && l > flow->start) {
					/* add edge to the target instruction for wakeup call */
					assert(pc < l);	/* only dependencies on earlier instructions */
					l -= flow->start;
					if (flow->nodes[n]) {
						/* add wakeup to tail of list */
						for (i = n; flow->edges[i] > 0; i = flow->edges[i])
							;
						flow->nodes[etop] = l;
						flow->edges[etop] = -1;
						flow->edges[i] = etop;
						etop++;
						if (etop == size) {
							int *tmp;
							/* in case of realloc failure, the original
							 * pointers will be freed by the caller */
							tmp = (int *) GDKrealloc(flow->nodes,
													 sizeof(int) * 2 * size);
							if (tmp == NULL) {
								GDKfree(assign);
								throw(MAL, "dataflow",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
							}
							flow->nodes = tmp;
							tmp = (int *) GDKrealloc(flow->edges,
													 sizeof(int) * 2 * size);
							if (tmp == NULL) {
								GDKfree(assign);
								throw(MAL, "dataflow",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
							}
							flow->edges = tmp;
							size *= 2;
						}
					} else {
						flow->nodes[n] = l;
						flow->edges[n] = -1;
					}
					flow->status[l].blocks++;
				}
			}
		}

		for (j = 0; j < p->retc; j++)
			assign[getArg(p, j)] = pc;	/* ensure recognition of dependency on first instruction and constant */
	}
	GDKfree(assign);

	return MAL_SUCCEED;
}

/*
 * Parallel processing is mostly driven by dataflow, but within this context
 * there may be different schemes to take instructions into execution.
 * The admission scheme (and wrapup) are the necessary scheduler hooks.
 * A scheduler registers the functions needed and should release them
 * at the end of the parallel block.
 * They take effect after we have ensured that the basic properties for
 * execution hold.
 */
static str
DFLOWscheduler(DataFlow flow, struct worker *w)
{
	int last;
	int i;
	int j;
	InstrPtr p;
	int tasks = 0, actions = 0;
	str ret = MAL_SUCCEED;
	FlowEvent fe, f = 0;

	if (flow == NULL)
		throw(MAL, "dataflow", "DFLOWscheduler(): Called with flow == NULL");
	actions = flow->stop - flow->start;
	if (actions == 0)
		throw(MAL, "dataflow", "Empty dataflow block");
	/* initialize the eligible statements */
	fe = flow->status;

	ATOMIC_DEC(&flow->cntxt->workers);
	MT_lock_set(&flow->flowlock);
	for (i = 0; i < actions; i++)
		if (fe[i].blocks == 0) {
			p = getInstrPtr(flow->mb, fe[i].pc);
			if (p == NULL) {
				MT_lock_unset(&flow->flowlock);
				ATOMIC_INC(&flow->cntxt->workers);
				throw(MAL, "dataflow",
					  "DFLOWscheduler(): getInstrPtr(flow->mb,fe[i].pc) returned NULL");
			}
			fe[i].argclaim = 0;
			for (j = p->retc; j < p->argc; j++)
				fe[i].argclaim += getMemoryClaim(fe[0].flow->mb,
												 fe[0].flow->stk, p, j, FALSE);
			flow->status[i].state = DFLOWrunning;
			q_enqueue(todo, flow->status + i);
		}
	MT_lock_unset(&flow->flowlock);
	MT_sema_up(&w->s);

	while (actions != tasks) {
		f = q_dequeue(flow->done, NULL);
		if (ATOMIC_GET(&exiting))
			break;
		if (f == NULL) {
			ATOMIC_INC(&flow->cntxt->workers);
			throw(MAL, "dataflow",
				  "DFLOWscheduler(): q_dequeue(flow->done) returned NULL");
		}

		/*
		 * When an instruction is finished we have to reduce the blocked
		 * counter for all dependent instructions.  for those where it
		 * drops to zero we can scheduler it we do it here instead of the scheduler
		 */

		MT_lock_set(&flow->flowlock);
		tasks++;
		for (last = f->pc - flow->start;
			 last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending) {
				flow->status[i].argclaim += f->hotclaim;
				if (flow->status[i].blocks == 1) {
					flow->status[i].blocks--;
					flow->status[i].state = DFLOWrunning;
					q_enqueue(todo, flow->status + i);
				} else {
					flow->status[i].blocks--;
				}
			}
		MT_lock_unset(&flow->flowlock);
	}
	/* release the worker from its specific task (turn it into a
	 * generic worker) */
	ATOMIC_PTR_SET(&w->cntxt, NULL);
	ATOMIC_INC(&flow->cntxt->workers);
	/* wrap up errors */
	assert(flow->done->last == 0);
	if ((ret = ATOMIC_PTR_XCG(&flow->error, NULL)) != NULL) {
		TRC_DEBUG(MAL_SERVER, "Errors encountered: %s\n", ret);
	}
	return ret;
}

/* called and returns with dataflowLock locked, temporarily unlocks
 * join the thread associated with the worker and destroy the structure */
static inline void
finish_worker(struct worker *t)
{
	t->flag = FINISHING;
	MT_lock_unset(&dataflowLock);
	MT_join_thread(t->id);
	MT_sema_destroy(&t->s);
	ATOMIC_PTR_DESTROY(&t->cntxt);
	GDKfree(t);
	MT_lock_set(&dataflowLock);
}

/* We create a pool of GDKnr_threads-1 generic workers, that is,
 * workers that will take on jobs from any clients.  In addition, we
 * create a single specific worker per client (i.e. each time we enter
 * here).  This specific worker will only do work for the client for
 * which it was started.  In this way we can guarantee that there will
 * always be progress for the client, even if all other workers are
 * doing something big.
 *
 * When all jobs for a client have been done (there are no more
 * entries for the client in the queue), the specific worker turns
 * itself into a generic worker.  At the same time, we signal that one
 * generic worker should exit and this function returns.  In this way
 * we make sure that there are once again GDKnr_threads-1 generic
 * workers. */
str
runMALdataflow(Client cntxt, MalBlkPtr mb, int startpc, int stoppc,
			   MalStkPtr stk)
{
	DataFlow flow = NULL;
	str msg = MAL_SUCCEED;
	int size;
	bit *ret;
	struct worker *t;

	if (stk == NULL)
		throw(MAL, "dataflow", "runMALdataflow(): Called with stk == NULL");
	ret = getArgReference_bit(stk, getInstrPtr(mb, startpc), 0);
	*ret = FALSE;

	assert(stoppc > startpc);

	/* check existence of workers */
	if (todo == NULL) {
		/* create thread pool */
		if (GDKnr_threads <= 1 || DFLOWinitialize() < 0) {
			/* no threads created, run serially */
			*ret = TRUE;
			return MAL_SUCCEED;
		}
	}
	assert(todo);
	/* in addition, create one more worker that will only execute
	 * tasks for the current client to compensate for our waiting
	 * until all work is done */
	MT_lock_set(&dataflowLock);
	/* join with already exited threads */
	while (exited_workers != NULL) {
		assert(exited_workers->flag == EXITED);
		struct worker *t = exited_workers;
		exited_workers = exited_workers->next;
		finish_worker(t);
	}
	assert(cntxt != NULL);
	if (free_workers != NULL) {
		t = free_workers;
		assert(t->flag == FREE);
		assert(free_count > 0);
		free_count--;
		free_workers = t->next;
		t->next = workers;
		workers = t;
		t->flag = WAITING;
		ATOMIC_PTR_SET(&t->cntxt, cntxt);
		MT_sema_up(&t->s);
	} else {
		t = GDKmalloc(sizeof(*t));
		if (t != NULL) {
			*t = (struct worker) {
				.flag = WAITING,
			};
			ATOMIC_PTR_INIT(&t->cntxt, cntxt);
			MT_sema_init(&t->s, 0, "DFLOWsema"); /* placeholder name */
			if (MT_create_thread(&t->id, DFLOWworker, t,
								 MT_THR_JOINABLE, "DFLOWworkerXXXX") < 0) {
				ATOMIC_PTR_DESTROY(&t->cntxt);
				MT_sema_destroy(&t->s);
				GDKfree(t);
				t = NULL;
			} else {
				t->next = workers;
				workers = t;
			}
		}
		if (t == NULL) {
			/* cannot start new thread, run serially */
			*ret = TRUE;
			MT_lock_unset(&dataflowLock);
			return MAL_SUCCEED;
		}
	}
	MT_lock_unset(&dataflowLock);

	flow = (DataFlow) GDKzalloc(sizeof(DataFlowRec));
	if (flow == NULL)
		throw(MAL, "dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	flow->cntxt = cntxt;
	flow->mb = mb;
	flow->stk = stk;
	flow->set_qry_ctx = MT_thread_get_qry_ctx() != NULL;

	/* keep real block count, exclude brackets */
	flow->start = startpc + 1;
	flow->stop = stoppc;

	flow->done = q_create("flow->done");
	if (flow->done == NULL) {
		GDKfree(flow);
		throw(MAL, "dataflow",
			  "runMALdataflow(): Failed to create flow->done queue");
	}

	flow->status = (FlowEvent) GDKzalloc((stoppc - startpc + 1) *
										 sizeof(FlowEventRec));
	if (flow->status == NULL) {
		q_destroy(flow->done);
		GDKfree(flow);
		throw(MAL, "dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	size = DFLOWgraphSize(mb, startpc, stoppc);
	size += stoppc - startpc;
	flow->nodes = (int *) GDKzalloc(sizeof(int) * size);
	if (flow->nodes == NULL) {
		GDKfree(flow->status);
		q_destroy(flow->done);
		GDKfree(flow);
		throw(MAL, "dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	flow->edges = (int *) GDKzalloc(sizeof(int) * size);
	if (flow->edges == NULL) {
		GDKfree(flow->nodes);
		GDKfree(flow->status);
		q_destroy(flow->done);
		GDKfree(flow);
		throw(MAL, "dataflow", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	MT_lock_init(&flow->flowlock, "flow->flowlock");
	ATOMIC_PTR_INIT(&flow->error, NULL);
	msg = DFLOWinitBlk(flow, mb, size);

	if (msg == MAL_SUCCEED)
		msg = DFLOWscheduler(flow, t);

	GDKfree(flow->status);
	GDKfree(flow->edges);
	GDKfree(flow->nodes);
	q_destroy(flow->done);
	MT_lock_destroy(&flow->flowlock);
	ATOMIC_PTR_DESTROY(&flow->error);
	GDKfree(flow);

	/* we created one worker, now tell one worker to exit again */
	MT_lock_set(&todo->l);
	todo->exitcount++;
	MT_lock_unset(&todo->l);
	MT_sema_up(&todo->s);

	return msg;
}

str
deblockdataflow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = getArgReference_int(stk, pci, 0);
	int *val = getArgReference_int(stk, pci, 1);
	(void) cntxt;
	(void) mb;
	*ret = *val;
	return MAL_SUCCEED;
}

static void
stopMALdataflow(void)
{
	ATOMIC_SET(&exiting, 1);
	if (todo) {
		MT_lock_set(&dataflowLock);
		/* first wake up all running threads */
		int n = 0;
		for (struct worker *t = free_workers; t; t = t->next)
			n++;
		for (struct worker *t = workers; t; t = t->next)
			n++;
		while (n-- > 0) {
			/* one UP for each thread we know about */
			MT_sema_up(&todo->s);
		}
		while (free_workers) {
			struct worker *t = free_workers;
			assert(free_count > 0);
			free_count--;
			free_workers = free_workers->next;
			MT_sema_up(&t->s);
			finish_worker(t);
		}
		while (workers) {
			struct worker *t = workers;
			workers = workers->next;
			finish_worker(t);
		}
		while (exited_workers) {
			struct worker *t = exited_workers;
			exited_workers = exited_workers->next;
			finish_worker(t);
		}
		MT_lock_unset(&dataflowLock);
	}
}

void
mal_dataflow_reset(void)
{
	stopMALdataflow();
	workers = exited_workers = NULL;
	if (todo) {
		MT_lock_destroy(&todo->l);
		MT_sema_destroy(&todo->s);
		GDKfree(todo);
	}
	todo = 0;					/* pending instructions */
	ATOMIC_SET(&exiting, 0);
}
