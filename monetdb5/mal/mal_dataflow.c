/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) M Kersten
 * Out of order execution
 * The alternative is to execute the instructions out of order
 * using dataflow dependencies and as an independent process.
 * Dataflow processing only works on a code
 * sequence that does not include additional (implicit) flow of control
 * statements and, ideally, consist of expensive BAT operations.
 * The dataflow interpreter selects cheap instructions
 * using a simple costfunction based on the size of the BATs involved.
 *
 * The dataflow portion is identified as a guarded block,
 * whose entry is controlled by the function language.dataflow();
 * This way the function can inform the caller to skip the block
 * when dataflow execution was performed.
 *
 * The flow graphs should be organized such that parallel threads can
 * access it mostly without expensive locking.
 */
#include "monetdb_config.h"
#include "mal_dataflow.h"
#include "mal_private.h"
#include "mal_runtime.h"
#include "mal_resource.h"

#define DFLOWpending 0		/* runnable */
#define DFLOWrunning 1		/* currently in progress */
#define DFLOWwrapup  2		/* done! */
#define DFLOWretry   3		/* reschedule */
#define DFLOWskipped 4		/* due to errors */

/* The per instruction status of execution */
typedef struct FLOWEVENT {
	struct DATAFLOW *flow;/* execution context */
	int pc;         /* pc in underlying malblock */
	int blocks;     /* awaiting for variables */
	sht state;      /* of execution */
	lng clk;
	sht cost;
	lng hotclaim;   /* memory foot print of result variables */
	lng argclaim;   /* memory foot print of arguments */
	lng maxclaim;   /* memory foot print of  largest argument, counld be used to indicate result size */
} *FlowEvent, FlowEventRec;

typedef struct queue {
	int size;	/* size of queue */
	int last;	/* last element in the queue */
	int exitcount;	/* how many threads should exit */
	FlowEvent *data;
	MT_Lock l;	/* it's a shared resource, ie we need locks */
	MT_Sema s;	/* threads wait on empty queues */
} Queue;

/*
 * The dataflow dependency is administered in a graph list structure.
 * For each instruction we keep the list of instructions that
 * should be checked for eligibility once we are finished with it.
 */
typedef struct DATAFLOW {
	Client cntxt;   /* for debugging and client resolution */
	MalBlkPtr mb;   /* carry the context */
	MalStkPtr stk;
	int start, stop;    /* guarded block under consideration*/
	FlowEvent status;   /* status of each instruction */
	str error;          /* error encountered */
	int *nodes;         /* dependency graph nodes */
	int *edges;         /* dependency graph */
	MT_Lock flowlock;   /* lock to protect the above */
	Queue *done;        /* instructions handled */
} *DataFlow, DataFlowRec;

static struct worker {
	MT_Id id;
	enum {IDLE, RUNNING, JOINING, EXITED} flag;
	Client cntxt;				/* client we do work for (NULL -> any) */
	MT_Sema s;
} workers[THREADS];

static Queue *todo = 0;	/* pending instructions */

#ifdef ATOMIC_LOCK
static MT_Lock exitingLock MT_LOCK_INITIALIZER("exitingLock");
#endif
static volatile ATOMIC_TYPE exiting = 0;
static MT_Lock dataflowLock MT_LOCK_INITIALIZER("dataflowLock");

void
mal_dataflow_reset(void)
{
	stopMALdataflow();
	memset((char*) workers, 0,  sizeof(workers));
	if( todo) {
		GDKfree(todo->data);
		MT_lock_destroy(&todo->l);
		MT_sema_destroy(&todo->s);
		GDKfree(todo);
	}
	todo = 0;	/* pending instructions */
	exiting = 0;
}

/*
 * Calculate the size of the dataflow dependency graph.
 */
static int
DFLOWgraphSize(MalBlkPtr mb, int start, int stop)
{
	int cnt = 0;
	int i;

	for (i = start; i < stop; i++)
		cnt += getInstrPtr(mb, i)->argc;
	return cnt;
}

/*
 * The dataflow execution is confined to a barrier block.
 * Within the block there are multiple flows, which, in principle,
 * can be executed in parallel.
 */

static Queue*
q_create(int sz, const char *name)
{
	Queue *q = (Queue*)GDKmalloc(sizeof(Queue));

	if (q == NULL)
		return NULL;
	q->size = ((sz << 1) >> 1); /* we want a multiple of 2 */
	q->last = 0;
	q->data = (FlowEvent*) GDKmalloc(sizeof(FlowEvent) * q->size);
	if (q->data == NULL) {
		GDKfree(q);
		return NULL;
	}
	q->exitcount = 0;

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

/* keep a simple LIFO queue. It won't be a large one, so shuffles of requeue is possible */
/* we might actually sort it for better scheduling behavior */
static void
q_enqueue_(Queue *q, FlowEvent d)
{
	assert(q);
	assert(d);
	if (q->last == q->size) {
		q->size <<= 1;
		q->data = (FlowEvent*) GDKrealloc(q->data, sizeof(FlowEvent) * q->size);
		assert(q->data);
	}
	q->data[q->last++] = d;
}
static void
q_enqueue(Queue *q, FlowEvent d)
{
	assert(q);
	assert(d);
	MT_lock_set(&q->l);
	q_enqueue_(q, d);
	MT_lock_unset(&q->l);
	MT_sema_up(&q->s);
}

/*
 * A priority queue over the hot claims of memory may
 * be more effective. It priorizes those instructions
 * that want to use a big recent result
 */

#ifdef USE_MAL_ADMISSION
static void
q_requeue_(Queue *q, FlowEvent d)
{
	int i;

	assert(q);
	assert(d);
	if (q->last == q->size) {
		/* enlarge buffer */
		q->size <<= 1;
		q->data = (FlowEvent*) GDKrealloc(q->data, sizeof(FlowEvent) * q->size);
		assert(q->data);
	}
	for (i = q->last; i > 0; i--)
		q->data[i] = q->data[i - 1];
	q->data[0] = d;
	q->last++;
}
static void
q_requeue(Queue *q, FlowEvent d)
{
	assert(q);
	assert(d);
	MT_lock_set(&q->l);
	q_requeue_(q, d);
	MT_lock_unset(&q->l);
	MT_sema_up(&q->s);
}
#endif

static FlowEvent
q_dequeue(Queue *q, Client cntxt)
{
	FlowEvent r = NULL, s = NULL;
	//int i;

	assert(q);
	MT_sema_down(&q->s);
	if (ATOMIC_GET(exiting, exitingLock))
		return NULL;
	MT_lock_set(&q->l);
	if (cntxt) {
		int i, minpc = -1;

		for (i = q->last - 1; i >= 0; i--) {
			if (q->data[i]->flow->cntxt == cntxt) {
				if(minpc < 0){
					minpc = i;
					s = q->data[i];
				}
				r = q->data[i];
				if( s && r && s->pc > r->pc){
					minpc = i;
					s = r;
				}
			}
		}
		if( minpc >= 0){
			r = q->data[minpc];
			i = minpc;
			q->last--;
			while (i < q->last) {
				q->data[i] = q->data[i + 1];
				i++;
			}
		} else r = NULL;

		MT_lock_unset(&q->l);
		return r;
	}
	if (q->exitcount > 0) {
		q->exitcount--;
		MT_lock_unset(&q->l);
		return NULL;
	}
	assert(q->last > 0);
	if (q->last > 0) {
		/* LIFO favors garbage collection */
		r = q->data[--q->last];
/*  Line coverage test shows it is an expensive loop that is hardly ever leads to adjustment
		for(i= q->last-1; r &&  i>=0; i--){
			s= q->data[i];
			if( s && s->flow && s->flow->stk &&
			    r && r->flow && r->flow->stk &&
			    s->flow->stk->tag < r->flow->stk->tag){
				q->data[i]= r;
				r = s;
			}
		}
*/
		q->data[q->last] = 0;
	}
	/* else: terminating */
	/* try out random draw *
	{
		int i;
		i = rand() % q->last;
		r = q->data[i];
		for (i++; i < q->last; i++)
			q->data[i - 1] = q->data[i];
		q->last--; i
	}
	 */

	MT_lock_unset(&q->l);
	assert(r);
	return r;
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
	DataFlow flow;
	FlowEvent fe = 0, fnxt = 0;
	int id = (int) (t - workers);
	Thread thr;
	str error = 0;
	int i,last;
	Client cntxt;
	InstrPtr p;

	thr = THRnew("DFLOWworker");

#ifdef _MSC_VER
	srand((unsigned int) GDKusec());
#endif
	GDKsetbuf(GDKmalloc(GDKMAXERRLEN)); /* where to leave errors */
	GDKerrbuf[0] = 0;
	MT_lock_set(&dataflowLock);
	cntxt = t->cntxt;
	MT_lock_unset(&dataflowLock);
	if (cntxt) {
		/* wait until we are allowed to start working */
		MT_sema_down(&t->s);
	}
	while (1) {
		if (fnxt == 0) {
			MT_lock_set(&dataflowLock);
			cntxt = t->cntxt;
			MT_lock_unset(&dataflowLock);
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
		} else
			fe = fnxt;
		if (ATOMIC_GET(exiting, exitingLock)) {
			break;
		}
		fnxt = 0;
		assert(fe);
		flow = fe->flow;
		assert(flow);

		/* whenever we have a (concurrent) error, skip it */
		MT_lock_set(&flow->flowlock);
		if (flow->error) {
			MT_lock_unset(&flow->flowlock);
			q_enqueue(flow->done, fe);
			continue;
		}
		MT_lock_unset(&flow->flowlock);

#ifdef USE_MAL_ADMISSION
		if (MALrunningThreads() > 2 && MALadmission(fe->argclaim, fe->hotclaim)) {
			// never block on deblockdataflow()
			p= getInstrPtr(flow->mb,fe->pc);
			if( p->fcn != (MALfcn) deblockdataflow){
				fe->hotclaim = 0;   /* don't assume priority anymore */
				fe->maxclaim = 0;
				if (todo->last == 0)
					MT_sleep_ms(DELAYUNIT);
				q_requeue(todo, fe);
				continue;
			}
		}
#endif
		error = runMALsequence(flow->cntxt, flow->mb, fe->pc, fe->pc + 1, flow->stk, 0, 0);
		PARDEBUG fprintf(stderr, "#executed pc= %d wrk= %d claim= " LLFMT "," LLFMT "," LLFMT " %s\n",
						 fe->pc, id, fe->argclaim, fe->hotclaim, fe->maxclaim, error ? error : "");
#ifdef USE_MAL_ADMISSION
		/* release the memory claim */
		MALadmission(-fe->argclaim, -fe->hotclaim);
#endif
		/* update the numa information. keep the thread-id producing the value */
		p= getInstrPtr(flow->mb,fe->pc);
		for( i = 0; i < p->argc; i++)
			flow->mb->var[getArg(p,i)]->worker = thr->tid;

		MT_lock_set(&flow->flowlock);
		fe->state = DFLOWwrapup;
		MT_lock_unset(&flow->flowlock);
		if (error) {
			MT_lock_set(&flow->flowlock);
			/* only collect one error (from one thread, needed for stable testing) */
			if (!flow->error)
				flow->error = error;
			MT_lock_unset(&flow->flowlock);
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
#ifdef USE_MAL_ADMISSION
	{
		InstrPtr p = getInstrPtr(flow->mb, fe->pc);
		assert(p);
		fe->hotclaim = 0;
		fe->maxclaim = 0;

		for (i = 0; i < p->retc; i++){
			lng footprint;
			footprint = getMemoryClaim(flow->mb, flow->stk, p, i, FALSE);
			fe->hotclaim += footprint;
			if( footprint > fe->maxclaim) fe->maxclaim = footprint;
		}
	}
#endif
		MT_lock_set(&flow->flowlock);

		for (last = fe->pc - flow->start; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending &&
				flow->status[i].blocks == 1) {
				flow->status[i].state = DFLOWrunning;
				flow->status[i].blocks = 0;
				flow->status[i].hotclaim = fe->hotclaim;
				flow->status[i].argclaim += fe->hotclaim;
				if( flow->status[i].maxclaim < fe->maxclaim)
					flow->status[i].maxclaim = fe->maxclaim;
				fnxt = flow->status + i;
				break;
			}
		MT_lock_unset(&flow->flowlock);

		q_enqueue(flow->done, fe);
		if ( fnxt == 0 && malProfileMode) {
			int last;
			MT_lock_set(&todo->l);
			last = todo->last;
			MT_lock_unset(&todo->l);
			if (last == 0)
				profilerHeartbeatEvent("wait");
		}
	}
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
	THRdel(thr);
	MT_lock_set(&dataflowLock);
	t->flag = EXITED;
	MT_lock_unset(&dataflowLock);
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
	int i, limit;
	int created = 0;

	MT_lock_set(&mal_contextLock);
	if (todo) {
		/* somebody else beat us to it */
		MT_lock_unset(&mal_contextLock);
		return 0;
	}
	todo = q_create(2048, "todo");
	if (todo == NULL) {
		MT_lock_unset(&mal_contextLock);
		return -1;
	}
	for (i = 0; i < THREADS; i++)
		MT_sema_init(&workers[i].s, 0, "DFLOWinitialize");
	limit = GDKnr_threads ? GDKnr_threads - 1 : 0;
#ifdef NEED_MT_LOCK_INIT
	ATOMIC_INIT(exitingLock);
	MT_lock_init(&dataflowLock, "dataflowLock");
#endif
	MT_lock_set(&dataflowLock);
	for (i = 0; i < limit; i++) {
		workers[i].flag = RUNNING;
		workers[i].cntxt = NULL;
		if (MT_create_thread(&workers[i].id, DFLOWworker, (void *) &workers[i], MT_THR_JOINABLE) < 0)
			workers[i].flag = IDLE;
		else
			created++;
	}
	MT_lock_unset(&dataflowLock);
	if (created == 0) {
		/* no threads created */
		q_destroy(todo);
		todo = NULL;
		MT_lock_unset(&mal_contextLock);
		return -1;
	}
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
	PARDEBUG fprintf(stderr, "#Initialize dflow block\n");
	assign = (int *) GDKzalloc(mb->vtop * sizeof(int));
	if (assign == NULL)
		throw(MAL, "dataflow", "DFLOWinitBlk(): Failed to allocate assign");
	etop = flow->stop - flow->start;
	for (n = 0, pc = flow->start; pc < flow->stop; pc++, n++) {
		p = getInstrPtr(mb, pc);
		if (p == NULL) {
			GDKfree(assign);
			throw(MAL, "dataflow", "DFLOWinitBlk(): getInstrPtr() returned NULL");
		}

		/* initial state, ie everything can run */
		flow->status[n].flow = flow;
		flow->status[n].pc = pc;
		flow->status[n].state = DFLOWpending;
		flow->status[n].cost = -1;
		flow->status[n].flow->error = NULL;

		/* administer flow dependencies */
		for (j = p->retc; j < p->argc; j++) {
			/* list of instructions that wake n-th instruction up */
			if (!isVarConstant(mb, getArg(p, j)) && (k = assign[getArg(p, j)])) {
				assert(k < pc); /* only dependencies on earlier instructions */
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
					if( etop == size){
						flow->nodes = (int*) GDKrealloc(flow->nodes, sizeof(int) * 2 * size);
						flow->edges = (int*) GDKrealloc(flow->edges, sizeof(int) * 2 * size);
						size *=2;
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
					PARDEBUG fprintf(stderr, "#endoflife for %s is %d -> %d\n", getVarName(mb, getArg(p, j)), n + flow->start, l);
					assert(pc < l); /* only dependencies on earlier instructions */
					l -= flow->start;
					if (flow->nodes[n]) {
						/* add wakeup to tail of list */
						for (i = n; flow->edges[i] > 0; i = flow->edges[i])
							;
						flow->nodes[etop] = l;
						flow->edges[etop] = -1;
						flow->edges[i] = etop;
						etop++;
						if( etop == size){
							flow->nodes = (int*) GDKrealloc(flow->nodes, sizeof(int) * 2 * size);
							flow->edges = (int*) GDKrealloc(flow->edges, sizeof(int) * 2 * size);
							size *=2;
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
			assign[getArg(p, j)] = pc;  /* ensure recognition of dependency on first instruction and constant */
	}
	GDKfree(assign);
	PARDEBUG {
		for (n = 0; n < flow->stop - flow->start; n++) {
			mnstr_printf(GDKstdout, "#[%d] %d: ", flow->start + n, n);
			printInstruction(GDKstdout, mb, 0, getInstrPtr(mb, n + flow->start), LIST_MAL_ALL);
			mnstr_printf(GDKstdout, "#[%d]Dependents block count %d wakeup", flow->start + n, flow->status[n].blocks);
			for (j = n; flow->edges[j]; j = flow->edges[j]) {
				mnstr_printf(GDKstdout, "%d ", flow->start + flow->nodes[j]);
				if (flow->edges[j] == -1)
					break;
			}
			mnstr_printf(GDKstdout, "\n");
		}
	}
#ifdef USE_MAL_ADMISSION
	memorypool = memoryclaims = 0;
#endif
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
/*
static void showFlowEvent(DataFlow flow, int pc)
{
	int i;
	FlowEvent fe = flow->status;

	fprintf(stderr, "#end of data flow %d done %d \n", pc, flow->stop - flow->start);
	for (i = 0; i < flow->stop - flow->start; i++)
		if (fe[i].state != DFLOWwrapup && fe[i].pc >= 0) {
			fprintf(stderr, "#missed pc %d status %d %d  blocks %d", fe[i].state, i, fe[i].pc, fe[i].blocks);
			printInstruction(GDKstdout, fe[i].flow->mb, 0, getInstrPtr(fe[i].flow->mb, fe[i].pc), LIST_MAL_MAPI);
		}
}
*/

static str
DFLOWscheduler(DataFlow flow, struct worker *w)
{
	int last;
	int i;
#ifdef USE_MAL_ADMISSION
	int j;
	InstrPtr p;
#endif
	int tasks=0, actions;
	str ret = MAL_SUCCEED;
	FlowEvent fe, f = 0;

	if (flow == NULL)
		throw(MAL, "dataflow", "DFLOWscheduler(): Called with flow == NULL");
	actions = flow->stop - flow->start;
	if (actions == 0)
		throw(MAL, "dataflow", "Empty dataflow block");
	/* initialize the eligible statements */
	fe = flow->status;

	MT_lock_set(&flow->flowlock);
	for (i = 0; i < actions; i++)
		if (fe[i].blocks == 0) {
#ifdef USE_MAL_ADMISSION
			p = getInstrPtr(flow->mb,fe[i].pc);
			if (p == NULL) {
				MT_lock_unset(&flow->flowlock);
				throw(MAL, "dataflow", "DFLOWscheduler(): getInstrPtr(flow->mb,fe[i].pc) returned NULL");
			}
			for (j = p->retc; j < p->argc; j++)
				fe[i].argclaim = getMemoryClaim(fe[0].flow->mb, fe[0].flow->stk, p, j, FALSE);
#endif
			q_enqueue(todo, flow->status + i);
			flow->status[i].state = DFLOWrunning;
			PARDEBUG fprintf(stderr, "#enqueue pc=%d claim=" LLFMT "\n", flow->status[i].pc, flow->status[i].argclaim);
		}
	MT_lock_unset(&flow->flowlock);
	MT_sema_up(&w->s);

	PARDEBUG fprintf(stderr, "#run %d instructions in dataflow block\n", actions);

	while (actions != tasks ) {
		f = q_dequeue(flow->done, NULL);
		if (ATOMIC_GET(exiting, exitingLock))
			break;
		if (f == NULL)
			throw(MAL, "dataflow", "DFLOWscheduler(): q_dequeue(flow->done) returned NULL");

		/*
		 * When an instruction is finished we have to reduce the blocked
		 * counter for all dependent instructions.  for those where it
		 * drops to zero we can scheduler it we do it here instead of the scheduler
		 */

		MT_lock_set(&flow->flowlock);
		tasks++;
		for (last = f->pc - flow->start; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending) {
				flow->status[i].argclaim += f->hotclaim;
				if (flow->status[i].blocks == 1 ) {
					flow->status[i].state = DFLOWrunning;
					flow->status[i].blocks--;
					q_enqueue(todo, flow->status + i);
					PARDEBUG fprintf(stderr, "#enqueue pc=%d claim= " LLFMT "\n", flow->status[i].pc, flow->status[i].argclaim);
				} else {
					flow->status[i].blocks--;
				}
			}
		MT_lock_unset(&flow->flowlock);
	}
	/* release the worker from its specific task (turn it into a
	 * generic worker) */
	MT_lock_set(&dataflowLock);
	w->cntxt = NULL;
	MT_lock_unset(&dataflowLock);
	/* wrap up errors */
	assert(flow->done->last == 0);
	if (flow->error ) {
		PARDEBUG fprintf(stderr, "#errors encountered %s ", flow->error ? flow->error : "unknown");
		ret = flow->error;
	}
	return ret;
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
runMALdataflow(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk)
{
	DataFlow flow = NULL;
	str msg = MAL_SUCCEED;
	int size;
	bit *ret;
	int i;

#ifdef DEBUG_FLOW
	fprintf(stderr, "#runMALdataflow for block %d - %d\n", startpc, stoppc);
	printFunction(GDKstdout, mb, 0, LIST_ALL);
#endif

	/* in debugging mode we should not start multiple threads */
	if (stk == NULL)
		throw(MAL, "dataflow", "runMALdataflow(): Called with stk == NULL");
	ret = getArgReference_bit(stk,getInstrPtr(mb,startpc),0);
	*ret = FALSE;
	if (stk->cmd) {
		*ret = TRUE;
		return MAL_SUCCEED;
	}

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
	{
		int joined;
		do {
			joined = 0;
			for (i = 0; i < THREADS; i++) {
				if (workers[i].flag == EXITED) {
					workers[i].flag = JOINING;
					workers[i].cntxt = NULL;
					joined = 1;
					MT_lock_unset(&dataflowLock);
					MT_join_thread(workers[i].id);
					MT_lock_set(&dataflowLock);
					workers[i].flag = IDLE;
				}
			}
		} while (joined);
	}
	for (i = 0; i < THREADS; i++) {
		if (workers[i].flag == IDLE) {
			/* only create specific worker if we are not doing a
			 * recursive call */
			if (stk->calldepth > 1) {
				int j;
				MT_Id pid = MT_getpid();

				/* doing a recursive call: copy specificity from
				 * current worker to new worker */
				workers[i].cntxt = NULL;
				for (j = 0; j < THREADS; j++) {
					if (workers[j].flag == RUNNING && workers[j].id == pid) {
						workers[i].cntxt = workers[j].cntxt;
						break;
					}
				}
			} else {
				/* not doing a recursive call: create specific worker */
				workers[i].cntxt = cntxt;
			}
			workers[i].flag = RUNNING;
			if (MT_create_thread(&workers[i].id, DFLOWworker, (void *) &workers[i], MT_THR_JOINABLE) < 0) {
				/* cannot start new thread, run serially */
				*ret = TRUE;
				workers[i].flag = IDLE;
				MT_lock_unset(&dataflowLock);
				return MAL_SUCCEED;
			}
			break;
		}
	}
	MT_lock_unset(&dataflowLock);
	if (i == THREADS) {
		/* no empty thread slots found, run serially */
		*ret = TRUE;
		return MAL_SUCCEED;
	}

	flow = (DataFlow)GDKzalloc(sizeof(DataFlowRec));
	if (flow == NULL)
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow");

	flow->cntxt = cntxt;
	flow->mb = mb;
	flow->stk = stk;
	flow->error = 0;

	/* keep real block count, exclude brackets */
	flow->start = startpc + 1;
	flow->stop = stoppc;

	MT_lock_init(&flow->flowlock, "flow->flowlock");
	flow->done = q_create(stoppc- startpc+1, "flow->done");
	if (flow->done == NULL) {
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		throw(MAL, "dataflow", "runMALdataflow(): Failed to create flow->done queue");
	}

	flow->status = (FlowEvent)GDKzalloc((stoppc - startpc + 1) * sizeof(FlowEventRec));
	if (flow->status == NULL) {
		q_destroy(flow->done);
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow->status");
	}
	size = DFLOWgraphSize(mb, startpc, stoppc);
	size += stoppc - startpc;
	flow->nodes = (int*)GDKzalloc(sizeof(int) * size);
	if (flow->nodes == NULL) {
		GDKfree(flow->status);
		q_destroy(flow->done);
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow->nodes");
	}
	flow->edges = (int*)GDKzalloc(sizeof(int) * size);
	if (flow->edges == NULL) {
		GDKfree(flow->nodes);
		GDKfree(flow->status);
		q_destroy(flow->done);
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow->edges");
	}
	msg = DFLOWinitBlk(flow, mb, size);

	if (msg == MAL_SUCCEED)
		msg = DFLOWscheduler(flow, &workers[i]);

	GDKfree(flow->status);
	GDKfree(flow->edges);
	GDKfree(flow->nodes);
	q_destroy(flow->done);
	MT_lock_destroy(&flow->flowlock);
	GDKfree(flow);

	/* we created one worker, now tell one worker to exit again */
	MT_lock_set(&todo->l);
	todo->exitcount++;
	MT_lock_unset(&todo->l);
	MT_sema_up(&todo->s);

	return msg;
}

str
deblockdataflow( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    int *ret = getArgReference_int(stk,pci,0);
    int *val = getArgReference_int(stk,pci,1);
    (void) cntxt;
    (void) mb;
    *ret = *val;
    return MAL_SUCCEED;
}

void
stopMALdataflow(void)
{
	int i;

	ATOMIC_SET(exiting, 1, exitingLock);
	if (todo) {
		for (i = 0; i < THREADS; i++)
			MT_sema_up(&todo->s);
		MT_lock_set(&dataflowLock);
		for (i = 0; i < THREADS; i++) {
			if (workers[i].flag != IDLE && workers[i].flag != JOINING) {
				workers[i].flag = JOINING;
				MT_lock_unset(&dataflowLock);
				MT_join_thread(workers[i].id);
				MT_lock_set(&dataflowLock);
			}
			workers[i].flag = IDLE;
		}
		MT_lock_unset(&dataflowLock);
	}
}
