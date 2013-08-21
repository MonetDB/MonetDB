/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/

/*
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
#include "mal_client.h"

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
} *FlowEvent, FlowEventRec;

typedef struct queue {
	int size;	/* size of queue */
	int last;	/* last element in the queue */
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

#define MAXQ 256
static Queue *todos[MAXQ] = {0};	/* pending instructions organized by dataflow block */
static bit occupied[MAXQ]={0}; 		/* worker pool is in use? */
static int volatile exiting = 0;

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

	(void) name; /* in case MT_LOCK_TRACE is not enabled in gdk_system.h */
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
	MT_lock_set(&q->l, "q_enqueue");
	q_enqueue_(q, d);
	MT_lock_unset(&q->l, "q_enqueue");
	MT_sema_up(&q->s, "q_enqueue");
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
	MT_lock_set(&q->l, "q_requeue");
	q_requeue_(q, d);
	MT_lock_unset(&q->l, "q_requeue");
	MT_sema_up(&q->s, "q_requeue");
}
#endif

static void *
q_dequeue(Queue *q)
{
	void *r = NULL;

	assert(q);
	MT_sema_down(&q->s, "q_dequeue");
	if (exiting)
		return NULL;
	MT_lock_set(&q->l, "q_dequeue");
	assert(q->last > 0);
	if (q->last > 0) {
		/* LIFO favors garbage collection */
		r = (void*) q->data[--q->last];
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

	MT_lock_unset(&q->l, "q_dequeue");
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
DFLOWworker(void *t)
{
	DataFlow flow;
	FlowEvent fe = 0, fnxt = 0;
	Thread thr;
	str error = 0;
	Queue *todo = *(Queue **) t;
	int i,last;

	thr = THRnew("DFLOWworker");

	GDKsetbuf(GDKmalloc(GDKMAXERRLEN)); /* where to leave errors */
	GDKerrbuf[0] = 0;
	while (1) {
		if (fnxt == 0)
			fe = q_dequeue(todo);
		else
			fe = fnxt;
		if (exiting) {
			break;
		}
		fnxt = 0;
		assert(fe);
		flow = fe->flow;
		assert(flow);

		/* whenever we have a (concurrent) error, skip it */
		if (flow->error) {
			q_enqueue(flow->done, fe);
			continue;
		}

		/* skip all instructions when we have encontered an error */
		if (flow->error == 0) {
#ifdef USE_MAL_ADMISSION
			if (MALadmission(fe->argclaim, fe->hotclaim)) {
				fe->hotclaim = 0;   /* don't assume priority anymore */
				if (todo->last == 0)
					MT_sleep_ms(DELAYUNIT);
				q_requeue(todo, fe);
				continue;
			}
#endif
			error = runMALsequence(flow->cntxt, flow->mb, fe->pc, fe->pc + 1, flow->stk, 0, 0);
			PARDEBUG mnstr_printf(GDKstdout, "#executed pc= %d wrk= %d claim= " LLFMT "," LLFMT " %s\n",
								  fe->pc, (int)((Queue **)t - todos), fe->argclaim, fe->hotclaim, error ? error : "");
#ifdef USE_MAL_ADMISSION
			/* release the memory claim */
			MALadmission(-fe->argclaim, -fe->hotclaim);
#endif

			fe->state = DFLOWwrapup;
			if (error) {
				MT_lock_set(&flow->flowlock, "runMALdataflow");
				/* only collect one error (from one thread, needed for stable testing) */
				if (!flow->error) 
					flow->error = error;
				MT_lock_unset(&flow->flowlock, "runMALdataflow");
				/* after an error we skip the rest of the block */
				q_enqueue(flow->done, fe);
				continue;
			}
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
		//for (i = 0; i < p->retc; i++)
			//fe->hotclaim += getMemoryClaim(flow->mb, flow->stk, p, i, FALSE);
		}
#endif
		MT_lock_set(&flow->flowlock, "MALworker");
	
		for (last = fe->pc - flow->start; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending &&
				flow->status[i].blocks == 1) {
				flow->status[i].state = DFLOWrunning;
				flow->status[i].blocks = 0;
				flow->status[i].hotclaim = fe->hotclaim;
				flow->status[i].argclaim += fe->hotclaim;
				fnxt = flow->status + i;
				break;
			}
		MT_lock_unset(&flow->flowlock, "MALworker");

		q_enqueue(flow->done, fe);
		if ( fnxt == 0) {
			if (todo->last == 0)
				profilerHeartbeatEvent("wait");
		}
	}
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
	THRdel(thr);
}

/* 
 * Create an interpreter pool.
 * One worker will adaptively be available for each client.
 * The remainder are taken from the GDKnr_threads argument and
 * typically is equal to the number of cores.
 * A recursive MAL function call would make for one worker less,
 * which limits the number of cores for parallel processing.
 * The workers are assembled in a local table to enable debugging.
 *
 * BEWARE, failure to create a new worker thread is not an error
 * but would lead to serial execution.
 */
static int
DFLOWinitialize(void)
{
	int i, threads, grp;
	MT_Id worker;

	threads = GDKnr_threads ? GDKnr_threads : 1;
	MT_lock_set(&mal_contextLock, "DFLOWinitialize");
	for(grp = 0; grp< MAXQ; grp++)
		if ( occupied[grp] == FALSE){
			occupied[grp] = TRUE;
			break;
		}
	MT_lock_unset(&mal_contextLock, "DFLOWinitialize");
	if (grp > THREADS) {
		// continue non-parallel
		return -1;
	}
	if ( todos[grp] )
		return grp;

	todos[grp] = q_create(2048, "todo");
	if (todos[grp] == NULL) 
		return -1;

	// associate a set of workers with the pool
	for (i = 0; grp>= 0 && i < threads; i++){
		if (MT_create_thread(&worker, DFLOWworker, (void *) &todos[grp], MT_THR_JOINABLE) < 0) {
			//Can not create interpreter thread
			grp = -1;
		}
		if (worker == 0) {
			//Failed to create interpreter thread
			grp = -1;
		}
	}
	return grp;
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
	PARDEBUG printf("Initialize dflow block\n");
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
					assert(etop < size);
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
				l = getEndOfLife(mb, getArg(p, j));
				if (l != pc && l < flow->stop && l > flow->start) {
					/* add edge to the target instruction for wakeup call */
					PARDEBUG mnstr_printf(GDKstdout, "endoflife for %s is %d -> %d\n", getVarName(mb, getArg(p, j)), n + flow->start, l);
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
						assert(etop < size);
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
	PARDEBUG
	for (n = 0; n < flow->stop - flow->start; n++) {
		mnstr_printf(GDKstdout, "#[%d] %d: ", flow->start + n, n);
		printInstruction(GDKstdout, mb, 0, getInstrPtr(mb, n + flow->start), LIST_MAL_STMT | LIST_MAPI);
		mnstr_printf(GDKstdout, "#[%d]Dependents block count %d wakeup", flow->start + n, flow->status[n].blocks);
		for (j = n; flow->edges[j]; j = flow->edges[j]) {
			mnstr_printf(GDKstdout, "%d ", flow->start + flow->nodes[j]);
			if (flow->edges[j] == -1)
				break;
		}
		mnstr_printf(GDKstdout, "\n");
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

	mnstr_printf(GDKstdout, "#end of data flow %d done %d \n", pc, flow->stop - flow->start);
	for (i = 0; i < flow->stop - flow->start; i++)
		if (fe[i].state != DFLOWwrapup && fe[i].pc >= 0) {
			mnstr_printf(GDKstdout, "#missed pc %d status %d %d  blocks %d", fe[i].state, i, fe[i].pc, fe[i].blocks);
			printInstruction(GDKstdout, fe[i].flow->mb, 0, getInstrPtr(fe[i].flow->mb, fe[i].pc), LIST_MAL_STMT | LIST_MAPI);
		}
}
*/

static str
DFLOWscheduler(DataFlow flow, Queue *todo)
{
	int last;
	int i;
#ifdef USE_MAL_ADMISSION
	//int j;
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

	MT_lock_set(&flow->flowlock, "MALworker");
	for (i = 0; i < actions; i++)
		if (fe[i].blocks == 0) {
#ifdef USE_MAL_ADMISSION
			p = getInstrPtr(flow->mb,fe[i].pc);
			if (p == NULL) {
				MT_lock_unset(&flow->flowlock, "MALworker");
				throw(MAL, "dataflow", "DFLOWscheduler(): getInstrPtr(flow->mb,fe[i].pc) returned NULL");
			}
			//for (j = p->retc; j < p->argc; j++)
				//fe[i].argclaim = getMemoryClaim(fe[0].flow->mb, fe[0].flow->stk, p, j, FALSE);
#endif
			q_enqueue(todo, flow->status + i);
			flow->status[i].state = DFLOWrunning;
			PARDEBUG mnstr_printf(GDKstdout, "#enqueue pc=%d claim=" LLFMT "\n", flow->status[i].pc, flow->status[i].argclaim);
		}
	MT_lock_unset(&flow->flowlock, "MALworker");

	PARDEBUG mnstr_printf(GDKstdout, "#run %d instructions in dataflow block\n", actions);

	while (actions != tasks ) {
		f = q_dequeue(flow->done);
		if (exiting)
			break;
		if (f == NULL)
			throw(MAL, "dataflow", "DFLOWscheduler(): q_dequeue(flow->done) returned NULL");

		/*
		 * When an instruction is finished we have to reduce the blocked
		 * counter for all dependent instructions.  for those where it
		 * drops to zero we can scheduler it we do it here instead of the scheduler
		 */

		MT_lock_set(&flow->flowlock, "MALworker");
		tasks++;
		for (last = f->pc - flow->start; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending) {
				flow->status[i].argclaim += f->hotclaim;
				if (flow->status[i].blocks == 1 ) {
					flow->status[i].state = DFLOWrunning;
					flow->status[i].blocks--;
					q_enqueue(todo, flow->status + i);
					PARDEBUG
					mnstr_printf(GDKstdout, "#enqueue pc=%d claim= " LLFMT "\n", flow->status[i].pc, flow->status[i].argclaim);
				} else {
					flow->status[i].blocks--;
				}
			} 
		MT_lock_unset(&flow->flowlock, "MALworker");
	}
	/* wrap up errors */
	assert(flow->done->last == 0);
	if (flow->error ) {
		PARDEBUG mnstr_printf(GDKstdout, "#errors encountered %s ", flow->error ? flow->error : "unknown");
		ret = flow->error;
	}
	return ret;
}

str
runMALdataflow(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk)
{
	DataFlow flow = NULL;
	str msg = MAL_SUCCEED;
	int size, pool;
	int *ret;

#ifdef DEBUG_FLOW
	mnstr_printf(GDKstdout, "runMALdataflow for block %d - %d\n", startpc, stoppc);
	printFunction(GDKstdout, mb, 0, LIST_MAL_STMT | LIST_MAPI);
#endif

	/* in debugging mode we should not start multiple threads */
	if (stk == NULL)
		throw(MAL, "dataflow", "runMALdataflow(): Called with stk == NULL");
	ret = (int*) getArgReference(stk,getInstrPtr(mb,startpc),0);
	if (stk->cmd){
		*ret = TRUE;
		return MAL_SUCCEED;
	}
	/* too many threads turns dataflow processing off */
	if ( cntxt->idx > MAXQ){
		*ret = TRUE;
		return MAL_SUCCEED;
	}

	assert(stoppc > startpc);

	/* check existence of free worker group, resort to sequential upon failure */
	if( (pool= DFLOWinitialize()) < 0){
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

	MT_lock_init(&flow->flowlock, "DFLOWworker");
	flow->done = q_create(stoppc- startpc+1, "flow->done");
	if (flow->done == NULL) {
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		occupied[pool]= FALSE;
		throw(MAL, "dataflow", "runMALdataflow(): Failed to create flow->done queue");
	}

	flow->status = (FlowEvent)GDKzalloc((stoppc - startpc + 1) * sizeof(FlowEventRec));
	if (flow->status == NULL) {
		q_destroy(flow->done);
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		occupied[pool]= FALSE;
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
		occupied[pool]= FALSE;
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow->nodes");
	}
	flow->edges = (int*)GDKzalloc(sizeof(int) * size);
	if (flow->edges == NULL) {
		GDKfree(flow->nodes);
		GDKfree(flow->status);
		q_destroy(flow->done);
		MT_lock_destroy(&flow->flowlock);
		GDKfree(flow);
		occupied[pool]= FALSE;
		throw(MAL, "dataflow", "runMALdataflow(): Failed to allocate flow->edges");
	}
	msg = DFLOWinitBlk(flow, mb, size);

	if (msg == MAL_SUCCEED)
		msg = DFLOWscheduler(flow,todos[pool]);

	occupied[pool]= FALSE;
	GDKfree(flow->status);
	GDKfree(flow->edges);
	GDKfree(flow->nodes);
	q_destroy(flow->done);
	MT_lock_destroy(&flow->flowlock);
	GDKfree(flow);
	return msg;
}
