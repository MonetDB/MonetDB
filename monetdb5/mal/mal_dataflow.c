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
 * Copyright August 2008-2012 MonetDB B.V.
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
#include "mal_dataflow.h"

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
	MT_Lock l;	/* its a shared resource, ie we need locks */
	MT_Sema s;	/* threads wait on empty queues */
} queue;

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
	queue *done;        /* instructions handled */
} *DataFlow, DataFlowRec;

static MT_Id workers[THREADS];
static queue *todo = 0;	/* pending instructions */

/* does not seem to have a major impact */
lng memorypool = 0;      /* memory claimed by concurrent threads */
int memoryclaims = 0;    /* number of threads active with expensive operations */

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
 * Running all eligible instructions in parallel creates
 * resource contention. This means we should implement
 * an admission control scheme where threads are temporarily
 * postponed if the claim for memory exceeds a threshold
 * In general such contentions will be hard to predict,
 * because they depend on the algorithm, the input sizes,
 * concurrent use of the same variables, and the output produced.
 *
 * The heuristic is based on calculating the storage footprint
 * of the operands and assuming it preferrably should fit in memory.
 * Ofcourse, there may be intermediate structures being
 * used and the size of the result is not a priori known.
 * For this, we use a high watermark on the amount of
 * physical memory we pre-allocate for the claims.
 *
 * Instructions are eligible to be executed when the
 * total footprint of all concurrent executions stays below
 * the high-watermark or it is the single expensive
 * instruction being started.
 *
 * When we run out of memory, the instruction is delayed.
 * How long depends on the other instructions to free up
 * resources. The current policy simple takes a local
 * decision by delaying the instruction based on its
 * past and the size of the memory pool size.
 * The waiting penalty decreases with each step to ensure
 * it will ultimately taken into execution, with possibly
 * all resource contention effects.
 *
 * Another option would be to maintain a priority queue of
 * suspended instructions.
 */

/*
 * The memory claim is the estimate for the amount of memory hold.
 * Views are consider cheap and ignored
 */
lng
getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, int pc, int i, int flag)
{
	lng total = 0, vol = 0;
	BAT *b;
	InstrPtr pci = getInstrPtr(mb,pc);

	(void)mb;
	if (stk->stk[getArg(pci, i)].vtype == TYPE_bat) {
		b = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
		if (b == NULL)
			return 0;
		if (flag && isVIEW(b)) {
			BBPunfix(b->batCacheid);
			return 0;
		}
		heapinfo(&b->H->heap); total += vol;
		heapinfo(b->H->vheap); total += vol;
		hashinfo(b->H->hash); total += vol;

		heapinfo(&b->T->heap); total += vol;
		heapinfo(b->T->vheap); total += vol;
		hashinfo(b->T->hash); total += vol;
		if ( b->T->hash == 0  || b->H->hash ==0)	/* assume one hash claim */
			total+= BATcount(b) * sizeof(lng);
		total = total > (lng)(MEMORY_THRESHOLD * monet_memory) ? (lng)(MEMORY_THRESHOLD * monet_memory) : total;
		BBPunfix(b->batCacheid);
	}
	return total;
}

/*
 * The hotclaim indicates the amount of data recentely written.
 * as a result of an operation. The argclaim is the sum over the hotclaims
 * for all arguments.
 * The argclaim provides a hint on how much we actually may need to execute
 * The hotclaim is a hint how large the result would be.
 */
#ifdef USE_DFLOW_ADMISSION
/* experiments on sf-100 on small machine showed no real improvement */
int
DFLOWadmission(lng argclaim, lng hotclaim)
{
	/* optimistically set memory */
	if (argclaim == 0)
		return 0;

	MT_lock_set(&mal_contextLock, "DFLOWdelay");
	if (memoryclaims < 0)
		memoryclaims = 0;
	if (memorypool <= 0 && memoryclaims == 0)
		memorypool = (lng)(MEMORY_THRESHOLD * monet_memory);

	if (argclaim > 0) {
		if (memoryclaims == 0 || memorypool > argclaim + hotclaim) {
			memorypool -= (argclaim + hotclaim);
			memoryclaims++;
			PARDEBUG
			mnstr_printf(GDKstdout, "#DFLOWadmit %3d thread %d pool " LLFMT "claims " LLFMT "," LLFMT "\n",
						 memoryclaims, THRgettid(), memorypool, argclaim, hotclaim);
			MT_lock_unset(&mal_contextLock, "DFLOWdelay");
			return 0;
		}
		PARDEBUG
		mnstr_printf(GDKstdout, "#Delayed due to lack of memory " LLFMT " requested " LLFMT " memoryclaims %d\n", memorypool, argclaim + hotclaim, memoryclaims);
		MT_lock_unset(&mal_contextLock, "DFLOWdelay");
		return -1;
	}
	/* release memory claimed before */
	memorypool += -argclaim - hotclaim;
	memoryclaims--;
	PARDEBUG
	mnstr_printf(GDKstdout, "#DFLOWadmit %3d thread %d pool " LLFMT " claims " LLFMT "," LLFMT "\n",
				 memoryclaims, THRgettid(), memorypool, argclaim, hotclaim);
	MT_lock_unset(&mal_contextLock, "DFLOWdelay");
	return 0;
}
#endif

/*
 * The dataflow execution is confined to a barrier block.
 * Within the block there are multiple flows, which, in principle,
 * can be executed in parallel.
 */

static queue*
q_create(int sz)
{
	queue *q = (queue*)GDKmalloc(sizeof(queue));

	if (q == NULL)
		return NULL;
	q->size = ((sz << 1) >> 1); /* we want a multiple of 2 */
	q->last = 0;
	q->data = (void*)GDKmalloc(sizeof(void*) * q->size);
	if (q->data == NULL) {
		GDKfree(q);
		return NULL;
	}

	MT_lock_init(&q->l, "q_create");
	MT_sema_init(&q->s, 0, "q_create");
	return q;
}


/* keep a simple LIFO queue. It won't be a large one, so shuffles of requeue is possible */
/* we might actually sort it for better scheduling behavior */
static void
q_enqueue_(queue *q, FlowEvent d)
{
	assert(d);
	if (q->last == q->size) {
		q->size <<= 1;
		q->data = GDKrealloc(q->data, sizeof(void*) * q->size);
	}
	q->data[q->last++] = d;
}
static void
q_enqueue(queue *q, FlowEvent d)
{
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

static void
q_requeue_(queue *q, FlowEvent d)
{
	int i;

	assert(d);
	if (q->last == q->size) {
		/* enlarge buffer */
		q->size <<= 1;
		q->data = GDKrealloc(q->data, sizeof(void*) * q->size);
	}
	for (i = q->last; i > 0; i--)
		q->data[i] = q->data[i - 1];
	q->data[0] = (void*)d;
	q->last++;
}
static void
q_requeue(queue *q, FlowEvent d)
{
	assert(d);
	MT_lock_set(&q->l, "q_requeue");
	q_requeue_(q, d);
	MT_lock_unset(&q->l, "q_requeue");
	MT_sema_up(&q->s, "q_requeue");
}

static void *
q_dequeue(queue *q)
{
	void *r = NULL;

	MT_sema_down(&q->s, "q_dequeue");
	assert(q->last);
	MT_lock_set(&q->l, "q_dequeue");
	if (q->last > 0)
		/* LIFO favors garbage collection */
		r = q->data[--q->last];
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

/*
 * A consequence of multiple threads is that they may claim more
 * space than available. This may cause GDKmalloc to fail.
 * In many cases this situation will be temporary, because
 * threads will ultimately release resources.
 * Therefore, we wait for it.
 *
 * Alternatively, a front-end can set the flow administration
 * program counter to -1, which leads to a soft abort.
 * [UNFORTUNATELY this approach does not (yet) work
 * because there seem to a possibility of a deadlock
 * between incref and bbptrim. Furthermore, we have
 * to be assured that the partial executed instruction
 * does not lead to ref-count errors.]
 *
 * The worker produces a result which will potentially unblock
 * instructions. This it can find itself without the help of the scheduler
 * and without the need for a lock. (does it?, parallel workers?)
 * It could also give preference to an instruction that eats away the object
 * just produced. THis way it need not be saved on disk for a long time.
 */
static int asleep = 0;

/* Thread chain context switch decision and multi-user balancing. */
/* Delay the threads if too much competition arises and memory
 * becomes scarce */
/* If in the mean time memory becomes free, or too many sleep,
 * re-enable worker */
/* It may happen that all threads enter the wait state. So, keep
 * one running at all time */
/* this routine can be extended to manage multiple query streams.
 * By keeping the query start time in the client record we can delay
 * them when resource stress occurs.
 */
static void
MALresourceFairness(Client cntxt, lng usec)
{
	long rss = MT_getrss();
	long delay, clk = (GDKusec() - usec) / 1000;
	double factor = 1.0;
	if (rss > MEMORY_THRESHOLD * monet_memory && clk > DELAYUNIT && todo->last) {
		MT_lock_set(&mal_delayLock, "runMALdataflow");
		asleep++;
		MT_lock_unset(&mal_delayLock, "runMALdataflow");

		PARDEBUG mnstr_printf(GDKstdout, "#delay %d initial %ld\n", cntxt->idx, clk);
		while (clk > 0) {
			/* always keep two running to avoid all waiting for
			 * a chain context switch */
			if (asleep >= GDKnr_threads - 1)
				break;
			/* speed up wake up when we have memory or too many sleepers */
			rss = MT_getrss();
			if (rss < MEMORY_THRESHOLD * monet_memory)
				break;
			factor = ((double) rss) / (MEMORY_THRESHOLD * monet_memory);
			delay = (long) (DELAYUNIT * (factor > 1.0 ? 1.0 : factor));
			delay = (long) (delay * (1.0 - (asleep - 1) / GDKnr_threads));
			if (delay)
				MT_sleep_ms(delay);
			clk -= DELAYUNIT;
		}
		MT_lock_set(&mal_delayLock, "runMALdataflow");
		asleep--;
		MT_lock_unset(&mal_delayLock, "runMALdataflow");
		PARDEBUG mnstr_printf(GDKstdout, "#delayed finished thread %d asleep %d\n", cntxt->idx, asleep);
	}
}

static void
DFLOWworker(void *t)
{
	DataFlow flow;
	FlowEvent fe = 0;
	int id = (MT_Id *) t - workers;
	Thread thr;
	str error = 0;

	InstrPtr p;
	int i;
	long usec = 0;

	thr = THRnew("DFLOWworker");

	GDKsetbuf(GDKmalloc(GDKMAXERRLEN)); /* where to leave errors */
	GDKerrbuf[0] = 0;
	while (1) {
		fe = q_dequeue(todo);
		assert(fe);
		flow = fe->flow;

		/* whenever we have a (concurrent) error, skip it */
		if (flow->error) {
			q_enqueue(flow->done, fe);
			continue;
		}

#ifdef USE_DFLOW_ADMISSION
		if (DFLOWadmission(fe->argclaim, fe->hotclaim)) {
			fe->hotclaim = 0;   /* don't assume priority anymore */
			if (todo->last == 0)
				MT_sleep_ms(DELAYUNIT);
			q_requeue(todo, fe);
			continue;
		}
#endif
		/* skip all instructions when we have encontered an error */
		if (flow->error == 0) {
			usec = GDKusec();
			error = runMALsequence(flow->cntxt, flow->mb, fe->pc, fe->pc + 1, flow->stk, 0, 0);
			PARDEBUG mnstr_printf(GDKstdout, "#executed pc= %d wrk= %d claim= " LLFMT "," LLFMT " %s\n",
								  fe->pc, id, fe->argclaim, fe->hotclaim, error ? error : "");
			fe->state = DFLOWwrapup;
			if (error) {
				MT_lock_set(&flow->flowlock, "runMALdataflow");
				if (flow->error) {
					/* collect all errors encountered */
					str z = (char *) GDKrealloc(flow->error, strlen(flow->error) + strlen(error) + 2);
					if (z) {
						if (z[strlen(z) - 1] != '\n')
							strcat(z, "\n");
						strcat(z, error);
					}
					flow->error = z;
					GDKfree(error);
				} else
					flow->error = error;
				MT_lock_unset(&flow->flowlock, "runMALdataflow");
				/* after an error we skip the rest of the block */
				q_enqueue(flow->done, fe);
				continue;
			}
		}

		PARDEBUG mnstr_printf(GDKstdout, "#execute pc= %d wrk= %d finished %s\n", fe->pc, id, flow->error ? flow->error : "");

#ifdef USE_DFLOW_ADMISSION
		/* release the memory claim */
		DFLOWadmission(-fe->argclaim, -fe->hotclaim);
#endif

		/* see if you can find an eligible instruction that uses the
		 * result just produced. Then we can continue with it right
		 * away.  We are just looking forward for the last block, which means we
		 * are safe from concurrent actions.
		 * All eligible instructions are queued
		 */
#ifdef USE_DFLOW_ADMISSION
		fe->hotclaim = 0;
		p = getInstrPtr(flow->mb, fe->pc);
		for (i = 0; i < p->retc; i++)
			fe->hotclaim += getMemoryClaim(flow->mb, flow->stk, fe->pc, i, FALSE);
#endif
		q_enqueue(flow->done, fe);
		MALresourceFairness(flow->cntxt, usec);
	}
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
	THRdel(thr);
}

/* 
 * Create a set of DFLOW interpreters.
 * One worker will adaptively be available for each client.
 * The remainder are taken from the GDKnr_threads argument and
 * typically is equal to the number of cores
 * The workers are assembled in a local table to enable debugging.
 */
static void
DFLOWinitialize(void)
{
	int i, limit;

	if (todo)
		return;
	MT_lock_set(&mal_contextLock, "DFLOWinitialize");
	todo = q_create(2048);
	limit = GDKnr_threads ? GDKnr_threads : 1;
	for (i = 0; i < limit; i++)
		MT_create_thread(&workers[i], DFLOWworker, (void *) &workers[i], MT_THR_JOINABLE);
	MT_lock_unset(&mal_contextLock, "DFLOWinitialize");
}
 
/*
 * The dataflow administration is based on administration of
 * how many variables are still missing before it can be executed.
 * For each instruction we keep a list of instructions whose
 * blocking counter should be decremented upon finishing it.
 */
static void
DFLOWinitBlk(DataFlow flow, MalBlkPtr mb, int size)
{
	int pc, i, j, k, l, n, etop = 0;
	int *assign;
	InstrPtr p;

	PARDEBUG printf("Initialize dflow block\n");
	assign = (int *) GDKzalloc(mb->vtop * sizeof(int));
	etop = flow->stop - flow->start;
	for (n = 0, pc = flow->start; pc < flow->stop; pc++, n++) {
		p = getInstrPtr(mb, pc);

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
#ifdef USE_DFLOW_ADMISSION
	memorypool = memoryclaims = 0;
#endif
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

static str
DFLOWscheduler(DataFlow flow)
{
	int queued = 0, last;
	int i, pc = 0;
#ifdef USE_DFLOW_ADMISSION
	int j;
	InstrPtr p;
#endif
	int actions = flow->stop - flow->start;
	str ret = MAL_SUCCEED;
	FlowEvent fe, f = 0;

	if (actions == 0)
		throw(MAL, "dataflow", "Empty dataflow block");
	/* initialize the eligible statements */
	fe = flow->status;

	if (fe[0].flow->cntxt->flags & timerFlag)
		fe[0].flow->cntxt->timer = GDKusec();

	/* enter all dependencies before releasing the queue  */
	for (i = 0; i < actions; i++)
		if (fe[i].blocks == 0) {
#ifdef USE_DFLOW_ADMISSION
			p = getInstrPtr(flow->mb,fe[i].pc);
			for (j = p->retc; j < p->argc; j++)
				fe[i].argclaim = getMemoryClaim(fe[0].flow->mb, fe[0].flow->stk, fe[i].pc, j, FALSE);
#endif
			q_enqueue(todo, flow->status + i);
			flow->status[i].state = DFLOWrunning;
			queued++;
			PARDEBUG mnstr_printf(GDKstdout, "#enqueue pc=%d claim=" LLFMT " queue %d\n", flow->status[i].pc, flow->status[i].argclaim, queued);
		}

	PARDEBUG mnstr_printf(GDKstdout, "#run %d instructions in dataflow block\n", actions);

	while (actions > 0 ) {
		PARDEBUG mnstr_printf(GDKstdout, "#waiting for results, queued %d\n", queued);
		f = q_dequeue(flow->done);
		actions--;

		/*
		 * When an instruction is finished we have to reduce the blocked
		 * counter for all dependent instructions.  for those where it
		 * drops to zero we can scheduler it Moreover, we add the return
		 * variable claim size to the target instruction and remember
		 * the last increment as hotclaim.
		 */

		/* enter all dependencies before releasing the queue  */
		/* otherwise you can be overtaken by a worker */

		for (last = f->pc - flow->start; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
			if (flow->status[i].state == DFLOWpending) {
				flow->status[i].argclaim += f->hotclaim;
				if (flow->status[i].blocks == 1 ) {
					queued++;
					q_enqueue(todo, flow->status + i);
					flow->status[i].state = DFLOWrunning;
					flow->status[i].blocks--;
					PARDEBUG
					mnstr_printf(GDKstdout, "#enqueue pc=%d claim= " LLFMT " queued= %d\n", flow->status[i].pc, flow->status[i].argclaim, queued);
				} else {
					flow->status[i].blocks--;
				}
			} 
	}
	/* wrap up errors */
	if (flow->error ) {
		PARDEBUG mnstr_printf(GDKstdout, "#errors encountered %s ", flow->error ? flow->error : "unknown");
		ret = flow->error;
	}
	PARDEBUG showFlowEvent(flow, pc);
	return ret;
}

str
runMALdataflow(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk)
{
	DataFlow flow = NULL;
	str ret = MAL_SUCCEED;
	int size;

#ifdef DEBUG_FLOW
	mnstr_printf(GDKstdout, "runMALdataflow for block %d - %d\n", startpc, stoppc);
	printFunction(GDKstdout, mb, 0, LIST_MAL_STMT | LIST_MAPI);
#endif

	/* in debugging mode we should not start multiple threads */
	if (stk->cmd)
		return MAL_SUCCEED;

	assert(stoppc > startpc);

	/* check existence of workers */
	if (workers[0] == 0)
		DFLOWinitialize();
	assert(workers[0]);
	assert(todo);

	flow = (DataFlow)GDKzalloc(sizeof(DataFlowRec));

	flow->cntxt = cntxt;
	flow->mb = mb;
	flow->stk = stk;
	flow->error = 0;

	/* keep real block count, exclude brackets */
	flow->start = startpc + 1;
	flow->stop = stoppc;

	MT_lock_init(&flow->flowlock, "DFLOWworker");
	flow->done = q_create(stoppc- startpc);

	flow->status = (FlowEvent)GDKzalloc((stoppc - startpc + 1) * sizeof(FlowEventRec));
	size = DFLOWgraphSize(mb, startpc, stoppc);
	flow->nodes = (int*)GDKzalloc(sizeof(int) * size);
	flow->edges = (int*)GDKzalloc(sizeof(int) * size);
	DFLOWinitBlk(flow, mb, size);

	ret = DFLOWscheduler(flow);

	GDKfree(flow->status);
	GDKfree(flow->edges);
	GDKfree(flow->nodes);
	GDKfree(flow->done);
	GDKfree(flow);
	return ret;
}
