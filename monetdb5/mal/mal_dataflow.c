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

typedef struct queue {
	int size;	/* size of queue */
	int last;	/* last element in the queue */
	void **data;
	MT_Lock l;	/* its a shared resource, ie we need locks */
	MT_Sema s;	/* threads wait on empty queues */
} queue;


/*
 * The dataflow dependency is administered in a graph list structure.
 * For each instruction we keep the list of instructions that
 * should be checked for eligibility once we are finished with it.
 */
typedef struct {
	MT_Id tid;
	int id;
	queue *todo;		/* pending actions for this client */
	lng clk;
	struct DataFlow *flow;
} FlowTask;

typedef struct FLOWSTATUS {
	Client cntxt;		/* for debugging and client resolution */
	MalBlkPtr mb;		/* carry the context */
	MalStkPtr stk;
	int pc;			/* pc in underlying malblock */
	int blocks; 	/* awaiting for variables */
	sht state;		/* of execution */
	sht cost;
	lng hotclaim;	/* memory foot print of result variables */
	lng argclaim;	/* memory foot print of arguments */
	str error;
} *FlowStatus, FlowStatusRec;

typedef struct DataFlow {
	int start, stop;	/* guarded block under consideration*/
	FlowStatus status;		/* status of each instruction */
	int *nodes;			/* dependency graph nodes */
	int *edges;			/* dependency graph */
	queue *done;		/* work finished */
	queue *todo;		/* pending actions for this client */
	int    nway;		/* number of workers */
	FlowTask *worker;	/* worker threads for the client */
	struct DataFlow *free;	/* free list */
} *DataFlow, DataFlowRec;

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
getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int flag)
{
	lng total = 0, vol = 0;
	BAT *b;

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
/* experiments on sf-100 on small machine showed no real improvement
*/
int
DFLOWadmission(lng argclaim, lng hotclaim)
{
	/* optimistically set memory */
	if (argclaim == 0)
		return 0;

	mal_set_lock(mal_contextLock, "DFLOWdelay");
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
			mal_unset_lock(mal_contextLock, "DFLOWdelay");
			return 0;
		}
		PARDEBUG
		mnstr_printf(GDKstdout, "#Delayed due to lack of memory " LLFMT " requested " LLFMT " memoryclaims %d\n", memorypool, argclaim + hotclaim, memoryclaims);
		mal_unset_lock(mal_contextLock, "DFLOWdelay");
		return -1;
	}
	/* release memory claimed before */
	memorypool += -argclaim - hotclaim;
	memoryclaims--;
	PARDEBUG
	mnstr_printf(GDKstdout, "#DFLOWadmit %3d thread %d pool " LLFMT " claims " LLFMT "," LLFMT "\n",
				 memoryclaims, THRgettid(), memorypool, argclaim, hotclaim);
	mal_unset_lock(mal_contextLock, "DFLOWdelay");
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

/*
static void
q_destroy(queue *q)
{
	GDKfree(q->data);
	GDKfree(q);
}
*/

/* keep a simple LIFO queue. It won't be a large one, so shuffles of requeue is possible */
/* we might actually sort it for better scheduling behavior */
static void
q_enqueue_(queue *q, FlowStatus d)
{
	if (q->last == q->size) {
		/* enlarge buffer */
		q->size <<= 1;
		q->data = GDKrealloc(q->data, sizeof(void*) * q->size);
	}
	q->data[q->last++] = (void*)d;
}
static void
q_enqueue(queue *q, FlowStatus d)
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
q_requeue_(queue *q, void *d)
{
	int i;
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
q_requeue(queue *q, void *d)
{
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
	MT_lock_set(&q->l, "q_dequeue");
	assert(q->last > 0);
	/* LIFO favors garbage collection */
	r = q->data[--q->last];
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
	return r;
}

/* it makes sense to give priority to those
 * instructions that carry a lot of temporary arguments
 * It will reduce the footprint of the database.
 */
static void
queue_sort(queue *q)
{
	int i, j;
	void *f;

	for (i = 0; i < q->last; i++)
		for (j = i + 1; j < q->last; j++)
			if (((FlowStatus)q->data[i])->argclaim > ((FlowStatus)q->data[j])->argclaim) {
				f = q->data[i];
				q->data[i] = q->data[j];
				q->data[j] = f;
			}
	/* decay, because it is likely flushed */
	for (i = 0; i < q->last; i++)
		((FlowStatus)q->data[i])->argclaim /= 2;
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
static str
DFLOWstep(FlowTask *t, FlowStatus fs)
{
	DataFlow flow = t->flow;
	int stkpc = fs->pc;
	int stamp = -1;

	ValPtr lhs, rhs, v;
	int i, k;
	int exceptionVar = -1;
	str ret = MAL_SUCCEED;
	ValPtr backup = GDKzalloc(fs->mb->maxarg * sizeof(ValRecord));
	int *garbage = (int*)GDKzalloc(fs->mb->maxarg * sizeof(int));
	Client cntxt = fs->cntxt;
	MalBlkPtr mb = fs->mb;
	MalStkPtr stk = fs->stk;
	int startpc = fs->pc;
	InstrPtr pci;
	lng oldtimer = 0;
	MT_Lock *lock = &flow->done->l;
	int tid = t->id, prevpc = 0;
	RuntimeProfileRecord runtimeProfile;

	runtimeProfileInit(mb, &runtimeProfile, cntxt->flags & memoryFlag);
	if (stk == NULL || stkpc < 0)
		throw(MAL, "mal.interpreter", MAL_STACK_FAIL);

	pci = getInstrPtr(fs->mb, stkpc);
#ifdef DEBUG_FLOW
	printf("#EXECUTE THREAD %d \n", tid);
	printInstruction(GDKstdout, flow->mb, 0, pci, LIST_MAL_STMT | LIST_MAPI);
#endif
	if (stk->cmd || mb->trap) {
		lng tm = 0;
		if (oldtimer)
			tm = GDKusec();
		if (cntxt->flags & bbpFlag)
			BBPTraceCall(cntxt, mb, stk, prevpc);
		prevpc = stkpc;
		mdbStep(cntxt, mb, stk, getPC(mb, pci));
		if (stk->cmd == 'x' || cntxt->mode == FINISHING) {
			/* need a way to skip */
			stkpc = mb->stop;
			fs->state = -1;
			GDKfree(backup);
			GDKfree(garbage);
			return ret;
		}
		if (oldtimer) {
			/* ignore debugger waiting time*/
			tm = GDKusec() - tm;
			oldtimer += tm;
		}
	}

	//runtimeProfileBegin(cntxt, mb, stk, stkpc, &runtimeProfile, 0);
	FREE_EXCEPTION(ret);
	ret = MAL_SUCCEED;
	if (pci->recycle > 0)
		t->clk = GDKusec();
	if (!RECYCLEentry(cntxt, mb, stk, pci)){
		runtimeProfileBegin(cntxt, mb, stk, stkpc, &runtimeProfile, 1);
/*
 * Before we execute an instruction the variables to be garbage collected
 * are identified. In the post-execution phase they are removed.
 */
		if (garbageControl(pci)) {
			for (i = 0; i < pci->argc; i++) {
				int a = getArg(pci, i);

				backup[i].vtype = 0;
				backup[i].len = 0;
				backup[i].val.pval = 0;
				garbage[i] = -1;
				if (stk->stk[a].vtype == TYPE_bat && getEndOfLife(mb, a) == stkpc && isNotUsedIn(pci, i + 1, a))
					garbage[i] = a;

				if (i < pci->retc && stk->stk[a].vtype == TYPE_bat) {
					backup[i] = stk->stk[a];
					stamp = BBPcurstamp();
				} else if (i < pci->retc &&
						   0 < stk->stk[a].vtype &&
						   stk->stk[a].vtype < TYPE_any &&
						   ATOMextern(stk->stk[a].vtype)) {
					backup[i] = stk->stk[a];
				}
			}
		}
		/*
		 * The number of instructions allowed is severely limited.
		 * We don't allow sequential flow control here, which is enforced by the dataflow optimizer;
		 */
		switch (pci->token) {
		case ASSIGNsymbol:
			/* assignStmt(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t)*/
			for (k = 0, i = pci->retc; k < pci->retc && i < pci->argc; i++, k++) {
				lhs = &stk->stk[pci->argv[k]];
				rhs = &stk->stk[pci->argv[i]];
				VALcopy(lhs, rhs);
				if (lhs->vtype == TYPE_bat && lhs->val.bval)
					BBPincref(lhs->val.bval, TRUE);
			}
			FREE_EXCEPTION(ret);
			ret = 0;
			runtimeTiming(cntxt, mb, stk, pci, tid, lock, &runtimeProfile);
			break;
		case PATcall:
			/* patterncall(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t) */
			if (pci->fcn == NULL) {
				ret = createScriptException(mb, stkpc, MAL, NULL,
					"address of pattern %s.%s missing", pci->modname, pci->fcnname);
			} else {
				ret = (str)(*pci->fcn)(cntxt, mb, stk, pci);
			}
			runtimeTiming(cntxt, mb, stk, pci, tid, lock, &runtimeProfile);
			break;
		case CMDcall:
			/* commandcall(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t) */
			ret =malCommandCall(stk, pci);
			runtimeTiming(cntxt, mb, stk, pci, tid, lock, &runtimeProfile);
			break;
		case FACcall:
/*
 * Factory calls are more involved. At this stage it is a synchrononous
 * call to the factory manager.
 * Factory calls should deal with the reference counting.
 */
			/* factorycall(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t) */
			if (pci->blk == NULL)
				ret = createScriptException(mb, stkpc, MAL, NULL,
					"reference to MAL function missing");
			else {
				/* show call before entering the factory */
				if (cntxt->itrace || mb->trap) {
					lng t = 0;

					if (stk->cmd == 0)
						stk->cmd = cntxt->itrace;
					if (oldtimer)
						t = GDKusec();
					mdbStep(cntxt, pci->blk, stk, 0);
					if (stk->cmd == 'x' || cntxt->mode == FINISHING) {
						stk->cmd = 0;
						stkpc = mb->stop;
					}
					if (oldtimer) {
						/* ignore debugger waiting time*/
						t = GDKusec() - t;
				oldtimer += t;
					}
				}
				ret = runFactory(cntxt, pci->blk, mb, stk, pci);
			}
			runtimeTiming(cntxt, mb, stk, pci, tid, lock, &runtimeProfile);
			break;
		case FCNcall:
/*
 * MAL function calls are relatively expensive, because they have to assemble
 * a new stack frame and do housekeeping, such as garbagecollection of all
 * non-returned values.
 */
			/* functioncall(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t) */
			{	MalStkPtr nstk;
				InstrPtr q;
				int ii, arg;

				stk->pcup = stkpc;
				nstk = prepareMALstack(pci->blk, pci->blk->vsize);
				if (nstk == 0)
					throw(MAL,"mal.interpreter",MAL_STACK_FAIL);

				/*safeguardStack*/
				nstk->stkdepth = nstk->stksize + stk->stkdepth;
				nstk->calldepth = stk->calldepth + 1;
				nstk->up = stk;
				if (nstk->calldepth > 256)
					throw(MAL, "mal.interpreter", MAL_CALLDEPTH_FAIL);
				if ((unsigned)nstk->stkdepth > THREAD_STACK_SIZE / sizeof(mb->var[0]) / 4 && THRhighwater())
					/* we are running low on stack space */
					throw(MAL, "mal.interpreter", MAL_STACK_FAIL);

				/* copy arguments onto destination stack */
				q= getInstrPtr(pci->blk,0);
				arg = q->retc;
				for (ii = pci->retc; ii < pci->argc; ii++,arg++) {
					lhs = &nstk->stk[q->argv[arg]];
					rhs = &stk->stk[pci->argv[ii]];
					VALcopy(lhs, rhs);
					if (lhs->vtype == TYPE_bat)
						BBPincref(lhs->val.bval, TRUE);
				}
				runtimeProfileBegin(cntxt, pci->blk, nstk, 0, &runtimeProfile, 1);
				ret = runMALsequence(cntxt, pci->blk, 1, pci->blk->stop, nstk, stk, pci);
				GDKfree(nstk);
				runtimeTiming(cntxt, pci->blk, nstk, 0, tid, lock, &runtimeProfile);
			}
			break;
		case NOOPsymbol:
		case REMsymbol:
			break;
		default:
			if (pci->token < 0) {
				/* temporary NOOP instruction */
				break;
			}
			ret = createScriptException(mb, stkpc, MAL,
				NULL, "unkown operation");
		}
		if (pci->token != FACcall) {
			/* Provide debugging support */
			if (GDKdebug & (CHECKMASK|PROPMASK) && exceptionVar < 0) {
				BAT *b;

				for (i = 0; i < pci->retc; i++) {
					if (garbage[i] == -1 && stk->stk[getArg(pci, i)].vtype == TYPE_bat &&
						stk->stk[getArg(pci, i)].val.bval) {
						b = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
						if (b == NULL) {
							ret = createException(MAL, "mal.propertyCheck", RUNTIME_OBJECT_MISSING);
							continue;
						}
						if (b->batStamp <= stamp) {
							if (GDKdebug & PROPMASK) {
								BATassertProps(b);
							}
						} else if (GDKdebug & CHECKMASK) {
							BATassertProps(b);
						}
						BBPunfix(b->batCacheid);
					}
				}
			}
			if (pci->recycle > 0) {
				RECYCLEexit(cntxt, mb, stk, pci, t->clk);
			}
			if (ret == MAL_SUCCEED && garbageControl(pci)) {
				for (i = 0; i < pci->argc; i++) {
					int a = getArg(pci, i);

					if (isaBatType(getArgType(mb, pci, i))) {
						bat bid = stk->stk[a].val.bval;

						/* update the bigfoot information only if we need to gc */
						if (cntxt->flags & bigfootFlag)
							updateBigFoot(cntxt, bid, TRUE);
						if (i < pci->retc && backup[i].val.bval) {
							if (backup[i].val.bval != bid && i < pci->retc) {
								/* possible garbage collect the variable */
								if (cntxt->flags & bigfootFlag)
									updateBigFoot(cntxt, backup[i].val.bval, FALSE);
							}
							BBPdecref(backup[i].val.bval, TRUE);
							backup[i].val.bval = 0;
						}
						if (garbage[i] >= 0) {
							bid = ABS(stk->stk[garbage[i]].val.bval);
							BBPdecref(bid, TRUE);
							PARDEBUG mnstr_printf(GDKstdout, "#GC pc=%d bid=%d %s done\n", stkpc, bid, getVarName(mb, garbage[i]));
							stk->stk[garbage[i]].val.bval = 0;
						}
					} else if (i < pci->retc &&
							   0 < stk->stk[a].vtype &&
							   stk->stk[a].vtype < TYPE_any &&
							   ATOMextern(stk->stk[a].vtype)) {
						if (backup[i].val.pval &&
							backup[i].val.pval != stk->stk[a].val.pval) {
							if (backup[i].val.pval)
								GDKfree(backup[i].val.pval);
							if (i >= pci->retc) {
								stk->stk[a].val.pval = 0;
								stk->stk[a].len = 0;
							}
							backup[i].len = 0;
							backup[i].val.pval = 0;
						}
					}
				}
			}
		}
		/* exceptionHndlr(FAST,fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret,t) */
		if (GDKerrbuf && GDKerrbuf[0]) {
			str oldret = ret;
			ret = catchKernelException(cntxt, ret);
			if (ret != oldret)
				FREE_EXCEPTION(oldret);
		}

		if (ret != MAL_SUCCEED) {
			str msg = 0;

			if (stk->cmd || mb->trap) {
				mnstr_printf(cntxt->fdout, "!ERROR: %s\n", ret);
				stk->cmd = '\n'; /* in debugging go to step mode */
				mdbStep(cntxt, mb, stk, stkpc);
				if (stk->cmd == 'x' || stk->cmd == 'q' || cntxt->mode == FINISHING) {
					stkpc = mb->stop;
					fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
				}
				if (stk->cmd == 'r') {
					stk->cmd = 'n';
					stkpc = startpc;
					exceptionVar = -1;
					fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
				}
			}
			/* Detect any exception received from the implementation. */
			/* The first identifier is an optional exception name */
			if (strstr(ret, "!skip-to-end")) {
				GDKfree(ret);       /* no need to check for M5OutOfMemory */
				ret = MAL_SUCCEED;
				stkpc = mb->stop;
				fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
			}
			/*
			 * Exceptions are caught based on their name, which is part of the
			 * exception message. The ANYexception variable catches all.
			 */
			exceptionVar = -1;
			msg = strchr(ret, ':');
			if (msg) {
				*msg = 0;
				exceptionVar = findVariableLength(mb, ret, (int)(msg - ret));
				*msg = ':';
			}
			if (exceptionVar == -1)
				exceptionVar = findVariableLength(mb, (str)"ANYexception", 12);

			/* unknown exceptions lead to propagation */
			if (exceptionVar == -1) {
				runtimeProfileExit(cntxt, mb, stk, &runtimeProfile);
				if (cntxt->qtimeout && time(NULL) - stk->clock.tv_usec > cntxt->qtimeout)
					throw(MAL, "mal.interpreter", RUNTIME_QRY_TIMEOUT);
				stkpc = mb->stop;
				fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
			}
			/* assure correct variable type */
			if (getVarType(mb, exceptionVar) == TYPE_str) {
				/* watch out for concurrent access */
				mal_set_lock(mal_contextLock, "exception handler");
				v = &stk->stk[exceptionVar];
				if (v->val.sval)
					FREE_EXCEPTION(v->val.sval);    /* old exception*/
				v->vtype = TYPE_str;
				v->val.sval = ret;
				v->len = (int)strlen(v->val.sval);
				ret = 0;
				mal_unset_lock(mal_contextLock, "exception handler");
			} else {
				mnstr_printf(cntxt->fdout, "%s", ret);
				FREE_EXCEPTION(ret);
			}
			/* position yourself at the catch instruction for further decisions */
			/* skipToCatch(exceptionVar,@2,@3) */
			if (stk->cmd == 'C' || mb->trap) {
				stk->cmd = 'n';
				if (cntxt->flags & bbpFlag)
					BBPTraceCall(cntxt, mb, stk, prevpc);
				prevpc = stkpc;
				mdbStep(cntxt, mb, stk, stkpc);
				if (stk->cmd == 'x' || cntxt->mode == FINISHING) {
					stkpc = mb->stop;
					fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
				}
			}
			/* skip to catch block or end */
			for (; stkpc < mb->stop; stkpc++) {
				InstrPtr l = getInstrPtr(mb, stkpc);
				if (l->barrier == CATCHsymbol) {
					int j;
					for (j = 0; j < l->retc; j++)
						if (getArg(l, j) == exceptionVar)
							break;
						else if (getArgName(mb, l, j) ||
								 strcmp(getArgName(mb, l, j), "ANYexception") == 0)
							break;
					if (j < l->retc)
						break;
				}
			}
			if (stkpc == mb->stop) {
				runtimeProfileExit(cntxt, mb, stk, &runtimeProfile);
				runtimeProfile.ppc = 0; /* also finalize function call event */
				runtimeProfileExit(cntxt, mb, stk, &runtimeProfile);
				if (cntxt->qtimeout && time(NULL) - stk->clock.tv_usec > cntxt->qtimeout)
					throw(MAL, "mal.interpreter", RUNTIME_QRY_TIMEOUT);
				fs->pc = -fs->pc; GDKfree(backup); GDKfree(garbage); return ret;
			}
			pci = getInstrPtr(mb, stkpc);
		}
		runtimeProfileExit(cntxt, mb, stk, &runtimeProfile);
	}
	if (cntxt->qtimeout && time(NULL) - stk->clock.tv_usec > cntxt->qtimeout)
		throw(MAL, "mal.interpreter", RUNTIME_QRY_TIMEOUT);
	if (ret)
		fs->pc = -fs->pc;
	GDKfree(backup);
	GDKfree(garbage);
	return ret;
}

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

static void
runDFLOWworker(void *t)
{
	FlowStatus fs, nxtfs = 0;
	FlowTask *task = (FlowTask*)t;
	InstrPtr p;
	Thread thr;
	int i, local = 0, last = 0;
	long usec=0;

	thr = THRnew(MT_getpid(), "DFLOWworker");
	GDKsetbuf(GDKmalloc(GDKMAXERRLEN));	/* where to leave errors */
	GDKerrbuf[0] = 0;
	while (task) {
		local = nxtfs != 0;
		if (nxtfs == 0) {
			fs = (FlowStatus)q_dequeue(task->todo);

#ifdef USE_DFLOW_ADMISSION
			if (DFLOWadmission(fs->argclaim, fs->hotclaim)) {
				fs->hotclaim = 0;   /* don't assume priority anymore */
				if ( task->todo->last == 0)
						MT_sleep_ms(DELAYUNIT);
				q_requeue(task->todo, fs);
				nxtfs = 0;
				continue;
			}
#endif
			usec = GDKusec();
		} else
			/* always execute, it does not affect memory claims */
			fs = nxtfs;
		assert(fs->pc > 0);
		PARDEBUG mnstr_printf(GDKstdout, "#execute pc= %d thr= %d claim= " LLFMT "," LLFMT " %s\n", fs->pc, task->id, fs->argclaim, fs->hotclaim, fs->error ? fs->error : "");
		fs->error = DFLOWstep(task, fs);

		PARDEBUG mnstr_printf(GDKstdout, "#execute pc= %d thr= %d finished %s\n", fs->pc, task->id, fs->error ? fs->error : "");

#ifdef USE_DFLOW_ADMISSION
		/* release the memory claim */
		if ( nxtfs ==0 )
			DFLOWadmission(-fs->argclaim, -fs->hotclaim);
#endif

		p = getInstrPtr(fs->mb, ABS(fs->pc));
		fs->hotclaim = 0;
		for (i = 0; i < p->retc; i++)
			fs->hotclaim += getMemoryClaim(fs->mb, fs->stk, p, i, FALSE);

		/* see if you can find an eligible instruction that uses the
		 * result just produced. Then we can continue with it right away.
		 * We are just looking for the last block, which means we are safe from concurrent actions
		 * All eligable instructions are queued 
		 */
		nxtfs = 0;
		if (fs->pc >= 0)
			for (last = fs->pc - task->flow->start; last >= 0 && (i = task->flow->nodes[last]) > 0; last = task->flow->edges[last])
				if (task->flow->status[i].state == DFLOWpending &&
					task->flow->status[i].blocks == 1) {
					task->flow->status[i].state = DFLOWrunning;
					task->flow->status[i].blocks = 0;
					task->flow->status[i].hotclaim = fs->hotclaim;
					task->flow->status[i].argclaim += fs->hotclaim;
					task->flow->status[i].error = NULL;
					if ( nxtfs )
						q_enqueue(task->todo, nxtfs);
					nxtfs = task->flow->status + i;
				}
			PARDEBUG if (nxtfs) mnstr_printf(GDKstdout, "#continue pc= %d thr= %d claim= " LLFMT "\n", nxtfs->pc, task->id, task->flow->status[i].argclaim);

		/* all non-local choices are handled by the main scheduler */
		/* we always return the instruction handled */
		/* be careful, the local continuation should be last in the queue */
		if (local)
			q_requeue(task->flow->done, fs);
		else
			q_enqueue(task->flow->done, fs);
		/* Thread chain context switch decision. */
		/* Delay the threads if too much competition arises */
		/* If in the mean time memory becomes free, or too many sleep, re-enable worker */
		/* It may happen that all threads enter the wait state. So, keep one running at all time */
		if ( nxtfs == 0){
			long delay, clk = (GDKusec()- usec)/1000;
			double factor = 1.0;
			if ( clk > DELAYUNIT ) {
				mal_set_lock(mal_contextLock, "runMALdataflow");
				asleep++;
				/* speedup as we see more threads asleep */
				clk = (long) (clk * (1.0- asleep/GDKnr_threads));
				/* always keep one running to avoid all waiting for a chain context switch */
				if ( asleep >= GDKnr_threads)
					clk = -2 * DELAYUNIT;
				mal_unset_lock(mal_contextLock, "runMALdataflow");
				/* if there are no other instructions in the queue, then simply wait for them */
				if ( task->todo->last ==  0) 
					clk = -3 * DELAYUNIT;
	
				PARDEBUG mnstr_printf(GDKstdout,"#delay %d initial %ld\n", task->id, clk);
				while (clk > 0 ){
					/* speed up wake up when we have memory or too many sleepers */
					factor = MT_getrss()/(MEMORY_THRESHOLD * monet_memory);
					delay = (long)( DELAYUNIT * (factor > 1.0 ? 1.0:factor));
					delay = (long) (delay * (1.0 - asleep/GDKnr_threads));
					if ( delay)
						MT_sleep_ms( delay );
					clk -= DELAYUNIT;
				}
				mal_set_lock(mal_contextLock, "runMALdataflow");
				asleep--;
				mal_unset_lock(mal_contextLock, "runMALdataflow");
				PARDEBUG mnstr_printf(GDKstdout,"#delayed finished thread %d asleep %d\n", task->id, asleep);
			}
		}
	}
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
	THRdel(thr);
}

/*
 * The dataflow administration is based on administration of
 * how many variables are still missing before it can be executed.
 * For each instruction we keep a list of instructions whose
 * blocking counter should be decremented upon finishing it.
 */
static void
DFLOWinit(DataFlow flow, Client cntxt, MalBlkPtr mb, MalStkPtr stk, int size)
{
	int pc, i, j, k, l, n, etop = 0;
	int *assign;
	InstrPtr p;

	PARDEBUG printf("Initialize dflow block\n");
	assign = (int*)GDKzalloc(mb->vtop * sizeof(int));
	etop = flow->stop - flow->start;
	for (n = 0, pc = flow->start; pc < flow->stop; pc++, n++) {
		p = getInstrPtr(mb, pc);

		/* initial state, ie everything can run */
		flow->status[n].cntxt = cntxt;
		flow->status[n].mb = mb;
		flow->status[n].stk = stk;
		flow->status[n].pc = pc;
		flow->status[n].state = DFLOWpending;
		flow->status[n].cost = -1;
		flow->status[n].error = NULL;

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
					(void)size;
					assert(etop < size);
				} else {
					flow->nodes[k] = n;
					flow->edges[k] = -1;
				}

				flow->status[n].blocks++;
			}

			/* list of instructions to be woken up explicitly */
			if ( !isVarConstant(mb, getArg(p, j)) ) {
				/* be careful, watch out for garbage collection interference */
				/* those should be scheduled after all its other uses */
				l = getEndOfLife(mb, getArg(p, j));
				if (l != pc && l < flow->stop && l > flow->start) {
					/* add edge to the target instruction for wakeup call */
					PARDEBUG mnstr_printf(GDKstdout, "endoflife for %s is %d -> %d\n", getVarName(mb, getArg(p,j)), n + flow->start, l);
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
static void showFlowStatus(DataFlow flow, int pc)
{
	int i;
	FlowStatus fs= flow->status;

		mnstr_printf(GDKstdout, "#end of data flow %d done %d \n", pc, flow->stop - flow->start);
		for (i = 0; i < flow->stop - flow->start; i++)
			if (fs[i].state != DFLOWwrapup && fs[i].pc >= 0) {
				mnstr_printf(GDKstdout, "#missed pc %d status %d %d  blocks %d", fs[i].state, i, fs[i].pc, fs[i].blocks);
				printInstruction(GDKstdout, fs[i].mb, 0, getInstrPtr(fs[i].mb, fs[i].pc), LIST_MAL_STMT | LIST_MAPI);
			}
}

static str
DFLOWscheduler(DataFlow flow)
{
	int queued = 0, oldq = 0, last;
	int i,pc = 0;
#ifdef USE_DFLOW_ADMISSION
	int j;
	InstrPtr p;
#endif
	int todo = flow->stop - flow->start;
	str ret = MAL_SUCCEED;
	FlowStatus fs, f = 0;

	if (todo == 0)
		throw(MAL, "dataflow", "Empty dataflow block");
	/* initialize the eligible statements */
	fs = flow->status;

/* old code
    assert(f->stk->wrapup == 0);
    assert(f->stk->admit == 0);
 */

	if (fs[0].cntxt->flags & timerFlag)
		fs[0].cntxt->timer = GDKusec();

	/* enter all dependencies before releasing the queue  */
	MT_lock_set(&flow->todo->l, "q_enqueue");
	for (i = 0; i < todo; i++)
		if (flow->status[i].blocks == 0) {
#ifdef USE_DFLOW_ADMISSION
			p = getInstrPtr(fs[0].mb, flow->start + i );
			for (j = p->retc; j < p->argc; j++)
				flow->status[i].argclaim += getMemoryClaim(flow->status[0].mb, flow->status[0].stk, p, j, FALSE);
#endif
			queued++;
			flow->status[i].state = DFLOWrunning;
			PARDEBUG mnstr_printf(GDKstdout, "#enqueue pc=%d claim=" LLFMT " queue %d\n", flow->status[i].pc, flow->status[i].argclaim, queued);
			q_enqueue_(flow->todo, flow->status + i);
		}
	MT_lock_unset(&flow->todo->l, "q_enqueue");
	while (oldq++ < queued)
		MT_sema_up(&flow->todo->s, "q_enqueue");

	/* consume the remainder */
	PARDEBUG mnstr_printf(GDKstdout, "#run %d instructions in dataflow block\n", todo);
	while (queued || todo > 0 ) {
		PARDEBUG mnstr_printf(GDKstdout, "#waiting for results, queued %d todo %d\n", queued, todo);
		f = q_dequeue(flow->done);
		queued--;
		todo = todo > 0 ? todo -1: 0;

		if (f->pc < 0) {
			PARDEBUG mnstr_printf(GDKstdout, "#errors encountered %s ", f->error ? f->error : "unknown");
			if (ret == MAL_SUCCEED)
				ret = f->error;
			else {
				/* collect all errors encountered */
				str z = (char*)GDKmalloc(strlen(ret) + strlen(f->error) + 2);
				if (z) {
					strcpy(z, ret);
					if (z[strlen(z) - 1] != '\n') strcat(z, "\n");
					strcat(z, f->error);
					GDKfree(f->error);
					GDKfree(ret);
					ret = z;
				}
			}
			/* first error terminates the batch a.s.a.p. */
			todo = 0;
		}

		/*
		 * When an instruction is finished we have to reduce the blocked
		 * counter for all dependent instructions.  for those where it
		 * drops to zero we can scheduler it Moreover, we add the return
		 * variable claim size to the target instruction and remember
		 * the last increment as hotclaim.
		 */
		f->state = DFLOWwrapup;
		last = ABS(f->pc) - flow->start;
		PARDEBUG mnstr_printf(GDKstdout, "#finished pc=%d claim " LLFMT "\n", f->pc, f->hotclaim);

		/* enter all dependencies before releasing the queue  */
		MT_lock_set(&flow->todo->l, "q_enqueue");

		oldq = queued;
		if ( f->pc > 0 )
			for (; last >= 0 && (i = flow->nodes[last]) > 0; last = flow->edges[last])
				if (flow->status[i].state == DFLOWpending) {
					flow->status[i].argclaim += f->hotclaim;
					if (flow->status[i].blocks == 1 && ret == MAL_SUCCEED) {
						queued++;
						q_enqueue_(flow->todo, flow->status + i);
						flow->status[i].state = DFLOWrunning;
						flow->status[i].blocks--;
						PARDEBUG
							mnstr_printf(GDKstdout, "#enqueue pc=%d claim= " LLFMT " queued= %d\n", flow->status[i].pc, flow->status[i].argclaim, queued);
					} else {
						if (ret == MAL_SUCCEED)
							PARDEBUG mnstr_printf(GDKstdout, "#await   pc %d block %d claim= " LLFMT "\n", flow->start + i, flow->status[i].blocks, flow->status[i].argclaim);
						flow->status[i].blocks--;
					}
				} else { /* worker stole the candidate */
					PARDEBUG mnstr_printf(GDKstdout, "#woke up pc %d block %d claim " LLFMT "\n", flow->start + i, flow->status[i].blocks, flow->status[i].argclaim);
					queued++;
					oldq++;
				}
		if (0 && oldq != queued) /* invalidate */
			queue_sort(flow->todo);
		MT_lock_unset(&flow->todo->l, "q_enqueue");

		if (ret == MAL_SUCCEED)
			while (oldq++ < queued)
				MT_sema_up(&flow->todo->s, "q_enqueue");
	}
	PARDEBUG showFlowStatus(flow,pc);
	return ret;
}

static DataFlow flows = NULL;
static int workerid = 0;

str runMALdataflow(Client cntxt, MalBlkPtr mb, int startpc,
				   int stoppc, MalStkPtr stk, MalStkPtr env, InstrPtr pcicaller)
{
	DataFlow flow = NULL;
	str ret = MAL_SUCCEED;
	int size;

#ifdef DEBUG_FLOW
	mnstr_printf(GDKstdout, "runMALdataflow for block %d - %d\n", startpc, stoppc);
	printFunction(GDKstdout, mb, 0, LIST_MAL_STMT | LIST_MAPI);
#endif

	(void)env;
	(void)pcicaller;

	/* in debugging mode we should not start multiple threads */
	if (stk->cmd)
		return MAL_SUCCEED;

	assert(stoppc > startpc);
	mal_set_lock(mal_contextLock, "runMALdataflow");
	flow = flows;

	if (flow) {
		flows = flow->free;
	} else {
		int i;

		flow = (DataFlow)GDKzalloc(sizeof(DataFlowRec));

		/* seems enough for the time being */
		flow->done = q_create(2048);
		flow->todo = q_create(2048);

		/* queues are available? */
		if (flow->done == NULL || flow->todo == NULL) {
			mal_unset_lock(mal_contextLock, "runMALdataflow");
			return MAL_SUCCEED;
		}

		flow->worker = NULL;
		flow->nway = GDKnr_threads ? GDKnr_threads : 1;
		flow->worker = (FlowTask *)GDKzalloc(sizeof(FlowTask) * flow->nway);
		for (i = 0; i < flow->nway; i++) {
			flow->worker[i].id = workerid++;
			flow->worker[i].todo = flow->todo;
			flow->worker[i].flow = flow;
			/* create the thread and let it wait */
			MT_create_thread(&flow->worker[i].tid, runDFLOWworker, flow->worker + i, MT_THR_DETACHED);
		}
	}
	/* keep real block count, exclude brackets */
	flow->start = startpc + 1;
	flow->stop = stoppc;

	flow->status = (FlowStatus)GDKzalloc((flow->stop - flow->start + 1) * sizeof(FlowStatusRec));
	size = DFLOWgraphSize(mb, startpc, stoppc);
	flow->nodes = (int*)GDKzalloc(sizeof(int) * size);
	flow->edges = (int*)GDKzalloc(sizeof(int) * size);
	DFLOWinit(flow, cntxt, mb, stk, size);
	mal_unset_lock(mal_contextLock, "runMALdataflow");

	ret = DFLOWscheduler(flow);
	GDKfree(flow->status);
	flow->status = 0;
	GDKfree(flow->edges);
	flow->edges = 0;
	GDKfree(flow->nodes);
	flow->nodes = 0;
	mal_set_lock(mal_contextLock, "runMALdataflow");
	flow->free = flows;
	flows = flow;
	mal_unset_lock(mal_contextLock, "runMALdataflow");
	return ret;
}
