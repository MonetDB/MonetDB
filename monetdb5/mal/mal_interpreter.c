/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * Author M. Kersten
 * The MAL Interpreter
 */
#include "monetdb_config.h"
#include "mal_runtime.h"
#include "mal_interpreter.h"
#include "mal_resource.h"
#include "mal_listing.h"
#include "mal_debugger.h"   /* for mdbStep() */
#include "mal_type.h"
#include "mal_private.h"

static lng qptimeout = 0; /* how often we print still running queries (usec) */

void
setqptimeout(lng usecs)
{
	qptimeout = usecs;
}

inline
ptr getArgReference(MalStkPtr stk, InstrPtr pci, int k)
{
	/* the C standard says: "A pointer to a union object, suitably
	 * converted, points to each of its members (or if a member is a
	 * bit-field, then to the unit in which it resides), and vice
	 * versa." */
	return (ptr) &stk->stk[pci->argv[k]].val;
}

str malCommandCall(MalStkPtr stk, InstrPtr pci)
{
	str ret= MAL_SUCCEED;

	switch (pci->argc) {
	case 0: ret = (*pci->fcn)();
		break;
	case 1: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0));
		break;
	case 2: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1));
		break;
	case 3: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2));
		break;
	case 4: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3));
		break;
	case 5: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4));
		break;
	case 6: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5));
		break;
	case 7: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6));
		break;
	case 8: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7));
		break;
	case 9: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8));
		break;
	case 10: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9));
		break;
	case 11: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10));
		break;
	case 12: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10),
			getArgReference(stk, pci, 11));
		break;
	case 13: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10),
			getArgReference(stk, pci, 11),
			getArgReference(stk, pci, 12));
		break;
	case 14: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10),
			getArgReference(stk, pci, 11),
			getArgReference(stk, pci, 12),
			getArgReference(stk, pci, 13));
		break;
	case 15: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10),
			getArgReference(stk, pci, 11),
			getArgReference(stk, pci, 12),
			getArgReference(stk, pci, 13),
			getArgReference(stk, pci, 14));
		break;
	case 16: ret = (*pci->fcn)(
			getArgReference(stk, pci, 0),
			getArgReference(stk, pci, 1),
			getArgReference(stk, pci, 2),
			getArgReference(stk, pci, 3),
			getArgReference(stk, pci, 4),
			getArgReference(stk, pci, 5),
			getArgReference(stk, pci, 6),
			getArgReference(stk, pci, 7),
			getArgReference(stk, pci, 8),
			getArgReference(stk, pci, 9),
			getArgReference(stk, pci, 10),
			getArgReference(stk, pci, 11),
			getArgReference(stk, pci, 12),
			getArgReference(stk, pci, 13),
			getArgReference(stk, pci, 14),
			getArgReference(stk, pci, 15));
		break;
	default:
		throw(MAL, "mal.interpreter", "too many arguments for command call");
	}
	return ret;
}

/*
 * Copy the constant values onto the stack frame
 * Also we cannot overwrite values on the stack as this maybe part of a
 * sequence of factory calls.
 */
#define initStack(S, R)\
	for (i = S; i < mb->vtop; i++) {\
		lhs = &stk->stk[i];\
		if (isVarConstant(mb, i) > 0) {\
			if (!isVarDisabled(mb, i)) {\
				rhs = &getVarConstant(mb, i);\
				if(VALcopy(lhs, rhs) == NULL) \
					R = 0; \
			}\
		} else {\
			lhs->vtype = getVarGDKType(mb, i);\
			lhs->val.pval = 0;\
			lhs->len = 0;\
		}\
	} 

int
isNotUsedIn(InstrPtr p, int start, int a)
{
	int k;
	for (k = start; k < p->argc; k++)
		if (getArg(p, k) == a)
			return 0;
	return 1;
}

MalStkPtr
prepareMALstack(MalBlkPtr mb, int size)
{
	MalStkPtr stk = NULL;
	int i, res = 1;
	ValPtr lhs, rhs;

	stk = newGlobalStack(size);
	if (!stk)
		return NULL;
	stk->stktop = mb->vtop;
	stk->blk = mb;
	stk->workers = 0;
	stk->memory = 0;
	initStack(0, res);
	if(!res) {
		freeStack(stk);
		return NULL;
	}
	return stk;
}

str runMAL(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr env)
{
	MalStkPtr stk = NULL;
	int i;
	ValPtr lhs, rhs;
	str ret;
	(void) mbcaller;

	/* Prepare a new interpreter call. This involves two steps, (1)
	 * allocate the minimum amount of stack space needed, some slack
	 * resources are included to permit code optimizers to add a few
	 * variables at run time, (2) copying the arguments into the new
	 * stack frame.
	 *
	 * The env stackframe is set when a MAL function is called
	 * recursively.  Alternatively, there is no caller but a stk to be
	 * re-used for interpretation.  We assume here that it aligns with
	 * the variable table of the routine being called.
	 *
	 * allocate space for value stack the global stack should be large
	 * enough
	 */
	cntxt->lastcmd= time(0);
	ATOMIC_SET(&cntxt->lastprint, GDKusec());
	if (env != NULL) {
		int res = 1;
		stk = env;
		if (mb != stk->blk)
			throw(MAL, "mal.interpreter","misalignment of symbols");
		if (mb->vtop > stk->stksize)
			throw(MAL, "mal.interpreter","stack too small");
		initStack(env->stkbot, res);
		if(!res)
			throw(MAL, "mal.interpreter", MAL_MALLOC_FAIL);
	} else {
		stk = prepareMALstack(mb, mb->vsize);
		if (stk == 0)
			throw(MAL, "mal.interpreter", MAL_STACK_FAIL);
		stk->blk = mb;
		stk->cmd = cntxt->itrace;    /* set debug mode */
		/*safeguardStack*/
		if( env){
			stk->stkdepth = stk->stksize + env->stkdepth;
			stk->calldepth = env->calldepth + 1;
			stk->up = env;
			if (stk->calldepth > 256)
				throw(MAL, "mal.interpreter", MAL_CALLDEPTH_FAIL);
		}
		/*
		 * An optimization is to copy all constant variables used in
		 * functions immediately onto the value stack. Then we do not
		 * have to check for their location later on any more. At some
		 * point, the effect is optimal, if at least several constants
		 * are referenced in a function (a gain on tst400a of 20% has
		 * been observed due the small size of the function).
		 */
	}
	if (stk->cmd && env && stk->cmd != 'f')
		stk->cmd = env->cmd;
	ret = runMALsequence(cntxt, mb, 1, 0, stk, env, 0);

	/* pass the new debug mode to the caller */
	if (stk->cmd && env && stk->cmd != 'f')
		env->cmd = stk->cmd;
	if (!stk->keepAlive && garbageControl(getInstrPtr(mb, 0)))
		garbageCollector(cntxt, mb, stk, env != stk);
	if (stk && stk != env)
		freeStack(stk);
	if (ret == MAL_SUCCEED && cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout)
		throw(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
	return ret;
}

/* Single instruction
 * It is possible to re-enter the interpreter at a specific place.
 * This is used in the area where we need to support co-routines.
 *
 * A special case for MAL interpretation is to execute just one instruction.
 * This is typically used by optimizers and schedulers that need part of the
 * answer to direct their actions. Or, a dataflow scheduler could step in
 * to enforce a completely different execution order.
 */
str reenterMAL(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, MalStkPtr stk)
{
	str ret;
	int keepAlive;

	if (stk == NULL)
		throw(MAL, "mal.interpreter", MAL_STACK_FAIL);
	keepAlive = stk->keepAlive;
	ret = runMALsequence(cntxt, mb, startpc, stoppc, stk, 0, 0);

	/* pass the new debug mode to the caller */
	if (keepAlive == 0 && garbageControl(getInstrPtr(mb, 0)))
		garbageCollector(cntxt, mb, stk, stk != 0);
	return ret;
}

/*
 * Front ends may benefit from a more direct call to any of the MAL
 * procedural abstractions. The argument list points to the arguments
 * for the block to be executed. An old stack frame may be re-used,
 * but it is then up to the caller to ensure it is properly
 * initialized.
 * The call does not return values, they are ignored.
 */
str
callMAL(Client cntxt, MalBlkPtr mb, MalStkPtr *env, ValPtr argv[], char debug)
{
	MalStkPtr stk = NULL;
	str ret = MAL_SUCCEED;
	int i;
	ValPtr lhs;
	InstrPtr pci = getInstrPtr(mb, 0);

	cntxt->lastcmd= time(0);

	switch (pci->token) {
	case FUNCTIONsymbol:
	case FCNcall:
		/*
		 * Prepare the stack frame for this operation. Copy all the arguments
		 * in place. We assume that the caller has supplied pointers for
		 * all arguments and return values.
		 */
		if (*env == NULL) {
			stk = prepareMALstack(mb, mb->vsize);
			if (stk == NULL)
				throw(MAL, "mal.interpreter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			stk->up = 0;
			*env = stk;
		} else {
			ValPtr lhs, rhs;
			int res = 1;

			stk = *env;
			initStack(0, res);
			if(!res)
				throw(MAL, "mal.interpreter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		assert(stk);
		for (i = pci->retc; i < pci->argc; i++) {
			lhs = &stk->stk[pci->argv[i]];
			if (VALcopy(lhs, argv[i]) == NULL)
				throw(MAL, "mal.interpreter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			if (lhs->vtype == TYPE_bat)
				BBPretain(lhs->val.bval);
		}
		stk->cmd = debug;
		ret = runMALsequence(cntxt, mb, 1, 0, stk, 0, 0);
		break;
	case FACTORYsymbol:
	case FACcall:
		ret = callFactory(cntxt, mb, argv, debug);
		break;
	case PATcall:
	case CMDcall:
	default:
		throw(MAL, "mal.interpreter", RUNTIME_UNKNOWN_INSTRUCTION);
	}
	if (stk) 
		garbageCollector(cntxt, mb, stk, TRUE);
	if ( ret == MAL_SUCCEED && cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout)
		throw(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
	return ret;
}

/*
 * The core of the interpreter is presented next. It takes the context
 * information and starts the interpretation at the designated
 * instruction.  Note that the stack frame is aligned and initialized
 * in the enclosing routine.  When we start executing the first
 * instruction, we take the wall-clock time for resource management.
 */
str runMALsequence(Client cntxt, MalBlkPtr mb, int startpc,
				   int stoppc, MalStkPtr stk, MalStkPtr env, InstrPtr pcicaller)
{
	ValPtr lhs, rhs, v;
	int i, k;
	InstrPtr pci = 0;
	int exceptionVar;
	str ret = MAL_SUCCEED, localGDKerrbuf= GDKerrbuf;
	ValRecord backups[16];
	ValPtr backup;
	int garbages[16], *garbage;
	int stkpc = 0;
	RuntimeProfileRecord runtimeProfile, runtimeProfileFunction;
	lng lastcheck = 0;
	int	startedProfileQueue = 0;
#define CHECKINTERVAL 1000 /* how often do we check for client disconnect */
	runtimeProfile.ticks = runtimeProfileFunction.ticks = 0;

	if (stk == NULL)
		throw(MAL, "mal.interpreter", MAL_STACK_FAIL);

	/* prepare extended backup and garbage structures */
	if (startpc+1 == stoppc) {
		pci = getInstrPtr(mb, startpc);
		if (pci->argc > 16) {
			backup = GDKmalloc(pci->argc * sizeof(ValRecord));
			garbage = (int*)GDKzalloc(pci->argc * sizeof(int));
			if( backup == NULL || garbage == NULL) {
				GDKfree(backup);
				GDKfree(garbage);
				throw(MAL, "mal.interpreter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		} else {
			backup = backups;
			garbage = garbages;
			memset(garbages, 0, sizeof(garbages));
		}
	} else if ( mb->maxarg > 16 ){
		backup = GDKmalloc(mb->maxarg * sizeof(ValRecord));
		garbage = (int*)GDKzalloc(mb->maxarg * sizeof(int));
		if( backup == NULL || garbage == NULL) {
			GDKfree(backup);
			GDKfree(garbage);
			throw(MAL, "mal.interpreter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	} else {
		backup = backups;
		garbage = garbages;
		memset(garbages, 0, sizeof(garbages));
	}

	/* also produce event record for start of function */
	if ( startpc == 1 &&  startpc < mb->stop ){
		startedProfileQueue = 1;
		runtimeProfileInit(cntxt, mb, stk);
		runtimeProfileBegin(cntxt, mb, stk, getInstrPtr(mb,0), &runtimeProfileFunction);
		mb->starttime = GDKusec();
		if (cntxt->sessiontimeout && mb->starttime - cntxt->session > cntxt->sessiontimeout) {
			if ( backup != backups) GDKfree(backup);
			if ( garbage != garbages) GDKfree(garbage);
			throw(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_SESSION_TIMEOUT);
		}
	} 
	stkpc = startpc;
	exceptionVar = -1;

	while (stkpc < mb->stop && stkpc != stoppc) {
		// incomplete block being executed, requires at least signature and end statement
		pci = getInstrPtr(mb, stkpc);
		if (cntxt->mode == FINISHCLIENT){
			stkpc = stoppc;
			if (ret == MAL_SUCCEED)
				ret= createException(MAL, "mal.interpreter", "prematurely stopped client");
			break;
		}
#ifndef NDEBUG
		if (cntxt->itrace || stk->status) {
			if (stk->status == 'p'){
				// execution is paused
				while ( stk->status == 'p')
					MT_sleep_ms(50);
				continue;
			}
			if ( stk->status == 'q')
				stk->cmd = 'x';

			if (stk->cmd == 0)
				stk->cmd = cntxt->itrace;
			mdbStep(cntxt, mb, stk, stkpc);
			if (stk->cmd == 'x' ) {
				stk->cmd = 0;
				stkpc = mb->stop;
				ret= createException(MAL, "mal.interpreter", "prematurely stopped client");
				break;
			}
		}
#endif

		//Ensure we spread system resources over multiple users as well.
		runtimeProfileBegin(cntxt, mb, stk, pci, &runtimeProfile);
		if (runtimeProfile.ticks > lastcheck + CHECKINTERVAL) {
			if (cntxt->fdin && !mnstr_isalive(cntxt->fdin->s)) {
				cntxt->mode = FINISHCLIENT;
				stkpc = stoppc;
				ret= createException(MAL, "mal.interpreter", "prematurely stopped client");
				break;
			}
			lastcheck = runtimeProfile.ticks;
		}

		if (qptimeout > 0) {
			lng t = GDKusec();
			ATOMIC_BASE_TYPE lp = ATOMIC_GET(&cntxt->lastprint);
			if ((lng) lp + qptimeout < t) {
				/* if still the same, replace lastprint with current
				 * time and print the query */
				if (ATOMIC_CAS(&cntxt->lastprint, &lp, t)) {
					const char *q = cntxt->getquery ? cntxt->getquery(cntxt) : NULL;
					TRC_INFO(MAL_SERVER, "%s: query already running "LLFMT"s: %.200s\n",
							cntxt->mythread->name,
							(lng) (time(0) - cntxt->lastcmd),
							q ? q : "");
				}
			}
		}

		/* The interpreter loop
		 * The interpreter is geared towards execution a MAL
		 * procedure together with all its descendant
		 * invocations. As such, it provides the MAL abtract
		 * machine processor.
		 *
		 * The value-stack frame of the surrounding scope is
		 * needed to resolve binding values.  Getting (putting) a
		 * value from (into) a surrounding scope should be guarded
		 * with the exclusive access lock.  This situation is
		 * encapsulated by a bind() function call, whose
		 * parameters contain the access mode required.
		 *
		 * The formal procedure arguments are assumed to always
		 * occupy the first elements in the value stack.
		 *
		 * Before we execute an instruction the variables to be
		 * garbage collected are identified. In the post-execution
		 * phase they are removed.
		 */
		for (i = 0; i < pci->retc; i++) 
			backup[i] = stk->stk[getArg(pci, i)];

		if (garbageControl(pci)) {
			for (i = 0; i < pci->argc; i++) {
				int a = getArg(pci, i);

				if (stk->stk[a].vtype == TYPE_bat && getEndScope(mb, a) == stkpc && isNotUsedIn(pci, i + 1, a))
					garbage[i] = a;
				else
					garbage[i] = -1;
			}
		}

		freeException(ret);
		ret = MAL_SUCCEED;
		switch (pci->token) {
		case ASSIGNsymbol:
			/* Assignment command
			 * The assignment statement copies values around on
			 * the stack frame, including multiple assignments.
			 *
			 * Pushing constants/initial values onto the stack is
			 * a separate operation.  It takes the constant value
			 * discovered at compile time and stored in the symbol
			 * table and moves it to the stackframe location. This
			 * activity is made part of the start-up procedure.
			 *
			 * The before after calls should be reconsidered here,
			 * because their. They seem superflous and the way
			 * they are used will cause errors in multi-assignment
			 * statements.
			 */
			for (k = 0, i = pci->retc; k < pci->retc && i < pci->argc; i++, k++) {
				lhs = &stk->stk[pci->argv[k]];
				rhs = &stk->stk[pci->argv[i]];
				if(VALcopy(lhs, rhs) == NULL) {
					ret = createException(MAL, "mal.interpreter", MAL_MALLOC_FAIL);
					break;
				} else if (lhs->vtype == TYPE_bat && !is_bat_nil(lhs->val.bval))
					BBPretain(lhs->val.bval);
			}
			break;
		case PATcall:
			if (pci->fcn == NULL) {
				ret = createException(MAL,"mal.interpreter", "address of pattern %s.%s missing", pci->modname, pci->fcnname);
			} else {
				ret = (*pci->fcn)(cntxt, mb, stk, pci);
#ifndef NDEBUG
				if (ret == MAL_SUCCEED) {
					/* check that the types of actual results match
					 * expected results */
					for (i = 0; i < pci->retc; i++) {
						int a = getArg(pci, i);
						int t = getArgType(mb, pci, i);

						if (isaBatType(t)) {
							bat bid = stk->stk[a].val.bval;
							BAT *_b = BATdescriptor(bid);
							t = getBatType(t);
							assert(stk->stk[a].vtype == TYPE_bat);
							assert(is_bat_nil(bid) ||
								   t == TYPE_any ||
								   ATOMtype(_b->ttype) == ATOMtype(t));
							if(_b) BBPunfix(bid);
						} else {
							assert(t == stk->stk[a].vtype);
						}
					}
				}
#endif
			}
			break;
		case CMDcall:
			ret = malCommandCall(stk, pci);
#ifndef NDEBUG
			if (ret == MAL_SUCCEED) {
				/* check that the types of actual results match
				 * expected results */
				for (i = 0; i < pci->retc; i++) {
					int a = getArg(pci, i);
					int t = getArgType(mb, pci, i);

					if (isaBatType(t)) {
						bat bid = stk->stk[a].val.bval;
						t = getBatType(t);
						assert(stk->stk[a].vtype == TYPE_bat);
						assert(is_bat_nil(bid) ||
							   t == TYPE_any ||
							   ATOMtype(BBP_desc(bid)->ttype) == ATOMtype(t));
					} else {
						assert(t == stk->stk[a].vtype);
					}
				}
			}
#endif
			break;
		case FACcall:
			/*
			 * Factory calls are more involved. At this stage it
			 * is a synchrononous call to the factory manager.
			 * Factory calls should deal with the reference
			 * counting.
			 */
			if (pci->blk == NULL)
				ret = createException(MAL,"mal.interpreter", "%s.%s[%d] reference to MAL function missing", getModuleId(pci), getFunctionId(pci), pci->pc);
			else {
				/* show call before entering the factory */
#ifndef NDEBUG
				if (cntxt->itrace) {
					if (stk->cmd == 0)
						stk->cmd = cntxt->itrace;
					mdbStep(cntxt, pci->blk, stk, 0);
					if (stk->cmd == 'x') {
						stk->cmd = 0;
						stkpc = mb->stop;
					}
				}
#endif
				ret = runFactory(cntxt, pci->blk, mb, stk, pci);
			}
			break;
		case FCNcall:
			/*
			 * MAL function calls are relatively expensive,
			 * because they have to assemble a new stack frame and
			 * do housekeeping, such as garbagecollection of all
			 * non-returned values.
			 */
			{	MalStkPtr nstk;
				InstrPtr q;
				int ii, arg;

				stk->pcup = stkpc;
				nstk = prepareMALstack(pci->blk, pci->blk->vsize);
				if (nstk == 0){
					ret= createException(MAL,"mal.interpreter",MAL_STACK_FAIL);
					break;
				}

				/*safeguardStack*/
				nstk->stkdepth = nstk->stksize + stk->stkdepth;
				nstk->calldepth = stk->calldepth + 1;
				nstk->up = stk;
				if (nstk->calldepth > 256) {
					ret= createException(MAL, "mal.interpreter", MAL_CALLDEPTH_FAIL);
					GDKfree(nstk);
					break;
				}
				if ((unsigned)nstk->stkdepth > THREAD_STACK_SIZE / sizeof(mb->var[0]) / 4 && THRhighwater()){
					/* we are running low on stack space */
					ret= createException(MAL, "mal.interpreter", MAL_STACK_FAIL);
					GDKfree(nstk);
					break;
				}

				/* copy arguments onto destination stack */
				q= getInstrPtr(pci->blk,0);
				arg = q->retc;
				for (ii = pci->retc; ii < pci->argc; ii++,arg++) {
					lhs = &nstk->stk[q->argv[arg]];
					rhs = &stk->stk[pci->argv[ii]];
					if(VALcopy(lhs, rhs) == NULL) {
						GDKfree(nstk);
						ret = createException(MAL, "mal.interpreter", MAL_MALLOC_FAIL);
						break;
					} else if (lhs->vtype == TYPE_bat)
						BBPretain(lhs->val.bval);
				}
				if (ret == MAL_SUCCEED && ii == pci->argc) {
					ret = runMALsequence(cntxt, pci->blk, 1, pci->blk->stop, nstk, stk, pci);
					for (ii = 0; ii < nstk->stktop; ii++)
						if (ATOMextern(nstk->stk[ii].vtype))
							GDKfree(nstk->stk[ii].val.pval);
					GDKfree(nstk);
				}
			}
			break;
		case NOOPsymbol:
		case REMsymbol:
			break;
		case ENDsymbol:
			if (getInstrPtr(mb, 0)->token == FACTORYsymbol)
				ret = shutdownFactory(cntxt, mb);
			runtimeProfileExit(cntxt, mb, stk, pci, &runtimeProfile);
			runtimeProfileExit(cntxt, mb, stk, getInstrPtr(mb,0), &runtimeProfileFunction);
			if (pcicaller && garbageControl(getInstrPtr(mb, 0)))
				garbageCollector(cntxt, mb, stk, TRUE);
			if (cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout){
				freeException(ret); /* overrule exception */
				ret= createException(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
				break;
			}
			stkpc = mb->stop;	// force end of loop
			continue;
		default: {
			str w;
			if (pci->token < 0) {
				/* temporary NOOP instruction */
				break;
			}
			w= instruction2str(mb, 0, pci, FALSE);
			if(w) {
				ret = createException(MAL,"interpreter", "unkown operation:%s", w);
				GDKfree(w);
			} else {
				ret = createException(MAL,"interpreter", "failed instruction2str");
			}
			// runtimeProfileBegin already sets the time in the instruction
			if (cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout){
				freeException(ret);	/* in case it's set */
				ret = createException(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
				break;
			}

			stkpc= mb->stop;
			continue;
		}	}

		/* monitoring information should reflect the input arguments,
		   which may be removed by garbage collection  */
		/* BEWARE, the SQL engine or MAL function could zap the block, leaving garbage behind in pci */
		/* this hack means we loose a closing event */
		if( mb->stop <= 1)
			continue;
		runtimeProfileExit(cntxt, mb, stk, pci, &runtimeProfile);
		/* check for strong debugging after each MAL statement */
		/* when we find a timeout situation, then the result is already known 
		 * and assigned,  the backup version is not removed*/
		if ( pci->token != FACcall && ret== MAL_SUCCEED) {
			for (i = 0; i < pci->retc; i++) {
				lhs = &backup[i];
				if (BATatoms[lhs->vtype].atomUnfix &&
					(*BATatoms[lhs->vtype].atomUnfix)(VALget(lhs)) != GDK_SUCCEED) {
					ret = createException(MAL, "mal.propertyCheck", GDK_EXCEPTION);
				}
				if (ATOMextern(lhs->vtype) &&
					lhs->val.pval &&
					lhs->val.pval != ATOMnilptr(lhs->vtype) &&
					lhs->val.pval != stk->stk[getArg(pci, i)].val.pval)
					GDKfree(lhs->val.pval);
			}
			if (GDKdebug & (CHECKMASK|PROPMASK) && exceptionVar < 0) {
				BAT *b;

				for (i = 0; i < pci->retc; i++) {
					if (garbage[i] == -1 && stk->stk[getArg(pci, i)].vtype == TYPE_bat &&
						!is_bat_nil(stk->stk[getArg(pci, i)].val.bval)) {
						assert(stk->stk[getArg(pci, i)].val.bval > 0);
						b = BBPquickdesc(stk->stk[getArg(pci, i)].val.bval, false);
						if (b == NULL) {
							if (ret == MAL_SUCCEED)
								ret = createException(MAL, "mal.propertyCheck", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
							continue;
						}
						b = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
						if (b) {
							BATassertProps(b);
							BBPunfix(b->batCacheid);
						}
					}
				}
			}

			/* general garbage collection */
			if (ret == MAL_SUCCEED && garbageControl(pci)) {
				for (i = 0; i < pci->argc; i++) {
					int a = getArg(pci, i);

					if (isaBatType(getArgType(mb, pci, i))) {
						bat bid = stk->stk[a].val.bval;

						if (garbage[i] >= 0) {
							bid = stk->stk[garbage[i]].val.bval;
							stk->stk[garbage[i]].val.bval = bat_nil;
							BBPrelease(bid);
						}
					}
				}
			}
		}

		/* Exception handling */
		if (localGDKerrbuf && localGDKerrbuf[0]) {
			if( ret == MAL_SUCCEED)
				ret = createException(MAL,"mal.interpreter",GDK_EXCEPTION);
			// TODO take properly care of the GDK exception
			localGDKerrbuf[0]=0;
		}

		if (ret != MAL_SUCCEED) {
			str msg = 0;

#ifndef NDEBUG
			if (stk->cmd) {
				mnstr_printf(cntxt->fdout, "!ERROR: %s\n", ret);
				stk->cmd = '\n'; /* in debugging go to step mode */
				mdbStep(cntxt, mb, stk, stkpc);
				if (stk->cmd == 'x' || stk->cmd == 'q' ) {
					stkpc = mb->stop;
					continue;
				}
				if (stk->cmd == 'r') {
					stk->cmd = 'n';
					stkpc = startpc;
					exceptionVar = -1;
					continue;
				}
			}
#endif
			/* Detect any exception received from the implementation. */
			/* The first identifier is an optional exception name */
			if (strstr(ret, "!skip-to-end")) {
				freeException(ret);
				ret = MAL_SUCCEED;
				stkpc = mb->stop;
				continue;
			}
			/*
			 * Exceptions are caught based on their name, which is part of the
			 * exception message. The ANYexception variable catches all.
			 */
			exceptionVar = -1;
			msg = strchr(ret, ':');
			if (msg) {
				exceptionVar = findVariableLength(mb, ret, (int)(msg - ret));
			}
			if (exceptionVar == -1)
				exceptionVar = findVariable(mb, "ANYexception");

			/* unknown exceptions lead to propagation */
			if (exceptionVar == -1) {
                if (cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout)
                    ret= createException(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
				stkpc = mb->stop;
				continue;
			}
			/* assure correct variable type */
			if (getVarType(mb, exceptionVar) == TYPE_str) {
				/* watch out for concurrent access */
				MT_lock_set(&mal_contextLock);
				v = &stk->stk[exceptionVar];
				if (v->val.sval)
					freeException(v->val.sval);    /* old exception*/
				v->vtype = TYPE_str;
				v->val.sval = ret;
				v->len = strlen(v->val.sval);
				ret = MAL_SUCCEED;
				MT_lock_unset(&mal_contextLock);
			} else {
				mnstr_printf(cntxt->fdout, "%s", ret);
				freeException(ret);
				ret = MAL_SUCCEED;
			}
			/* position yourself at the catch instruction for further decisions */
			/* skipToCatch(exceptionVar,@2,@3) */
#ifndef NDEBUG
			if (stk->cmd == 'C') {
				stk->cmd = 'n';
				mdbStep(cntxt, mb, stk, stkpc);
				if (stk->cmd == 'x' ) {
					stkpc = mb->stop;
					continue;
				}
			}
#endif
			/* skip to catch block or end */
			for (; stkpc < mb->stop; stkpc++) {
				InstrPtr l = getInstrPtr(mb, stkpc);
				if (l->barrier == CATCHsymbol) {
					int j;
					for (j = 0; j < l->retc; j++)
						if (getArg(l, j) == exceptionVar)
							break;
						else if (strcmp(getArgName(mb, l, j), "ANYexception") == 0)
							break;
					if (j < l->retc)
						break;
				}
			}
			if (stkpc == mb->stop) {
				if (cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout){
					ret= createException(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
					stkpc = mb->stop;
				}
				continue;
			}
			pci = getInstrPtr(mb, stkpc);
		}

		/*
		 * After the expression has been evaluated we should check for
		 * a possible change in the control flow.
		 */
		switch (pci->barrier) {
		case BARRIERsymbol:
			v = &stk->stk[getDestVar(pci)];
			/* skip to end of barrier, depends on the type */
			switch (v->vtype) {
			case TYPE_bit:
				if (v->val.btval == FALSE || is_bit_nil(v->val.btval))
					stkpc = pci->jump;
				break;
			case TYPE_bte:
				if (is_bte_nil(v->val.btval))
					stkpc = pci->jump;
				break;
			case TYPE_oid:
				if (is_oid_nil(v->val.oval))
					stkpc = pci->jump;
				break;
			case TYPE_sht:
				if (is_sht_nil(v->val.shval))
					stkpc = pci->jump;
				break;
			case TYPE_int:
				if (is_int_nil(v->val.ival))
					stkpc = pci->jump;
				break;
			case TYPE_lng:
				if (is_lng_nil(v->val.lval))
					stkpc = pci->jump;
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				if (is_hge_nil(v->val.hval))
					stkpc = pci->jump;
				break;
#endif
			case TYPE_flt:
				if (is_flt_nil(v->val.fval))
					stkpc = pci->jump;
				break;
			case TYPE_dbl:
				if (is_dbl_nil(v->val.dval))
					stkpc = pci->jump;
				break;
			case TYPE_str:
				if (strNil(v->val.sval))
					stkpc = pci->jump;
				break;
			default:
				ret = createException(MAL,"mal.interpreter", "%s: Unknown barrier type", getVarName(mb, getDestVar(pci)));
			}
			stkpc++;
			break;
		case LEAVEsymbol:
		case REDOsymbol:
			v = &stk->stk[getDestVar(pci)];
			/* skip to end of barrier, depending on the type */
			switch (v->vtype) {
			case TYPE_bit:
				if (v->val.btval == TRUE )
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_str:
				if (!strNil(v->val.sval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_oid:
				if (!is_oid_nil(v->val.oval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_sht:
				if (!is_sht_nil(v->val.shval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_int:
				if (!is_int_nil(v->val.ival))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_bte:
				if (!is_bte_nil(v->val.btval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_lng:
				if (!is_lng_nil(v->val.lval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				if (!is_hge_nil(v->val.hval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
#endif
			case TYPE_flt:
				if (!is_flt_nil(v->val.fval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			case TYPE_dbl:
				if (!is_dbl_nil(v->val.dval))
					stkpc = pci->jump;
				else
					stkpc++;
				break;
			default:
				break;
			}
			break;
		case CATCHsymbol:
			/* catch blocks are skipped unless
			   searched for explicitly*/
			if (exceptionVar < 0) {
				stkpc = pci->jump;
				break;
			}
			exceptionVar = -1;
			stkpc++;
			break;
		case EXITsymbol:
			if (getDestVar(pci) == exceptionVar)
				exceptionVar = -1;
			stkpc++;
			break;
		case RAISEsymbol:
			exceptionVar = getDestVar(pci);
			//freeException(ret);
			ret = MAL_SUCCEED;
			if (getVarType(mb, getDestVar(pci)) == TYPE_str) {
				char nme[256];
				snprintf(nme,256,"%s.%s[%d]", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb,0)), stkpc);
				ret = createException(MAL, nme, "%s", stk->stk[getDestVar(pci)].val.sval);
			}
			/* skipToCatch(exceptionVar, @2, stk) */
#ifndef NDEBUG
			if (stk->cmd == 'C') {
				stk->cmd = 'n';
				mdbStep(cntxt, mb, stk, stkpc);
				if (stk->cmd == 'x' ) {
					stkpc = mb->stop;
					continue;
				}
			}
#endif
			/* skip to catch block or end */
			for (; stkpc < mb->stop; stkpc++) {
				InstrPtr l = getInstrPtr(mb, stkpc);
				if (l->barrier == CATCHsymbol) {
					int j;
					for (j = 0; j < l->retc; j++)
						if (getArg(l, j) == exceptionVar)
							break;
						else if (strcmp(getArgName(mb, l, j), "ANYexception") == 0)
							break;
					if (j < l->retc)
						break;
				}
			}
			if (stkpc == mb->stop) {
				runtimeProfileExit(cntxt, mb, stk, pci, &runtimeProfile);
				runtimeProfileExit(cntxt, mb, stk, getInstrPtr(mb,0), &runtimeProfileFunction);
				break;
			}
			if (stkpc == mb->stop)
				ret = mb->errors = createMalException(mb, stkpc, TYPE,
					"Exception raised\n");
			break;
		case YIELDsymbol:     /* to be defined */
			if( startedProfileQueue)
				runtimeProfileFinish(cntxt, mb, stk);
			if ( backup != backups) GDKfree(backup);
			if ( garbage != garbages) GDKfree(garbage);
			return yieldFactory(mb, pci, stkpc);
		case RETURNsymbol:
			/* Return from factory involves cleanup */

			if (getInstrPtr(mb, 0)->token == FACTORYsymbol) {
				yieldResult(mb, pci, stkpc);
				shutdownFactory(cntxt, mb);
			} else {
				/* a fake multi-assignment */
				if (env != NULL && pcicaller != NULL) {
					InstrPtr pp = pci;
					pci = pcicaller;
					for (i = 0; i < pci->retc; i++) {
						rhs = &stk->stk[pp->argv[i]];
						lhs = &env->stk[pci->argv[i]];
						if(VALcopy(lhs, rhs) == NULL) {
							ret = createException(MAL, "mal.interpreter", MAL_MALLOC_FAIL);
							break;
						} else if (lhs->vtype == TYPE_bat)
							BBPretain(lhs->val.bval);
					}
					if (garbageControl(getInstrPtr(mb, 0)))
						garbageCollector(cntxt, mb, stk, TRUE);
					/* reset the clock */
					runtimeProfileExit(cntxt, mb, stk, pp, &runtimeProfile);
					runtimeProfileExit(cntxt, mb, stk, getInstrPtr(mb,0), &runtimeProfileFunction);
				} 
			}
			stkpc = mb->stop;
			continue;
		default:
			stkpc++;
		}
		if (cntxt->querytimeout && mb->starttime && GDKusec()- mb->starttime > cntxt->querytimeout){
			if (ret == MAL_SUCCEED)
				ret= createException(MAL, "mal.interpreter", SQLSTATE(HYT00) RUNTIME_QRY_TIMEOUT);
			stkpc= mb->stop;
		}
	}

	/* if we could not find the exception variable, cascade a new one */
	if (exceptionVar >= 0) {
		char nme[256];
		snprintf(nme,256,"%s.%s[%d]", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb,0)), stkpc);
		if (ret != MAL_SUCCEED) {
			str new, n;
			n = createException(MAL,nme,"exception not caught");
			if (n) {
				new = GDKzalloc(strlen(ret) + strlen(n) +16);
				if (new){
					strcpy(new, ret);
					if( new[strlen(new)-1] != '\n')
						strcat(new,"\n");
					strcat(new,"!");
					strcat(new,n);
					freeException(n);
					freeException(ret);
					ret = new;
				} else ret = n;
			}
		} else {
			ret = createException(MAL, nme, "Exception not caught");
		}
	}
	if( startedProfileQueue)
		runtimeProfileFinish(cntxt, mb, stk);
	if ( backup != backups) GDKfree(backup);
	if ( garbage != garbages) GDKfree(garbage);
	return ret;
}


/*
 * MAL API
 * The linkage between MAL interpreter and compiled C-routines
 * is kept as simple as possible.
 * Basically we distinguish four kinds of calling conventions:
 * CMDcall, FCNcall, FACcall, and  PATcall.
 * The FCNcall indicates calling a MAL procedure, which leads
 * to a recursive call to the interpreter.
 *
 * CMDcall initiates calling a linked function, passing pointers
 * to the parameters and result variable, i.e.  f(ptr a0,..., ptr aN)
 * The function returns a MAL-SUCCEED upon success and a pointer
 * to an exception string upon failure.
 * Failure leads to raise-ing an exception in the interpreter loop,
 * by either looking up the relevant exception message in the module
 * administration or construction of a standard string.
 *
 * The PATcall initiates a call which contains the MAL context,
 * i.e. f(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
 * The mb provides access to the code definitions. It is primarilly
 * used by routines intended to manipulate the code base itself, such
 * as the optimizers. The Mal stack frame pointer provides access
 * to the values maintained. The arguments passed are offsets
 * into the stack frame rather than pointers to the actual value.
 *
 * BAT parameters require some care. Ideally, a BAT should not be kept
 * around long. This would mean that each time we access a BAT it has to be
 * pinned in memory and upon leaving the function, it is unpinned.
 * This degrades performance significantly.
 * After the parameters are fixed, we can safely free the destination
 * variable and re-initialize it to nil.
 *
 */

/*
 * The type dispatching table in getArgReference can be removed if we
 * determine at compile time the address offset within a ValRecord.
 * We leave this optimization for the future, it leads to about 10%
 * improvement (100ms for 1M calls).
 *
 * Flow of control statements
 * Each assignment (function call) may be part of the initialization
 * of a barrier- block. In that case we have to test the
 * outcome of the operation and possibly skip the block altogether.
 * The latter is implemented as a linear scan for the corresponding
 * labeled statemtent. This might be optimized later.
 *
 * You can skip to a catch block by searching for the corresponding 'lab'
 * The return value should be set to pass the error automatically upon
 * reaching end of function block.
 */

/*
 * Each time we enter a barrier block, we could keep its position in the
 * interpreter stack frame. It forms the starting point to issue a redo.
 * Unfortunately, this does not easily work in the presence of optimizers, which
 * may change the order/block structure. Therefore, we simple have to search
 * the beginning or ensure that during chkProgram the barrier/redo/leave/catch
 * jumps are re-established.
 *
 * Exception handling
 * Calling a built-in or user-defined routine may lead to an error or a
 * cached status message to be dealt with in MAL.
 * To improve error handling in MAL, an exception handling
 * scheme based on @sc{catch}-@sc{exit} blocks. The @sc{catch}
 * statement identifies a (string-valued) variable, which carries the
 * exception message from
 * the originally failed routine or @sc{raise} exception assignment.
 * During normal processing @sc{catch}-@sc{exit} blocks are simply skipped.
 * Upon receiving an exception status from a function call, we set the
 * exception variable and skip to the first associated @sc{catch}-@sc{exit}
 * block.
 * MAL interpretation then continues until it reaches the end of the block.
 * If no exception variable was defined, we should abandon the function
 * alltogether searching for a catch block at a higher layer.
 *
 * For the time being we have ignored cascaded/stacked exceptions.
 * The policy is to pass the first recognized exception to a context
 * in which it can be handled.
 *
 * Exceptions raised within a linked-in function requires some care.
 * First, the called procedure does not know anything about the MAL
 * interpreter context. Thus, we need to return all relevant information
 * upon leaving the linked library routine.
 *
 * Second, exceptional cases can be handled deeply in the recursion, where they
 * may also be handled, i.e. by issueing an GDKerror message. The upper layers
 * merely receive a negative integer value to indicate occurrence of an
 * error somewhere in the calling sequence.
 * We then have to also look into GDKerrbuf to see if there was
 * an error raised deeply inside the system.
 *
 * The policy is to require all C-functions to return a string-pointer.
 * Upon a successfull call, it is a NULL string. Otherwise it contains an
 * encoding of the exceptional state encountered. This message
 * starts with the exception identifer, followed by contextual details.
 */

/*
 * Garbage collection
 * Garbage collection is relatively straightforward, because most values are
 * retained on the stackframe of an interpreter call. However, two storage
 * types and possibly user-defined type garbage collector definitions
 * require attention: BATs and strings.
 *
 * A key issue is to deal with temporary BATs in an efficient way.
 * References to bats in the buffer pool may cause dangling references
 * at the language level. This appears as soons as your share
 * a reference and delete the BAT from one angle. If not carefull, the
 * dangling pointer may subsequently be associated with another BAT
 *
 * All string values are private to the VALrecord, which means they
 * have to be freed explicitly before a MAL function returns.
 * The first step is to always safe the destination variable
 * before a function call is made.
 */
void garbageElement(Client cntxt, ValPtr v)
{
	(void) cntxt;
	if (ATOMstorage(v->vtype) == TYPE_str) {
		GDKfree(v->val.sval);
		v->val.sval = NULL;
		v->len = 0;
	} else if (v->vtype == TYPE_bat) {
		/*
		 * All operations are responsible to properly set the
		 * reference count of the BATs being produced or destroyed.
		 * The libraries should not leave the
		 * physical reference count being set. This is only
		 * allowed during the execution of a GDK operation.
		 * All references should be logical.
		 */
		bat bid = v->val.bval;
		/* printf("garbage collecting: %d lrefs=%d refs=%d\n",
		   bid, BBP_lrefs(bid),BBP_refs(bid));*/
		v->val.bval = bat_nil;
		if (is_bat_nil(bid))
			return;
		if (!BBP_lrefs(bid))
			return;
		BBPrelease(bid);
	} else if (0 < v->vtype && v->vtype < MAXATOMS && ATOMextern(v->vtype)) {
		GDKfree(v->val.pval);
		v->val.pval = 0;
		v->len = 0;
	}
}

/*
 * Before we return from the interpreter, we should free all
 * dynamically allocated objects and adjust the BAT reference counts.
 * Early experience shows that for small stack frames the overhead
 * is about 200 ms for a 1M function call loop (tst400e). This means that
 * for the time being we do not introduce more complex garbage
 * administration code.
 *
 * Also note that for top-level stack frames (no environment available),
 * we should retain the value stack because it acts as a global variables.
 * This situation is indicated by the 'global' in the stack frame.
 * Upon termination of the session, the stack should be cleared.
 * Beware that variables may be know polymorphic, their actual
 * type should be saved for variables that recide on a global
 * stack frame.
 */
void garbageCollector(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int flag)
{
	int k;
	ValPtr v;

	assert(mb->vtop <= mb->vsize);
	assert(stk->stktop <= stk->stksize);
	(void) flag;
	(void)mb;
	(void)cntxt;
	for (k = 0; k < stk->stktop; k++) {
	//	if (isVarCleanup(mb, k) ){
			garbageElement(cntxt, v = &stk->stk[k]);
			v->vtype = TYPE_int;
			v->val.ival = int_nil;
	//	}
	}
}
