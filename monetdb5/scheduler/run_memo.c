/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @f run_memo
 * @a M. Kersten
 * @+ Memo-based Query Execution
 * Modern cost-based query optimizers use a memo structure
 * to organize the search space for an efficient query execution plan.
 * For example, consider an oid join path 'A.B.C.D'.
 * We can start the evaluation at any point in this path.
 *
 * Its memo structure can be represented by a (large) MAL program.
 * The memo levels are encapsulated with a @code{choice} operator.
 * The arguments of the second dictate which instructions to consider
 * for cost evaluation.
 * @example
 *  ...
 *  scheduler.choice("getVolume");
 *  T1:= algebra.join(A,B);
 *  T2:= algebra.join(B,C);
 *  T3:= algebra.join(C,D);
 *  scheduler.choice("getVolume",T1,T2,T3);
 *  T4:= algebra.join(T1,C);
 *  T5:= algebra.join(A,T2);
 *  T6:= algebra.join(T2,D);
 *  T7:= algebra.join(B,T3);
 *  T8:= algebra.join(C,D);
 *  scheduler.choice("getVolume",T4,T5,T6,T7,T8);
 *  T9:= algebra.join(T4,D);
 *  T10:= algebra.join(T5,D);
 *  T11:= algebra.join(A,T6);
 *  T12:= algebra.join(A,T7);
 *  T13:= algebra.join(T1,T8);
 *  scheduler.choice("getVolume",T9,T10,T11,T12,T13);
 *  answer:= scheduler.pick(T9, T10, T11, T12, T13);
 * @end example
 *
 * The @code{scheduler.choice()} operator calls a builtin @code{getVolume}
 * for each target variable and expects an integer-valued cost.
 * In this case it returns the total number of bytes uses as arguments.
 *
 * The target variable with the lowest
 * cost is chosen for execution and remaining variables are turned into
 * a temporary NOOP operation.(You may want to re-use the memo)
 * They are skipped by the interpreter, but also in subsequent
 * calls to the scheduler. It reduces the alternatives as we proceed
 * in the plan.
 *
 * A built-in naive cost function is used.
 * It would be nice if the user could provide a
 * private cost function defined as a @code{pattern}
 * with a polymorphic argument for the target and a @code{:lng} result.
 * Its implementation can use the complete context information to
 * make a decision. For example, it can trace the potential use
 * of the target variable in subsequent statements to determine
 * a total cost when this step is taken towards the final result.
 *
 * A complete plan likely includes other expressions to
 * prepare or use the target variables before reaching the next
 * choice point. It is the task of the choice operator
 * to avoid any superfluous operation.
 *
 * The MAL block should be privately owned by the caller,
 * which can be assured with @code{scheduler.isolation()}.
 *
 * A refinement of the scheme is to make cost analysis
 * part of the plan as well. Then you don't have to
 * include a hardwired cost function.
 * @example
 *  Acost:= aggr.count(A);
 *  Bcost:= aggr.count(B);
 *  Ccost:= aggr.count(C);
 *  T1cost:= Acost+Bcost;
 *  T2cost:= Bcost+Ccost;
 *  T3cost:= Ccost+Dcost;
 *  scheduler.choice(T1cost,T1, T2cost,T2, T3cost,T3);
 *  T1:= algebra.join(A,B);
 *  T2:= algebra.join(B,C);
 *  T3:= algebra.join(C,D);
 *  ...
 * @end example
 * The current implementation assumes a regular plan
 * and unique use of variables.
 */
/*
 * @+ Memorun implementation
 * The code below is a mixture of generic routines and
 * sample implementations to run the tests.
 */
#include "monetdb_config.h"
#include "run_memo.h"
#include "mal_runtime.h"

static void
propagateNonTarget(MalBlkPtr mb, int pc)
{
	int i;
	InstrPtr p;
	str scheduler = putName("scheduler");

	for (; pc < mb->stop; pc++) {
		p = getInstrPtr(mb, pc);
		if (getModuleId(p) == scheduler)
			continue;
		for (i = 0; i < p->argc; i++)
			if (isVarDisabled(mb, getArg(p, i)) && p->token >= 0)
				p->token = -p->token;  /* temporary NOOP */
		if (p->token < 0)
			for (i = 0; i < p->retc; i++)
				setVarDisabled(mb, getArg(p, i));
	}
}
/*
 * THe choice operator first searches the next one to identify
 * the fragment to be optimized and to gain access to the variables
 * without the need to declare them upfront.
 */
str
RUNchoice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int target;
	lng cost, mincost;
	int i, j, pc;
	char *nme;
	InstrPtr q;

	pc = getPC(mb, p);
	for (i = pc + 1; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if (getModuleId(p) == getModuleId(q) &&
			getFunctionId(p) == getFunctionId(q)) {
			p = q;
			break;
		}
	}
	if (i == mb->stop)
		return MAL_SUCCEED;
	target = getArg(p, 2);
	if (getArgType(mb, p, 1) == TYPE_int && p->argc >= 3 && (p->argc - 1) % 2 == 0) {
		/* choice pairs */
		mincost = *getArgReference_int(stk, p, 1);
		for (i = 3; i < p->argc; i += 2) {
			cost = *getArgReference_int(stk, p, i);
			if (cost < mincost && !isVarDisabled(mb, getArg(p, i + 1))) {
				mincost = cost;
				target = getArg(p, i + 1);
			}
		}
	} else if (getArgType(mb, p, 1) == TYPE_str) {
		nme = *getArgReference_str(stk, p, 1);
		/* should be generalized to allow an arbitrary user defined function */
		if (strcmp(nme, "getVolume") != 0)
			throw(MAL, "scheduler.choice", ILLEGAL_ARGUMENT "Illegal cost function");

		mincost = -1;
		for (j = 2; j < p->argc; j++) {
			if (!isVarDisabled(mb, getArg(p, j)))
				for (i = pc + 1; i < mb->stop; i++) {
					InstrPtr q = getInstrPtr(mb, i);
					if (p->token >= 0 && getArg(q, 0) == getArg(p, j)) {
						cost = getVolume(stk, q, 1);
						if (cost > 0 && (cost < mincost || mincost == -1)) {
							mincost = cost;
							target = getArg(p, j);
						}
						break;
					}
				}

		}
	}
#ifdef DEBUG_RUN_MEMORUN
	mnstr_printf(cntxt->fdout, "#function target %s cost %d\n", getVarName(mb, target), mincost);
#else
	(void) cntxt;
#endif
	/* remove non-qualifying variables */
	for (i = 2; i < p->argc; i += 2)
		if (getArg(p, i) != target) {
			setVarDisabled(mb, getArg(p, i - 1));
			setVarDisabled(mb, getArg(p, i));
		}

	propagateNonTarget(mb, pc + 1);
#ifdef DEBUG_RUN_MEMORUN
	mnstr_printf(cntxt->fdout, "#cost choice selected %s %d\n",
			getVarName(mb, target), mincost);
	printFunction(cntxt->fdout, mb, 1);
#endif
	return MAL_SUCCEED;
}
/*
 * At the end of the query plan we save the result in
 * a separate variable.
 */
str
RUNpickResult(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	ValPtr lhs, rhs;
	int i;

	(void) cntxt;
	lhs = &stk->stk[getArg(p, 0)];
	for (i = p->retc; i < p->argc; i++)
		if (!isVarDisabled(mb, getArg(p, i))) {
			rhs = &stk->stk[getArg(p, i)];
			if ((rhs)->vtype < TYPE_str)
				*lhs = *rhs;
			else
				VALcopy(lhs, rhs);
			if (lhs->vtype == TYPE_bat)
				BBPincref(lhs->val.bval, TRUE);
			return MAL_SUCCEED;
		}

	throw(MAL, "scheduler.pick", OPERATION_FAILED "No result available");
}
/*
 * The routine below calculates a cost based on the BAT volume in bytes.
 * The MAL compiler ensures that all arguments have been
 * assigned a value.
 */
str
RUNvolumeCost(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *cost = getArgReference_lng(stk, p, 0);
	(void) mb;

	(void) cntxt;
	*cost = getVolume(stk, p, 0); /* calculate total input size */
	return MAL_SUCCEED;
}
/*
 * The second example shows how you can look into the remaining
 * instructions to assess the total cost if you follow the path
 * starting at the argument given.
 */
str
RUNcostPrediction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *cost = getArgReference_lng(stk, p, 0);
	(void) mb;
	(void) cntxt;

	*cost = 0;
	return MAL_SUCCEED;
}

