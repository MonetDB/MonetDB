/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a M. Kersten
 * @* Scheduler framework
 * A key design decision for this MonetDB  version is to create a clean
 * processing pipeline: compile-, optimize-, schedule- and execute.
 * The MAL scheduler controls the actual execution of a program through a number of
 * techniques described here. It accepts a program with the intend to execute it
 * as soon as possible with the best possible behavior.
 *
 * Note that the scheduler should only work on temporary plans. Keeping an
 * execution schedule in the symbol table calls for a sound administration
 * to invalidate it. This assumed that such decisions are taken by a cooperative
 * front-end compiler, e.g. SQL. For the remainder we assume a volatile MAL
 * program is available.
 *
 * Recall that the MAL optimizer already had an opportunity to massage the program
 * into something that has a better execution behavior. These optimizations
 * necessarily rely on static (or stale) information, such as histograms, indices,
 * and rewrite heuristics. It is the task of the scheduler to achieve this
 * performance objective (or better), while at the same time guaranteeing
 * great throughput.
 *
 * Unlike the optimizer, the scheduler has access to the calling parameters,
 * runtime stack for the about-to-be-executed plan, and system resources,
 * such as globally cached results. Moreover, a scheduler can be called at any time
 * during a MAL interpreter loop to re-consider the actions to be taken.
 * This gives many more opportunities to improve performance.
 * This does not hold for e.g. compiled code.
 *
 * Much like the optimizer framework, there are many aspects you can deal with
 * at scheduling time. The easiest is to simply do nothing and rely on
 * the MAL interpreter, which is the default action when no scheduler directive
 * is found.
 *
 * In most cases, however, you would like a scheduler to take a specific action.
 * It typically rewrites, extends, or reshuffles the query plan as it is being
 * prepared for execution or even during its execution.
 * In this process it can use the optimizer infrastructure, provided the
 * changes are isolated to a private instance. This should be prepared
 * before the interpreter is started and requires care during execution to
 * ensure proper behaviour. The upfront cost attached is to make a complete
 * copy of the program code and to assure that the definitions in the
 * symbol table remain intact.
 *
 * To make its life easy, it is assumed that all scheduler decisions to be
 * taken before the back-end is called reside in the module 'scheduler'.
 * Any action to be taken during execution is kept in module 'rescheduler'.
 *
 * What scheduler operators are provided?
 *
 * The generic scheduler presented here shows a series of techniques that
 * might be useful in construction of your own scheduler.
 *
 * @emph{Run isolation (RUNisolated)}
 * Goal: to isolate changes to the query plan from others
 * Rationale: A scheduler may change the order, and actual function calls.
 * These changes should be confined to a single execution. The next time around
 * there may be a different situation to take care off. This is achieved by
 * replacing the current program with a private copy.
 * Note that this process may involve a deep analysis to identify the pieces
 * needed for isolation, e.g. for SQL we would extract a copy from the SQL query
 * cache using code expansion.
 * Just massaging the call to the cached plan is not effective.
 * Impact: cost of program copying may become an issue.
 *
 * @emph{Run trace (RUNtracing)}
 * Goal: to collect performance data for either direct monitoring or post-analysis
 * Rationale:The performance profiling option in the interpreter is overly expensive,
 * because it is also called for simple statements. One way out of this, is to
 * inject specific performance metric calls at places where it counts.
 * For example, you could wrap calls calls to a specific module or operation.
 *
 * @emph{Run notification (RUNnotification)}
 * Goal: to inject notification calls to awaiting servers
 * Rationale: Operations on the database may require side effects to take place,
 * such as activating a trigger (implemented as a factory)
 *
 * @emph{Update notification (RUNupdateNotification)}
 * Goal: Updates to the Boxes may have to  forwarded to stand-by replicas.
 * Rationale: actually a refinement of the notification scheme, but geared at
 * maintaining a replicated system.
 *
 * @emph{Materialized Execution Scheduler (MEscheduler)}
 * Goal:to transfor the program into fragment-based processing
 * Rationale: to reduce the impact of resource limitations, such
 * as main memory, in the face of materialized intermediate.
 *
 * @emph{Run parallelization (RUNparallel)}
 * Goal: to improve throughput/response time by exploiting parallel hardware
 *
 * @emph{Run stepping RUNstepping}
 * goal: to return to the scheduler at specific points in the program
 * execution. For example to re-consider the scheduling actions.
 * Note that this requires a scheme to 'backtrack' in the scenario
 *
 * @emph{Cache management (RUNbatCaching}
 * Goal: to capture expensive BAT operations and to keep them around for
 * re-use.
 *
 * @emph{JIT compilation}
 * Use the Mcc just in time compilation feature to derive and link
 * a small C-program.
 *
 * What the scheduler could do more?
 * A separete thread of control can be used to administer the
 * information needed by concurrent scheduler calls.
 *
 * One of the areas to be decided upon is whether the scheduler is
 * also the place to manage the stack spaces. It certainly could
 * reconcile parallel executions by inspection of the stack.
 *
 * Upon starting the scheduler decisions, we should (regularly)
 * refresh our notion on the availability of resources.
 * This is kept around in a global structure and is the basis for
 * micro scheduling decisions.
 */

#include "monetdb_config.h"
#include "run_pipeline.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "opt_prelude.h"
#include "opt_macro.h"

/*
 * The implementation approach of the scheduler aligns with that of the
 * optimizer. We look for specific scheduler module calls and act accordingly.
 * The result should be a MAL block that can be executed by the corresponding
 * engine.
 */
/*
 * The second example is derived from the SQL environment, which
 * produces two MAL functions: one (already) stored in the query cache
 * and a program to be executed in the client record.
 * The latter lives only for the duration of the call and need
 * not be safeguarded. Thus, we immediately call the scheduler
 * to expand the code from the query cache, propagating the
 * actual parameters.
 * The SQL specific scheduler is called to bind the table
 * columns and to make statistics known as properties.
 * This creates a situation where a cost-based optimizer could
 * step in and re-arrange the plan, which is ignored for now.
 * @example
 * function sql_cache.qry01(int A1, bit A2);
 * 	t1:= sql.bind("schema","tables",0,0);
 * 	...
 * end qry01;
 *
 * function main();
 * 	scheduler.inline("sql_cache","qry01");
 * 	scheduler.sqlbind();
 * 	sql_cache.qry01(12,false);
 * @end example
 *
 * These scheduler interventions are called for by the SQL compiler.
 * It may have flagged the cached version as a high potential for
 * dynamic optimization, something that is not known at the MAL level.
 * [The current optimizer also generates a factory, that should be
 * done here and upon explicit request]
 */
#if 0
str
RUNinline(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	Symbol qc;
	str modnme = getVarConstant(mb, getArg(p, 1)).val.sval;
	str fcnnme = getVarConstant(mb, getArg(p, 2)).val.sval;

	(void) stk;
	(void) p;
	qc = findSymbol(cntxt ->nspace, getName(modnme),
			putName(fcnnme));

	if (qc)
		MACROprocessor(cntxt, mb, qc);

	return MAL_SUCCEED;
}
#endif

/*
 * @-
 * The SQL scheduler is presented merely as an example. The real one
 * is located in  the SQL libraries for it needs specific information.
 */
#if 0
str
RUNsqlbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	str msg = MAL_SUCCEED;
	Symbol sqlbind = findSymbol(cntxt ->nspace, getName("sql"), getName("bind"));
	MALfcn f = NULL;

	if (sqlbind )
		f = getSignature(sqlbind)->fcn;

	if ( f )
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (p->fcn == f) {
			if ((msg = reenterMAL(cntxt, mb, i, i + 1, stk)))
				break;
			/* fetch the BAT properties and turn off this instruction */
			p->token = NOOPsymbol;
		}
	}
#ifdef DEBUG_MAL_SCHEDULER
	mnstr_printf(cntxt->fdout, "scheduler.sqlbind results\n");
	printFunction(cntxt->fdout, mb, stk, LIST_MAL_ALL);
#endif
	return msg;
}
#endif

