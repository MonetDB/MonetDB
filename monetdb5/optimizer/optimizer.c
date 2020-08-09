/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* Author(s) Martin Kersten
 * This module contains the framework for inclusion query transformers, i.e.
 * C-programs geared at optimizing a piece of MAL.
 * The query transformer appears at the language level as an ordinary function,
 * but it is effective only at a specific execution phase.
 *
 * Each optimizer function has access to the runtime scope of the
 * routine in which it is called. This can be used to maintain status
 * information between successive calls.
 *
 * The routines below are linked with the kernel by default
*/
#include "monetdb_config.h"
#include "optimizer.h"
#include "mal_debugger.h"
#include "optimizer_private.h"
#include "opt_pipes.h"

/*
 * Upon loading the module it should inspect the scenario table
 * for any unresolved references to the MALoptimizer and set the
 * callback function.
*/
str
optimizer_prelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) stk;
	(void) mb;
	(void) p;
	updateScenario("mal", "MALoptimizer", (MALfcn) MALoptimizer);
	optimizerInit();
	//return compileAllOptimizers(cntxt); causes problems
	return MAL_SUCCEED;
}

str
optimizer_epilogue(void *ret)
{
	(void)ret;
	opt_pipes_reset();
	return MAL_SUCCEED;
}

/*
 * MAL functions can be optimized explicitly using the routines below.
 * Beware, the function names should be known as literal strings, because
 * you may not know the runtime situation.
*/

str
QOToptimize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str modnme;
	str fcnnme;
	Symbol s;

	(void) stk;
	if (stk != 0) {
		modnme = *getArgReference_str(stk, pci, 1);
		fcnnme = *getArgReference_str(stk, pci, 2);
	} else {
		modnme = getArgDefault(mb, pci, 1);
		fcnnme = getArgDefault(mb, pci, 2);
	}
	s = findSymbol(cntxt->usermodule, putName(modnme), fcnnme);
	if (s == NULL)
		throw(MAL, "optimizer.optimize", SQLSTATE(HY002) SEMANTIC_OPERATION_MISSING);
	removeInstruction(mb, pci);
	addtoMalBlkHistory(s->def);
	return optimizeMALBlock(cntxt, s->def);
}

#include "opt_macro.h"

#include "mel.h"
static mel_func optimizer_init_funcs[] = {
 pattern("optimizer", "aliases", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "aliases", OPTwrapper, false, "Alias removal optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "coercions", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "coercions", OPTwrapper, false, "Handle simple type coercions", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "commonTerms", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "commonTerms", OPTwrapper, false, "Common sub-expression optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "candidates", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "candidates", OPTwrapper, false, "Mark candidate list variables", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "volcano", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "volcano", OPTwrapper, false, "Simulate volcano style execution", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "constants", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "constants", OPTwrapper, false, "Duplicate constant removal optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "profiler", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "profiler", OPTwrapper, false, "Collect properties for the profiler", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "costModel", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "costModel", OPTwrapper, false, "Estimate the cost of a relational expression", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "dataflow", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "dataflow", OPTwrapper, false, "Dataflow bracket code injection", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "deadcode", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "deadcode", OPTwrapper, false, "Dead code optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "emptybind", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "emptybind", OPTwrapper, false, "Evaluate empty set expressions.", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "jit", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "jit", OPTwrapper, false, "Propagate candidate lists in just-in-time optimization", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "evaluate", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "evaluate", OPTwrapper, false, "Evaluate constant expressions once.", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "garbageCollector", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "garbageCollector", OPTwrapper, false, "Garbage collector optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "generator", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "generator", OPTwrapper, false, "Sequence generator optimizer", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "querylog", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "querylog", OPTwrapper, false, "Collect SQL query statistics", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "prelude", optimizer_prelude, false, "Initialize the optimizer", noargs),
 command("optimizer", "epilogue", optimizer_epilogue, false, "release the resources held by the optimizer module", args(1,1, arg("",void))),
 pattern("optimizer", "optimize", QOToptimize, false, "Optimize a specific operation", args(0,2, arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "inline", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "inline", OPTwrapper, false, "Expand inline functions", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "projectionpath", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "projectionpath", OPTwrapper, false, "Join path constructor", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "macro", OPTmacro, false, "Inline the code of the target function.", args(1,3, arg("",void),arg("targetmod",str),arg("targetfcn",str))),
 pattern("optimizer", "macro", OPTmacro, false, "Inline a target function used in a specific function.", args(1,5, arg("",void),arg("mod",str),arg("fcn",str),arg("targetmod",str),arg("targetfcn",str))),
 pattern("optimizer", "orcam", OPTorcam, false, "Inverse macro processor for current function", args(1,3, arg("",void),arg("targetmod",str),arg("targetfcn",str))),
 pattern("optimizer", "orcam", OPTorcam, false, "Inverse macro, find pattern and replace with a function call.", args(1,5, arg("",void),arg("mod",str),arg("fcn",str),arg("targetmod",str),arg("targetfcn",str))),
 pattern("optimizer", "mergetable", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "mergetable", OPTwrapper, false, "Resolve the multi-table definitions", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "mitosis", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "mitosis", OPTwrapper, false, "Modify the plan to exploit parallel processing on multiple cores", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "multiplex", OPTwrapper, false, "Compiler for multiplexed instructions.", args(1,1, arg("",void))),
 pattern("optimizer", "multiplex", OPTwrapper, false, "Compiler for multiplexed instructions.", args(1,3, arg("",void),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "matpack", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "matpack", OPTwrapper, false, "Unroll the mat.pack operation", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "json", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "json", OPTwrapper, false, "Unroll the mat.pack operation", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "reduce", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "reduce", OPTwrapper, false, "Reduce the stack space claims", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "remap", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "remap", OPTwrapper, false, "Remapping function calls to a their multiplex variant", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "remoteQueries", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "remoteQueries", OPTwrapper, false, "Resolve the multi-table definitions", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "reorder", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "reorder", OPTwrapper, false, "Reorder by dataflow dependencies", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "optimizer_stats", OPTstatistics, false, "Get optimizer use statistics, i.e. calls and total time", args(3,3, batarg("",str),batarg("",int),batarg("",lng))),
 pattern("optimizer", "pushselect", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "pushselect", OPTwrapper, false, "Push selects down projections", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "pushproject", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "pushproject", OPTwrapper, false, "Push selects down projections", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "oltp", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "oltp", OPTwrapper, false, "Inject the OLTP locking primitives.", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "wlc", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "wlc", OPTwrapper, false, "Inject the workload capture-replay primitives.", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("optimizer", "postfix", OPTwrapper, false, "", args(1,1, arg("",str))),
 pattern("optimizer", "postfix", OPTwrapper, false, "Postfix the plan,e.g. pushing projections", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_optimizer_mal)
{ mal_module("optimizer", NULL, optimizer_init_funcs); }
