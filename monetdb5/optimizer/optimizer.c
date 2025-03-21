/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "mal_scenario.h"
#include "optimizer.h"
#include "optimizer_private.h"
#include "opt_pipes.h"
#include "mal_session.h"

str
optimizer_epilogue(void *ret)
{
	(void) ret;
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
		throw(MAL, "optimizer.optimize",
			  SQLSTATE(HY002) SEMANTIC_OPERATION_MISSING);
	removeInstruction(mb, pci);
	return optimizeMALBlock(cntxt, s->def);
}

#define optwrapper_pattern(NAME, DESC) \
	pattern("optimizer", NAME, OPTwrapper, false, "", args(1,1, arg("",str))), \
	pattern("optimizer", NAME, OPTwrapper, false, DESC, args(1,3, arg("",str),arg("mod",str),arg("fcn",str)))


#include "mel.h"
static mel_func optimizer_init_funcs[] = {
	optwrapper_pattern("aliases", "Alias removal optimizer"),
	optwrapper_pattern("coercions", "Handle simple type coercions"),
	optwrapper_pattern("commonTerms", "Common sub-expression optimizer"),
	optwrapper_pattern("candidates", "Mark candidate list variables"),
	optwrapper_pattern("constants", "Duplicate constant removal optimizer"),
	optwrapper_pattern("profiler", "Collect properties for the profiler"),
	optwrapper_pattern("costModel",
					   "Estimate the cost of a relational expression"),
	optwrapper_pattern("dataflow", "Dataflow bracket code injection"),
	optwrapper_pattern("deadcode", "Dead code optimizer"),
	optwrapper_pattern("emptybind", "Evaluate empty set expressions"),
	optwrapper_pattern("evaluate", "Evaluate constant expressions once"),
	optwrapper_pattern("garbageCollector", "Garbage collector optimizer"),
	optwrapper_pattern("generator", "Sequence generator optimizer"),
	optwrapper_pattern("querylog", "Collect SQL query statistics"),
	optwrapper_pattern("minimalfast", "Fast compound minimal optimizer pipe"),
	optwrapper_pattern("defaultfast", "Fast compound default optimizer pipe"),
	optwrapper_pattern("wrapper", "Fake optimizer"),
	command("optimizer", "epilogue", optimizer_epilogue, false,
			"release the resources held by the optimizer module",
			args(1, 1, arg("", void))),
	pattern("optimizer", "optimize", QOToptimize, false,
			"Optimize a specific operation",
			args(0, 2, arg("mod", str), arg("fcn", str))),
	optwrapper_pattern("inline", "Expand inline functions"),
	optwrapper_pattern("projectionpath", "Join path constructor"),
	optwrapper_pattern("mergetable", "Resolve the multi-table definitions"),
	optwrapper_pattern("mitosis",
					   "Modify the plan to exploit parallel processing on multiple cores"),
	optwrapper_pattern("multiplex", "Compiler for multiplexed instructions"),
	optwrapper_pattern("matpack", "Unroll the mat.pack operation"),
	optwrapper_pattern("reduce", "Reduce the stack space claims"),
	optwrapper_pattern("remap",
					   "Remapping function calls to a their multiplex variant"),
	optwrapper_pattern("remoteQueries", "Resolve the multi-table definitions"),
	optwrapper_pattern("reorder", "Reorder by dataflow dependencies"),
	pattern("inspect", "optimizer_stats", OPTstatistics, false,
			"Get optimizer use statistics, i.e. calls and total time",
			args(3, 3, batarg("", str), batarg("", int), batarg("", lng))),
	optwrapper_pattern("pushselect", "Push selects down projections"),
	optwrapper_pattern("postfix", "Postfix the plan,e.g. pushing projections"),
	optwrapper_pattern("strimps", "Use strimps index if appropriate"),
	optwrapper_pattern("for", "Push for decompress down"),
	optwrapper_pattern("dict", "Push dict decompress down"),
	{.imp = NULL}
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_optimizer_mal)
{
	mal_module2("optimizer", NULL, optimizer_init_funcs, NULL, NULL);
}
