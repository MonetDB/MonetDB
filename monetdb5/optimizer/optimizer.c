/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	return MAL_SUCCEED;
}


int debugOpt = 0;
str
QOTdebugOptimizers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	debugOptimizers(cntxt, mb, stk, pci);
	debugOpt = 1;
	return MAL_SUCCEED;
}

str
QOTclrdebugOptimizers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	debugOpt = 0;
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
	s = findSymbol(cntxt->nspace, putName(modnme), fcnnme);
	if (s == NULL)
		throw(MAL, "optimizer.optimize", SEMANTIC_OPERATION_MISSING);
	removeInstruction(mb, pci);
	addtoMalBlkHistory(s->def,"start optimizer");
	return optimizeMALBlock(cntxt, s->def);
}

str
QOTshowFlowGraph(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str fname;
	str modnme;
	str fcnnme;
	Symbol s = NULL;

	(void) cntxt;
	if (stk != 0) {
		modnme = *getArgReference_str(stk, p, 1);
		fcnnme = *getArgReference_str(stk, p, 2);
		fname = *getArgReference_str(stk, p, 3);
	} else {
		modnme = getArgDefault(mb, p, 1);
		fcnnme = getArgDefault(mb, p, 2);
		fname = getArgDefault(mb, p, 3);
	}


	s = findSymbol(cntxt->nspace,putName(modnme), putName(fcnnme));

	if (s == NULL) {
		char buf[1024];
		snprintf(buf,1024, "%s.%s", modnme, fcnnme);
		throw(MAL, "optimizer.showFlowGraph", RUNTIME_OBJECT_UNDEFINED ":%s", buf);
	}
	showFlowGraph(s->def, stk, fname);
	return MAL_SUCCEED;
}

str
QOTshowPlan(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme;
	str fcnnme;
	Symbol s = NULL;

	if (stk != 0) {
		modnme = *getArgReference_str(stk, p, 1);
		fcnnme = *getArgReference_str(stk, p, 2);
	} else {
		modnme = getArgDefault(mb, p, 1);
		fcnnme = getArgDefault(mb, p, 2);
	}

	mnstr_printf(cntxt->fdout,"#showPlan()\n");
	removeInstruction(mb, p);
	if( modnme ) {
		s = findSymbol(cntxt->nspace, putName(modnme), putName(fcnnme));

		if (s == NULL) {
			char buf[1024];
			snprintf(buf,1024, "%s.%s", modnme, fcnnme);
			throw(MAL, "optimizer.showPlan", RUNTIME_OBJECT_UNDEFINED ":%s", buf);
		}
		mb= s->def;
	}
	printFunction(cntxt->fdout, mb, 0, LIST_INPUT);
	return MAL_SUCCEED;
}
