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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
 * Beware, the function names should be known as literalstrings, because
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
		modnme = *(str*) getArgReference(stk, pci, 1);
		fcnnme = *(str*) getArgReference(stk, pci, 2);
	} else {
		modnme = getArgDefault(mb, pci, 1);
		fcnnme = getArgDefault(mb, pci, 2);
	}
	s = findSymbol(cntxt->nspace, putName(modnme,strlen(modnme)), fcnnme);
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
		modnme = *(str*) getArgReference(stk, p, 1);
		fcnnme = *(str*) getArgReference(stk, p, 2);
		fname = *(str*) getArgReference(stk, p, 3);
	} else {
		modnme = getArgDefault(mb, p, 1);
		fcnnme = getArgDefault(mb, p, 2);
		fname = getArgDefault(mb, p, 3);
	}


	s = findSymbol(cntxt->nspace,putName(modnme, strlen(modnme)), putName(fcnnme, strlen(fcnnme)));

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
		modnme = *(str*) getArgReference(stk, p, 1);
		fcnnme = *(str*) getArgReference(stk, p, 2);
	} else {
		modnme = getArgDefault(mb, p, 1);
		fcnnme = getArgDefault(mb, p, 2);
	}

	mnstr_printf(cntxt->fdout,"#showPlan()\n");
	removeInstruction(mb, p);
	if( modnme ) {
		s = findSymbol(cntxt->nspace, putName(modnme, strlen(modnme)), putName(fcnnme, strlen(fcnnme)));

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
