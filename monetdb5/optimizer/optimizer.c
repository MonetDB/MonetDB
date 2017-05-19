/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
	optPipeInit();
	optimizerInit();
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
		throw(MAL, "optimizer.optimize", SEMANTIC_OPERATION_MISSING);
	removeInstruction(mb, pci);
	addtoMalBlkHistory(s->def);
	return optimizeMALBlock(cntxt, s->def);
}

