/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * Selectively inject serialization operations when we know the
 * raw footprint of the query exceeds 80% of RAM.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_strimps.h"

// delaying the startup should not be continued throughout the plan
// after the startup phase there should be intermediate work to do
//A heuristic to check it
#define MAXdelays 128

str
OPTstrimpsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, slimit, actions=0;
	bool needed = false;
	// int mvcvar = -1;
	InstrPtr p, q, *old = mb->stmt;
	str msg = MAL_SUCCEED;
	int res;

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */

	limit= mb->stop;

	if ( mb->inlineProp )
		goto bailout;

	for(i=0; i < limit; i++) {
		p = old[i];
		if (getModuleId(p) == algebraRef &&
			getFunctionId(p) == likeselectRef) {
			needed = true;
			break;
		}
	}

	if (!needed)
		goto bailout;

	if (newMalBlkStmt(mb, mb->ssize + 20) < 0)
		throw(MAL,"optimizer.strimps", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	slimit = mb->stop;

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (p->token == ENDsymbol) {
			pushInstruction(mb,p);
			break;
		}

		/* Look for bind operations on strings, because for those we migh need strimps */

		if (getModuleId(p) == algebraRef &&
			getFunctionId(p) == likeselectRef) {
			q = newInstruction(mb, strimpsRef, strimpFilterSelectRef);
			res = newTmpVariable(mb, newBatType(TYPE_oid));
			setDestVar(q, res);
			q = pushArgument(mb, q, getArg(p, 1));
			q = pushArgument(mb, q, getArg(p, 2));
			q = pushArgument(mb, q, getArg(p, 3));
			q = pushArgument(mb, q, getArg(p, 6));

			pushInstruction(mb, q);
			typeChecker(cntxt->usermodule, mb, q, mb->stop - 1, TRUE);

			getArg(p, 2) = res;
			// setArgument(mb, p, 2, res);

			actions++;
			/* continue; */
		}
		pushInstruction(mb, p);
	}
	(void)slimit;
	for (; i < slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);

	/* Defense line against incorrect plans */
	if (actions){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment */
bailout:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
