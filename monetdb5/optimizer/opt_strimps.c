/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
	int i, limit;
	// int mvcvar = -1;
	int count=0;
	InstrPtr p,q,r, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;
	/* int res, nvar; */
	/* ValRecord cst; */
	int res;

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */


	if ( mb->inlineProp )
		return MAL_SUCCEED;

	limit= mb->stop;
	if ( newMalBlkStmt(mb, mb->ssize + 20) < 0)
		throw(MAL,"optimizer.volcano", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (getModuleId(p) == algebraRef && getFunctionId(p) == likeselectRef) {
			q = newInstruction(0, strimpsRef, mkstrimpsRef); /* This should be void? */
			setDestVar(q, newTmpVariable(mb, TYPE_void));
			q = addArgument(mb, q, getArg(p, 1));

			pushInstruction(mb, q);
			typeChecker(cntxt->usermodule, mb, q, mb->stop-1, TRUE);

			/* cst.vtype = TYPE_bit; */
			/* nvar = defConstant(mb, TYPE_bit, &cst); */
			r = newInstruction(mb, strimpsRef, strimpFilterSelectRef);
			res = newTmpVariable(mb, newBatType(TYPE_oid));
			setDestVar(r, res);
			r = addArgument(mb, r, getArg(p, 1));
			r = addArgument(mb, r, getArg(p, 2));
			r = addArgument(mb, r, getArg(p, 3));
			r = addArgument(mb, r, getArg(p, 6));

			pushInstruction(mb, r);
			// typeChecker(cntxt->usermodule, mb, r, mb->stop-1, TRUE);

			count++;
		}
		pushInstruction(mb, p);
	}
	GDKfree(old);

    /* Defense line against incorrect plans */
    if( count){
        msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
        	msg = chkFlow(mb);
	if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","strimps",count,usec);
    newComment(mb,buf);
	if( count > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
