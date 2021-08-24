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
	int i, limit, slimit, needed =0, actions=0;
	// int mvcvar = -1;
	InstrPtr p, q, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;
	/* int res, nvar; */
	/* ValRecord cst; */
	int res;

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */

	limit= mb->stop;

	if ( mb->inlineProp )
		return MAL_SUCCEED;

	for(i=0; i < limit; i++) {
		p = old[i];
		if (getModuleId(p) == algebraRef && getFunctionId(p) == likeselectRef)
			needed = 1;
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

		if (getModuleId(p) == algebraRef && getFunctionId(p) == likeselectRef) {

			/* cst.vtype = TYPE_bit; */
			/* nvar = defConstant(mb, TYPE_bit, &cst); */
			q = newInstruction(mb, strimpsRef, strimpFilterSelectRef);
			res = newTmpVariable(mb, newBatType(TYPE_oid));
			setDestVar(q, res);
			q = addArgument(mb, q, getArg(p, 1));
			q = addArgument(mb, q, getArg(p, 2));
			q = addArgument(mb, q, getArg(p, 3));
			q = addArgument(mb, q, getArg(p, 6));

			pushInstruction(mb, q);
			typeChecker(cntxt->usermodule, mb, q, mb->stop-1, TRUE);

			p = setArgument(mb, p, 2, getArg(q, 0));

			actions++;
		}
		pushInstruction(mb, p);
	}
	(void)slimit;
	/* for (; i < slimit; i++) */
	/* 	if (old[i]) */
	/* 		freeInstruction(old[i]); */
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
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","strimps",actions,usec);
	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
