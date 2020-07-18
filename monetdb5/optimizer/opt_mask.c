/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* (c) Martin Kersten
 * This optimizer injects the MSK operations to reduce footprint
 */
#include "monetdb_config.h"
#include "opt_mask.h"

str
OPTmaskImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k, limit, slimit;
	InstrPtr p=0, q=0, *old= mb->stmt;
	int actions = 0;
	int *varused=0;
	char buf[256];
	lng usec = GDKusec();
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	if ( mb->inlineProp )
		return MAL_SUCCEED;

	varused = GDKzalloc(2 * mb->vtop * sizeof(int));
	if (varused == NULL)
		return MAL_SUCCEED;

	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		msg= createException(MAL,"optimizer.deadcode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	// Consolidate the actual need for variables
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( p == 0)
			continue; //left behind by others?
		pushInstruction(mb, p);

		if ( getModuleId(p) == algebraRef && (getFunctionId(p) == selectRef || getFunctionId(p) == thetaselectRef)){
			k=  getArg(p,0);
			// setDestVar(p, newTmpVariable(mb, newBatType(TYPE_msk))); TODO
			setDestVar(p, newTmpVariable(mb, newBatType(TYPE_oid)));
			setModuleId(p, maskRef);

			q = newInstruction(mb, maskRef, umaskRef);
			getArg(q,0) = k;
			q= pushArgument(mb, q, getArg(p,0));
			pushInstruction(mb,q);
			actions ++;
		} 
	}

	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        msg = chkTypes(cntxt->usermodule, mb, FALSE);
        if (!msg)
		msg = chkFlow(mb);
        if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","mask",actions, usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

wrapup:
	if(old) GDKfree(old);
	if(varused) GDKfree(varused);
	return msg;
}
