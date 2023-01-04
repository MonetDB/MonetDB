/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* (c) Martin Kersten
 * This optimizer injects the MSK operations to reduce footprint
 */
#include "monetdb_config.h"
#include "opt_mask.h"

str
OPTmaskImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, limit, slimit;
	InstrPtr p=0, q=0, r=0, *old= NULL;
	int actions = 0;
	int *varused=0;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) stk;		/* to fool compilers */

	if ( mb->inlineProp )
		goto wrapup;

	varused = GDKzalloc(2 * mb->vtop * sizeof(int));
	if (varused == NULL)
		goto wrapup;

	limit = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(varused);
		throw(MAL,"optimizer.deadcode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	// Consolidate the actual need for variables
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( p == 0)
			continue; //left behind by others?
		for(j=0; j< p->retc; j++){
			k =  getArg(p,j);
			if( isaBatType(getArgType(mb, p,j)) && getBatType(getArgType(mb, p, j)) == TYPE_msk){
				// remember we have encountered a mask producing function
				varused[k] = k;
			}
		}
		for(j=p->retc; j< p->argc; j++){
			k =  getArg(p,j);
			if(varused[k]) {
				// we should actually postpone its reconstruction to JIT
				r = newInstruction(mb, maskRef, umaskRef);
				getArg(r,0) = k;
				r= pushArgument(mb, r, varused[k]);
				pushInstruction(mb,r);
				varused[k] = 0;
			}
		}
		pushInstruction(mb, p);

		if ( getModuleId(p) == algebraRef && (getFunctionId(p) == selectRef || getFunctionId(p) == thetaselectRef)){
			k=  getArg(p,0);
			setDestVar(p, newTmpVariable(mb, newBatType(TYPE_oid)));
			setVarFixed(mb,getArg(p,0));

			// Inject a dummy pair, just based on :oid for now and later to be switched to :msk
			q = newInstruction(mb, maskRef, maskRef);
			setDestVar(q, newTmpVariable(mb, newBatType(TYPE_msk)));
			setVarFixed(mb,getArg(q,0));
			q= pushArgument(mb, q, getArg(p,0));
			pushInstruction(mb,q);
			varused[k] = getArg(q,0);
			actions++;	
		} 
	}

	for(; i<slimit; i++)
		if( old[i])
			pushInstruction(mb, old[i]);
	/* Defense line against incorrect plans */
	if( actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment */
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);

	if(old) GDKfree(old);
	if(varused) GDKfree(varused);
	return msg;
}
