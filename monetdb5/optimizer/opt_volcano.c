/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * Selectively inject serialization operations when we know the
 * raw footprint of the query exceeds 80% of RAM.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_volcano.h"

// delaying the startup should not be continued throughout the plan
// after the startup phase there should be intermediate work to do
//A heuristic to check it
#define MAXdelays 128

str
OPTvolcanoImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, actions = 0;
	int mvcvar = -1;
	int count=0;
	InstrPtr p,q, *old = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;		/* to fool compilers */

	if ( mb->inlineProp )
		goto wrapup;

	old = mb->stmt;
	limit= mb->stop;
	if ( newMalBlkStmt(mb, mb->ssize + 20) < 0)
		throw(MAL,"optimizer.volcano", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	actions = 1;
	for (i = 0; i < limit; i++) {
		p = old[i];

		pushInstruction(mb,p);
		if( getModuleId(p) == sqlRef && getFunctionId(p)== mvcRef ){
			mvcvar = getArg(p,0);
			continue;
		}

		if( count < MAXdelays && getModuleId(p) == algebraRef ){
			if( getFunctionId(p) == selectRef ||
				getFunctionId(p) == thetaselectRef ||
				getFunctionId(p) == likeselectRef ||
				getFunctionId(p) == joinRef
			){
				q= newInstruction(0,languageRef,blockRef);
				setDestVar(q, newTmpVariable(mb,TYPE_any));
				q =  addArgument(mb,q,mvcvar);
				q =  addArgument(mb,q,getArg(p,0));
				mvcvar=  getArg(q,0);
				pushInstruction(mb,q);
				count++;
			}
			continue;
		}
		if( count < MAXdelays && getModuleId(p) == groupRef ){
			if( getFunctionId(p) == subgroupdoneRef || getFunctionId(p) == groupdoneRef ){
				q= newInstruction(0,languageRef,blockRef);
				setDestVar(q, newTmpVariable(mb,TYPE_any));
				q =  addArgument(mb,q,mvcvar);
				q =  addArgument(mb,q,getArg(p,0));
				mvcvar=  getArg(q,0);
				pushInstruction(mb,q);
				count++;
			}
		}
		if( getModuleId(p) == sqlRef){
			if ( getFunctionId(p) == bindRef ||
				getFunctionId(p) == bindidxRef ||
				getFunctionId(p)== tidRef ||
				getFunctionId(p)== appendRef ||
				getFunctionId(p)== updateRef ||
				getFunctionId(p)== claimRef ||
				getFunctionId(p)== deleteRef
			){
				setArg(p,p->retc,mvcvar);
			}
		}
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
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
