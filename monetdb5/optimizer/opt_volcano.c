/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Selectively inject serialization operations when we know the
 * raw footprint of the query exceeds 80% of RAM.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_volcano.h"

int
OPTvolcanoImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit;
	int mvcvar = -1;
	InstrPtr p,q, *old = mb->stmt;

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */

    if ( mb->inlineProp )
        return 0;

    limit= mb->stop;
    if ( newMalBlkStmt(mb, mb->ssize + 20) < 0)
		return 0;

	for (i = 0; i < limit; i++) {
		p = old[i];

		pushInstruction(mb,p);
		if( getModuleId(p) == sqlRef && getFunctionId(p)== mvcRef ){
			mvcvar = getArg(p,0);
			continue;
		}

		if( getModuleId(p) == algebraRef ){
			if( getFunctionId(p) == subselectRef ||
				getFunctionId(p) == thetasubselectRef ||
				getFunctionId(p) == likesubselectRef ||
				getFunctionId(p) == subjoinRef
			){
				q= newStmt(mb, languageRef, blockRef);
				q =  pushArgument(mb,q,mvcvar);
				q =  pushArgument(mb,q,getArg(p,0));
				mvcvar=  getArg(q,0);
			}
			continue;
		}
		if( getModuleId(p) == groupRef ){
			if( getFunctionId(p) == subgroupdoneRef ){
				q= newStmt(mb, languageRef, blockRef);
				q =  pushArgument(mb,q,mvcvar);
				q =  pushArgument(mb,q,getArg(p,0));
				mvcvar=  getArg(q,0);
			}
		}
		if( getModuleId(p) == sqlRef){
			if ( getFunctionId(p) == bindRef ||
				getFunctionId(p) == bindidxRef || 
				getFunctionId(p)== tidRef ||
				getFunctionId(p)== appendRef ||
				getFunctionId(p)== updateRef ||
				getFunctionId(p)== deleteRef
			){
				setArg(p,p->retc,mvcvar);
			}
		}
	} 
	GDKfree(old);
	return 1;
}
