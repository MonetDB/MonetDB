/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * Mark the production and use of candidate lists.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_candidates.h"

int
OPTcandidatesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	InstrPtr p;
	char  buf[256];
	lng usec = GDKusec();

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb,i);
		if( p->token == ASSIGNsymbol && isVarCList(mb,getArg(p,1)))
			setVarCList(mb,getArg(p,0));
		if( getModuleId(p) == sqlRef){
			if(getFunctionId(p) == tidRef) 
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == subdeltaRef) 
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == bindRef && p->retc == 2) 
				setVarCList(mb,getArg(p,0));
		}
		if( getModuleId(p) == algebraRef ){
			if(getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef)
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == likesubselectRef || getFunctionId(p) == likethetasubselectRef)
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == subinterRef )
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == subuniqueRef )
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == firstnRef )
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == mergecandRef )
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == intersectcandRef )
				setVarCList(mb,getArg(p,0));
			if(getFunctionId(p) == crossRef ){
				setVarCList(mb,getArg(p,0));
				setVarCList(mb,getArg(p,1));
			}
		}
		if( getModuleId(p) == generatorRef){
			if(getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef)
				setVarCList(mb,getArg(p,0));
		}
	}

    /* Defense line against incorrect plans */
	/* plan remains unaffected */
	//chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	//chkFlow(cntxt->fdout, mb);
	//chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","candidates",1,GDKusec() -usec);
    newComment(mb,buf);

	return 1;
}
