/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
		if( p->token == ASSIGNsymbol) {
			int j;
			for (j = 0; j < p->retc && j + p->retc < p->argc; j++)
				if (isVarCList(mb,getArg(p,p->retc + j)))
					setVarCList(mb,getArg(p,j));
		}
		if( getModuleId(p) == sqlRef){
			if(getFunctionId(p) == tidRef) 
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == subdeltaRef) 
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == emptybindRef && p->retc == 2) 
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == bindRef && p->retc == 2) 
				setVarCList(mb,getArg(p,0));
		}
		else if( getModuleId(p) == algebraRef ){
			if(getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef)
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == likesubselectRef || getFunctionId(p) == likethetasubselectRef)
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == subinterRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == subuniqueRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == firstnRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == mergecandRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == intersectcandRef )
				setVarCList(mb,getArg(p,0));
		}
		else if( getModuleId(p) == generatorRef){
			if(getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef)
				setVarCList(mb,getArg(p,0));
		}
		else if (getModuleId(p) == sampleRef) {
			if (getFunctionId(p) == subuniformRef)
				setVarCList(mb, getArg(p, 0));
		}
		else if (getModuleId(p) == groupRef && p->retc > 1) {
			if (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef)
				setVarCList(mb, getArg(p, 1));
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
