/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Mark the production and use of candidate lists.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_candidates.h"

str
OPTcandidatesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	InstrPtr p;
	char  buf[256];
	lng usec = GDKusec();
	str msg= MAL_SUCCEED;

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
		}
		else if( getModuleId(p) == algebraRef ){
			if(getFunctionId(p) == selectRef || getFunctionId(p) == thetaselectRef)
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == likeselectRef)
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == intersectRef || getFunctionId(p) == differenceRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == uniqueRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == firstnRef )
				setVarCList(mb,getArg(p,0));
			else if(getFunctionId(p) == subsliceRef )
				setVarCList(mb,getArg(p,0));
			else if (getFunctionId(p) == projectionRef &&
					 isVarCList(mb,getArg(p,p->retc + 0)) &&
					 isVarCList(mb,getArg(p,p->retc + 1)))
				setVarCList(mb,getArg(p,0));
		}
		else if( getModuleId(p) == generatorRef){
			if(getFunctionId(p) == selectRef || getFunctionId(p) == thetaselectRef)
				setVarCList(mb,getArg(p,0));
		}
		else if (getModuleId(p) == sampleRef) {
			if (getFunctionId(p) == subuniformRef)
				setVarCList(mb, getArg(p, 0));
		}
		else if (getModuleId(p) == groupRef && p->retc > 1) {
			if (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef ||
			    getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef)
				setVarCList(mb, getArg(p, 1));
		} else if (getModuleId(p) == batRef) {
			if (getFunctionId(p) == mergecandRef ||
				getFunctionId(p) == intersectcandRef ||
				getFunctionId(p) == diffcandRef ||
				getFunctionId(p) == mirrorRef)
				setVarCList(mb,getArg(p,0));
		}
	}

    /* Defense line against incorrect plans */
	/* plan remains unaffected */
	// msg = chkTypes(cntxt->usermodule, mb, FALSE);
	// if( ms== MAL_SUCCEED)
	//	msg = chkFlow(mb);
	// if( ms== MAL_SUCCEED)
	// 	msg = chkDeclarations(mb);
	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions= 1 time=" LLFMT " usec","candidates",usec);
	newComment(mb,buf);
	addtoMalBlkHistory(mb);
	return msg;
}
