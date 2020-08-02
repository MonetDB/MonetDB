/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* This optimized injects code to exploit the filter capabilities of imprints.
 * The focus now is to make likeselect faster.
 * The supportive routines are available in gdk_imprints.c
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_imprints.h"

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')

str
OPTimprintsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, actions = 0;
	bool doit = false;
	lng usec = GDKusec();
	InstrPtr q;
	InstrPtr p=0, *old= mb->stmt;
	char buf[256];
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;
	(void) pci;

	/* Check if we need to derive a new plan at all */
	for( i = 0; i< mb->stop; i++){
		p= old[i];
		if( getFunctionId(p) == likeselectRef || getFunctionId(p) == ilikeselectRef || getFunctionId(p) == imatchRef){
			doit = true;
			break;
		}
	}

	if( ! doit)
		goto wrapup;

	limit = mb->stop;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		msg= createException(MAL,"optimizer.imprints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	/* Remove the result from any join/group instruction when it is not used later on */
	for( i = 0; i< limit; i++){
		p= old[i];
		if ( getModuleId(p) == pcreRef && (getFunctionId(p) == matchRef || getFunctionId(p) == imatchRef) ){
		}
		if ( getModuleId(p) == batalgebraRef && (getFunctionId(p) == likeRef || getFunctionId(p) == ilikeRef) ){
		}
		if ( getModuleId(p) == batalgebraRef && (getFunctionId(p) == not_likeRef || getFunctionId(p) == not_ilikeRef) ){
		}
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == ilikeselectRef){
		}
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == likeselectRef &&  isCandidateList(mb, p, 2)){
			q = copyInstruction(p);
			getArg(q,0) = newTmpVariable(mb, getArgType(mb, p,2));
			setModuleId(q, imprintsRef);
			setFunctionId(q, likeselectRef);
			getArg(p,2) = getArg(q,0);
			pushInstruction(mb,q);
			actions++;
		}
		pushInstruction(mb,p);
	}
	/* Defense line against incorrect plans */
	if( actions ){
		 msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
		 	msg = chkDeclarations(mb);
	}
	if(old) GDKfree(old);
wrapup:
	usec= GDKusec() - usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec", "imprints", actions, usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
