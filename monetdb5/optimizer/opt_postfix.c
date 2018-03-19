/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* The SQL code generator can not always look ahead to avoid
 * generation of intermediates.
 * Some of these patterns are captured in a postfix optimalisation.
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_postfix.h"

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')
str
OPTpostfixImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
#if 0							// Don't use this right now
	int i, j, slimit, limit, actions=0;
	InstrPtr *old = 0;
    InstrPtr q, *vars = 0;
	lng usec = GDKusec();
	char buf[256];
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) cntxt;

	limit = mb->stop;
	slimit = mb->ssize;
	/* the first postfix concerns pushing projections into the count()  we check if it is needed*/
	for ( i = 0; i < limit; i++){
		p= getInstrPtr(mb,i);
		if( getFunctionId(p) == countRef  && getModuleId(p)== aggrRef && p->argc == 2){
			actions ++;
		}
	}
	if( actions == 0)
		goto finished;

    vars= (InstrPtr*) GDKzalloc(sizeof(InstrPtr)* mb->vtop);
    if (vars == NULL)
        throw(MAL,"optimizer.batcalc", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	old = mb->stmt;
    if (newMalBlkStmt(mb, mb->ssize) < 0) {
        msg= createException(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);
        goto wrapup;
    }
	/* collect where variables are assigned last */
	for ( i = 0; i < limit; i++){
		p= old[i];
		for( j = 0; j< p->retc; j++)
			vars[getArg(p,j)]= p;
	}
	/* construct the new plan */
	for ( i = 0; i < limit; i++)
	{
		p= old[i];
		if( getFunctionId(p) == countRef  && getModuleId(p)== aggrRef && p->argc == 2 ){
			q= vars[getArg(p,1)];
			if( getArg(q,0) == getArg(p,1) && getFunctionId(q) == projectionRef && getModuleId(q) == algebraRef && q->argc == 3 && isCandidateList(mb, q, 1)){
				getArg(p,1) = getArg(q,2);
				p = pushArgument(mb, p, getArg(q,1));
			}
		}
		pushInstruction(mb,p);
	}
	for( ;i < slimit; i++)
		if( old[i])
			freeInstruction(old[i]);

	/* Defense line against incorrect plans */
	if( actions ){
		chkTypes(cntxt->usermodule, mb, FALSE);
		chkFlow(mb);
		chkDeclarations(mb);
	}
    /* keep all actions taken as a post block comment and update statics */
    GDKfree(old);
finished:
	usec= GDKusec() - usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec", "postfix", actions, usec);
    newComment(mb,buf);
	addtoMalBlkHistory(mb);

wrapup:
	if(vars) GDKfree(vars);
	return msg;
#else
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	return MAL_SUCCEED;
#endif
}
