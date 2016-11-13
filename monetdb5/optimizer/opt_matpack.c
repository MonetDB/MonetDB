/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * This simple module unrolls the mat.pack into an incremental sequence.
 * This could speedup parallel processing and releases resources faster.
 */
#include "monetdb_config.h"
#include "opt_matpack.h"

int 
OPTmatpackImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int v, i, j, limit, slimit;
	InstrPtr p,q;
	int actions = 0;
	InstrPtr *old;
	char *packIncrementRef = putName("packIncrement");
	char buf[256];
	lng usec = GDKusec();

	//if ( !optimizerIsApplied(mb,"multiplex") )
		//return 0;
	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */
	old= mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb,mb->stop) < 0)
		return 0;
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( getModuleId(p) == matRef  && getFunctionId(p) == packRef && isaBatType(getArgType(mb,p,1))) {
			q = newInstruction(0, matRef, packIncrementRef);
			setDestVar(q, newTmpVariable(mb, getArgType(mb,p,1)));\
			q = pushArgument(mb, q, getArg(p,1));
			v = getArg(q,0);
			q = pushInt(mb,q, p->argc - p->retc);
			pushInstruction(mb,q);
			typeChecker(cntxt->fdout, cntxt->nspace,mb,q,TRUE);

			for ( j = 2; j < p->argc; j++) {
				q = newInstruction(0, matRef, packIncrementRef);
				q = pushArgument(mb, q, v);
				q = pushArgument(mb, q, getArg(p,j));
				setDestVar(q, newTmpVariable(mb, getVarType(mb,v)));
				v = getArg(q,0);
				pushInstruction(mb,q);
				typeChecker(cntxt->fdout, cntxt->nspace,mb,q,TRUE);
			}
			getArg(q,0) = getArg(p,0);
			freeInstruction(p);
			actions++;
			continue;
		}
		pushInstruction(mb,p);
	} 
	for(; i<slimit; i++)
		if (old[i]) 
			freeInstruction(old[i]);
	GDKfree(old);

    /* Defense line against incorrect plans */
    if( actions > 0){
        //chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        //chkFlow(cntxt->fdout, mb);
        //chkDeclarations(cntxt->fdout, mb);
    }
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","matpack",actions,GDKusec() - usec);
    newComment(mb,buf);

	return actions;
}
