/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* (c) Martin Kersten
 */
#include "monetdb_config.h"
#include "opt_deadcode.h"

str 
OPTdeadcodeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k, se,limit, slimit;
	InstrPtr p=0, *old= mb->stmt;
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

	varused = GDKzalloc(mb->vtop * sizeof(int));
	if (varused == NULL)
		return MAL_SUCCEED;

	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		msg= createException(MAL,"optimizer.deadcode",MAL_MALLOC_FAIL);
		goto wrapup;
	}

	// Calculate the instructions in which a variable is used.
	// Variables can be used multiple times in an instruction.
	for (i = 1; i < limit; i++) {
		p = old[i];
		for( k=p->retc; k<p->argc; k++)
			varused[getArg(p,k)]++;
	}

	// Consolidate the actual need for variables
	for (i = limit; i >= 0; i--) {
		p = old[i];
		if( p == 0)
			continue; //left behind by others?

		if( getModuleId(p)== sqlRef && getFunctionId(p)== assertRef ){
			varused[getArg(p,0)]++; // force keeping 
			continue;
		}
		if ( getModuleId(p) == batRef && isUpdateInstruction(p) && !p->barrier){
			/* bat.append and friends are intermediates that need not be retained 
			 * unless they are used */
		} else
		if (hasSideEffects(mb, p, FALSE) || !isLinearFlow(p) || 
				(p->retc == 1 && mb->unsafeProp) || p->barrier /* ==side-effect */){
			varused[getArg(p,0)]++; // force keeping it
			continue;
		}
			
		// The results should be used somewhere
		se = 0;
		for ( k=0; k < p->retc; k++)
			se += varused[getArg(p,k)] > 0;
		
		// Reduce input variable count when garbage is detected
		if (se == 0 )
			for ( k=p->retc; k < p->argc; k++)
				varused[getArg(p,k)]--;
	}
	
	// Now we can simply copy the intructions and discard useless ones.
	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++) {
		if ((p = old[i]) != NULL) {
			if( p->token == ENDsymbol){
				pushInstruction(mb,p);
				// Also copy the optimizer trace information
				for(i++; i<limit; i++)
					if(old[i])
						pushInstruction(mb,old[i]);
				break;
			}

			// Is the instruction still relevant?
			se = 0;
			for ( k=0; k < p->retc; k++)
				se += varused[getArg(p,k)] > 0;
	
			if (se)
				pushInstruction(mb,p);
			else {
				freeInstruction(p);
				actions ++;
			}
		}
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
	/* we don't create or change existing structures */
    //if( actions > 0){
        //chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        //chkDeclarations(cntxt->fdout, mb);
    //}
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","deadcode",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

wrapup:
	if(old) GDKfree(old);
	if(varused) GDKfree(varused);
	return msg;
}
