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
 */
#include "monetdb_config.h"
#include "opt_deadcode.h"

str
OPTdeadcodeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k, se,limit, slimit;
	InstrPtr p=0, *old= NULL;
	int actions = 0;
	int *varused=0;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) stk;		/* to fool compilers */

	if ( mb->inlineProp )
		goto wrapup;

	varused = GDKzalloc(mb->vtop * sizeof(int));
	if (varused == NULL)
		goto wrapup;

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(varused);
		throw(MAL,"optimizer.deadcode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	//mnstr_printf(cntxt->fdout,"deadcode limit %d ssize %d vtop %d vsize %d\n", limit, (int)(mb->ssize), mb->vtop, (int)(mb->vsize));

	// Calculate the instructions in which a variable is used.
	// Variables can be used multiple times in an instruction.
	for (i = 1; i < limit; i++) {
		p = old[i];
		for( k=p->retc; k<p->argc; k++)
			varused[getArg(p,k)]++;
		if ( blockCntrl(p) )
			for( k= 0; k < p->retc; k++)
				varused[getArg(p,k)]++;
	}

	// Consolidate the actual need for variables
	for (i = limit; i >= 0; i--) {
		p = old[i];
		if( p == 0)
			continue; //left behind by others?

		if ( getModuleId(p) == batRef && isUpdateInstruction(p) && !p->barrier){
			/* bat.append and friends are intermediates that need not be retained
			 * unless they are not used outside of an update */
			if( varused[getArg(p,1)] > 1 )
				varused[getArg(p,0)]++; // force keeping it
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
	/* save the free instructions records for later */
	for(; i<slimit; i++)
		if(old[i]){
			pushInstruction(mb,old[i]);
		}
	/* Defense line against incorrect plans */
	/* we don't create or change existing structures */
		// no type change msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if( actions > 0){
		msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);

	if(old) GDKfree(old);
	if(varused) GDKfree(varused);
	return msg;
}
