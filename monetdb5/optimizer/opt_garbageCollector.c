/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_garbageCollector.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_builder.h"
#include "opt_prelude.h"

/*
 * Keeping variables around beyond their end-of-life-span
 * can be marked with the proper 'keep'.
 * Set the program counter to ease profiling
 */
int
OPTgarbageCollectorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, n = 0, limit, vlimit, depth=0, slimit;
	InstrPtr p, q, *old;
	int actions = 0;
	Lifespan span;

	(void) pci;
	(void) cntxt;
	(void) stk;
	if ( mb->inlineProp)
		return 0;


	old= mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	vlimit = mb->vtop;

	// move SQL query to front
	p = NULL;
	for(i = limit; i> 2; i--){
		if(mb->stmt[i] && getModuleId(mb->stmt[i]) == querylogRef && getFunctionId(mb->stmt[i]) == defineRef ){
			p = mb->stmt[i];
			p = pushInt(mb,p,i+1);
			break;
		}
	}
	if( p != NULL){
		for(  ; i > 1; i--)
			mb->stmt[i] = mb->stmt[i-1];
		mb->stmt[1] = p;
		mb->stmt[1]->token = ASSIGNsymbol;
	}
	span = setLifespan(mb);
	if ( span == NULL)
		return 0;

	if ( newMalBlkStmt(mb,mb->ssize) < 0) {
		GDKfree(span);
		return 0;
	}

	p = NULL;
	for (i = 0; i < limit; i++) {
		p = old[i];
		p->gc &=  ~GARBAGECONTROL;
		p->pc = i;

		if ( p->barrier == RETURNsymbol){
			pushInstruction(mb, p);
			continue;
		}
		if (blockStart(p) )
			depth++;
		if ( p->token == ENDsymbol)
			break;
		
		pushInstruction(mb, p);
		n = mb->stop-1;
		for (j = 0; j < p->argc; j++) {
			if (getEndLifespan(span,getArg(p,j)) == i && isaBatType(getArgType(mb, p, j)) ){
				mb->var[getArg(p,j)]->eolife = n;
				p->gc |= GARBAGECONTROL;
			} 
		}
		if (blockExit(p) ){
			/* force garbage collection of all within upper block */
			depth--;
			for (k = 0; k < vlimit; k++) {
				if (getBeginLifespan(span,k) > 0  &&
					getEndLifespan(span,k) == i &&
					isaBatType(getVarType(mb,k)) ){
						q= newAssignment(mb);
						getArg(q,0) = k;
						setVarUDFtype(mb,k);
						setVarFixed(mb,k);
						q= pushNil(mb,q, getVarType(mb,k));
						q->gc |= GARBAGECONTROL;
						mb->var[k]->eolife = mb->stop-1;
						actions++;
				}
			}
		}
	}
	assert(p);
	assert( p->token == ENDsymbol);
	pushInstruction(mb, p);
	for (i++; i < limit; i++) 
		pushInstruction(mb, old[i]);
	for (; i < slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
	getInstrPtr(mb,0)->gc |= GARBAGECONTROL;
	GDKfree(old);
	OPTDEBUGgarbageCollector{ 
		int k;
		mnstr_printf(cntxt->fdout, "#Garbage collected BAT variables \n");
		for ( k =0; k < vlimit; k++)
		mnstr_printf(cntxt->fdout,"%10s eolife %3d  begin %3d lastupd %3d end %3d\n",
			getVarName(mb,k), mb->var[k]->eolife,
			getBeginLifespan(span,k), getLastUpdate(span,k), getEndLifespan(span,k));
		chkFlow(cntxt->fdout,mb);
		printFunction(cntxt->fdout,mb, 0, LIST_MAL_ALL);
		mnstr_printf(cntxt->fdout, "End of GCoptimizer\n");
	}
	GDKfree(span);

	return actions+1;
}

