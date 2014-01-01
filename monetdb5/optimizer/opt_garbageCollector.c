/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "opt_garbageCollector.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_builder.h"
#include "opt_prelude.h"
#include "mal_properties.h"

/*
 * Keeping variables around beyond their end-of-life-span
 * can be marked with the proper 'keep'.
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
	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL)
		return 0;

	span = setLifespan(mb);
	if ( span == NULL)
		return 0;

	old= mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	vlimit = mb->vtop;
	if ( newMalBlkStmt(mb,mb->ssize) < 0) {
		GDKfree(span);
		return 0;
	}

	p = NULL;
	for (i = 0; i < limit; i++) {
		p = old[i];
		p->gc &=  ~GARBAGECONTROL;

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
					isaBatType(getVarType(mb,k)) &&
					varGetProp(mb, k, keepProp) == NULL){
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
		mnstr_printf(cntxt->fdout, "End of GCoptimizer\n");
	}
	GDKfree(span);

	return actions+1;
}

