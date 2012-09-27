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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "opt_groups.h"
#include "group.h"

int
OPTgroupsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, actions=0;
	int *pc;
	InstrPtr q;
	InstrPtr *old;
	int limit,slimit;

	(void) cntxt;
	(void) stk;
	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL) {
		return 0;
	}

	/* beware, new variables and instructions are introduced */
	pc= (int*) GDKzalloc(sizeof(int)* mb->vtop * 2); /* to find last assignment */
	if ( pc == NULL) {
		return 0;
	}

	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb,mb->ssize) <0) {
		GDKfree(pc);
		return 0;
	}

	for (i = 0; i<limit; i++){
		p= old[i];
		if (0 && getModuleId(p) == groupRef && p->argc == 4 && (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef)){
			setFunctionId(p, multicolumnsRef);
			pc[getArg(p,0)] = i;
			pc[getArg(p,1)] = i;
			pc[getArg(p,2)] = i;
			actions++;
			OPTDEBUGgroups {
				mnstr_printf(cntxt->fdout,"#new groups instruction\n");
				printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
			}
		}
		if (0 && getModuleId(p) == groupRef && p->argc == 5 && (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef)){
			/*
			 * @-
			 * Try to expand its argument list with what we have found so far.
			 * This creates a series of derive paths, many of which will be removed during deadcode elimination.
			 */
			if (pc[getArg(p,4)]){
				q= copyInstruction(getInstrPtr(mb,pc[getArg(p,4)]));
				q= pushArgument(mb,q, getArg(p,4));
				getArg(q,0) = getArg(p,0);
				getArg(q,1) = getArg(p,1);
				getArg(q,2) = getArg(p,2);
				pc[getArg(q,0)] = i;
				pc[getArg(q,1)] = i;
				pc[getArg(q,2)] = i;
				freeInstruction(p);
				p= q;
				OPTDEBUGgroups{
					mnstr_printf(cntxt->fdout,"#new groups instruction extension\n");
					printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
				}
			}
		} 
		pushInstruction(mb,p);
	}
	for(; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(pc);
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_groups: %d statements glued\n",actions);
	return actions;
}
