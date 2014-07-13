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
#include "opt_groups.h"
#include "group.h"

int
OPTgroupsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, actions=0;
	InstrPtr q,p;
	InstrPtr *old, *ref;
	int limit,slimit;
	int *used;

	(void) cntxt;
	(void) stk;
	(void) pci;
	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL) {
		return 0;
	}

// Code should first be synchronized with mergetable
// And a proper re-ordering test should proof its validity
	if (1)
		return 0;

	/* beware, new variables and instructions are introduced */
	ref= (InstrPtr*) GDKzalloc(sizeof(InstrPtr) * mb->vtop); /* to find last assignment */
	if ( ref == NULL) {
		return 0;
	}
	used= (int*) GDKzalloc(sizeof(InstrPtr) * mb->vtop); /* use count  */
	if( used == NULL){
		GDKfree(ref);
		return 0;
	}

	OPTDEBUGgroups {
		mnstr_printf(cntxt->fdout,"Group by optimizer\n");
		printFunction(cntxt->fdout,mb,0,LIST_MAL_STMT);
	}
	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb,mb->ssize) <0) {
		GDKfree(ref);
		return 0;
	}

	// determine use count for all variables
	for (i = 0; i<limit; i++){
		p= old[i];
		for(j= p->retc; j<p->argc; j++)
			ref[getArg(p,j)]++;
	}
	

	for (i = 0; i<limit; i++){
		p= old[i];
		if (getModuleId(p) == groupRef ){
			if (p->argc == 4 && getFunctionId(p) == subgroupRef  && used[getArg(p,1)] ==0 && used[getArg(p,2)] == 0){
				setFunctionId(p, multicolumnRef);
				ref[getArg(p,0)] = p;
				actions++;
				OPTDEBUGgroups {
					mnstr_printf(cntxt->fdout,"#new groups instruction\n");
					printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
				}
			} else
			if (p->argc == 5 && getFunctionId(p) == subgroupRef  && (q= ref[getArg(p,p->argc-1)]) && used[getArg(p,1)] ==0 && used[getArg(p,2)] == 0){
				p->argc--;
				for( j = q->argc-1; j>= q->retc; j--)
					p = setArgument(mb,p,p->retc, getArg(q,j));
				ref[getArg(p,0)] = p;
				setFunctionId(p, multicolumnRef);
				OPTDEBUGgroups{
					mnstr_printf(cntxt->fdout,"#new groups instruction extension\n");
					printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
				}
			} else
			if (p->argc == 5 && getFunctionId(p) == subgroupdoneRef && (q= ref[getArg(p,p->argc-1)]) ){
				/*
				 * Expand its argument list with what we have found so far.
				 * This creates a series of derive paths, many of which will be removed during deadcode elimination.
				 */
				p->argc--;
				for( j = q->argc-1; j>= q->retc; j--)
					p = setArgument(mb,p,p->retc, getArg(q,j));
				ref[getArg(p,0)] = p;
				setFunctionId(p, multicolumnRef);
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
	GDKfree(ref);
	GDKfree(used);
	OPTDEBUGgroups if( actions) {
		mnstr_printf(cntxt->fdout,"Result of group by optimizer\n");
		printFunction(cntxt->fdout,mb,0,LIST_MAL_STMT);
	}
	return actions;
}
