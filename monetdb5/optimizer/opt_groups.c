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
OPTgroupsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, actions=0;
	InstrPtr q;
	InstrPtr *old, *ref;
	int limit,slimit;

	(void) cntxt;
	(void) stk;
	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL) {
		return 0;
	}

	/* beware, new variables and instructions are introduced */
	ref= (InstrPtr*) GDKzalloc(sizeof(InstrPtr) * mb->vtop); /* to find last assignment */
	if ( ref == NULL) {
		return 0;
	}

	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb,mb->ssize) <0) {
		GDKfree(ref);
		return 0;
	}

	for (i = 0; i<limit; i++){
		p= old[i];
		if (getModuleId(p) == groupRef && p->argc == 4 && getFunctionId(p) == subgroupRef ){
			setFunctionId(p, multicolumnsRef);
			ref[getArg(p,0)] = p;
			actions++;
			OPTDEBUGgroups {
				mnstr_printf(cntxt->fdout,"#new groups instruction\n");
				printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
			}
		}
		if (getModuleId(p) == groupRef && p->argc == 5 && getFunctionId(p) == subgroupdoneRef && ref[getArg(p,4)] != NULL){
			/*
			 * Try to expand its argument list with what we have found so far.
			 * This creates a series of derive paths, many of which will be removed during deadcode elimination.
			 */
			q= copyInstruction(ref[getArg(p,4)]);
			q= pushArgument(mb, q, getArg(p,3));
			getArg(q,0) = getArg(p,0);
			getArg(q,1) = getArg(p,1);
			getArg(q,2) = getArg(p,2);
			ref[getArg(q,0)] = q;
			freeInstruction(p);
			p= q;
			OPTDEBUGgroups{
				mnstr_printf(cntxt->fdout,"#new groups instruction extension\n");
				printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
			}
		} 
		pushInstruction(mb,p);
	}
	for(; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(ref);
	return actions;
}
