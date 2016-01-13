/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* (c) M. Ivanova, M. Kersten
 * Determine the instructions to be handled by the recycler.
 *
 * Instructions that break the linear flow also stop further
 * recycling. This does not hold for dataflow blocks.
 * Update statements are not recycled. They trigger cleaning of
 * the recycle cache.
 * Each variable be trigger a recycling action only once
 */
#include "monetdb_config.h"
#include "opt_recycler.h"
#include "opt_dataflow.h"
#include "mal_instruction.h"

static int isFunctionArgument(MalBlkPtr mb, int varid){
	int i;
	InstrPtr p = getInstrPtr(mb,0);
	for ( i=p->retc; i<p->argc; i++)
		if ( getArg(p,i) == varid) 
			return TRUE;
	return FALSE;
}

int
OPTrecyclerImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, cnt, cand, actions = 1, marks = 0;
	InstrPtr *old, q,p;
	int limit;
	char *recycled;

	(void) cntxt;
	(void) stk;
	(void) pci;

	limit = mb->stop;
	old = mb->stmt;

	/* watch out, newly created instructions may introduce new variables */
	recycled = GDKzalloc(sizeof(char) * mb->vtop * 2);
	if (recycled == NULL)
		return 0;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(recycled);
		return 0;
	}
	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++) {
		p = old[i];
		if (p->token == ENDsymbol )
			break;
		/* the first non-dataflow barrier breaks the recycler code*/
		if (blockStart(p) && !(getFunctionId(p) && getFunctionId(p) == dataflowRef) )
			break;

		if ( isUpdateInstruction(p) || hasSideEffects(p,TRUE)){
			/*  update instructions are not recycled but monitored*/
			pushInstruction(mb, p);
			if (isUpdateInstruction(p)) {
				if (getModuleId(p) == batRef && isaBatType(getArgType(mb, p, 1))) {
					q = newFcnCall(mb, "recycle", "reset");
					pushArgument(mb, q, getArg(p, 1));
					actions++;
				}
				if (getModuleId(p) == sqlRef) {
					q= copyInstruction(p);
					getModuleId(q) = recycleRef;
					actions++;
				}
			}
			continue;
		}
		// Not all instruction may be recycled. In particular, we should avoid
		// MAL function with implicit/recursive side effects. 
		// This can not always be detected easily. Likewise, we ignore cheap operations
		// Therefore, we use a safe subset to start with
		if ( ! (getModuleId(p) == sqlRef || getModuleId(p)== batRef || 
				getModuleId(p) == algebraRef || getModuleId(p)==batcalcRef ||
				getModuleId(p)== aggrRef || getModuleId(p)== groupRef ||
				getModuleId(p)== batstrRef || getModuleId(p)== batmmathRef ||
				getModuleId(p)== arrayRef || getModuleId(p)== batmtimeRef ||
				getModuleId(p)== batcalcRef || getModuleId(p)== pcreRef ||
				getModuleId(p)== mtimeRef || getModuleId(p) == calcRef  ||
				getModuleId(p)== dateRef || getModuleId(p) == timestampRef  ||
				getModuleId(p)== matRef )
			){
			pushInstruction(mb,p);
			continue;
		}

		/* general rule: all arguments should be constants or recycled*/
		cnt = 0;
		for (j = p->retc; j < p->argc; j++)
			if (recycled[getArg(p, j)] || isVarConstant(mb, getArg(p, j)) || isFunctionArgument(mb,getArg(p,j)) )
				cnt++;
		cand = 0;
		for (j =0; j< p->retc; j++)
			if (recycled[getArg(p, j)] ==0)
				cand++;
		if (cnt == p->argc - p->retc && cand == p->retc) {
			marks++;
			p->recycle = RECYCLING; /* this instruction is to be monitored */
			for (j = 0; j < p->retc; j++)
				recycled[getArg(p, j)] = 1;
		}
		pushInstruction(mb, p);
	}
	for (; i < limit; i++) 
		pushInstruction(mb, old[i]);
	GDKfree(old);
	GDKfree(recycled);
	mb->recycle = marks > 0;
	OPTDEBUGrecycle {
		mnstr_printf(cntxt->fdout, "#recycle optimizer: ");
		printFunction(cntxt->fdout,mb, 0, LIST_MAL_ALL);
	}
	return actions + marks;
}
