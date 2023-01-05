/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * This simple module unrolls the mat.pack into an incremental sequence.
 * This could speedup parallel processing and releases resources faster.
 */
#include "monetdb_config.h"
#include "opt_matpack.h"

str
OPTmatpackImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int v, i, j, limit, slimit;
	InstrPtr p,q;
	int actions = 0;
	InstrPtr *old = NULL;
	str msg = MAL_SUCCEED;

	if( isOptimizerUsed(mb, pci, mergetableRef) <= 0){
		goto wrapup;
	}

	(void) cntxt;
	(void) stk;		/* to fool compilers */
	for( i = 1; i < mb->stop; i++)
		if( getModuleId(getInstrPtr(mb,i)) == matRef  && getFunctionId(getInstrPtr(mb,i)) == packRef && isaBatType(getArgType(mb,getInstrPtr(mb,i),1)))
			break;
	if( i == mb->stop)
		goto wrapup;

	old= mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb,mb->stop) < 0)
		throw(MAL,"optimizer.matpack", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < limit; i++) {
		p = old[i];
		if( getModuleId(p) == matRef  && getFunctionId(p) == packRef && isaBatType(getArgType(mb,p,1))) {
			q = newInstruction(0, matRef, packIncrementRef);
			setDestVar(q, newTmpVariable(mb, getArgType(mb,p,1)));\
			q = addArgument(mb, q, getArg(p,1));
			v = getArg(q,0);
			q = pushInt(mb,q, p->argc - p->retc);
			pushInstruction(mb,q);
			typeChecker(cntxt->usermodule,mb,q, mb->stop-1, TRUE);

			for ( j = 2; j < p->argc; j++) {
				q = newInstruction(0, matRef, packIncrementRef);
				q = addArgument(mb, q, v);
				q = addArgument(mb, q, getArg(p,j));
				setDestVar(q, newTmpVariable(mb, getVarType(mb,v)));
				v = getArg(q,0);
				pushInstruction(mb,q);
				typeChecker(cntxt->usermodule,mb,q, mb->stop-1, TRUE);
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
			pushInstruction(mb, old[i]);
	GDKfree(old);

	/* Defense line against incorrect plans */
	if( actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
