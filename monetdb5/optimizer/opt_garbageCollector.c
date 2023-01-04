/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_garbageCollector.h"
#include "mal_interpreter.h"
#include "mal_builder.h"
#include "mal_function.h"
#include "opt_prelude.h"

/* The garbage collector is focused on removing temporary BATs only.
 * Leaving some garbage on the stack is an issue.
 *
 * The end-of-life of a BAT may lay within block bracket. This calls
 * for care, as the block may trigger a loop and then the BATs should
 * still be there.
 *
 * The life time of such BATs is forcefully terminated after the block exit.
 */

str
OPTgarbageCollectorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit;
	InstrPtr p;
	int actions = 0;
	str msg = MAL_SUCCEED;

	(void) stk;
	if ( mb->inlineProp)
		goto wrapup;

	limit = mb->stop;


	// move SQL query definition to the front for event profiling tools
	p = NULL;
	for(i = 0; i < limit; i++)
		if(mb->stmt[i] && getModuleId(mb->stmt[i]) == querylogRef && getFunctionId(mb->stmt[i]) == defineRef ){
			p = getInstrPtr(mb,i);
			break;
		}

	if( p != NULL){
		for(  ; i > 1; i--)
			mb->stmt[i] = mb->stmt[i-1];
		mb->stmt[1] = p;
		actions = 1;
	}

	// Actual garbage collection stuff, just mark them for re-assessment
	p = NULL;
	for (i = 0; i < limit; i++) {
		p = getInstrPtr(mb, i);
		p->gc &=  ~GARBAGECONTROL;
		p->typechk = TYPE_UNKNOWN;
		/* Set the program counter to ease profiling */
		p->pc = i;
		if ( p->token == ENDsymbol)
			break;
	}

	//mnstr_printf(cntxt->fdout,"garbacollector limit %d ssize %d vtop %d vsize %d\n", limit, (int)(mb->ssize), mb->vtop, (int)(mb->vsize));
	/* A good MAL plan should end with an END instruction */
	if( p && p->token != ENDsymbol){
		throw(MAL, "optimizer.garbagecollector", SQLSTATE(42000) "Incorrect MAL plan encountered");
	}
	/* move sanity check to other optimizer */
	getInstrPtr(mb,0)->gc |= GARBAGECONTROL;

	/* leave a consistent scope admin behind */
	setVariableScope(mb);
	/* Defense line against incorrect plans */
	if( actions > 0){
		if (!msg)
			msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment */
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
