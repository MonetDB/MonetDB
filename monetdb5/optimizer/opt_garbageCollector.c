/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
	char buf[1024];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;
#ifndef NDEBUG
	int j;
	int *used;
#endif

	(void) pci;
	(void) stk;
	if ( mb->inlineProp)
		return 0;

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
#ifndef NDEBUG
	int vlimit = mb->vtop;
	used = (int *) GDKzalloc(vlimit * sizeof(int));
#endif
	p = NULL;
	for (i = 0; i < limit; i++) {
		p = getInstrPtr(mb, i);
		p->gc &=  ~GARBAGECONTROL;
		p->typechk = TYPE_UNKNOWN;
		/* Set the program counter to ease profiling */
		p->pc = i;
#ifndef NDEBUG
		if ( i > 1 && getModuleId(p) != languageRef && getModuleId(p) != querylogRef && getModuleId(p) != sqlRef && !p->barrier)
			for( j=0; j< p->retc; j++)
				used[getArg(p,j)] = i;
		if ( getModuleId(p) != languageRef && getFunctionId(p) != passRef){
			for(j= p->retc ; j< p->argc; j++)
				used[getArg(p,j)] = 0;
			}
#endif
		if ( p->token == ENDsymbol)
			break;
	}

	/* A good MAL plan should end with an END instruction */
	if( p && p->token != ENDsymbol){
#ifndef NDEBUG
		GDKfree(used);
#endif
		throw(MAL, "optimizer.garbagecollector", SQLSTATE(42000) "Incorrect MAL plan encountered");
	}
#ifndef NDEBUG
	/* Leave a message behind when we have created variables and not used them */
	for(i=0; i< vlimit; i++)
	if( used[i]){
		p = getInstrPtr(mb, used[i]);
		if( p){
			str msg = instruction2str(mb, NULL, p, LIST_MAL_ALL);
			snprintf(buf,1024,"Unused variable %s: %s", getVarName(mb, i), msg);
			GDKfree(msg);
			newComment(mb,buf);
		}
	}
	GDKfree(used);
#endif
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
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","garbagecollector",actions, usec);
	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
