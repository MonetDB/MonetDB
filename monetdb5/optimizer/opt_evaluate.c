/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_evaluate.h"
#include "opt_aliases.h"

static int
OPTallConstant(Client cntxt, MalBlkPtr mb, InstrPtr p)
{
	int i;
	(void)cntxt;

	if ( !(p->token == ASSIGNsymbol ||
		   getModuleId(p) == calcRef ||
		   getModuleId(p) == strRef ||
		   getModuleId(p) == mtimeRef ||
		   getModuleId(p) == mmathRef))
		return FALSE;
	if (getModuleId(p) == mmathRef && strcmp(getFunctionId(p), "rand") == 0)
		return FALSE;

	for (i = p->retc; i < p->argc; i++)
		if (isVarConstant(mb, getArg(p, i)) == FALSE)
			return FALSE;
	for (i = 0; i < p->retc; i++) {
		if (isaBatType(getArgType(mb, p, i)))
			return FALSE;
		if ( mb->unsafeProp )
			return FALSE;
	}
	return TRUE;
}

static int OPTsimpleflow(MalBlkPtr mb, int pc)
{
	int i, block =0, simple= TRUE;
	InstrPtr p;

	for ( i= pc; i< mb->stop; i++){
		p =getInstrPtr(mb,i);
		if (blockStart(p))
			block++;
		if ( blockExit(p))
			block--;
		if ( blockCntrl(p))
			simple= FALSE;
		if ( block == 0){
			return simple;
		}
	}
	return FALSE;
}

/* barrier blocks can only be dropped when they are fully excluded.  */
static str
OPTremoveUnusedBlocks(Client cntxt, MalBlkPtr mb)
{
	/* catch and remove constant bounded blocks */
	int i, j = 0, action = 0, block = -1, skip = 0, multipass = 1;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	while(multipass--){
		block = -1;
		skip = 0;
		j = 0;
		for (i = 0; i < mb->stop; i++) {
			p = mb->stmt[i];
			if (blockExit(p) && block == getArg(p,0) ){
					block = -1;
					skip = 0;
					freeInstruction(p);
					mb->stmt[i]= 0;
					continue;
			}
			if (p->argc == 2 && blockStart(p) && block < 0 && isVarConstant(mb, getArg(p, 1)) && getArgType(mb, p, 1) == TYPE_bit ){
				if( getVarConstant(mb, getArg(p, 1)).val.btval == 0)
				{
					block = getArg(p,0);
					skip ++;
					action++;
				}
				// Try to remove the barrier statement itself (when true).
				if ( getVarConstant(mb, getArg(p, 1)).val.btval == 1  && OPTsimpleflow(mb,i))
				{
					block = getArg(p,0);
					skip = 0;
					action++;
					freeInstruction(p);
					mb->stmt[i]= 0;
					continue;
				}
			} else
			if( p->argc == 2 &&  blockStart(p) && block >= 0 && skip == 0 && isVarConstant(mb, getArg(p, 1)) && getArgType(mb, p, 1) == TYPE_bit && multipass == 0)
				multipass++;
			if (skip){
				freeInstruction(p);
				mb->stmt[i]= 0;
			} else
				mb->stmt[j++] = p;
		}
		mb->stop = j;
		for (; j < i; j++)
			mb->stmt[j] = NULL;
	}
	if (action)
		msg = chkTypes(cntxt->usermodule, mb, TRUE);
	return msg;
}

str
OPTevaluateImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	int i, k, limit, *alias = 0, barrier;
	MalStkPtr env = NULL;
	int debugstate = cntxt->itrace, actions = 0, constantblock = 0;
	int *assigned = 0, use;
	str msg = MAL_SUCCEED;

	(void)stk;

	if ( mb->inlineProp )
		goto wrapup;

	cntxt->itrace = 0;

	assigned = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if (assigned == NULL)
		throw(MAL,"optimzier.evaluate", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	alias = (int*)GDKzalloc(mb->vsize * sizeof(int) * 2); /* we introduce more */
	if (alias == NULL){
		GDKfree(assigned);
		throw(MAL,"optimzier.evaluate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	// arguments are implicitly assigned by context
	p = getInstrPtr(mb, 0);
	for ( k =p->retc;  k < p->argc; k++)
		assigned[getArg(p,k)]++;
	limit = mb->stop;
	for (i = 1; i < limit; i++) {
		p = getInstrPtr(mb, i);
		// The double count emerging from a barrier exit is ignored.
		if (! blockExit(p) || (blockExit(p) && p->retc != p->argc))
		for ( k =0;  k < p->retc; k++)
		if ( p->retc != p->argc || p->token != ASSIGNsymbol )
			assigned[getArg(p,k)]++;
	}

	for (i = 1; i < limit && cntxt->mode != FINISHCLIENT; i++) {
		p = getInstrPtr(mb, i);
		// to avoid management of duplicate assignments over multiple blocks
		// we limit ourselves to evaluation of the first assignment only.
		use = assigned[getArg(p,0)] == 1 && !(p->argc == p->retc && blockExit(p));
		for (k = p->retc; k < p->argc; k++)
			if (alias[getArg(p, k)])
				getArg(p, k) = alias[getArg(p, k)];
		/* be aware that you only assign once to a variable */
		if (use && p->retc == 1 && getFunctionId(p) && OPTallConstant(cntxt, mb, p) && !isUnsafeFunction(p)) {
			barrier = p->barrier;
			p->barrier = 0;
			if ( env == NULL) {
				env = prepareMALstack(mb,  2 * mb->vsize);
				if (!env) {
					msg = createException(MAL,"optimizer.evaluate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto wrapup;
				}
				env->keepAlive = TRUE;
			}
			msg = reenterMAL(cntxt, mb, i, i + 1, env);
			p->barrier = barrier;
			if (msg == MAL_SUCCEED) {
				int nvar;
				ValRecord cst;

				actions++;
				cst.vtype = 0;
				VALcopy(&cst, &env->stk[getArg(p, 0)]);
				/* You may not overwrite constants.  They may be used by
				 * other instructions */
				nvar = defConstant(mb, getArgType(mb, p, 0), &cst);
				if( nvar >= 0)
					getArg(p,1) = nvar;
				if (nvar >= env->stktop) {
					VALcopy(&env->stk[getArg(p, 1)], &getVarConstant(mb, getArg(p, 1)));
					env->stktop = getArg(p, 1) + 1;
				}
				alias[getArg(p, 0)] = getArg(p, 1);
				p->argc = 2;
				p->token = ASSIGNsymbol;
				clrFunction(p);
				p->barrier = barrier;
				/* freeze the type */
				setVarFixed(mb,getArg(p,1));
			} else {
				/* if there is an error, we should postpone message handling,
					as the actual error (eg. division by zero ) may not happen) */
				freeException(msg);
				msg= MAL_SUCCEED;
				mb->errors = 0;
			}
		}
		constantblock +=  blockStart(p) && OPTallConstant(cntxt, mb, p);	/* default */
	}
	// produces errors in SQL when enabled
	if ( constantblock)
		msg = OPTremoveUnusedBlocks(cntxt, mb);
	cntxt->itrace = debugstate;

		/* Defense line against incorrect plans */
	/* Plan is unaffected */
	if (!msg)
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
		msg = chkFlow(mb);
	if (!msg)
		msg = chkDeclarations(mb);
		/* keep all actions taken as a post block comment */

wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);

	if (env) {
		assert(env->stktop < env->stksize);
		freeStack(env);
	}
	if (assigned)
		GDKfree(assigned);
	if (alias)
		GDKfree(alias);
	return msg;
}
