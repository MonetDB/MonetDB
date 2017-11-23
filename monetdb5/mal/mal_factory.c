/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * For documentation see website
 */
#include "monetdb_config.h"
#include "mal_factory.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_exception.h"
#include "mal_session.h"
#include "mal_debugger.h"
#include "mal_namespace.h"
#include "mal_private.h"

static int
findPlant(Client cntxt, MalBlkPtr mb){
	int i, lastplant= cntxt->lastPlant;
	Plant cpl = cntxt->plants;

	for(i=0; i< lastplant; i++)
		if( cpl[i].factory == mb)
			return i;
	return -1;
}

mal_export Plant newPlant(Client cntxt, MalBlkPtr mb);

/*
 * A new plant is constructed. The properties of the factory
 * should be known upon compile time. They are retrieved from
 * the signature of the factory definition.
 */
Plant
newPlant(Client cntxt, MalBlkPtr mb)
{
	Plant p, plim;
	MalStkPtr stk;

	plim = cntxt->plants + cntxt->lastPlant;
	for (p = cntxt->plants; p < plim && p->factory; p++)
		;
	stk = newGlobalStack(mb->vsize);
	if (cntxt->lastPlant == MAXPLANTS || stk == NULL){
		if( stk) GDKfree(stk);
		return 0;
	}
	if (p == plim)
		cntxt->lastPlant++;
	p->factory = mb;
	p->id = cntxt->plantId++;

	p->pc = 1;		/* where we start */
	p->stk = stk;
	p->stk->blk = mb;
	p->stk->keepAlive = TRUE;
	return p;
}

str
runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci)
{
	Plant pl=0, cpl;
	int firstcall= TRUE, i, k;
	InstrPtr psig = getInstrPtr(mb, 0);
	ValPtr lhs, rhs;
	char cmd;
	str msg;

#ifdef DEBUG_MAL_FACTORY
	fprintf(stderr, "#factoryMgr called\n");
#endif
	if(!cntxt->plants){
		cntxt->plants = GDKzalloc(MAXPLANTS * sizeof(PlantRecord));
		if(!cntxt->plants)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	cpl = cntxt->plants;

	/* the lookup can be largely avoided by handing out the index
	   upon factory definition. todo
		Alternative is to move them to the front
	 */
	for(i=0; i< cntxt->lastPlant; i++)
	if( cpl[i].factory == mb){
		if(i > 0 && i< cntxt->lastPlant ){
			PlantRecord prec= cpl[i-1];
			cpl[i-1] = cpl[i];
			cpl[i]= prec;
			i--;
		}
		pl= cpl+i;
		firstcall= FALSE;
		break;
	}
	if (pl == 0) {
		/* compress the plant table*/
		for(k=i=0;i<=cntxt->lastPlant; i++)
		if( cpl[i].inuse)
			cpl[k++]= cpl[i];
		cntxt->lastPlant = k;
		/* initialize a new plant using the owner policy */
		pl = newPlant(cntxt, mb);
		if (pl == NULL)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	/*
	 * We have found a factory to process the request.
	 * Let's call it as a synchronous action, without concern on parallelism.
	 */
	/* remember context */
	pl->caller = mbcaller;
	pl->env = stk;
	pl->pci = pci;
	pl->inuse = 1;
	/* inherit debugging */
	cmd = stk->cmd;
	if ( pl->stk == NULL)
		throw(MAL, "factory.call", "internal error, stack frame missing");

	/* copy the calling arguments onto the stack
	   of the factory */
	i = psig->retc;
	for (k = pci->retc; i < pci->argc; i++, k++) {
		lhs = &pl->stk->stk[psig->argv[k]];
		/* variable arguments ? */
		if (k == psig->argc - 1)
			k--;

		rhs = &pl->env->stk[getArg(pci, i)];
		if (VALcopy(lhs, rhs) == NULL)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		if( lhs->vtype == TYPE_bat )
			BBPretain(lhs->val.bval);
	}
	if (mb->errors)
		throw(MAL, "factory.call", PROGRAM_GENERAL);
	if (firstcall ){
		/* initialize the stack */
		for(i= psig->argc; i< mb->vtop; i++) {
			lhs = &pl->stk->stk[i];
			if( isVarConstant(mb,i) > 0 ){
				if( !isVarDisabled(mb,i)){
					rhs = &getVarConstant(mb,i);
					if (VALcopy(lhs,rhs) == NULL)
						throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			} else{
				lhs->vtype = getVarGDKType(mb,i);
				lhs->val.pval = 0;
				lhs->len = 0;
			}
		}
		pl->stk->stkbot= mb->vtop;	/* stack already initialized */
		msg = runMAL(cntxt, mb, 0, pl->stk);
	} else {
		msg = reenterMAL(cntxt, mb, pl->pc, -1, pl->stk);
	}
	/*if(pl->stk) { TODO check the memory leaks in factories
		i = psig->retc;
		for (k = pci->retc; i < pci->argc; i++, k++) { //for subsequent calls, the previous arguments must be freed
			lhs = &pl->stk->stk[psig->argv[k]];
			 variable arguments ?
			if (k == psig->argc - 1)
				k--;
			if (lhs->vtype == TYPE_str) {
				GDKfree(lhs->val.sval);
				lhs->val.sval = NULL;
			}
		}
	}*/
	/* propagate change in debugging status */
	if (cmd && pl->stk && pl->stk->cmd != cmd && cmd != 'x')
		for (; stk; stk = stk->up)
			stk->cmd = pl->stk->cmd;
	return msg;
}
/*
 * The shortcut operator for factory calls assumes that the user is
 * not interested in the results produced.
 */
str
callFactory(Client cntxt, MalBlkPtr mb, ValPtr argv[], char flag){
	Plant pl;
	InstrPtr psig = getInstrPtr(mb, 0);
	int i;
	ValPtr lhs,rhs;
	MalStkPtr stk;
	str ret;

	if(!cntxt->plants){
		cntxt->plants = GDKzalloc(MAXPLANTS * sizeof(PlantRecord));
		if(!cntxt->plants)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		memset((void*) (cntxt->plants), 0, MAXPLANTS * sizeof(PlantRecord));
	}

	i= findPlant(cntxt, mb);
	if( i< 0) {
		/* first call? prepare the factory */
		pl = newPlant(cntxt, mb);
		if (pl == NULL)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		/* remember context, which does not exist. */
		pl->caller = 0;
		pl->env = 0;
		pl->pci = 0;
		pl->inuse = 1;
		stk = pl->stk;
		/* initialize the stack */
		stk->stktop= mb->vtop;
		stk->stksize= mb->vsize;
		stk->blk= mb;
		stk->up = 0;
		stk->cmd= flag;
		/* initialize the stack */
		for(i= psig->argc; i< mb->vtop; i++)
		if( isVarConstant(mb,i) > 0 ){
			lhs = &stk->stk[i];
			rhs = &getVarConstant(mb,i);
			if (VALcopy(lhs,rhs) == NULL)
				throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		} else {
			lhs = &stk->stk[i];
			lhs->vtype = getVarGDKType(mb,i);
		}
		pl->stk= stk;
	} else  {
		pl= cntxt->plants + i;
		/*
		 * When you re-enter the factory the old arguments should be
		 * released to make room for the new ones.
		 */
		for (i = psig->retc; i < psig->argc; i++) {
			lhs = &pl->stk->stk[psig->argv[i]];
			if( lhs->vtype == TYPE_bat )
				BBPrelease(lhs->val.bval);
		}
	}
	/* copy the calling arguments onto the stack of the factory */
	i = psig->retc;
	for (i = psig->retc; i < psig->argc; i++) {
		lhs = &pl->stk->stk[psig->argv[i]];
		if (VALcopy(lhs, argv[i]) == NULL)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		if( lhs->vtype == TYPE_bat )
			BBPretain(lhs->val.bval);
	}
	ret=  reenterMAL(cntxt, mb, pl->pc, -1, pl->stk);
	/* garbage collect the string arguments, these positions
	   will simply be overwritten the next time.
	for (i = psig->retc; i < psig->argc; i++)
		garbageElement(lhs = &pl->stk->stk[psig->argv[i]]);
	*/
	return ret;
}

/*
 * Upon reaching the yield operator, the factory is
 * suspended until the next request arrives.
 * The information in the target list should be delivered
 * to the caller stack frame.
 */
int
yieldResult(Client cntxt, MalBlkPtr mb, InstrPtr p, int pc)
{
	Plant pl, cpl = cntxt->plants, plim = cntxt->plants + cntxt->lastPlant;
	ValPtr lhs, rhs;
	int i;

	(void) p;
	(void) pc;
	for (pl = cpl; pl < plim; pl++)
		if (pl->factory == mb ) {
			if( pl->env == NULL)
				return(int) (pl - cpl);
			for (i = 0; i < p->retc; i++) {
#ifdef DEBUG_MAL_FACTORY
				fprintf(stderr,"#lhs %d rhs %d\n", getArg(pl->pci, i), getArg(p, i));
#endif
				rhs = &pl->stk->stk[getArg(p, i)];
				lhs = &pl->env->stk[getArg(pl->pci, i)];
				if (VALcopy(lhs, rhs) == NULL)
					return -1;
			}
			return (int) (pl - cpl);
		}
	return -1;
}

str
yieldFactory(Client cntxt, MalBlkPtr mb, InstrPtr p, int pc)
{
	Plant pl;
	int i;

	i = yieldResult(cntxt, mb, p, pc);

	if (i>=0) {
		pl = cntxt->plants+i;
		pl->pc = pc + 1;
		pl->caller = NULL;
		pl->pci = NULL;
		pl->env = NULL;
		return MAL_SUCCEED;
	}
	throw(MAL, "factory.yield", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

/*
 * A return from a factory body implies removal of
 * all state information.
 * This code should also prepare for handling factories
 * that are still running threads in parallel.
 */

str
shutdownFactory(Client cntxt, MalBlkPtr mb)
{
	Plant pl, plim;

	if(!cntxt->plants)
		return MAL_SUCCEED;

	plim = cntxt->plants + cntxt->lastPlant;
	for (pl = cntxt->plants; pl < plim; pl++)
		if (pl->factory == mb) {
			/* MSresetVariables(mb, pl->stk, 0);*/
			/* freeStack(pl->stk); there may be a reference?*/
			/* we are inside the body of the factory and about to return */
			pl->factory = 0;
			if (pl->stk) {
				pl->stk->keepAlive = FALSE;
				garbageCollector(cntxt, mb, pl->stk,TRUE);
				GDKfree(pl->stk);
			}
			pl->stk=0;
			pl->pc = 0;
			pl->inuse = 0;
			pl->caller = NULL;
			pl->pci = NULL;
			pl->env = NULL;
		}
	return MAL_SUCCEED;
}

str
shutdownFactoryByName(Client cntxt, Module m, str nme){
	Plant pl, plim;
	InstrPtr p;
	Symbol s;

	if(!cntxt->plants)
		return MAL_SUCCEED;

	plim = cntxt->plants + cntxt->lastPlant;
	for (pl = cntxt->plants; pl < plim; pl++)
		if (pl->factory ) {
			MalStkPtr stk;

			p= getInstrPtr(pl->factory,0);
			if( strcmp(nme, getFunctionId(p)) != 0) continue;
			s = findSymbolInModule(m, nme );
			if (s == NULL){
				throw(MAL, "factory.remove",
					OPERATION_FAILED " SQL entry '%s' not found",
					putName(nme));
			}
			stk = pl->stk;
			MSresetVariables(cntxt, pl->factory, stk, 0);
			shutdownFactory(cntxt, pl->factory);
			freeStack(stk);
			deleteSymbol(m,s);
			return MAL_SUCCEED;
		}
	return MAL_SUCCEED;
}

void mal_factory_reset(Client cntxt) {
	Plant pl, plim;

	if (cntxt->plants) {
		plim = cntxt->plants + cntxt->lastPlant;
		for (pl = cntxt->plants; pl < plim; pl++){
			/* MSresetVariables(mb, pl->stk, 0);*/
			/* freeStack(pl->stk); there may be a reference?*/
			/* we are inside the body of the factory and about to return */
			if (pl->stk) {
				pl->stk->keepAlive = FALSE;
				//garbageCollector(NULL, pl->factory, pl->stk, TRUE); /* this will be freed by the freeModule call */
				GDKfree(pl->stk);
			}
			pl->factory = 0;
			pl->stk=0;
			pl->pc = 0;
			pl->inuse = 0;
			pl->caller = NULL;
			pl->pci = NULL;
			pl->env = NULL;
		}
		GDKfree(cntxt->plants);
		cntxt->plants = 0;
	}
	cntxt->plantId = 1;
	cntxt->lastPlant = 0;
}
