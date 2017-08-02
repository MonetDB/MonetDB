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

typedef struct {
	int id;			/* unique plant number */
	MalBlkPtr factory;
	MalStkPtr stk;		/* private state */
	int pc;			/* where we are */
	int inuse;		/* able to handle it */
	int next;		/* next plant of same factory */
	int policy;		/* flags to control behavior */

	Client client;		/* who called it */
	MalBlkPtr caller;	/* from routine */
	MalStkPtr env;		/* with the stack  */
	InstrPtr pci;		/* with the instruction */
} PlantRecord, *Plant;

#define MAXPLANTS 256
static PlantRecord plants[MAXPLANTS];
static int lastPlant= 0;
static int plantId = 1;

mal_export Plant newPlant(MalBlkPtr mb);


static int
findPlant(MalBlkPtr mb){
	int i;
	for(i=0; i<lastPlant; i++)
	if( plants[i].factory == mb)
		return i;
	return -1;
}

str
runFactory(Client cntxt, MalBlkPtr mb, MalBlkPtr mbcaller, MalStkPtr stk, InstrPtr pci)
{
	Plant pl=0;
	int firstcall= TRUE, i, k;
	InstrPtr psig = getInstrPtr(mb, 0);
	ValPtr lhs, rhs;
	char cmd;
	str msg;

#ifdef DEBUG_MAL_FACTORY
	fprintf(stderr, "#factoryMgr called\n");
#endif
	/* the lookup can be largely avoided by handing out the index
	   upon factory definition. todo
		Alternative is to move them to the front
	 */
	for(i=0; i< lastPlant; i++)
	if( plants[i].factory == mb){
		if(i > 0 && i< lastPlant ){
			PlantRecord prec= plants[i-1];
			plants[i-1] = plants[i];
			plants[i]= prec;
			i--;
		}
		pl= plants+i;
		firstcall= FALSE;
		break;
	}
	if (pl == 0) {
		/* compress the plant table*/
		for(k=i=0;i<=lastPlant; i++)
		if( plants[i].inuse)
			plants[k++]= plants[i];
		lastPlant = k;
		/* initialize a new plant using the owner policy */
		pl = newPlant(mb);
		if (pl == NULL)
			throw(MAL, "factory.new", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	/*
	 * We have found a factory to process the request.
	 * Let's call it as a synchronous action, without concern on parallelism.
	 */
	/* remember context */
	pl->client = cntxt;
	pl->caller = mbcaller;
	pl->env = stk;
	pl->pci = pci;
	pl->inuse = 1;
	/* inherit debugging */
	cmd = stk->cmd;
	if ( pl->stk == NULL)
		throw(MAL, "factory.new", "internal error, stack frame missing");

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

	i= findPlant(mb);
	if( i< 0) {
		/* first call? prepare the factory */
		pl = newPlant(mb);
		if (pl == NULL)
			throw(MAL, "factory.call", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		/* remember context, which does not exist. */
		pl->client = cntxt;
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
		pl= plants+i;
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
 * A new plant is constructed. The properties of the factory
 * should be known upon compile time. They are retrieved from
 * the signature of the factory definition.
 */
Plant
newPlant(MalBlkPtr mb)
{
	Plant p, plim;
	MalStkPtr stk;

	plim = plants + lastPlant;
	for (p = plants; p < plim && p->factory; p++)
		;
	stk = newGlobalStack(mb->vsize);
	if (lastPlant == MAXPLANTS || stk == NULL){
		if( stk) GDKfree(stk);
		return 0;
	}
	if (p == plim)
		lastPlant++;
	p->factory = mb;
	p->id = plantId++;

	p->pc = 1;		/* where we start */
	p->stk = stk;
	p->stk->blk = mb;
	p->stk->keepAlive = TRUE;
	return p;
}

/*
 * Upon reaching the yield operator, the factory is
 * suspended until the next request arrives.
 * The information in the target list should be delivered
 * to the caller stack frame.
 */
int
yieldResult(MalBlkPtr mb, InstrPtr p, int pc)
{
	Plant pl, plim = plants + lastPlant;
	ValPtr lhs, rhs;
	int i;

	(void) p;
	(void) pc;
	for (pl = plants; pl < plim; pl++)
		if (pl->factory == mb ) {
			if( pl->env == NULL)
				return(int) (pl-plants);
			for (i = 0; i < p->retc; i++) {
#ifdef DEBUG_MAL_FACTORY
				fprintf(stderr,"#lhs %d rhs %d\n", getArg(pl->pci, i), getArg(p, i));
#endif
				rhs = &pl->stk->stk[getArg(p, i)];
				lhs = &pl->env->stk[getArg(pl->pci, i)];
				if (VALcopy(lhs, rhs) == NULL)
					return -1;
			}
			return (int) (pl-plants);
		}
	return -1;
}

str
yieldFactory(MalBlkPtr mb, InstrPtr p, int pc)
{
	Plant pl;
	int i;

	i = yieldResult(mb, p, pc);

	if (i>=0) {
		pl = plants+i;
		pl->pc = pc + 1;
		pl->client = NULL;
		pl->caller = NULL;
		pl->pci = NULL;
		pl->env = NULL;
		return MAL_SUCCEED;
	}
	throw(MAL, "factory.yield", RUNTIME_OBJECT_MISSING);
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

	plim = plants + lastPlant;
	for (pl = plants; pl < plim; pl++)
		if (pl->factory == mb) {
			/* MSresetVariables(mb, pl->stk, 0);*/
			/* freeStack(pl->stk); there may be a reference?*/
			/* we are inside the body of the factory and about to return */
			pl->factory = 0;
			if (pl->stk)
				pl->stk->keepAlive = FALSE;
			if ( pl->stk) {
				garbageCollector(cntxt, mb, pl->stk,TRUE);
				GDKfree(pl->stk);
			}
			pl->stk=0;
			pl->pc = 0;
			pl->inuse = 0;
			pl->client = NULL;
			pl->caller = NULL;
			pl->pci = NULL;
			pl->env = NULL;
			pl->client = NULL;
			pl->caller = NULL;
			pl->env= NULL;
			pl->pci = NULL;
		}
	return MAL_SUCCEED;
}

str
shutdownFactoryByName(Client cntxt, Module m, str nme){
	Plant pl, plim;
	InstrPtr p;
	Symbol s;

	plim = plants + lastPlant;
	for (pl = plants; pl < plim; pl++)
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

void mal_factory_reset(void)
{
	Plant pl, plim;

	plim = plants + lastPlant;
	for (pl = plants; pl < plim; pl++){
			/* MSresetVariables(mb, pl->stk, 0);*/
			/* freeStack(pl->stk); there may be a reference?*/
			/* we are inside the body of the factory and about to return */
			if (pl->stk) {
				pl->stk->keepAlive = FALSE;
				garbageCollector(NULL, pl->factory, pl->stk, TRUE);
				GDKfree(pl->stk);
			}
			pl->factory = 0;
			pl->stk=0;
			pl->pc = 0;
			pl->inuse = 0;
			pl->client = NULL;
			pl->caller = NULL;
			pl->pci = NULL;
			pl->env = NULL;
			pl->client = NULL;
			pl->caller = NULL;
			pl->env= NULL;
			pl->pci = NULL;
	}
	plantId = 1;
	lastPlant = 0;
}
