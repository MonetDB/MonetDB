/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/* (c) M. Kersten
 * Also include down-casting decisions on the SQL code produced
 */

#include "monetdb_config.h"
#include "opt_coercion.h"
#include "opt_aliases.h"

typedef struct{
	int pc;
	int fromtype;
	int totype;
	int src;
} Coercion;

/* Check coercions for numeric types towards :hge that can be handled with smaller ones.
 * For now, limit to +,-,/,*,% hge expressions
 * Not every combination may be available in the MAL layer, which calls
 * for a separate type check before fixing it.
 * Superflous coercion statements will be garbagecollected later on in the pipeline
 */
static void
coercionOptimizerCalcStep(Client cntxt, MalBlkPtr mb, int i, Coercion *coerce)
{
	InstrPtr p = getInstrPtr(mb,i);
	int r, a, b, varid;

	r = getBatType(getVarType(mb, getArg(p,0)));
#ifdef HAVE_HGE
	if ( r != TYPE_hge)
		return;
#endif
	if( getModuleId(p) != batcalcRef || getFunctionId(p) == 0) return;
	if( ! (getFunctionId(p) == plusRef || getFunctionId(p) == minusRef || getFunctionId(p) == mulRef || getFunctionId(p) == divRef || getFunctionId(p) == modRef) || p->argc !=3)
		return;

	a = getBatType(getVarType(mb, getArg(p,1)));
	b = getBatType(getVarType(mb, getArg(p,2)));
	varid = getArg(p,1);
	if ( a == r && coerce[varid].src && coerce[varid].fromtype < r )
	{
		// Remove upcast on first argument
		getArg(p,1) = coerce[varid].src;
		if ( chkInstruction(cntxt->usermodule, mb, p) || p->typechk == TYPE_UNKNOWN)
			getArg(p,1) = varid;
	}
	varid = getArg(p,2);
	if ( b == r && coerce[varid].src &&  coerce[varid].fromtype < r )
	{
		// Remove upcast on second argument
		getArg(p,2) = coerce[varid].src;
		if ( chkInstruction(cntxt->usermodule, mb, p) || p->typechk == TYPE_UNKNOWN)
			getArg(p,2) = varid;
	}
	return;
}

static void
coercionOptimizerAggrStep(Client cntxt, MalBlkPtr mb, int i, Coercion *coerce)
{
	InstrPtr p = getInstrPtr(mb,i);
	int r, k;

	(void) cntxt;

	if( getModuleId(p) != aggrRef || getFunctionId(p) == 0) return;
	if( ! (getFunctionId(p) == subavgRef ) || p->argc !=6)
		return;

	r = getBatType(getVarType(mb, getArg(p,0)));
	k = getArg(p,1);
	if( r == TYPE_dbl &&  coerce[k].src ){
		getArg(p,1) = coerce[k].src;
		if ( chkInstruction(cntxt->usermodule, mb, p) || p->typechk == TYPE_UNKNOWN)
			getArg(p,1)= k;
	}
	return;
}

str
OPTcoercionImplementation(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k, t;
	InstrPtr p;
	int actions = 0;
	const char *calcRef= putName("calc");
	Coercion *coerce = GDKzalloc(sizeof(Coercion) * mb->vtop);
	str msg = MAL_SUCCEED;

	if( coerce == NULL)
		throw(MAL,"optimizer.coercion", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	(void) cntxt;
	(void) stk;		/* to fool compilers */

	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == NULL)
			continue;
/* Downscale the type, avoiding hge storage when lng would be sufficient.
 */
#ifdef HAVE_HGE
		if ( getModuleId(p) == batcalcRef
			 && getFunctionId(p) == hgeRef
			 && p->retc == 1
			 && ( p->argc == 5
				   && isVarConstant(mb,getArg(p,1))
				   && getArgType(mb,p,1) == TYPE_int
				   && isVarConstant(mb,getArg(p,3))
				   && getArgType(mb,p,3) == TYPE_int
				   && isVarConstant(mb,getArg(p,4))
				   && getArgType(mb,p,4) == TYPE_int
				   /* from-scale == to-scale, i.e., no scale change */
				   && *(int*) getVarValue(mb, getArg(p,1)) == *(int*) getVarValue(mb, getArg(p,4)) ) ){
			k = getArg(p,0);
			coerce[k].pc= i;
			coerce[k].totype= TYPE_hge;
			coerce[k].src= getArg(p,2);
			coerce[k].fromtype= getBatType(getArgType(mb,p,2));
		}
#endif
		if ( getModuleId(p) == batcalcRef
			 && getFunctionId(p) == dblRef
			 && p->retc == 1
			 && ( p->argc == 2
				  || ( p->argc == 3
					   && isVarConstant(mb,getArg(p,1))
					   && getArgType(mb,p,1) == TYPE_int
					   //to-scale == 0, i.e., no scale change
					   && *(int*) getVarValue(mb, getArg(p,1)) == 0 ) ) ) {
			k = getArg(p,0);
			coerce[k].pc= i;
			coerce[k].totype= TYPE_dbl;
			coerce[k].src= getArg(p,1 + (p->argc ==3));
			coerce[k].fromtype= getBatType(getArgType(mb,p,1 + (p->argc ==3)));
		}
		coercionOptimizerAggrStep(cntxt,mb, i, coerce);
		coercionOptimizerCalcStep(cntxt,mb, i, coerce);
		if (getModuleId(p)==calcRef && p->argc == 2) {
			t = getVarType(mb, getArg(p,1));
			if (getVarType(mb, getArg(p,0)) == t && strcmp(getFunctionId(p), ATOMname(t)) == 0) {
				/* turn it into an assignment */
				clrFunction(p);
				actions ++;
			}
		}
	}
	/*
	 * This optimizer affects the flow, but not the type and declaration
	 * structure. A cheaper optimizer is sufficient.
	 */
	GDKfree(coerce);

	/* Defense line against incorrect plans */
	if( actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
