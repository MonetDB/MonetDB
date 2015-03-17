/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/* (c) M. Kersten
 * Also include down-casting decisions on the SQL code produced
 */

#include "monetdb_config.h"
#include "opt_coercion.h"

typedef struct{
	int pc;
	int fromtype;
	int totype;
	int src;
	int digits;
	int scale;
} Coercion;

static int
coercionOptimizerStep(MalBlkPtr mb, int i, InstrPtr p)
{
	int t, k, a, b;

	a = getArg(p, 0);
	b = getArg(p, 1);
	t = getVarType(mb, b);
	if (getVarType(mb, a) != t)
		return 0;
	if (strcmp(getFunctionId(p), ATOMname(t)) == 0) {
		removeInstruction(mb, p); /* dead code */
		for (; i < mb->stop; i++) {
			p = getInstrPtr(mb, i);
			for (k = p->retc; k < p->argc; k++)
				if (p->argv[k] == a)
					p->argv[k] = b;
		}
		return 1;
	}
	return 0;
}

/* Check coercions for numeric types that can be handled with smaller ones.
 * For now, limit to +,-,/,*,% hge expressions
 * To be extended to deal with math calls as well.
 */
static void
coercionOptimizerCalcStep(MalBlkPtr mb, int i, Coercion *coerce)
{
	InstrPtr p = getInstrPtr(mb,i);
	int r, a, b;

	if( getModuleId(p) != batcalcRef || getFunctionId(p) == 0) return;
	if( ! (getFunctionId(p) == plusRef || getFunctionId(p) == minusRef || getFunctionId(p) == mulRef || getFunctionId(p) == divRef || *getFunctionId(p) =='%') || p->argc !=3)
		return;

	r = getColumnType(getVarType(mb, getArg(p,0)));
	switch(r){
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		break;
	case TYPE_dbl:
	case TYPE_flt:
		/* to be determined */
	default:
		return;
	}
	a = getColumnType(getVarType(mb, getArg(p,1)));
	b = getColumnType(getVarType(mb, getArg(p,2)));
	if ( a == r && coerce[getArg(p,1)].src && coerce[getArg(p,1)].fromtype < r ) /*digit/scale test as well*/
		getArg(p,1) = coerce[getArg(p,1)].src;
	if ( b == r && coerce[getArg(p,2)].src &&  coerce[getArg(p,2)].fromtype < r ) /*digit/scale test as well*/
		getArg(p,2) = coerce[getArg(p,2)].src;
	return;
}

static void
coercionOptimizerAggrStep(MalBlkPtr mb, int i, Coercion *coerce)
{
	InstrPtr p = getInstrPtr(mb,i);
	int r, k;

	if( getModuleId(p) != aggrRef || getFunctionId(p) == 0) return;
	if( ! (getFunctionId(p) == subavgRef ) || p->argc !=6)
		return;

	r = getColumnType(getVarType(mb, getArg(p,0)));
	k = getArg(p,1);
	// check the digits/scale
	if( r == TYPE_dbl &&  coerce[k].src )
		getArg(p,1) = coerce[getArg(p,1)].src;
	return;
}

int
OPTcoercionImplementation(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k;
	InstrPtr p;
	int actions = 0;
	str calcRef= putName("calc",4);
	Coercion *coerce = GDKzalloc(sizeof(Coercion) * mb->vtop);

	if( coerce == NULL)
		return 0;
	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == NULL)
			continue;
/* Downscale the type, avoiding hge storage when lng would be sufficient.
 * The code template can be extended to handle other downscale options as well
 */
#ifdef HAVE_HGE
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == hgeRef && p->retc == 1 && p->argc == 5 && isVarConstant(mb,getArg(p,4)) && isVarConstant(mb,getArg(p,3)) ){
			k = getArg(p,0);
			coerce[k].pc= i;
			coerce[k].totype= TYPE_hge;
			coerce[k].src= getArg(p,2);
			coerce[k].fromtype= getColumnType(getArgType(mb,p,2));
			coerce[k].digits= getVarConstant(mb,getArg(p,3)).val.ival;
			coerce[k].scale= getVarConstant(mb,getArg(p,4)).val.ival;
		}
#endif
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == dblRef && p->retc == 1 && p->argc == 2 ){
			k = getArg(p,0);
			coerce[k].pc= i;
			coerce[k].totype= TYPE_dbl;
			coerce[k].src= getArg(p,1 + (p->argc ==3));
			coerce[k].fromtype= getColumnType(getArgType(mb,p,1 + (p->argc ==3)));
		}
		coercionOptimizerAggrStep(mb, i, coerce);
		coercionOptimizerCalcStep(mb, i, coerce);
		if (getModuleId(p)==calcRef && p->argc == 2) {
			k= coercionOptimizerStep(mb, i, p);
			actions += k;
			if( k) i--;
		}
	}
	/*
	 * This optimizer affects the flow, but not the type and declaration
	 * structure. A cheaper optimizer is sufficient.
	 */
	GDKfree(coerce);
	return actions;
}
