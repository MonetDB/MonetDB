/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_generator.h"

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#define errorCheck(P,MOD,I) \
setModuleId(P, generatorRef);\
typeChecker(cntxt->fdout, cntxt->nspace, mb, P, TRUE);\
if(P->typechk == TYPE_UNKNOWN){\
	setModuleId(P,MOD);\
	typeChecker(cntxt->fdout, cntxt->nspace, mb, P, TRUE);\
	setModuleId(series[I], generatorRef);\
	setFunctionId(series[I], seriesRef);\
	typeChecker(cntxt->fdout, cntxt->nspace, mb, series[I], TRUE);\
}\
pushInstruction(mb,P); 

#define casting(TPE)\
			k= getArg(p,1);\
			p->argc = p->retc;\
			q= newStmt(mb,calcRef,TPE##Ref);\
			setArgType(mb,q,0,TYPE_##TPE);\
			pushArgument(mb,q,getArg(series[k],1));\
			typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);\
			p = pushArgument(mb,p, getArg(q,0));\
			q= newStmt(mb,calcRef,TPE##Ref);\
			setArgType(mb,q,0,TYPE_##TPE);\
			pushArgument(mb,q,getArg(series[k],2));\
			typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);\
			p = pushArgument(mb,p, getArg(q,0));\
			if( p->argc == 4){\
				q= newStmt(mb,calcRef,TPE##Ref);\
				setArgType(mb,q,0,TYPE_##TPE);\
				pushArgument(mb,q,getArg(series[k],3));\
				typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);\
				p = pushArgument(mb,p, getArg(q,0));\
			}\
			setModuleId(p,generatorRef);\
			setFunctionId(p,parametersRef);\
			setVarUDFtype(mb,getArg(p,0));\
			series[getArg(p,0)] = p;\
			pushInstruction(mb,p);

int 
OPTgeneratorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p,q, *old, *series;
	int i, k, limit, slimit, actions=0;
	str m;
	str bteRef = getName("bte",3);
	str shtRef = getName("sht",3);
	str intRef = getName("int",3);
	str lngRef = getName("lng",3);
	str fltRef = getName("flt",3);
	str dblRef = getName("dbl",3);

	(void) cntxt;
	(void) stk;
	(void) pci;

	series = (InstrPtr*) GDKzalloc(sizeof(InstrPtr) * mb->vtop);
	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;

	// check applicability first
	for( i=0; i < limit; i++){
		p = old[i];
		if ( getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef)
			break;
	}
	if( i == limit)
		return 0;
	
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
	GDKfree(series);
		return 0;
	}

	for( i=0; i < limit; i++){
		p = old[i];
		if (p->token == ENDsymbol){
			pushInstruction(mb,p); 
			break;
		}
		if ( getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef){
			series[getArg(p,0)] = p;
			setModuleId(p, generatorRef);
			setFunctionId(p, parametersRef);
			typeChecker(cntxt->fdout, cntxt->nspace, mb, p, TRUE);
			pushInstruction(mb,p); 
		} else
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == subselectRef && series[getArg(p,1)]){
			errorCheck(p,algebraRef,getArg(p,1));
		} else
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == thetasubselectRef && series[getArg(p,1)]){
			errorCheck(p,algebraRef,getArg(p,1));
		} else
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == leftfetchjoinRef && series[getArg(p,2)]){
			errorCheck(p,algebraRef,getArg(p,2));
		} else
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == joinRef && series[getArg(p,2)] ){
			errorCheck(p,algebraRef,getArg(p,2));
		} else
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == joinRef && series[getArg(p,3)]){
			errorCheck(p,algebraRef,getArg(p,3));
		} else
		if ( getModuleId(p) == sqlRef && getFunctionId(p) ==  putName("exportValue",11) && isaBatType(getArgType(mb,p,0)) ){
			// interface expects scalar type only, not expressable in MAL signature
			mb->errors++;
			showException(cntxt->fdout, MAL, "generate_series", "internal error, generate_series is a table producing function");
		}else 
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == bteRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(bte);
		} else
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == shtRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(sht);
		} else
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == intRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(int);
		} else
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == lngRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(lng);
		} else
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == fltRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(flt);
		} else
		if ( getModuleId(p) == batcalcRef && getFunctionId(p) == dblRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(dbl);
		} else
		if ( getModuleId(p) == languageRef && getFunctionId(p) == passRef )
			pushInstruction(mb,p);
		else {
			// check for use without conversion
			for(k = p->retc; k < p->argc; k++)
			if( series[getArg(p,k)]){
				m = getModuleId(p);
				setModuleId(p, generatorRef);
				typeChecker(cntxt->fdout, cntxt->nspace, mb, p, TRUE);
				if(p->typechk == TYPE_UNKNOWN){
					setModuleId(p,m);
					typeChecker(cntxt->fdout, cntxt->nspace, mb, p, TRUE);
					setModuleId(series[getArg(p,k)], generatorRef);
					setFunctionId(series[getArg(p,k)], seriesRef);
					typeChecker(cntxt->fdout, cntxt->nspace, mb, series[getArg(p,k)], TRUE);
				}
			}
			pushInstruction(mb,p);
		}
	}
	for (i++; i < limit; i++)
        	pushInstruction(mb, old[i]);
	for (; i < slimit; i++)
		if (old[i])
        		freeInstruction(old[i]);
    	GDKfree(old);
    	GDKfree(series);

#ifdef VLT_DEBUG
	printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL);
#endif
	return actions;
}
