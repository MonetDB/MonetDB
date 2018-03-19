/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_generator.h"
#include "mal_builder.h"

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#define errorCheck(P,MOD,I) \
setModuleId(P, generatorRef);\
typeChecker(cntxt->usermodule, mb, P, TRUE);\
if(P->typechk == TYPE_UNKNOWN){\
	setModuleId(P,MOD);\
	typeChecker(cntxt->usermodule, mb, P, TRUE);\
	setModuleId(series[I], generatorRef);\
	setFunctionId(series[I], seriesRef);\
	typeChecker(cntxt->usermodule, mb, series[I], TRUE);\
}\
pushInstruction(mb,P); 

#define casting(TPE)\
			k= getArg(p,1);\
			p->argc = p->retc;\
			q= newInstruction(0,calcRef, TPE##Ref);\
			setDestVar(q, newTmpVariable(mb, TYPE_##TPE));\
			pushArgument(mb,q,getArg(series[k],1));\
			typeChecker(cntxt->usermodule, mb, q, TRUE);\
			p = pushArgument(mb,p, getArg(q,0));\
			pushInstruction(mb,q);\
			q= newInstruction(0,calcRef,TPE##Ref);\
			setDestVar(q, newTmpVariable(mb, TYPE_##TPE));\
			pushArgument(mb,q,getArg(series[k],2));\
			pushInstruction(mb,q);\
			typeChecker(cntxt->usermodule, mb, q, TRUE);\
			p = pushArgument(mb,p, getArg(q,0));\
			if( p->argc == 4){\
				q= newInstruction(0,calcRef,TPE##Ref);\
				setDestVar(q, newTmpVariable(mb, TYPE_##TPE));\
				pushArgument(mb,q,getArg(series[k],3));\
				typeChecker(cntxt->usermodule, mb, q, TRUE);\
				p = pushArgument(mb,p, getArg(q,0));\
				pushInstruction(mb,q);\
			}\
			setModuleId(p,generatorRef);\
			setFunctionId(p,parametersRef);\
			setVarUDFtype(mb,getArg(p,0));\
			series[getArg(p,0)] = p;\
			pushInstruction(mb,p);

str 
OPTgeneratorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p,q, *old, *series;
	int i, k, limit, slimit, actions=0;
	str m;
	str bteRef = getName("bte");
	str shtRef = getName("sht");
	str intRef = getName("int");
	str lngRef = getName("lng");
	str fltRef = getName("flt");
	str dblRef = getName("dbl");
	char buf[256];
	lng usec= GDKusec();

	(void) cntxt;
	(void) stk;
	(void) pci;

	series = (InstrPtr*) GDKzalloc(sizeof(InstrPtr) * mb->vtop);
	if(series == NULL)
		throw(MAL,"optimizer.generator", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;

	// check applicability first
	for( i=0; i < limit; i++){
		p = old[i];
		if ( getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef)
			break;
	}
	if (i == limit) {
		GDKfree(series);
		return 0;
	}
	
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(series);
		throw(MAL,"optimizer.generator", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			typeChecker(cntxt->usermodule, mb, p, TRUE);
			pushInstruction(mb,p); 
		} else if ( getModuleId(p) == algebraRef && getFunctionId(p) == selectRef && series[getArg(p,1)]){
			errorCheck(p,algebraRef,getArg(p,1));
		} else if ( getModuleId(p) == algebraRef && getFunctionId(p) == thetaselectRef && series[getArg(p,1)]){
			errorCheck(p,algebraRef,getArg(p,1));
		} else if ( getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef && series[getArg(p,2)]){
			errorCheck(p,algebraRef,getArg(p,2));
		} else if ( getModuleId(p) == sqlRef && getFunctionId(p) ==  putName("exportValue") && isaBatType(getArgType(mb,p,0)) ){
			// interface expects scalar type only, not expressable in MAL signature
			mb->errors=createException(MAL, "generate_series", SQLSTATE(42000) "internal error, generate_series is a table producing function");
		}else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == bteRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(bte);
		} else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == shtRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(sht);
		} else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == intRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(int);
		} else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == lngRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(lng);
		} else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == fltRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(flt);
		} else if ( getModuleId(p) == batcalcRef && getFunctionId(p) == dblRef && series[getArg(p,1)] && p->argc == 2 ){
			casting(dbl);
		} else if ( getModuleId(p) == languageRef && getFunctionId(p) == passRef )
			pushInstruction(mb,p);
		else {
			// check for use without conversion
			for(k = p->retc; k < p->argc; k++)
			if( series[getArg(p,k)]){
				m = getModuleId(p);
				setModuleId(p, generatorRef);
				typeChecker(cntxt->usermodule, mb, p, TRUE);
				if(p->typechk == TYPE_UNKNOWN){
					setModuleId(p,m);
					typeChecker(cntxt->usermodule, mb, p, TRUE);
					setModuleId(series[getArg(p,k)], generatorRef);
					setFunctionId(series[getArg(p,k)], seriesRef);
					typeChecker(cntxt->usermodule, mb, series[getArg(p,k)], TRUE);
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
	fprintFunction(stderr,mb,0,LIST_MAL_ALL);
#endif

    /* Defense line against incorrect plans */
	/* all new/modified statements are already checked */
	//chkTypes(cntxt->usermodule, mb, FALSE);
	//chkFlow(mb);
	//chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","generator",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

	return MAL_SUCCEED;
}
