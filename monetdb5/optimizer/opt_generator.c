/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "opt_generator.h"

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */


static int
assignedOnce(MalBlkPtr mb, int varid)
{
	InstrPtr p;
	int i,j, c=0;

	for(i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		for( j = 0; j < p->retc; j++)
		if( getArg(p,j) == varid){
			c++;
			break;
		}
	}
	return c == 1;
}
static int
useCount(MalBlkPtr mb, int varid)
{
	InstrPtr p;
	int i,j, d,c=0;

	for(i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		d= 0;
		for( j = p->retc; j < p->argc; j++)
		if( getArg(p,j) == varid)
			d++;
		c += d > 0;
	}
	return c;
}

int 
OPTgeneratorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p,q;
	int i,j,k, actions=0, used, cases, blocked;

	(void) cntxt;
	(void) stk;
	(void) pci;

	for( i=1; i < mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef){
			/* found a target for propagation */
			used = 0;
			if ( assignedOnce(mb, getArg(p,0)) ){
				cases = useCount(mb, getArg(p,0));
				blocked = 0;
				for( j = i+1; j< mb->stop && blocked == 0; j++){
					q = getInstrPtr(mb,j);
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == subselectRef && getArg(q,1) == getArg(p,0)){
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					} else
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == thetasubselectRef && getArg(q,1) == getArg(p,0)){
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					} else
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == leftfetchjoinRef && getArg(q,2) == getArg(p,0)){
						// projection over a series
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					} else
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == joinRef && (getArg(q,2) == getArg(p,0) || getArg(q,3) == getArg(p,0))){
						// projection over a series
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						if(q->typechk == TYPE_UNKNOWN){
							setModuleId(q, algebraRef);
							typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						} else
							used++;
					} else
					if ( getModuleId(q) == sqlRef && getFunctionId(q) ==  putName("exportValue",11) && isaBatType(getArgType(mb,p,0)) ){
						// interface expects scalar type only, not expressable in MAL signature
						blocked++;
						mb->errors++;
						showException(cntxt->fdout, MAL, "generate_series", "internal error, generate_series is a table producing function");
					}else 
					if ( getModuleId(q) == languageRef && getFunctionId(q) == passRef && getArg(q,1) == getArg(p,0))
						// nothing happens in this instruction
						used++;
					else {
						// check for use without conversion
						for(k = q->retc; k < q->argc; k++)
						if( getArg(q,k) == getArg(p,0)){
							blocked++;
						}
						// materialize a copy and re-use where appropriate
					}
				}
				// fix the original, only when all use cases are replaced by the overloaded function
				if(used == cases && blocked == 0){
					setModuleId(p, generatorRef);
					setFunctionId(p, parametersRef);
					typeChecker(cntxt->fdout, cntxt->nspace, mb, p, TRUE);
				}
				if( used)
					actions++;
#ifdef VLT_DEBUG
				mnstr_printf(cntxt->fdout,"#generator target %d cases %d used %d error %d\n",getArg(p,0), cases, used, p->typechk);
#endif
			}
		}
	}
#ifdef VLT_DEBUG
	printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL);
#endif
	return actions;
}
