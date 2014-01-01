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
#include "opt_pushranges.h"
#include "mal_interpreter.h"	/* for showErrors() */

typedef struct RANGE{
	int used;		/* how often it has been used */
	int lcst, hcst; /* constant variables holding the range bounds */
	int  srcvar;	/* BAT variable on which the range depends */
	int lastupdate, lastrange;
}RangeRec, *Range;

static void 
printRange(Client cntxt, MalBlkPtr mb, Range rng, int idx){
	(void) rng;
	mnstr_printf(cntxt->fdout,"[%3d] %5s used=%d\tlcst=%s\t ", 
		idx, getVarName(mb,idx), rng[idx].used,
		(rng[idx].lcst? getVarName(mb,rng[idx].lcst):""));
	mnstr_printf(cntxt->fdout,"hcst=%s\tsource %s ", 
		(rng[idx].hcst? getVarName(mb,rng[idx].hcst):""), 
		(rng[idx].srcvar?getVarName(mb,rng[idx].srcvar):""));
	mnstr_printf(cntxt->fdout,"\tlu=%d\tr=%d", rng[idx].lastupdate, rng[idx].lastrange);
	mnstr_printf(cntxt->fdout,"\n");
}

int
OPTpushrangesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j, limit,actions=0;
	InstrPtr p, *old;
	int x,y,z;
	Range range;

	if( mb->errors) 
		return 0;

	range= (Range) GDKzalloc(mb->vtop * sizeof(RangeRec));
	if (range == NULL)
		return 0;
	OPTDEBUGpushranges
		mnstr_printf(cntxt->fdout,"#Range select optimizer started\n");
	(void) stk;
	(void) pci;
	
	limit = mb->stop;
	old = mb->stmt;
	/*
	 * In phase I we collect information about constants
	 */
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( p->barrier) 
			break; /* end of optimizer */
		for(j=p->retc; j< p->argc; j++)
			range[getArg(p,j)].used++;
		for(j=0; j<p->retc; j++){
			range[getArg(p,j)].lastupdate= i;
			if( range[getArg(p,j)].lastrange == 0)
				range[getArg(p,j)].lastrange= i;
		} 
		if( getModuleId(p)== algebraRef && 
			( getFunctionId(p)== selectRef || getFunctionId(p)== uselectRef) ){
			/*
			 * The operation X:= algebra.select(Y,L,H,Li,Hi) is analysed.
			 * First, we attempt to propagate the range known for Y onto the
			 * requested range of X. This may lead to smaller range of
			 * even the conclusion that X is necessarily empty.
			 * Of course, only under the condition that Y has not been changed by a
			 * side-effect since it was bound to X.
			 */
			x= getArg(p,1);
			y= getArg(p,2);
			if( range[x].lcst && isVarConstant(mb,y) ){
				/* merge lowerbound */
				if( ATOMcmp( getVarGDKType(mb,y), 
						VALptr( &getVarConstant(mb,range[x].lcst)), 
						VALptr( &getVarConstant(mb,y)) ) > 0){
					getArg(p,2)= range[x].lcst;
					z= range[x].srcvar;
					if( getArg(p,1) == x && 
						range[z].lastupdate == range[z].lastrange){
						getArg(p,1) = z;
						actions++;
					}
				}
				y= getArg(p,3);
				/* merge higherbound */
				if( ATOMcmp( getVarGDKType(mb,y), 
						VALptr( &getVarConstant(mb,range[x].hcst)), 
						VALptr( &getVarConstant(mb,y)) ) < 0 ||
					ATOMcmp( getVarGDKType(mb,y),
						VALptr( &getVarConstant(mb,y)),
						 ATOMnilptr(getVarType(mb,y)) ) == 0){
					getArg(p,3)= range[x].hcst;
					z= range[x].srcvar;
					if( getArg(p,1) == x && range[z].lastupdate == range[z].lastrange){
						getArg(p,1) = z;
						actions++;
					}
				}
			}
			/*
			 * The second step is to assign the result of this exercise to the
			 * result variable.
			 */
			x= getArg(p,0);
			if( isVarConstant(mb, getArg(p,2)) ){
				range[x].lcst = getArg(p,2);
				range[x].srcvar= getArg(p,1);
				range[x].lastupdate= range[x].lastrange = i;
			}
			if( isVarConstant(mb, getArg(p,3)) ){
				range[x].hcst = getArg(p,3);
				range[x].srcvar= getArg(p,1);
				range[x].lastupdate= range[x].lastrange = i;
			}
			/*
			 * If both range bounds are constant, we can also detect empty results.
			 * It is empty if L> H or when L=H and the bounds are !(true,true).
			 */
			x= getArg(p,2);
			y= getArg(p,3);
			if( isVarConstant(mb, x)  &&
				isVarConstant(mb, y)  ){
				z =ATOMcmp( getVarGDKType(mb,y),
                        VALptr( &getVarConstant(mb,x)),
                        VALptr( &getVarConstant(mb,y)));
				x=  p->argc > 4;
				x= x && isVarConstant(mb,getArg(p,4));
				x= x && isVarConstant(mb,getArg(p,5));
				x= x && getVarConstant(mb,getArg(p,4)).val.btval;
				x= x && getVarConstant(mb,getArg(p,5)).val.btval;
				if( z > 0 || (z==0 && p->argc>4 && !x)) {
					int var = getArg(p, 0);
					wrd zero = 0;
					ValRecord v, *vp;

					vp = VALset(&v, TYPE_wrd, &zero);
					varSetProp(mb, var, rowsProp, op_eq, vp);
					/* create an empty replacement */
					x = getArgType(mb, p, 1);
					p->argc=1;
					getModuleId(p)= batRef;
					getFunctionId(p)= newRef;
					p= pushArgument(mb,p, newTypeVariable(mb, getHeadType(x)));
					(void) pushArgument(mb,p, newTypeVariable(mb, getTailType(x)));
					actions++;
				}
			}
		}
	}
	OPTDEBUGpushranges
		for(j=0; j< mb->vtop; j++)
		if( range[j].used )
			printRange(cntxt, mb,range,j);
	/*
	 * Phase II, if we succeeded in pushing constants around and
	 * changing instructions, we might as well try once more to perform
	 * aliasRemoval, constantExpression, and pushranges.
	 */
	GDKfree(range);
	return actions;
}
