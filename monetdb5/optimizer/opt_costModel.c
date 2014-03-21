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
#include "opt_costModel.h"

/*
 * The cost formula are repetative
 */
#define newRows(W,X,Y,Z) {\
		ValRecord v;\
		c1 = getVarRows(mb, getArg(p,W));\
		c2 = getVarRows(mb, getArg(p,X));\
		if (c1 == -1 || c2 == -1) \
			continue;\
		k = (Y);\
		varSetProp(mb, getArg(p,Z), rowsProp, op_eq, VALset(&v,TYPE_wrd,&k));\
}
/*
 * The cost will be used in many places to make decisions.
 * Access should be fast.
 * The SQL front-end also makes the BAT index available as the
 * property bid. This can be used to access the BAT and involve
 * more properties into the decision procedure.
 * [to be done]
 * Also make sure you don't re-use variables, because then the
 * row count becomes non-deterministic.
 */
int
OPTcostModelImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	wrd k, c1, c2;
	InstrPtr p;

	(void) cntxt;
	(void) stk;
	(void) pci;

	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL)
		return 0;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p)==algebraRef) {
			 if (getFunctionId(p) == selectRef ||
				getFunctionId(p) == subselectRef ||
				getFunctionId(p) == thetaselectRef ||
				getFunctionId(p) == thetasubselectRef) {
				newRows(1,1, (c1 > 100 ? c1 / 2 +1: c1),0);
			} else if (
				getFunctionId(p) == selectNotNilRef  ||
				getFunctionId(p) == sortRef  ||
				getFunctionId(p) == subsortRef  ||
				getFunctionId(p) == projectRef  ){
				newRows(1,1,c1,0);
			} else if(getFunctionId(p) == kunionRef) {
				newRows(1,2,(c1+c2),0);
			} else if (getFunctionId(p)== kdifferenceRef) {
				newRows(1,2,(c1==0?0:c2==0?c1: c1 - c2 < 0 ? 1 : c1 - c2+1),0);
			} else if (getFunctionId(p) == joinRef ||
				getFunctionId(p) == leftfetchjoinRef ||
				getFunctionId(p) == leftjoinRef ||
				getFunctionId(p) == bandjoinRef ||
				getFunctionId(p) == leftfetchjoinPathRef ||
				getFunctionId(p) == leftjoinPathRef ) {
				/* assume 1-1 joins */
				newRows(1,2,(c1 < c2 ? c1 : c2),0);
			} else if (getFunctionId(p) == semijoinRef ||
						getFunctionId(p) == semijoinPathRef) {
				/* assume 1-1 semijoins */
				newRows(1,2,(c1 < c2? c1 : c2),0);
			} else
			if (getFunctionId(p) == crossRef) {
				newRows(1,2,((log((double) c1) + log((double) c2) > log(INT_MAX) ? INT_MAX : c1 * c2 +1)),0);
			} else
			if (getFunctionId(p) == tuniqueRef ) {
				newRows(1,1,( c1 < 50 ? c1 : c1 / 10+1),0);
			}
		} else if (getModuleId(p) == batcalcRef) {
			if( getFunctionId(p) == ifthenelseRef) {
				if( isaBatType(getArgType(mb,p,2) ) ) {
					newRows(2,2, c1,0);
				} else {
					newRows(3,3, c1,0);
				}
			} else if( isaBatType(getArgType(mb,p,1)) ){
					newRows(1,1, c1,0);
				} else {
					newRows(2, 2, c2,0);
				}
		} else if (getModuleId(p) == batstrRef) {
				newRows(1,1, c1,0);
		} else if (getModuleId(p) == batRef) {
			if (getFunctionId(p) == reverseRef ||
			    getFunctionId(p) == hashRef  ||
			    getFunctionId(p) == mirrorRef) {
				newRows(1,1, c1,0);
			} else if (getFunctionId(p) == appendRef ||
				   getFunctionId(p) == insertRef ){
				/*
				 * Updates are a little more complicated, because you have to
				 * propagate changes in the expected size up the expression tree.
				 * For example, the SQL snippet:
				 *     _49:bat[:oid,:oid]{rows=0,bid=622}  := sql.bind_dbat("sys","example",3);
				 *     _54 := bat.setWriteMode(_49);
				 *     bat.append(_54,_47,true);
				 * shows what is produced when it encounters a deletion. If a non-empty
				 * append is not properly passed back to _49, the emptySet
				 * optimizer might remove the complete deletion code.
				 * The same holds for replacement operations, which add information to
				 * an initially empty insertion BAT.
				 */
				if( isaBatType(getArgType(mb,p,2)) ){
					/* insert BAT */
					newRows(1,2, (c1 + c2+1),1);
				} else {
					/* insert scalars */
					newRows(1,1, (c1 +1),1);
				}
			} else if (getFunctionId(p) == deleteRef){
				if( isaBatType(getArgType(mb,p,2)) ){
					/* delete BAT */
					newRows(1,2, (c1 - c2 ==0? 1: c1-c2),1);
				} else {
					/* insert scalars */
					newRows(1,1, (c1==1?1: c1-1),1);
				}
			} else if (getFunctionId(p) == insertRef){
				newRows(1,1,( c1 + 1),0); /* faked */
			}
		} else if (getModuleId(p)==groupRef) {
			if (getFunctionId(p) ==subgroupRef ) {
				newRows(1,1,( c1 / 10+1),0);
			} else {
				newRows(1,1, c1,0);
			}
		} else if (getModuleId(p)== aggrRef) {
			if (getFunctionId(p) == sumRef ||
				getFunctionId(p) == minRef ||
				getFunctionId(p) == maxRef ||
				getFunctionId(p) == avgRef) {
				newRows(1,1, ( c1?c1:c1+1),0);
			} else	if (getFunctionId(p) == countRef){
				newRows(1,1, 1,0);
			}
		} else if( p->token == ASSIGNsymbol && p->argc== 2){
			/* copy the rows property */
			c1 = getVarRows(mb, getArg(p,1));
			if (c1 != -1) {
				ValRecord v;
				
				varSetProp(mb, getArg(p,0), rowsProp, op_eq, VALset(&v, TYPE_wrd, &c1));
			}
		}
	}
	return 1;
}
