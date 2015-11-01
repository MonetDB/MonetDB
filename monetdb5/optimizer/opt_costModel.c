/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_costModel.h"

/*
 * The cost formula are repetative
 */
#define newRows(W,X,Y,Z) {\
		c1 = getRowCnt(mb, getArg(p,W));\
		c2 = getRowCnt(mb, getArg(p,X));\
		if (c1 == -1 || c2 == -1) \
			continue;\
		setRowCnt(mb, getArg(p,Z), (BUN)(Y));\
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
	wrd c1, c2;
	InstrPtr p;

	(void) cntxt;
	(void) stk;
	(void) pci;

	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL)
		return 0;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p)==algebraRef) {
			 if (getFunctionId(p) == subselectRef ||
				getFunctionId(p) == thetasubselectRef) {
				newRows(1,2, (c1 > 2 ? c2 / 2 +1: c1/2+1),0);
			} else if (
				getFunctionId(p) == selectNotNilRef  ||
				getFunctionId(p) == sortRef  ||
				getFunctionId(p) == subsortRef  ||
				getFunctionId(p) == projectRef  ){
				newRows(1,1,c1,0);
			} else if (getFunctionId(p) == joinRef ||
				getFunctionId(p) == leftfetchjoinRef ||
				getFunctionId(p) == subbandjoinRef ||
				getFunctionId(p) == leftfetchjoinPathRef ) {
				/* assume 1-1 joins */
				newRows(1,2,(c1 < c2 ? c1 : c2),0);
			} else
			if (getFunctionId(p) == crossRef) {
				newRows(1,2,((log((double) c1) + log((double) c2) > log(INT_MAX) ? INT_MAX : c1 * c2 +1)),0);
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
			if (getFunctionId(p) == appendRef ||
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
			c1 = getRowCnt(mb, getArg(p,1));
			if (c1 != -1)
				setRowCnt(mb, getArg(p,0), c1);
		}
	}
	return 1;
}
