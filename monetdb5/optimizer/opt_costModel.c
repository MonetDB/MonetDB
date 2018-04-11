/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_costModel.h"

/*
 * The cost formula are repetative
 */
#define newRows(W,X,Y,Z) {\
		c1 = getRowCnt(mb, getArg(p,W));\
		c2 = getRowCnt(mb, getArg(p,X));\
		/* just to ensure that rowcnt was/is never set to -1 */\
		assert(c1 != (BUN) -1);\
		assert(c2 != (BUN) -1);\
		if (c1 == BUN_NONE || c2 == BUN_NONE) \
			continue;\
		setRowCnt(mb, getArg(p,Z), (Y));\
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
str
OPTcostModelImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	BUN c1, c2;
	InstrPtr p;
	char buf[256];
	lng usec = GDKusec();

	(void) cntxt;
	(void) stk;
	(void) pci;

	if ( mb->inlineProp )
		return MAL_SUCCEED;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p)==algebraRef) {
			 if (getFunctionId(p) == selectRef ||
				getFunctionId(p) == thetaselectRef) {
				newRows(1,2, (c1 > 2 ? c2 / 2 +1: c1/2+1),0);
			} else if (
				getFunctionId(p) == selectNotNilRef  ||
				getFunctionId(p) == sortRef  ||
				getFunctionId(p) == sortRef  ||
				getFunctionId(p) == projectRef  ){
				newRows(1,1,c1,0);
			} else if (getFunctionId(p) == joinRef ||
				getFunctionId(p) == projectionRef ||
				getFunctionId(p) == bandjoinRef ||
				getFunctionId(p) == projectionpathRef ) {
				/* assume 1-1 joins */
				newRows(1,2,(c1 < c2 ? c1 : c2),0);
			} else
			if (getFunctionId(p) == crossRef) {
				newRows(1,2,((log((double) c1) + log((double) c2) > log(INT_MAX) ? INT_MAX : c1 * c2 +1)),0);
				/* log sets errno if it cannot compute the log. This will then screw with code that checks errno */
				if (errno == ERANGE || errno == EDOM) {
					errno = 0;
				}
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
			if (getFunctionId(p) == appendRef ){
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
					newRows(1, 2, (c2 >= c1 ? 1 : c1 - c2), 1);
				} else {
					/* insert scalars */
					newRows(1, 1, (c1 <= 1 ? 1 : c1 - 1), 1);
				}
			} 
		} else if (getModuleId(p)==groupRef) {
			if (getFunctionId(p) ==subgroupRef || getFunctionId(p) ==groupRef ) {
				newRows(1,1,( c1 / 10+1),0);
			} else {
				newRows(1,1, c1,0);
			}
		} else if (getModuleId(p)== aggrRef) {
			if (getFunctionId(p) == sumRef ||
				getFunctionId(p) == minRef ||
				getFunctionId(p) == maxRef ||
				getFunctionId(p) == avgRef) {
				newRows(1, 1, (c1 != 0 ? c1 : 1), 0);
			} else	if (getFunctionId(p) == countRef){
				newRows(1,1, 1,0);
			}
		} else if( p->token == ASSIGNsymbol && p->argc== 2){
			/* copy the rows property */
			c1 = getRowCnt(mb, getArg(p,1));
			/* just to ensure that rowcnt was/is never set to -1 */
			assert(c1 != (BUN) -1);
			if (c1 != BUN_NONE)
				setRowCnt(mb, getArg(p,0), c1);
		}
	}
    /* Defense line against incorrect plans */
	/* plan remains unaffected */
	//chkTypes(cntxt->usermodule, mb, FALSE);
	//chkFlow(mb);
	//chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions= 1 time=" LLFMT " usec","costmodel",usec);
    newComment(mb,buf);
	addtoMalBlkHistory(mb);

	return MAL_SUCCEED;
}
