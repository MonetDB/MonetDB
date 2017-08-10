/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * Push the candidate list arithmetic operations
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_batcalc.h"

str
OPTbatcalcImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, limit, slimit, actions=0;
	int *vars = 0;
	InstrPtr p,q,*old = 0;
	lng usec = GDKusec();
	str msg= MAL_SUCCEED;
	char buf[256];

	(void) stk;
	(void) pci;
	(void) cntxt;

	// For now it is disabled, awaiting modified batcalc signatures
	return MAL_SUCCEED;

	limit = mb->stop;
        slimit = mb->ssize;
	vars= (int*) GDKzalloc(sizeof(int)* mb->vtop);
	if (vars == NULL)
		throw(MAL,"optimizer.batcalc", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	old= mb->stmt;
        if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(vars);
                throw(MAL,"optimizer.batcalc", SQLSTATE(HY001) MAL_MALLOC_FAIL);
        }

	/* collect where variables are assigned last */
	for ( i = 1; i < limit; i++){
		p= old[i];
		for( j = 0; j< p->retc; j++)
			vars[getArg(p,j)]= i;
	}

	/* replace the combinations:
	 * X_840 := algebra.projection(C_701,X_666);
	 * X_841 := batcalc.lng(X_840);
	 *
	 * into
	 * X_841 := batcalc.lng(X_666,C_701);
	 *
	 * and combination:
	 * X_850 := algebra.projection(C_701,X_667);
	 * X_875 := algebra.projection(C_701,X_678);
	 * X_883 := batcalc.-(100:lng,X_875);
	 * X_942 := batcalc.*(X_850,X_883);
	 *
	 * into
	 * X_883 := batcalc.-(100:lng, X_678, nil:bat, C_701)
	 * X_999 := batcalc.*(X_667, X_883, C_701, nil:bat)
	 *
	 */
	for( i=0; i< limit; i++){
		p = old[i];
		if( getModuleId(p) == batcalcRef ){
			actions++;
			if( isVarConstant(mb, getArg(p,1)) ){
				p = pushNil(mb,p,TYPE_bat);
			} else {
				q= old[vars[getArg(p,1)]];
				if( getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef && isVarCList(mb, getArg(q,1)) ){
					p = pushArgument(mb,p, getArg(q,1));
					getArg(p,1) = getArg(q,2);
				} else
					p = pushNil(mb,p,TYPE_bat);
			}
			if ( p->argc >3){
				if( isVarConstant(mb, getArg(p,2)) ){
					p = pushNil(mb,p,TYPE_bat);
				} else {
					q= old[vars[getArg(p,2)]];
					if( getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef && isVarCList(mb, getArg(q,1)) ){
						p = pushArgument(mb,p, getArg(q,1));
						getArg(p,2) = getArg(q,2);
					} else
						p = pushNil(mb,p,TYPE_bat);
				}
			}
		}
		pushInstruction(mb,p);
	}
	for(; i < slimit; i++)
		if( old[i])
			freeInstruction(old[i]);

	/* Defense line against incorrect plans */
	/* Plan is unaffected */
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
	/* keep all actions taken as a post block comment * and update stats */
	usec= GDKusec() - usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","batcalc",actions,usec);
	newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);
	GDKfree(vars);
	GDKfree(old);
	return msg;
}
