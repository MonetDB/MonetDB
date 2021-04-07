/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Constant Duplicate Removal
 * The compilers may generate an abundance of constants on
 * the stack. This simple optimizer merges them into a single reference.
 * This makes it easier to search for statement duplicates
 * and alias their variables.
 */
/* We should not look at constants in simple, side-effect functions, because
 * they can not be removed later on.
*/
/*
 * We have to keep an alias table to reorganize the program
 * after the variable stack has changed.
 * The plan may contain many constants and to check them all would be quadratic
 * in the size of the constant list.
 * The heuristic is to look back into the list only partially.
 * A hash structure could help out with further reduction.
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_constants.h"

str
OPTconstantsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, j, k = 1, n  = 0, fnd = 0, actions  = 0, limit = 0;
	int *alias = NULL, *index = NULL, *cand = NULL;
	VarPtr x,y, *cst = NULL;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;
	InstrPtr q;

	if( isSimpleSQL(mb)){
		goto wrapup;
	}
	alias= (int*) GDKzalloc(sizeof(int) * mb->vtop);
	cand= (int*) GDKzalloc(sizeof(int) * mb->vtop);
	cst= (VarPtr*) GDKzalloc(sizeof(VarPtr) * mb->vtop);
	index= (int*) GDKzalloc(sizeof(int) * mb->vtop);

	if ( alias == NULL || cst == NULL || index == NULL || cand == NULL){
		msg = createException(MAL,"optimizer.constants", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	(void) stk;
	(void) cntxt;

	for(i=0; i<mb->stop; i++){
		q = getInstrPtr(mb,i);
		if ( !q) {
			continue;
		}
		if ( getModuleId(q) == sqlRef && getFunctionId(q) != tidRef) {
			continue;
		}
		if( hasSideEffects(mb, q, 1) )
			continue;
		for(k= q->retc; k < q->argc; k++){
			j = getArg(q,k);
			if( cand[j] == 0) {
				cand[j] = isVarConstant(mb, j)  && isVarFixed(mb, j)  && getVarType(mb, j) != TYPE_ptr;
			}
		}
	}

	for (i=0; i< mb->vtop; i++)
		alias[ i]= i;
	for (i=0; i< mb->vtop; i++)
		if ( cand[i]) {
			x= getVar(mb,i);
			fnd = 0;
			limit = n - 128; // don't look to far back
			if ( x->type && x->value.vtype)
			for( k = n-1; k >= 0 && k > limit; k--){
				y= cst[k];
				if ( x->type == y->type &&
					 x->rowcnt == y->rowcnt &&
					 x->value.vtype == y->value.vtype &&
					ATOMcmp(x->value.vtype, VALptr(&x->value), VALptr(&y->value)) == 0){

					/* re-use a constant */
					alias[i]= index[k];
					fnd=1;
					actions++;
					break;
				}
			}
			if ( fnd == 0){
				cst[n]= x;
				index[n]= i;
				n++;
			}
		}

	if (actions)
		for (i = 0; i < mb->stop; i++){
			p= getInstrPtr(mb,i);
			for (k=0; k < p->argc; k++)
				getArg(p,k) = alias[getArg(p,k)];
		}

    /* Defense line against incorrect plans */
	/* Plan remains unaffected */
	// msg = chkTypes(cntxt->usermodule, mb, FALSE);
	// if (!msg)
	// 	msg = chkFlow(mb);
	// if(!msg)
	// 	msg = chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
wrapup:
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","constants",actions,usec);
	newComment(mb,buf);
	if (actions > 0)
		addtoMalBlkHistory(mb);

	if( cand) GDKfree(cand);
	if( alias) GDKfree(alias);
	if( cst) GDKfree(cst);
	if( index) GDKfree(index);
	return msg;
}
