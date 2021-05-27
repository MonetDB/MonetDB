/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* The SQL code generator can not always look ahead to avoid
 * generation of intermediates.
 * Some of these patterns are captured in a postfix optimalisation.
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_postfix.h"

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')
str
OPTpostfixImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, slimit, actions = 0;
	lng usec = GDKusec();
	char buf[256];
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;

	slimit = mb->stop;
	setVariableScope(mb);
	/* Remove the result from any join/group instruction when it is not used later on */
	for( i = 0; i< slimit; i++){
/* POSTFIX ACTION FOR THE JOIN CASE  */
		p= getInstrPtr(mb, i);
		if ( getModuleId(p) == algebraRef && p->retc == 2) {
			if ( getFunctionId(p) == leftjoinRef || getFunctionId(p) == outerjoinRef ||
				 getFunctionId(p) == bandjoinRef || getFunctionId(p) == rangejoinRef ||
				 getFunctionId(p) == likejoinRef) {
				if ( getVarEolife(mb, getArg(p, p->retc -1)) == i) {
					delArgument(p, p->retc -1);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					actions++;
					continue;
				}
			} else if ( getFunctionId(p) == semijoinRef || getFunctionId(p) == joinRef ||
				 getFunctionId(p) == thetajoinRef || getFunctionId(p) == outerjoinRef || getFunctionId(p) == crossRef) {
				int is_first_ret_not_used = getVarEolife(mb, getArg(p, p->retc -2)) == i;
				int is_second_ret_not_used = getVarEolife(mb, getArg(p, p->retc -1)) == i;

				if (getFunctionId(p) == semijoinRef && ((is_first_ret_not_used && getVarConstant(mb, getArg(p, 7)).val.btval != 1/*not single*/) || is_second_ret_not_used)) {
					/* Can't swap arguments on single semijoins */
					if (is_first_ret_not_used) {
						/* semijoin with just the right output is a join */
						getArg(p, 2) ^= getArg(p, 3); /* swap join inputs */
						getArg(p, 3) ^= getArg(p, 2);
						getArg(p, 2) ^= getArg(p, 3);

						getArg(p, 4) ^= getArg(p, 5); /* swap candidate lists */
						getArg(p, 5) ^= getArg(p, 4);
						getArg(p, 4) ^= getArg(p, 5);
						setFunctionId(p, joinRef);
						delArgument(p, 7); /* delete 'max_one' argument */
					} else {
						/* semijoin with just the left output is an intersection */
						setFunctionId(p, intersectRef);
					}

					delArgument(p, is_second_ret_not_used ? p->retc -1 : p->retc -2);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					actions++;
					continue;
				} else if (is_second_ret_not_used) {
					delArgument(p, p->retc -1);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					actions++;
					continue;
				} else if (is_first_ret_not_used && (getFunctionId(p) == joinRef || (getFunctionId(p) == thetajoinRef && isVarConstant(mb, getArg(p, 6))) ||
						   (getFunctionId(p) == crossRef && getVarConstant(mb, getArg(p, 4)).val.btval != 1/*not single*/))) {
					/* Can't swap arguments on single cross products */
					/* swap join inputs */
					getArg(p, 2) ^= getArg(p, 3);
					getArg(p, 3) ^= getArg(p, 2);
					getArg(p, 2) ^= getArg(p, 3);

					if (getFunctionId(p) != crossRef) { /* swap candidate lists */
						getArg(p, 4) ^= getArg(p, 5);
						getArg(p, 5) ^= getArg(p, 4);
						getArg(p, 4) ^= getArg(p, 5);
						if (getFunctionId(p) == thetajoinRef) { /* swap the comparison */
							ValRecord *x = &getVarConstant(mb, getArg(p, 6)), cst = {.vtype = TYPE_int};
							switch (x->val.ival) {
							case JOIN_LT:
								cst.val.ival = JOIN_GT;
								break;
							case JOIN_LE:
								cst.val.ival = JOIN_GE;
								break;
							case JOIN_GT:
								cst.val.ival = JOIN_LT;
								break;
							case JOIN_GE:
								cst.val.ival = JOIN_LE;
								break;
							default:
								cst.val.ival = x->val.ival;
							}
							setArg(p, 6, defConstant(mb, TYPE_int, &cst));
						}
					}
					delArgument(p, p->retc -2);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					actions++;
					continue;
				}
			}
		}
/* POSTFIX ACTION FOR THE EXTENT CASE  */
		if ( getModuleId(p) == groupRef && getFunctionId(p) == groupRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
			delArgument(p, p->retc -1);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			actions++;
			continue;
		}
		if ( getModuleId(p) == groupRef && getFunctionId(p) == subgroupRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
			delArgument(p, p->retc -1);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			actions++;
			continue;
		}
		if ( getModuleId(p) == groupRef && getFunctionId(p) == subgroupdoneRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
			delArgument(p, p->retc -1);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			actions++;
			continue;
		}
		if ( getModuleId(p) == groupRef && getFunctionId(p) == groupdoneRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
			delArgument(p, p->retc -1);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			actions++;
			continue;
		}
/* POSTFIX ACTION FOR SORT, could be dropping the last two */
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == sortRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
			delArgument(p, p->retc -1);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			actions++;
			if ( getModuleId(p) == algebraRef && getFunctionId(p) == sortRef && getVarEolife(mb, getArg(p, p->retc -1)) == i){
				delArgument(p, p->retc -1);
				typeChecker(cntxt->usermodule, mb, p, i, TRUE);
				actions++;
			}
			continue;
		}
	}
	/* Defense line against incorrect plans */
	if( actions ){
		// msg = chkTypes(cntxt->usermodule, mb, FALSE);
		// if (!msg)
		// 	msg = chkFlow(mb);
		// if (!msg)
		// 	msg = chkDeclarations(mb);
	}
	usec= GDKusec() - usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec", "postfix", actions, usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
