/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
		if ( getModuleId(p) == algebraRef) {
			if ( getFunctionId(p) == joinRef || getFunctionId(p) == leftjoinRef) {
				if ( getVarEolife(mb, getArg(p, p->retc -1)) == i) {
					delArgument(p, p->retc -1);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					actions++;
					continue;
				}
			} else if ( getFunctionId(p) == semijoinRef) {
				int is_first_ret_not_used = getVarEolife(mb, getArg(p, p->retc -2)) == i;
				int is_second_ret_not_used = getVarEolife(mb, getArg(p, p->retc -1)) == i;
				assert(!is_first_ret_not_used || !is_second_ret_not_used);
				if ( is_first_ret_not_used || is_second_ret_not_used) {
					delArgument(p, is_second_ret_not_used ? p->retc -1 : p->retc -2);
					/* semijoin with a single output is called intersect */
					setFunctionId(p,intersectRef);
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
