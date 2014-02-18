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
#include "opt_emptySet.h"
#include "opt_aliases.h"
#include "opt_deadcode.h"
#include "mal_builder.h"

#define propagate(X)									\
	do {												\
		p->token= ASSIGNsymbol;							\
		getArg(p,1)= getArg(p,X);						\
		p->argc = 2;									\
		p->fcn = 0;										\
		actions++;										\
		clrFunction(p);									\
		if(getArgType(mb,p,0)== getArgType(mb,p,X)){	\
			setModuleId(p,NULL);						\
			setFunctionId(p,NULL);						\
		} else {										\
			actions++;									\
			setModuleId(p,NULL);						\
			setFunctionId(p,NULL);						\
			alias[getArg(p,0)]= getArg(p,1);			\
		}												\
	} while (0)
	/*
	 * @-
	 * Be aware to handle alias mapping.
	 */

static int
ESevaluate(Client cntxt, MalBlkPtr mb, char *empty)
{
	int i, j, actions = 0;
	InstrPtr p;
	str existRef = putName("exist", 5);
	str kintersectRef = putName("kintersect", 10);
	str fragmentRef = putName("fragment", 8);
	int *alias;
	int runonce= FALSE;

	int limit = mb->stop, slimit= mb->ssize, ctop=0;
	InstrPtr *old = mb->stmt, *constraints;

	(void) cntxt;
	/* get query property */
	runonce = (varGetProp(mb, getArg(old[0], 0), runonceProp) != NULL);
	if (varGetProp(mb, getArg(old[0], 0), inlineProp) != NULL)
		return 0;

	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		return 0;
	constraints= (InstrPtr *) GDKmalloc(sizeof(InstrPtr)*slimit);
	if ( constraints == NULL) {
		GDKfree(mb->stmt);
		mb->stmt = old;
		mb->stop = limit;
		mb->ssize = slimit;
		return 0;
	}
	alias = (int*) GDKmalloc(sizeof(int)*mb->vtop);
	if( alias == NULL){
		GDKfree(mb->stmt);
		mb->stmt = old;
		mb->stop = limit;
		mb->ssize = slimit;
		GDKfree(constraints);
		return 0;
	}
	for(i=0;i<mb->vtop; i++) 
		alias[i]=i;

	/* Symbolic evaluation of the empty BAT variables */
	/* by looking at empty BAT arguments */
	for (i = 0; i < limit; i++) {
		char *f;
		p = old[i];

		pushInstruction(mb,p);
		if (p->token == ENDsymbol){
			for(i++; i<limit; i++)
				if (old[i])
					pushInstruction(mb,old[i]);
			break;
		}
		for(j=0; j<p->argc; j++)
			p->argv[j] = alias[getArg(p,j)];
 		/*
 		 * @-
 		 * The bulk of the intelligence lies in inspecting calling
 		 * sequences to filter and replace calls with empty arguments.
 		 */
 		f = getFunctionId(p);
		if (getModuleId(p) == sqlRef && 
		    empty[getArg(p,0)] &&
		   (f == bindRef || f == bindidxRef || f == binddbatRef)){
			InstrPtr q;
			/*
			 * @-
			 * The emptyset assertion is only needed once for relational insertions.
			 * We assume here that string constants have been matched already.
			 */
			if( f == bindRef && runonce == FALSE) {
				for( j=ctop-1; j>=0; j--){
					q= constraints[j];
					if( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, getVarConstant(mb,getArg(p,2)).val.sval)==0 &&
					    strcmp(getVarConstant(mb,getArg(q,3)).val.sval, getVarConstant(mb,getArg(p,3)).val.sval)==0 &&
					    getVarConstant(mb,getArg(p,5)).val.ival<=2 && /* no updates etc */
					    getVarConstant(mb,getArg(q,5)).val.ival == getVarConstant(mb,getArg(p,5)).val.ival
					) 
						/* don't generate the assertion */
						goto ignoreConstraint;
				}

				q = newStmt1(mb, constraintsRef, "emptySet");
				(void) pushArgument(mb, q, getArg(p,0) );
				constraints[ctop++]= p;
			}
		ignoreConstraint:
			continue;
		} 

		for (j = p->retc; j < p->argc; j++) {

			if (empty[getArg(p, j)]) {
				/* decode operations */
				if (getModuleId(p)== algebraRef) {
					if (f == existRef) {
						/* always false */
						setModuleId(p, NULL);
						setFunctionId(p, NULL);
						p->argc = 1;
						p->token = ASSIGNsymbol;
						(void) pushBit(mb, p, FALSE);
						actions++;
						break;
					} 
					if ( f == selectRef || 
					     f == tuniqueRef || 
					     f == likeRef  || 
					     f == sortRef  || 
					     f == sortTailRef  ||
					     f == sortHTRef  || 
					     f == sortTHRef  || 
					     f == kuniqueRef  ||
					     f == semijoinRef ||
					     f == kintersectRef  ||
					     f == fragmentRef ){

						/* result is empty */
						propagate(1);
						break;
					} 
					if ( f == differenceRef || 
					     f == kdifferenceRef ) {
						propagate(1);
						break;
					}
					if ( f == kunionRef) {
						/* copy non-empty argument */
						if( j == 1) {
							propagate(2);
						} else {
							propagate(1);
						}
						break;
					} 
				}
				if (getModuleId(p)== batRef) {
					if ( f == reverseRef || f == mirrorRef ){
						empty[getArg(p, 0)]= 1;
					}
				}
				/*
				 * @-
				 * If the target variable is empty and the function does not
				 * have a side-effect, we can replace it with a construction
				 * of the empty set. The dead-code optimizer will take care
				 * of removal of superflous constructions.
				 */
				if( p->retc==1 && p->token == ASSIGNsymbol &&
					!isLinearFlow(p) &&
					isaBatType(getArgType(mb,p,0))){
					int tpe=getArgType(mb,p,0);
					clrFunction(p);
					setModuleId(p, batRef);
					setFunctionId(p, newRef);
					p= pushArgument(mb, p, 
						newTypeVariable(mb, getHeadType(tpe)));
					(void) pushArgument(mb, p, 
						newTypeVariable(mb, getTailType(tpe)));
					actions++;
					break;
				}
			}
		}
	}
	for(; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	if (actions) {
		clrAllTypes(mb);	 /* force a complete resolve */
	}
	GDKfree(constraints);
	GDKfree(alias);
	return actions;
}
/*
 * @-
 * We first have to find all candidates for empty set removal.
 * They are recognized by an estimated zero row count and they
 * are not the target of an update.
 */

int
OPTemptySetImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	char *empty;
	int i;

	empty = (char *) GDKzalloc(mb->vsize * sizeof(char));
	if ( empty == NULL)
		return 0;
	(void) stk;
	(void) p;
	for (i = 0; i < mb->vtop; i++) {
		if (getVarRows(mb, i) == 0) {
			OPTDEBUGemptySet
				mnstr_printf(cntxt->fdout, "#START emptyset optimizer %d", i);
			empty[i] = 1;
		} 
	}
	OPTDEBUGemptySet mnstr_printf(cntxt->fdout, "\n");
	i= ESevaluate(cntxt, mb, empty);
	GDKfree(empty);
	return i;
}
