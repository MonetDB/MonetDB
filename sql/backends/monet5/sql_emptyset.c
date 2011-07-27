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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f sql_emptyset
 * @a M. Kersten
 * @- Readonly processing
 * The SQL implementation is based on delta tables to collect the updates.
 * A consequence is that both the plan becomes large and
 * the instruction recycler can not keep information around.
 *
 * Therefore, we perform a just in time optimization, propagating
 * all empty BATs (if possible) and run the modified plan.
 * generation.
 *
 * During empty set propagation, new candidates may appear. For example,
 * taking the intersection with an empty set creates a target variable
 * that is empty too. It becomes an immediate target for optimization.
 * The current implementation is conservative. A limited set of
 * instructions is considered. Any addition to the MonetDB instruction
 * set would call for assessment on their effect.
 *
 * The end-result can be further optimized using alias removal.
 * However, this is less effective then in the normal pipeline, because
 * the garbage collector has already injected additional NIL assignments.
 *
 * The purpose of this operation is to improve recycling, which
 * is called as well to identify operations of interest.
 */
#include "monetdb_config.h"
#include "sql_mvc.h"
#include "sql_emptyset.h"
str
SQLemptyset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str likeRef = putName("like", 4);
	str existRef = putName("exist", 5);
	str uniqueRef = putName("unique", 6);
	str suniqueRef = putName("sunique", 7);
	str kuniqueRef = putName("kunique", 7);
	str intersectRef = putName("intersect", 9);
	str sintersectRef = putName("sintersect", 10);
	str kintersectRef = putName("kintersect", 10);
	mvc *m = NULL;
	int i, j, act, actions = 0, start= getPC(mb,pci);
	char *empty;
	int limit;
	InstrPtr p= NULL, *old;
	str msg = getContext(cntxt,mb, &m, NULL);
	BAT *b;

	(void) stk;
	(void) p;
	(void) cntxt;
	if (msg)
		return msg;
	empty= (char*) GDKzalloc(mb->vtop);
#ifdef DEBUG_SQL_EMPTYSET
	printf("SQL_EMPTYSET \n");
	printFunction(cntxt->fdout, mb,0, LIST_MAL_STMT | LIST_MAPI);
#endif

	limit = mb->stop;
	old = mb->stmt;
	newMalBlkStmt(mb, mb->ssize); /* a new statement stack */

	for (i = 0; i < limit; i++) {
		char *f;
		p = old[i];
		if ( p == pci)
			continue;

		p= copyInstruction(p);
		/*
		 * @-
		 * The bulk of the intelligence lies in inspecting the SQL columns
		 * and to filter/replace calls with empty arguments.
		 */
		if (getModuleId(p) == sqlRef && (getFunctionId(p) == bindRef ||
			getFunctionId(p) == bindidxRef) ){
			b = mvc_bind(m, *(str*) getArgReference(stk,p,1),
				 *(str*) getArgReference(stk,p,2),
				 *(str*) getArgReference(stk,p,3),
				 *(int*) getArgReference(stk,p,4));
			setVarUDFtype(mb,getArg(p,0));
			if (b && BATcount(b) == 0){
#ifdef DEBUG_SQL_EMPTYSET
				mnstr_printf(cntxt->fdout,"empty %d %s\n",
					getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
				empty[getArg(p,0)] = 1;
				BBPreleaseref(b->batCacheid);
				p->token = ASSIGNsymbol;
				p->fcn = 0;
				getModuleId(p) = batRef;
				getFunctionId(p) = newRef;
				p->argc=1;
				p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
				p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
				pushInstruction(mb,p);
				actions++;
				continue;
			}
			if ( b )
				BBPreleaseref(b->batCacheid);
		}
		if (getModuleId(p) == sqlRef && getFunctionId(p) == binddbatRef ){
			b = mvc_bind_dbat(m, *(str*) getArgReference(stk,p,1),
				 *(str*) getArgReference(stk,p,2),
				 *(int*) getArgReference(stk,p,3));
			setVarUDFtype(mb,getArg(p,0));
			if ( b && BATcount(b) == 0){
#ifdef DEBUG_SQL_EMPTYSET
				mnstr_printf(cntxt->fdout,"empty %d %s\n",
					getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
				empty[getArg(p,0)] = 1;
				BBPreleaseref(b->batCacheid);
				p->token = ASSIGNsymbol;
				p->fcn = 0;
				getModuleId(p) = batRef;
				getFunctionId(p) = newRef;
				p->argc=1;
				p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
				p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
				pushInstruction(mb,p);
				actions++;
				continue;
			}
			if ( b )
				BBPreleaseref(b->batCacheid);
		}

		if( p->token == ENDsymbol){
			pushInstruction(mb,p);
			for(i++; i<limit; i++)
			if( old[i])
				pushInstruction(mb,copyInstruction(old[i]));
			break;
		}

		act= actions;
		for (j = p->retc; j < p->argc; j++) {
			if (empty[getArg(p, j)]) {
				/* decode operations */
				if (getModuleId(p)== algebraRef) {
					f= getFunctionId(p);
					if (f == existRef) {
						/* always false */
						setModuleId(p, NULL);
						setFunctionId(p, NULL);
						p->argc = 1;
						p->token = ASSIGNsymbol;
						p= pushBit(mb, p, FALSE);
						actions++;
						break;
					}
					if ( ( f == selectRef ||
					     f == uselectRef ||
					     f == thetauselectRef ||
					     /* f == markTRef ||  no, because you keep the seq */
					     f == tuniqueRef ||
					     f == likeRef  ||
					     f == sortRef  ||
					     f == sortTailRef  ||
					     f == sortHTRef  ||
					     f == sortTHRef  ||
					     f == uniqueRef  ||
					     f == suniqueRef  ||
					     f == kuniqueRef  ||
					     f == semijoinRef ) && j ==1 ){

						/* result is empty */
						p->token = ASSIGNsymbol;
						p->fcn = 0;
						getModuleId(p) = batRef;
						getFunctionId(p) = newRef;
						p->argc=1;
						p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
						p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
						empty[getArg(p,0)]=1;
						actions++;
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						break;
					}
					if ( f == joinRef) {
						p->token = ASSIGNsymbol;
						p->fcn = 0;
						getModuleId(p) = batRef;
						getFunctionId(p) = newRef;
						p->argc=1;
						p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
						p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
						empty[getArg(p,0)]=1;
						actions++;
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						break;
					}
					if ( f == differenceRef ||
					     f == sintersectRef  ||
					     f == kintersectRef  ||
					     f == intersectRef  ||
					     f == kdifferenceRef ) {
						if ( j == 1){
							p->token = ASSIGNsymbol;
							p->fcn = 0;
							getModuleId(p) = batRef;
							getFunctionId(p) = newRef;
							p->argc=1;
							p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
							p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
							empty[getArg(p,0)]=1;
							actions++;
						} else {
							p->token = ASSIGNsymbol;
							clrFunction(p);
							p->argc= 2;
						}
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						break;
					}
					if ( f == sunionRef ||
					     f == kunionRef ||
					     f == unionRef) {
						/* copy non-empty argument */
						if( j == 2) {
							p->token = ASSIGNsymbol;
							clrFunction(p);
							p->argc= 2;
							empty[getArg(p,0)] = empty[getArg(p,1)];
							actions++;
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						} else {
							p->token = ASSIGNsymbol;
							clrFunction(p);
							p->argc= 2;
							getArg(p,1)= getArg(p,2);
							empty[getArg(p,0)] = empty[getArg(p,1)];
							actions++;
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						}
						break;
					}
				}
				if ( getModuleId(p) == batRef ) {
					if ( getFunctionId(p) == reverseRef){
						p->token = ASSIGNsymbol;
						p->fcn = 0;
						getFunctionId(p) = newRef;
						p->argc=1;
						p= pushType(mb,p,getHeadType(getArgType(mb,p,0)));
						p= pushType(mb,p,getTailType(getArgType(mb,p,0)));
						actions++;
						empty[getArg(p,0)]=1;
#ifdef DEBUG_SQL_EMPTYSET
						mnstr_printf(cntxt->fdout,"empty %d %s\n",
							getArg(p,0), getVarName(mb,getArg(p,0)));
#endif
						break;
					}
				}
			}
		}
		/* we should reset all targets as not empty if nothing has changed */
		if ( act == actions)
			for( j=0; j< p->retc; j++)
				empty[getArg(p,j)]=0;
		pushInstruction(mb,p);
	}
	newStmt(mb,optimizerRef,putName("aliases",7));
	newStmt(mb,optimizerRef,putName("deadcode",8));
	/* newStmt(mb,optimizerRef,putName("recycle",7));*/
	if( actions ){
		clrAllTypes(mb);	 /* force a complete resolve */
		chkProgram(cntxt->nspace,mb);
#ifdef DEBUG_SQL_EMPTYSET
		if (mb->errors){
			printf("FINAL STAGE compile errors\n");
			printFunction(cntxt->fdout, mb,0, LIST_MAL_ALL);
		}
#endif
		if ( optimizeMALBlock(cntxt,mb) == MAL_SUCCEED){
#ifdef DEBUG_SQL_EMPTYSET
			printf("FINAL STAGE actions %d errors=%d restart= %d\n",
				actions, mb->errors,start+1);
			printFunction(cntxt->fdout, mb,0, LIST_MAL_STMT | LIST_MAPI);
#endif
			msg= reenterMAL(cntxt,mb,start+1,0,stk,0,0);
		}
#ifdef DEBUG_SQL_EMPTYSET
		else mnstr_printf(cntxt->fdout,"#optimizer fails:%s\n",msg);
#endif
	}
	/* restore after execution */
	for( i=0; i<mb->stop; i++){
		freeInstruction(getInstrPtr(mb,i));
		mb->stmt[i]= 0;
	}
	mb->stop= 0;
	for( i=0; i<limit; i++)
		pushInstruction(mb, old[i]);
	GDKfree(old);
	if (actions && msg == MAL_SUCCEED)
		throw(MAL,"sql.emptyset","!skip-to-end");
	return msg;
}

