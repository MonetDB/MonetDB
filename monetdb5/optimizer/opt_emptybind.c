/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* author M.Kersten
 * This optimizer hunts for the empty persistent tables accessed and propagates them.
 *
 * Patterns to look for:
 *  X_13 := algebra.projection(X_1,X_4);
 *  where either argument is empty
 *
 */
#include "monetdb_config.h"
#include "opt_emptybind.h"
#include "opt_aliases.h"
#include "opt_deadcode.h"
#include "mal_builder.h"

#define addresult(I)									\
	do {												\
		int tpe = getVarType(mb,getArg(p,I));			\
		q= newStmt(mb, batRef, newRef);					\
		getArg(q,0)= getArg(p,I);						\
		q = pushType(mb, q, getBatType(tpe));			\
		empty[getArg(q,0)]= i;							\
	} while (0)

#define emptyresult(I)									\
	do {												\
		int tpe = getVarType(mb,getArg(p,I));			\
		clrFunction(p);									\
		setModuleId(p,batRef);							\
		setFunctionId(p,newRef);						\
		p->argc = p->retc;								\
		p = pushType(mb,p, getBatType(tpe));			\
		setVarType(mb, getArg(p,0), tpe);				\
		setVarFixed(mb, getArg(p,0));					\
		empty[getArg(p,0)]= i;							\
	} while (0)


str
OPTemptybindImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j, actions =0;
	int *empty;
	int limit = mb->stop, slimit = mb->ssize;
	InstrPtr p, q, *old = mb->stmt, *updated;
	char buf[256];
	lng usec = GDKusec();
	str sch,tbl;
	int etop= 0, esize= 256;
	str msg = MAL_SUCCEED;

	//if ( optimizerIsApplied(mb,"emptybind") )
		//return 0;
	// use an instruction reference table to keep
	
	for( i=0; i< mb->stop; i++)
		actions += getFunctionId(getInstrPtr(mb,i)) == emptybindRef || getFunctionId(getInstrPtr(mb,i)) == emptybindidxRef;
	if( actions == 0)
		goto wrapup;
	actions = 0;

	// track of where 'emptybind' results are produced
	empty = (int *) GDKzalloc(mb->vsize * sizeof(int));
	if ( empty == NULL)
		throw(MAL,"optimizer.emptybind",MAL_MALLOC_FAIL);

	updated= (InstrPtr *) GDKzalloc(esize * sizeof(InstrPtr));
	if( updated == 0){
		GDKfree(empty);
		return 0;
	}
	(void) stk;

	/* Got an instructions V:= bat.new(:tpe) 
	 * The form the initial family of empty sets.
	 */

	(void) cntxt;
	(void) pci;

#ifdef DEBUG_OPT_EMPTYBIND
	fprintf(stderr "#Optimize Query Emptybind\n");
	fprintFunction(stderr, mb, 0, LIST_MAL_DEBUG);
#endif

	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(empty);
		GDKfree(updated);
		throw(MAL,"optimizer.emptybind",MAL_MALLOC_FAIL);
	}

	/* Symbolic evaluation of the empty BAT variables */
	/* by looking at empty BAT arguments */
	for (i = 0; i < limit; i++) {
		p = old[i];

		pushInstruction(mb,p);
		if (p->token == ENDsymbol){
			for(i++; i<limit; i++)
				if (old[i])
					pushInstruction(mb,old[i]);
			break;
		}

 		/*
 		 * The bulk of the intelligence lies in inspecting calling
 		 * sequences to filter and replace results 
 		 */
		if ( getModuleId(p) == batRef && getFunctionId(p) == newRef){
#ifdef DEBUG_OPT_EMPTYBIND
			fprintf(stderr, "#empty bat  pc %d var %d\n",i , getArg(p,0) );
#endif
			empty[getArg(p,0)] = i;
			continue;
		} 

		// any of these instructions leave a non-empty BAT behind
		if(p && getModuleId(p) == sqlRef && isUpdateInstruction(p)){
			if ( etop == esize){
				InstrPtr *tmp = updated;
				updated = (InstrPtr*) GDKrealloc( updated, (esize += 256) * sizeof(InstrPtr));
				if( updated == NULL){
					GDKfree(tmp);
					GDKfree(empty);
					goto wrapup;
				}
			}
			updated[etop++]= p;
		}

		/* restore the naming, dropping the runtime property 'empty' */
		if (getFunctionId(p) == emptybindRef) {
#ifdef DEBUG_OPT_EMPTYBIND
			fprintf(stderr, "#empty bind  pc %d var %d\n",i , getArg(p,0) );
#endif
			setFunctionId(p,bindRef);
			p->typechk= TYPE_UNKNOWN;
			empty[getArg(p,0)] = i;
			if( p->retc == 2){
				empty[getArg(p,1)] = i;
#ifdef DEBUG_OPT_EMPTYBIND
				fprintf(stderr, "#empty update bind  pc %d var %d\n",i , getArg(p,1) );
#endif
			}
			// replace the call into a empty bat creation unless the table was updated already in the same query 
			sch = getVarConstant(mb,getArg(p,2  + (p->retc==2))).val.sval;
			tbl = getVarConstant(mb,getArg(p,3  + (p->retc==2))).val.sval;
			for(j= 0; j< etop; j++){
				q= updated[j];
				if(q && getModuleId(q) == sqlRef && isUpdateInstruction(q)){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 &&
						 strcmp(getVarConstant(mb,getArg(q,3)).val.sval, tbl) == 0 ){
#ifdef DEBUG_OPT_EMPTYBIND
						fprintf(stderr, "#reset mark empty variable pc %d var %d\n",i , getArg(p,0) );
#endif
						empty[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							fprintf(stderr, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							empty[getArg(p,1)] = 0;
						}
						break;
					}
				}
				if(q && getModuleId(q) == sqlcatalogRef){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 ){
						empty[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							fprintf(stderr, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							empty[getArg(p,1)] = 0;
						}
						break;
					}
				}
			}
			if( empty[getArg(p,0)]){
                int tpe;
				actions++;
				if( p->retc == 2){
					tpe = getBatType(getVarType(mb,getArg(p,1)));
					q= newInstruction(0, batRef, newRef);
					q = pushType(mb,q,tpe);
					getArg(q,0)= getArg(p,1);
					setVarFixed(mb, getArg(p,0));
					pushInstruction(mb,q);
					empty[getArg(q,0)]= i;
				}

                tpe = getBatType(getVarType(mb,getArg(p,0)));
                clrFunction(p);
                setModuleId(p,batRef);
                setFunctionId(p,newRef);
                p->argc = p->retc = 1;
                p = pushType(mb,p,tpe);
				setVarFixed(mb, getArg(p,0));
				empty[getArg(p,0)]= i;
			}
			continue;
		}

		if (getFunctionId(p) == emptybindidxRef) {
#ifdef DEBUG_OPT_EMPTYBIND
			fprintf(stderr, "#empty bindidx  pc %d var %d\n",i , getArg(p,0) );
#endif
			setFunctionId(p,bindidxRef);
			p->typechk= TYPE_UNKNOWN;
			empty[getArg(p,0)] = i;
			// replace the call into a empty bat creation unless the table was updated already in the same query 
			sch = getVarConstant(mb,getArg(p,2  + (p->retc==2))).val.sval;
			tbl = getVarConstant(mb,getArg(p,3  + (p->retc==2))).val.sval;
			for(j= 0; j< etop; j++){
				q= updated[j];
				if(q && getModuleId(q) == sqlRef && (getFunctionId(q) == appendRef || getFunctionId(q) == updateRef )){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 &&
						 strcmp(getVarConstant(mb,getArg(q,3)).val.sval, tbl) == 0 ){
#ifdef DEBUG_OPT_EMPTYBIND
							fprintf(stderr, "#reset mark empty variable pc %d var %d\n",i , getArg(p,0) );
#endif
						empty[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							fprintf(stderr, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							empty[getArg(p,1)] = 0;
						}
						break;
					}
				}
				if(q && getModuleId(q) == sqlcatalogRef){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 ){
						empty[getArg(p,0)] = 0;
						break;
					}
				}
			}
			if( empty[getArg(p,0)]){
				int tpe;
				actions++;
				if( p->retc == 2){
					tpe = getBatType(getVarType(mb,getArg(p,1)));
					q= newInstruction(0, batRef, newRef);
					q = pushType(mb,q,tpe);
					getArg(q,0)= getArg(p,1);
					setVarFixed(mb,getArg(q,0));
					pushInstruction(mb,q);
					empty[getArg(q,0)]= i;
				}
				
				tpe = getBatType(getVarType(mb,getArg(p,0)));
				clrFunction(p);
				setModuleId(p,batRef);
				setFunctionId(p,newRef);
				p->argc = p->retc = 1;
				p = pushType(mb,p,tpe);
				setVarFixed(mb, getArg(p,0));
				empty[getArg(p,0)]= i;
			}
			continue;
		}

		// delta operations without updates+ insert can be replaced by an assignment
		if (getModuleId(p)== sqlRef && getFunctionId(p) == deltaRef  && p->argc ==5){
			if( empty[getArg(p,2)] && empty[getArg(p,3)] && empty[getArg(p,4)] ){
#ifdef DEBUG_OPT_EMPTYBIND
				fprintf(stderr, "#empty delta  pc %d var %d,%d,%d\n",i ,empty[getArg(p,2)], empty[getArg(p,3)], empty[getArg(p,4)] );
				fprintf(stderr, "#empty delta  pc %d var %d\n",i , getArg(p,0) );
#endif
				actions++;
				clrFunction(p);
				p->argc = 2;
				if ( empty[getArg(p,1)] ){
					empty[getArg(p,0)] = i;
				}
			}
			continue;
		}

		if (getModuleId(p)== sqlRef && getFunctionId(p) == projectdeltaRef) {
			if( empty[getArg(p,3)] && empty[getArg(p,4)] ){
#ifdef DEBUG_OPT_EMPTYBIND
				fprintf(stderr, "#empty projectdelta  pc %d var %d\n",i , getArg(p,0) );
#endif
				actions++;
				setModuleId(p,algebraRef);
				setFunctionId(p,projectionRef);
				p->argc = 3;
				p->typechk= TYPE_UNKNOWN;
			}
			continue;
		}
		if (getModuleId(p)== algebraRef){
			if( getFunctionId(p) == projectionRef) {
				if( empty[getArg(p,1)] || empty[getArg(p,2)] ){
#ifdef DEBUG_OPT_EMPTYBIND
					fprintf(stderr, "#empty projection  pc %d var %d\n",i , getArg(p,0) );
#endif
					actions++;
					emptyresult(0);
				}
			}
			if( getFunctionId(p) == thetaselectRef) {
				if( empty[getArg(p,1)] || empty[getArg(p,2)] ){
#ifdef DEBUG_OPT_EMPTYBIND
					fprintf(stderr, "#empty projection  pc %d var %d\n",i , getArg(p,0) );
#endif
					actions++;
					emptyresult(0);
				}
			}
		}
		if (getModuleId(p)== batRef && isUpdateInstruction(p)){
			if( empty[getArg(p,1)] && empty[getArg(p,2)]){
				emptyresult(0);
			} else
			if( empty[getArg(p,2)]){
				actions++;
				clrFunction(p);	
				p->argc = 2;
			}
		}
	}

#ifdef DEBUG_OPT_EMPTYBIND
	chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
	fprintf(stderr, "#Optimize Query Emptybind done\n");
	fprintFunction(stderr, mb, 0, LIST_MAL_DEBUG);
#endif

	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(empty);
	GDKfree(updated);
    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
wrapup:
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","emptybind",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);
	return msg;
}
