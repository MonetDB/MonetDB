/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	} while (0)


int
OPTemptybindImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j, actions =0;
	int *marked;
	int limit = mb->stop, slimit = mb->ssize;
	InstrPtr p, q, *old = mb->stmt, *updated;
	char buf[256];
	lng usec = GDKusec();
	str sch,tbl;
	int etop= 0, esize= 256;

	// use an instruction reference table to keep
	// track of where 'emptybind' results are produced
	marked = (int *) GDKzalloc(mb->vsize * sizeof(int));
	if ( marked == NULL)
		return 0;

	updated= (InstrPtr *) GDKzalloc(esize * sizeof(InstrPtr));
	if( updated == 0){
		GDKfree(marked);
		return 0;
	}
	(void) stk;

	/* Got an instructions V:= bat.new(:tpe) 
	 * The form the initial family of marked sets.
	 */

	(void) cntxt;
	(void) pci;

#ifdef DEBUG_OPT_EMPTYBIND
	mnstr_printf(GDKout, "#Optimize Query Emptybind\n");
	printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
#endif

	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		return 0;

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
			mnstr_printf(cntxt->fdout, "#empty bat  pc %d var %d\n",i , getArg(p,0) );
#endif
			marked[getArg(p,0)] = i;
			continue;
		} 

		// any of these instructions leave a non-empty BAT behind
		if(p && getModuleId(p) == sqlRef && isUpdateInstruction(p)){
			if ( etop == esize){
				updated = (InstrPtr*) GDKrealloc( updated, (esize += 256) * sizeof(InstrPtr));
				if( updated == NULL){
					GDKfree(marked);
					return 0;
				}
			}
			updated[etop++]= p;
		}

		/* restore the naming, dropping the runtime property 'marked' */
		if (getFunctionId(p) == emptybindRef) {
#ifdef DEBUG_OPT_EMPTYBIND
			mnstr_printf(cntxt->fdout, "#empty bind  pc %d var %d\n",i , getArg(p,0) );
#endif
			actions++;
			setFunctionId(p,bindRef);
			p->typechk= TYPE_UNKNOWN;
			marked[getArg(p,0)] = i;
			if( p->retc == 2){
				marked[getArg(p,1)] = i;
#ifdef DEBUG_OPT_EMPTYBIND
				mnstr_printf(cntxt->fdout, "#empty update bind  pc %d var %d\n",i , getArg(p,1) );
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
						mnstr_printf(cntxt->fdout, "#reset mark empty variable pc %d var %d\n",i , getArg(p,0) );
#endif
						marked[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							mnstr_printf(cntxt->fdout, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							marked[getArg(p,1)] = 0;
						}
						break;
					}
				}
				if(q && getModuleId(q) == sqlRef && getFunctionId(q) == catalogRef){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 ){
						marked[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							mnstr_printf(cntxt->fdout, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							marked[getArg(p,1)] = 0;
						}
						break;
					}
				}
			}
			if( marked[getArg(p,0)]){
                int tpe;
				if( p->retc == 2){
					tpe = getBatType(getVarType(mb,getArg(p,1)));
					q= newStmt(mb,batRef,newRef);
					q = pushType(mb,q,tpe);
					getArg(q,0)= getArg(p,1);
					setVarFixed(mb, getArg(p,0));
				}

                tpe = getBatType(getVarType(mb,getArg(p,0)));
                clrFunction(p);
                setModuleId(p,batRef);
                setFunctionId(p,newRef);
                p->argc = p->retc = 1;
                p = pushType(mb,p,tpe);
				setVarFixed(mb, getArg(p,0));
			}
			continue;
		}

		if (getFunctionId(p) == emptybindidxRef) {
#ifdef DEBUG_OPT_EMPTYBIND
			mnstr_printf(cntxt->fdout, "#empty bindidx  pc %d var %d\n",i , getArg(p,0) );
#endif
			actions++;
			setFunctionId(p,bindidxRef);
			p->typechk= TYPE_UNKNOWN;
			marked[getArg(p,0)] = i;
			// replace the call into a empty bat creation unless the table was updated already in the same query 
			sch = getVarConstant(mb,getArg(p,2  + (p->retc==2))).val.sval;
			tbl = getVarConstant(mb,getArg(p,3  + (p->retc==2))).val.sval;
			for(j= 0; j< etop; j++){
				q= updated[j];
				if(q && getModuleId(q) == sqlRef && (getFunctionId(q) == appendRef || getFunctionId(q) == updateRef )){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 &&
						 strcmp(getVarConstant(mb,getArg(q,3)).val.sval, tbl) == 0 ){
#ifdef DEBUG_OPT_EMPTYBIND
							mnstr_printf(cntxt->fdout, "#reset mark empty variable pc %d var %d\n",i , getArg(p,0) );
#endif
						marked[getArg(p,0)] = 0;
						if( p->retc == 2){
#ifdef DEBUG_OPT_EMPTYBIND
							mnstr_printf(cntxt->fdout, "#reset mark empty variable pc %d var %d\n",i , getArg(p,1) );
#endif
							marked[getArg(p,1)] = 0;
						}
						break;
					}
				}
				if(q && getModuleId(q) == sqlRef && getFunctionId(q) == catalogRef){
					if ( strcmp(getVarConstant(mb,getArg(q,2)).val.sval, sch) == 0 ){
						marked[getArg(p,0)] = 0;
						break;
					}
				}
			}
			if( marked[getArg(p,0)]){
				int tpe;
				if( p->retc == 2){
					tpe = getBatType(getVarType(mb,getArg(p,1)));
					q= newStmt(mb,batRef,newRef);
					q = pushType(mb,q,tpe);
					getArg(q,0)= getArg(p,1);
					setVarFixed(mb,getArg(q,0));
				}
				
				tpe = getBatType(getVarType(mb,getArg(p,0)));
				clrFunction(p);
				setModuleId(p,batRef);
				setFunctionId(p,newRef);
				p->argc = p->retc = 1;
				p = pushType(mb,p,tpe);
				setVarFixed(mb, getArg(p,0));
			}
			continue;
		}

		// delta operations without updates+ insert can be replaced by an assignment
		if (getModuleId(p)== sqlRef && getFunctionId(p) == deltaRef  && p->argc ==5){
			if( marked[getArg(p,2)] && marked[getArg(p,3)] && marked[getArg(p,4)] ){
#ifdef DEBUG_OPT_EMPTYBIND
				mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d,%d,%d\n",i ,marked[getArg(p,2)], marked[getArg(p,3)], marked[getArg(p,4)] );
				mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d\n",i , getArg(p,0) );
#endif
				actions++;
				clrFunction(p);
				p->argc = 2;
				if ( marked[getArg(p,1)] ){
					marked[getArg(p,0)] = i;
				}
			}
			continue;
		}

		if (getModuleId(p)== sqlRef && getFunctionId(p) == projectdeltaRef) {
			if( marked[getArg(p,3)] && marked[getArg(p,4)] ){
#ifdef DEBUG_OPT_EMPTYBIND
				mnstr_printf(cntxt->fdout, "#empty projectdelta  pc %d var %d\n",i , getArg(p,0) );
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
				if( marked[getArg(p,1)] || marked[getArg(p,2)] ){
#ifdef DEBUG_OPT_EMPTYBIND
					mnstr_printf(cntxt->fdout, "#empty projection  pc %d var %d\n",i , getArg(p,0) );
#endif
					actions++;
					emptyresult(0);
				}
			}
		}
	}

#ifdef DEBUG_OPT_EMPTYBIND
	chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
	mnstr_printf(GDKout, "#Optimize Query Emptybind done\n");
	printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
#endif

	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(marked);
	GDKfree(updated);
    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","emptybind",actions,GDKusec() - usec);
    newComment(mb,buf);
	return 1;
}
