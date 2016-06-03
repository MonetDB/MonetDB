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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

/* author M.Kersten
 * This optimizer hunts for the empty persistent tables accessed
 * and propagates them.
 */
#include "monetdb_config.h"
#include "opt_emptycolumn.h"
#include "opt_aliases.h"
#include "opt_deadcode.h"
#include "mal_builder.h"

#define propagate(X)									\
	do {												\
		clrFunction(p);									\
		getArg(p,1)= getArg(p,X);						\
		p->argc = 2;									\
		actions++;										\
	} while (0)


#define emptyresult(I)								\
	do {												\
		int tpe = getColumnType(getVarType(mb,getArg(p,I))); \
		clrFunction(p);									\
		setModuleId(p,batRef);							\
		setFunctionId(p,newRef);						\
		p->argc = p->retc;								\
		p = pushType(mb,p, TYPE_oid);					\
		p = pushType(mb,p,tpe);							\
	} while (0)


//#undef	OPTDEBUGemptycolumn
//#define	OPTDEBUGemptycolumn

int
OPTemptycolumnImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	int *marked;
	int limit = mb->stop;
	InstrPtr p, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();

	// use an instruction reference table to keep
	// track of where 'emptycolumn' results are produced
	marked = (int *) GDKzalloc(mb->vsize * sizeof(int));
	if ( marked == NULL)
		return 0;
	(void) stk;
	/* Got an instructions V:= bat.new(:tpe) 
	 * The form the initial family of marked sets.
	 */

	(void) cntxt;
	(void) pci;

	OPTDEBUGemptycolumn{
		mnstr_printf(GDKout, "Optimize Query Emptybind\n");
		printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
	}

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
			OPTDEBUGemptycolumn
				mnstr_printf(cntxt->fdout, "#empty bat  pc %d var %d\n",i , getArg(p,0) );
			marked[getArg(p,0)] = i;
			continue;
		} 

		/* restore the naming, dropping the runtime property 'marked' */
		if (getFunctionId(p) == emptycolumnRef) {
			OPTDEBUGemptycolumn
				mnstr_printf(cntxt->fdout, "#empty bind  pc %d var %d\n",i , getArg(p,0) );
			setFunctionId(p,bindRef);
			p->typechk= TYPE_UNKNOWN;
			marked[getArg(p,0)] = i;
			if( p->retc == 2){
				marked[getArg(p,1)] = i;
				OPTDEBUGemptycolumn
					mnstr_printf(cntxt->fdout, "#empty bind  pc %d var %d\n",i , getArg(p,1) );
			}
			// replace the call into a empty bat creation
			if( p->retc == 1){
				emptyresult(0);
			}
			continue;
		}

		if (getFunctionId(p) == emptycolumnidxRef) {
			OPTDEBUGemptycolumn
				mnstr_printf(cntxt->fdout, "#empty bindidx  pc %d var %d\n",i , getArg(p,0) );
			setFunctionId(p,bindidxRef);
			p->typechk= TYPE_UNKNOWN;
			marked[getArg(p,0)] = i;
			continue;
		}

		// delta operations without updates+ insert can be replaced by an assignment
		if (getModuleId(p)== sqlRef && getFunctionId(p) == deltaRef  && p->argc ==5){
			OPTDEBUGemptycolumn
				mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d,%d,%d\n",i ,marked[getArg(p,2)], marked[getArg(p,3)], marked[getArg(p,4)] );
			if( marked[getArg(p,2)] && marked[getArg(p,3)] && marked[getArg(p,4)] ){
				OPTDEBUGemptycolumn
					mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d\n",i , getArg(p,0) );
				clrFunction(p);
				p->argc = 2;
				marked[getArg(p,0)] = i;
			}
			continue;
		}

		if (getModuleId(p)== sqlRef && getFunctionId(p) == projectdeltaRef) {
			if( marked[getArg(p,3)] && marked[getArg(p,4)] ){
				OPTDEBUGemptycolumn
					mnstr_printf(cntxt->fdout, "#empty projectdelta  pc %d var %d\n",i , getArg(p,0) );
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
					OPTDEBUGemptycolumn
						mnstr_printf(cntxt->fdout, "#empty projection  pc %d var %d\n",i , getArg(p,0) );
					emptyresult(0);
				}
			}
		}
	}

	OPTDEBUGemptycolumn{
		chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
		mnstr_printf(GDKout, "Optimize Query Emptybind done\n");
		printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
	}

	GDKfree(old);
	GDKfree(marked);
    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","emptycolumn",1,GDKusec() - usec);
    newComment(mb,buf);
	return 1;
}
