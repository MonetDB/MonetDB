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
#include "opt_emptybind.h"
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

#define emptyresult(TPE)								\
	do {												\
		clrFunction(p);									\
		setModuleId(p, batRef);							\
		setFunctionId(p,newRef);						\
		p->argc = 1;									\
		cst.vtype=TYPE_void;							\
		cst.val.oval= oid_nil;							\
		cst.len = 0;									\
		(void) convertConstant(TPE, &cst);				\
		tmp = defConstant(mb,TPE,&cst);					\
		p= pushArgument(mb, p, tmp);						\
		actions++;										\
	} while (0)


//#undef	OPTDEBUGemptybind
//#define	OPTDEBUGemptybind
int
OPTemptybindImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, actions = 0;
	int *marked;
	int limit = mb->stop, slimit= mb->ssize;
	InstrPtr p, *old = mb->stmt;

	// use an instruction reference table to keep
	// track of where 'emptybind' results are produced
	marked = (int *) GDKzalloc(mb->vsize * sizeof(int));
	if ( marked == NULL)
		return 0;
	(void) stk;
	/* Got an instructions V:= bat.new(:tpe) 
	 * The form the initial family of marked sets.
	 */

	(void) cntxt;
	(void) pci;

	OPTDEBUGemptybind{
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
			OPTDEBUGemptybind
				mnstr_printf(cntxt->fdout, "#empty bat  pc %d var %d\n",i , getArg(p,0) );
			marked[getArg(p,0)] = i;
			continue;
		} 

		/* restore the naming, dropping the runtime property 'marked' */
		if (getFunctionId(p) == emptybindRef) {
			OPTDEBUGemptybind
				mnstr_printf(cntxt->fdout, "#empty bind  pc %d var %d\n",i , getArg(p,0) );
			setFunctionId(p,bindRef);
			marked[getArg(p,0)] = i;
			if( p->retc == 2){
				marked[getArg(p,1)] = i;
				OPTDEBUGemptybind
					mnstr_printf(cntxt->fdout, "#empty bind  pc %d var %d\n",i , getArg(p,1) );
			}
			continue;
		}

		if (getFunctionId(p) == emptybindidxRef) {
			OPTDEBUGemptybind
				mnstr_printf(cntxt->fdout, "#empty bindidx  pc %d var %d\n",i , getArg(p,0) );
			setFunctionId(p,bindidxRef);
			marked[getArg(p,0)] = i;
			continue;
		}

		// delta operations without updates+ insert can be replaced by an assignment
		if (getModuleId(p)== sqlRef && getFunctionId(p) == deltaRef  && p->argc ==5){
			OPTDEBUGemptybind
				mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d,%d,%d\n",i ,marked[getArg(p,2)], marked[getArg(p,3)], marked[getArg(p,4)] );
			if( marked[getArg(p,2)] && marked[getArg(p,3)] && marked[getArg(p,4)] ){
				OPTDEBUGemptybind
					mnstr_printf(cntxt->fdout, "#empty delta  pc %d var %d\n",i , getArg(p,0) );
				clrFunction(p);
				p->argc = 2;
				marked[getArg(p,0)] = i;
			}
			continue;
		}

		if (getModuleId(p)== sqlRef && getFunctionId(p) == projectdeltaRef) {
			if( marked[getArg(p,3)] && marked[getArg(p,4)] ){
				OPTDEBUGemptybind
					mnstr_printf(cntxt->fdout, "#empty projectdelta  pc %d var %d\n",i , getArg(p,0) );
					setModuleId(p,algebraRef);
					setFunctionId(p,projectionRef);
					p->argc = 3;
			}
			continue;
		}
	}
	for(; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);

	OPTDEBUGemptybind{
		mnstr_printf(GDKout, "Optimize Query Emptybind done\n");
		printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
	}

	GDKfree(old);
	GDKfree(marked);
	return actions;
}
