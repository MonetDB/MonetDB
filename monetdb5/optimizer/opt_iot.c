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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) M. Kersten
 * Assume simple queries . Clear out all non-iot schema related sql statements, except
 * for the bare minimum.
 */
/*
 * We keep a flow dependency table to detect.
 */
#include "monetdb_config.h"
#include "opt_iot.h"
#include "opt_deadcode.h"
#include "mal_interpreter.h"    /* for showErrors() */
#include "mal_builder.h"
#include "opt_statistics.h"
#include "opt_dataflow.h"

int
OPTiotImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int actions = 0, mvc=0;
	int i, j, limit, slimit;
	InstrPtr r, p, *old;
	int  movetofront=0;
	int *alias;
	char *tidlist;

	(void) pci;
	(void) mvc;

	OPTDEBUGiot {
		mnstr_printf(cntxt->fdout, "#iot optimizer started\n");
		printFunction(cntxt->fdout, mb, stk, LIST_MAL_DEBUG);
	} else
		(void) stk;

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, slimit) < 0)
		return 0;

	alias = (int *) GDKzalloc(mb->vtop * 2 * sizeof(int));
	tidlist = (char *) GDKzalloc(mb->vtop );
	if (alias == 0)
		return 0;

	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++)
		if (old[i]) {
			p = old[i];

			if (getModuleId(p) == iotRef && getFunctionId(p) == putName("window", 6) &&
				isVarConstant(mb, getArg(p, 1)) && isVarConstant(mb, getArg(p, 2)) && isVarConstant(mb, getArg(p, 3)))
				/* let's move the window to the start of the block  when it consists of constants*/
				movetofront=1;
			if (getModuleId(p) == iotRef && (getFunctionId(p) == putName("threshold", 9) || getFunctionId(p) == putName("beat", 4)) &&
				isVarConstant(mb, getArg(p, 1)) && isVarConstant(mb, getArg(p, 2)))
				/* let's move the threshold/beat to the start of the block  when it consists of constants*/
				movetofront=1;
			if( movetofront){
				movetofront =0;
				pushInstruction(mb, p);
				for (j = mb->stop - 1; j > 1; j--)
					mb->stmt[j] = mb->stmt[j - 1];
				mb->stmt[j] = p;
				continue;
			}

			if (p->token == ENDsymbol) {
				/* a good place to commit the SQL transaction */
				/* catch any exception left behind */
				r = newAssignment(mb);
				j = getArg(r, 0) = newVariable(mb, GDKstrdup("SQLexception"), TYPE_str);
				setVarUDFtype(mb, j);
				r->barrier = CATCHsymbol;
				r = newAssignment(mb);
				getArg(r, 0) = j;
				r->barrier = EXITsymbol;
				r = newAssignment(mb);
				j = getArg(r, 0) = newVariable(mb, GDKstrdup("MALexception"), TYPE_str);
				setVarUDFtype(mb, j);
				r->barrier = CATCHsymbol;
				r = newAssignment(mb);
				getArg(r, 0) = j;
				r->barrier = EXITsymbol;

				break;
			}

			if (getModuleId(p) == sqlRef && getFunctionId(p) == mvcRef)
				mvc = getArg(p, 0);

			/* trim the number of sql instructions dealing with baskets */
			if (getModuleId(p) == sqlRef && getFunctionId(p) == putName("affectedRows", 12)) {
				freeInstruction(p);
				continue;
			}

			/* remove consolidation of tid lists */
			if (getModuleId(p) == algebraRef && getFunctionId(p) == subjoinRef  && tidlist[getArg(p,1)]){
				alias[getArg(p, 0)] = getArg(p,2);
				freeInstruction(p);
				continue;
			}
			/* remove delta processing for baskets */
			if (getModuleId(p) == sqlRef && (getFunctionId(p) == deltaRef || getFunctionId(p) == subdeltaRef) ) {
				clrFunction(p);
				p->argc =2;
				pushInstruction(mb, p);
				continue;
			}

			/* remove delta processing for baskets */
			if (getModuleId(p) == sqlRef && (getFunctionId(p) == projectdeltaRef || getFunctionId(p) == subdeltaRef) ) {
				clrFunction(p);
				setModuleId(p,algebraRef);
				setFunctionId(p,subjoinRef);
				p->argc =3;
				pushInstruction(mb, p);
				continue;
			}

			for (j = 0; j < p->argc; j++)
				if (alias[getArg(p, j)])
					getArg(p, j) = alias[getArg(p, j)];

			if (getModuleId(p) == sqlRef && getFunctionId(p) == appendRef) {
				/* the appends come in multiple steps.
				   The first initializes an basket update statement,
				   which is triggered when we commit the transaction.
				 */
			}
			pushInstruction(mb, p);
		}

    /* take the remainder as is */
    for (; i<limit; i++)
        if (old[i])
            pushInstruction(mb,old[i]);
	(void) stk;
	(void) pci;

	OPTDEBUGiot {
		mnstr_printf(cntxt->fdout, "#iot optimizer intermediate\n");
		printFunction(cntxt->fdout, mb, stk, LIST_MAL_DEBUG);
	} 
	GDKfree(alias);
	GDKfree(tidlist);
	return actions;
}

