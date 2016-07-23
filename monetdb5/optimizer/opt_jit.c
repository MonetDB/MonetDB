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
 * This optimizer can be used for JIT optimization and moves
 * candidate lists into MAL operations where possible.
 * It should be ran after the candidates optimizer.
 * Specific snippets to be replaced
 *     C_1:bat[:oid] := sql.tid(X_0,"sys","t");
 *     X_4:bat[:int] := sql.bind(X_0,"sys","t","i",0);
 *     X_13 := algebra.projection(C_1,X_4);
 * projection can be avoided
 *
 * A candidate list can be pushed into the calculations
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "opt_jit.h"

int
OPTjitImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,actions = 0;
	int limit = mb->stop;
	InstrPtr p, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();

	(void) stk;
	(void) cntxt;
	(void) pci;
	return 0; // CURRENTLY NOT ENABLED
	OPTDEBUGjit{
		mnstr_printf(GDKout, "Optimize JIT\n");
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
	}

	OPTDEBUGjit{
		chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
		mnstr_printf(GDKout, "Optimize JIT done\n");
		printFunction(GDKout, mb, 0, LIST_MAL_DEBUG);
	}

	GDKfree(old);
    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","jit",actions,GDKusec() - usec);
    newComment(mb,buf);
	return 1;
}
