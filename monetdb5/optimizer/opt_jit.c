/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

str
OPTjitImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,actions = 0;
	int limit = mb->stop;
	InstrPtr p, q, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) cntxt;
	(void) pci;

	OPTDEBUGjit{
		fprintf(stderr, "#Optimize JIT\n");
		fprintFunction(stderr, mb, 0, LIST_MAL_DEBUG);
	}

	setVariableScope(mb);
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL,"optimizer.jit", MAL_MALLOC_FAIL);

	/* peephole optimization */
	for (i = 0; i < limit; i++) {
		p = old[i];

		if (p->token == ENDsymbol){
			for(; i<limit; i++)
				if (old[i])
					pushInstruction(mb,old[i]);
			break;
		}
		/* case 1
		 * X_527 := algebra.projection(C_353, X_329);
		 * X_535 := batcalc.-(100:lng, X_527); 
		 */
		if( getModuleId(p) == batcalcRef && *getFunctionId(p) == '-' && p->argc == 3 && isVarConstant(mb, getArg(p,1)) ){
			q= getInstrPtr(mb, getVar(mb,getArg(p,2))->updated);
			if ( q == 0)
				q= getInstrPtr(mb, getVar(mb,getArg(p,2))->declared);
			if( q && getArg(q,0) == getArg(p,2) && getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef ){
				getArg(p,2)=  getArg(q,2);
				p= pushArgument(mb,p, getArg(q,1));
				OPTDEBUGjit{
					fprintf(stderr, "#Optimize JIT case 1\n");
					fprintInstruction(stderr, mb,0,p,LIST_MAL_DEBUG);
				}
			}
		}
		pushInstruction(mb,p);
	}

	OPTDEBUGjit{
		chkTypes(cntxt->usermodule,mb,TRUE);
		GDKfree(msg);
		msg = MAL_SUCCEED;
		fprintf(stderr, "#Optimize JIT done\n");
		fprintFunction(stderr, mb, 0, LIST_MAL_DEBUG);
	}

	GDKfree(old);
    /* Defense line against incorrect plans */
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","jit",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);
	return msg;
}
