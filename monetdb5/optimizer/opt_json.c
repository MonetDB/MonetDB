/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * This simple module replaces the SQL rendering with a JSON rendering
 * Can be called after dataflow optimizer. The result appears as a string
 * in the calling environment.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "opt_json.h"

int 
OPTjsonImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, limit, slimit;
	int bu = 0, br = 0, bj = 0;
	str nme;
	InstrPtr p,q;
	int actions = 0;
	InstrPtr *old;
	char buf[256];
	lng usec = GDKusec();

	(void) pci;
	(void) cntxt;
	(void) stk;		/* to fool compilers */
	old= mb->stmt;
	limit= mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb,mb->stop) < 0)
		return 0;
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( getModuleId(p) == sqlRef  && getFunctionId(p) == affectedRowsRef) {
			q = newInstruction(0, jsonRef, resultSetRef);
			q = pushArgument(mb, q, bu);
			q = pushArgument(mb, q, br);
			q = pushArgument(mb, q, bj);
			j = getArg(q,0);
			p= getInstrPtr(mb,0);
			setDestVar(q, newTmpVariable(mb, TYPE_str));
			pushInstruction(mb,p);
			q = newInstruction(0, NULL, NULL);
			q->barrier = RETURNsymbol;
			getArg(q,0)= getArg(p,0);
			pushArgument(mb,q,j);
			pushInstruction(mb,q);
			continue;
		}
		if( getModuleId(p) == sqlRef  && getFunctionId(p) == rsColumnRef) {
			nme = getVarConstant(mb,getArg(p,4)).val.sval;
			if (strcmp(nme,"uuid")==0)
				bu = getArg(p,7);
			if (strcmp(nme,"lng")==0)
				br = getArg(p,7);
			if (strcmp(nme,"json")==0)
				bj = getArg(p,7);
			freeInstruction(p);
			continue;
		}
		pushInstruction(mb,p);
	} 
	for(; i<slimit; i++)
		if (old[i]) 
			freeInstruction(old[i]);
	GDKfree(old);
    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        chkDeclarations(cntxt->fdout, mb);
    }
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","json",actions,GDKusec() - usec);
    newComment(mb,buf);

	return actions;
}
