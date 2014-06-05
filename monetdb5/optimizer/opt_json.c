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
 *
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
			q = newStmt(mb, jsonRef, resultSetRef);
			q = pushArgument(mb, q, bu);
			q = pushArgument(mb, q, br);
			q = pushArgument(mb, q, bj);
			j = getArg(q,0);
			p= getInstrPtr(mb,0);
			setVarType(mb,getArg(p,0),TYPE_str);
			q = newReturnStmt(mb);
			getArg(q,0)= getArg(p,0);
			pushArgument(mb,q,j);
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
	return actions;
}
