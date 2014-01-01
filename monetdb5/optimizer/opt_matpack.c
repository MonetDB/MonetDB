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
 * This simple module unrolls the mat.pack into an incremental sequence.
 * This could speedup parallel processing and releases resources faster.
 */
#include "monetdb_config.h"
#include "opt_matpack.h"

int 
OPTmatpackImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int v, i, j, limit, slimit;
	InstrPtr p,q;
	int actions = 0;
	InstrPtr *old;
	char *packIncrementRef = putName("packIncrement", 13);

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
		if( getModuleId(p) == matRef  && getFunctionId(p) == packRef && isaBatType(getArgType(mb,p,1))) {
			q = newStmt(mb, matRef, packIncrementRef);
			v = getArg(q,0);
			setVarType(mb,v,getArgType(mb,p,1));
			q = pushArgument(mb, q, getArg(p,1));
			q = pushInt(mb,q, p->argc - p->retc);

			for ( j = 2; j < p->argc; j++) {
				q = newStmt(mb,matRef, packIncrementRef);
				q = pushArgument(mb, q, v);
				q = pushArgument(mb, q, getArg(p,j));
				setVarType(mb,getArg(q,0),getVarType(mb,v));
				v = getArg(q,0);
			}
			getArg(q,0) = getArg(p,0);
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
