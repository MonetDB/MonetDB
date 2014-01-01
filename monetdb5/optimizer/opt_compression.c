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
*/
#include "monetdb_config.h"
#include "opt_compression.h"

int
OPTcompressionImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, actions=0, limit, k;
	InstrPtr *old, q;
	char buf[PATHLENGTH];

	(void) stk;
	(void) cntxt;

	limit= mb->stop;
	old = mb->stmt;
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		return 0;
	pushInstruction(mb,old[0]);

	for (i = 1; i<limit; i++) {
		p = old[i];
		if (getModuleId(p)== sqlRef && (getFunctionId(p) == bindRef) ) {
			pushInstruction(mb,p);
			q= newStmt(mb,bbpRef,decompressRef);
			setArgType(mb,q,0, getArgType(mb,p,0));
			setVarUDFtype(mb,getArg(q,0));
			k = getArg(p,0);
			getArg(p,0)= getArg(q,0);
			getArg(q,0)= k;
			q= pushArgument(mb,q,getArg(p,0));
			snprintf(buf,PATHLENGTH,"%s_%s_%s_%d",
				getVarConstant(mb,getArg(p,1)).val.sval,
				getVarConstant(mb,getArg(p,2)).val.sval,
				getVarConstant(mb,getArg(p,3)).val.sval,
				getVarConstant(mb,getArg(p,4)).val.ival);
			p= pushStr(mb,q,buf);
			actions++;
			continue;
		} 
		pushInstruction(mb,p);
	}

	GDKfree(old);

	return actions;
}
