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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "opt_xid.h"

/*
 * Inject OID list compressions, to be used after garbagecontrol
 */
int 
OPTxidImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int type, i, j, se,limit;
	InstrPtr q, p=0, *old= mb->stmt;
	int actions = 0;
	int *alias;

	(void) pci;
	(void) stk;		/* to fool compilers */

	alias = GDKzalloc(sizeof(int) * mb->vtop);
	if (alias ==0)
		return 0;

	limit= mb->stop;
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		return 0;

	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++) {
		p= old[i];

		se = p->token == ENDsymbol;
		if( se){
			pushInstruction(mb,p);
			for(i++; i<limit; i++)
				if(old[i])
					pushInstruction(mb,old[i]);
			break;
		}
		if( p->token == NOOPsymbol)
			continue;
		/* decompress the arguments */
		pushInstruction(mb,old[i]);
		for ( j =p->retc; j< p->argc; j++)
		if ( alias[getArg(p,j)]){
			q = newStmt(mb,"xid","decompress");
			q= pushArgument(mb,q, alias[getArg(p,j)]);
			getArg(q,0) = getArg(p,j);
		}

		/* compress the result, now only on oid-oid */
		for ( j =0, type= getVarType(mb,getArg(p,j)); j< p->retc; j++, type= getVarType(mb,getArg(p,j)))
		if ( (getTailType(type) == TYPE_oid || getTailType(type) == TYPE_oid) && 
			(getHeadType(type) == TYPE_oid || getHeadType(type) == TYPE_oid) && alias[getArg(p,j)]==0 ){
			mnstr_printf(GDKout,"#got candidate %d head %d tail %d\n",getArg(p,j), getHeadType(getVarType(mb,getArg(p,j))),  getTailType(getVarType(mb,getArg(p,j))));
			q = newStmt(mb,"xid","compress");
			q= pushArgument(mb,q, getArg(p,j));
			alias [getArg(p,j)] = getArg(q,0);
		} 
	}
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_xid: %d statements removed\n", actions);
	GDKfree(old);
	GDKfree(alias);
	/* we may have uncovered new use-less operations */
	return actions;
}
