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
#include "opt_deadcode.h"

int 
OPTdeadcodeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k, se,limit, slimit;
	InstrPtr p=0, *old= mb->stmt;
	int actions = 0;

	(void) pci;
	(void) stk;		/* to fool compilers */

	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL)
		return 0;

	clrDeclarations(mb);
	chkDeclarations(cntxt->fdout, mb);
	limit= mb->stop;
	slimit = mb->ssize;
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
		if( p->token != NOOPsymbol)
		for (k = 0; k < p->retc; k++)
			if( isVarUsed(mb,getArg(p,k)) ){
				se++;
				break;
			} 

		if ( p->token == NOOPsymbol){
			freeInstruction(p);
			actions++;
		} else
		if( getModuleId(p)== sqlRef && getFunctionId(p)== assertRef &&
			isVarConstant(mb,getArg(p,1)) && getVarConstant(mb,getArg(p,1)).val.ival==0){
			freeInstruction(p);
			actions++;
		} else
		if (se || hasSideEffects(p, FALSE) || isUpdateInstruction(p) || !isLinearFlow(p) || 
				isProcedure(mb,p)  || 
				(p->retc == 1 && varGetProp( mb, getArg(p,0), unsafeProp ) != NULL) ||
				p->barrier /* ==side-effect */)
			pushInstruction(mb,p);
		else {
			freeInstruction(p);
			actions++;
		}
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	/* we may have uncovered new use-less operations */
	if (actions) 
		actions += OPTdeadcodeImplementation(cntxt,mb, stk, pci);
	return actions;
}
