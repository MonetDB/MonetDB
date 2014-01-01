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
/*
 * [NOTE the accumulator optimizer is known to produce
 * problems due to concurrent access to the BATs.
 * However, the last instruction for a BAT is now scheduled only
 * when all other uses have finished.
 *
 * The accumulator can be installed just before garbage collector,
 * because the other modules do not recognize batcalc operations with
 * more arguments.]
 */
#include "monetdb_config.h"
#include "opt_accumulators.h"
#include "mal_builder.h"

int
OPTaccumulatorsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit,slimit;
	InstrPtr p,q;
	Module scope = cntxt->nspace;
	int actions = 0;
	InstrPtr *old;
	Lifespan span;

	(void) pci;
	(void) stk;		/* to fool compilers */
	span = setLifespan(mb);
	if( span == NULL)
		return 0;
	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb,mb->stop) < 0)
		return 0;
	for (i = 0; i < limit; i++) {
		p = old[i];

		if( getModuleId(p) != batcalcRef ) {
			pushInstruction(mb,p);
			continue;
		}
		OPTDEBUGaccumulators
			printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
		if (p->retc==1 && p->argc == 2) {
			/* unary operation, avoid clash with binary */
			pushInstruction(mb,p);
			continue;
		}
		if( getLastUpdate(span,getArg(p,0)) != i ) {
			/* only consider the last update to this variable */
			pushInstruction(mb,p);
			continue;
		}

		if (p->retc==1  && p->argc == 3 && isaBatType(getArgType(mb,p,0))) {
			int b1 =getEndLifespan(span,getArg(p,1))<=i && getArgType(mb,p,1) == getArgType(mb,p,0);
			int b2 =getEndLifespan(span,getArg(p,2))<=i && getArgType(mb,p,2) == getArgType(mb,p,0) ;
			if ( b1 == 0 && b2 == 0){
				pushInstruction(mb,p);
				continue;
			}
			/* binary/unary operation, check arguments for being candidates */
			q= copyInstruction(p);
			p= pushBit(mb,p, b1);
			p= pushBit(mb,p, b2);

			typeChecker(cntxt->fdout, scope, mb, p, TRUE);
			if (mb->errors || p->typechk == TYPE_UNKNOWN) {
				OPTDEBUGaccumulators{
					mnstr_printf(cntxt->fdout,"# Failed typecheck");
					printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
				}
				/* reset instruction error buffer */
				cntxt->errbuf[0]=0;
				mb->errors = 0;
				freeInstruction(p);
				p=q; /* restore */
			} else  {
				OPTDEBUGaccumulators{
					mnstr_printf(cntxt->fdout, "#Found accumulation candidate ");
					mnstr_printf(cntxt->fdout, "%d: %d(%d)\n", i, getArg(p,0),getArg(p,2));
					printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
				}
				freeInstruction(q);
				actions++;  
			}
			OPTDEBUGaccumulators
				printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
		}
		pushInstruction(mb,p);
	} 
	for (i = limit; i<slimit; i++) 
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(span);
	return actions;
}
