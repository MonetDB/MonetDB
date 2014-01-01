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
#include "opt_commonTerms.h"
#include "mal_exception.h"
 /*
 * Caveat. A lot of time was lost due to constants that are indistinguisable
 * at the surface level. It may miss common expressions if their constants
 * are introduced too far apart in the MAL program.
 * It requires the constant optimizer to be ran first.
 */
int
OPTcommonTermsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, prop, candidate, barrier= 0, cnt;
	InstrPtr p, q;
	int actions = 0;
	int limit, slimit;
	int *alias;
	InstrPtr *old;
	int *list;	
	/* link all final constant expressions in a list */
	/* it will help to find duplicate sql.bind calls */
	int cstlist=0;
	int *vars;

	(void) cntxt;
	(void) stk;
	(void) pci;
	alias = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	list = (int*) GDKzalloc(sizeof(int) * mb->stop);
	vars = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if ( alias == NULL || list == NULL || vars == NULL){
		if(alias) GDKfree(alias);
		if(list) GDKfree(list);
		if(vars) GDKfree(vars);
		return 0;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0){
		GDKfree(alias);
		GDKfree(list);
		GDKfree(vars);
		return 0; 
	}

	for ( i = 0; i < limit; i++) {
		p = old[i];

		for ( k = 0; k < p->argc; k++)
		if ( alias[getArg(p,k)] )
			getArg(p,k) = alias[getArg(p,k)];

		/* Link the statement to the previous use, based on the last argument.*/
		if ( p->retc < p->argc ) {
			candidate = vars[getArg(p,p->argc-1)];
			if ( isVarConstant(mb, getArg(p,p->argc-1)) ){
				/* all instructions with constant tail are linked */
				list[i] = cstlist;
				cstlist = i;
			} else
				list[i]= vars[ getArg(p,p->argc-1) ];
			vars[getArg(p,p->argc-1)] = i;
		} else candidate = 0;

		pushInstruction(mb,p);
		if (p->token == ENDsymbol){
			/* wrap up the remainder */
			for(i++; i<limit; i++)
				if( old[i])
					pushInstruction(mb,old[i]);
			break;
		}
		/*
		 * @-
		 * Any non-empty barrier block signals the end of this optimizer,
		 * the impact of the block can affect the common code.
		 */
		barrier |= (p->barrier== BARRIERsymbol || p->barrier== CATCHsymbol) && old[i+1]->barrier!=EXITsymbol;
		/*
		 * @-
		 * Also block further optimization when you have seen an assert().
		 * This works particularly for SQL, because it is not easy to track
		 * the BAT identifier aliases to look for updates. The sql.assert
		 * at least tells us that an update is planned.
		 * Like all optimizer decisions, it is safe to stop.
		 */
		barrier |= getFunctionId(p) == assertRef;
		if (p->token == NOOPsymbol || p->token == ASSIGNsymbol || barrier /* || p->retc == p->argc */) {
#ifdef DEBUG_OPT_COMMONTERMS_MORE
				mnstr_printf(cntxt->fdout, "COMMON SKIPPED[%d] %d %d\n",i, barrier, p->retc == p->argc);
#endif
			continue;
		}

		/* from here we have a candidate to look for a match */
#ifdef DEBUG_OPT_COMMONTERMS_MORE
		mnstr_printf(cntxt->fdout,"#CANDIDATE[%d] ",i);
		printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
#endif
		prop = mayhaveSideEffects(cntxt, mb, p,TRUE) || isUpdateInstruction(p);
		j =	isVarConstant(mb, getArg(p,p->argc-1))? cstlist: candidate;
				
		cnt = mb->stop / 128 < 32? 32 : mb->stop/128;	/* limit search depth */
		if ( !prop)
		for (; cnt > 0 && j ; cnt--, j = list[j]) 
			if ( getFunctionId(q=getInstrPtr(mb,j)) == getFunctionId(p) && getModuleId(q) == getModuleId(p)  ){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
			mnstr_printf(cntxt->fdout,"#CANDIDATE %d, %d  %d %d ", i, j, 
				hasSameSignature(mb, p, q, p->retc), 
				hasSameArguments(mb, p, q));
				printInstruction(cntxt->fdout, mb, 0, q, LIST_MAL_ALL);
				mnstr_printf(cntxt->fdout," :%d %d %d=%d %d %d %d %d %d\n", 
					q->token != ASSIGNsymbol ,
					list[getArg(q,q->argc-1)],i,
					!hasCommonResults(p, q), 
					!mayhaveSideEffects(cntxt, mb, q, TRUE),
					!isUpdateInstruction(q),
					isLinearFlow(q),
					isLinearFlow(p));
#endif
				/*
				 * @-
				 * Simple assignments are not replaced either. They should be
				 * handled by the alias removal part. All arguments should
				 * be assigned their value before instruction p.
				 */
				if ( hasSameArguments(mb, p, q) && 
					hasSameSignature(mb, p, q, p->retc) && 
					!hasCommonResults(p, q) && 
					!isUnsafeFunction(q) && 
					isLinearFlow(q) 
				   ) {
						if (safetyBarrier(p, q) ){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
						mnstr_printf(cntxt->fdout,"#safetybarrier reached\n");
#endif
						break;
					}
#ifdef DEBUG_OPT_COMMONTERMS_MORE
						mnstr_printf(cntxt->fdout, "Found a common expression " "%d <-> %d\n", j, i);
						printInstruction(cntxt->fdout, mb, 0, q, LIST_MAL_ALL);
#endif
					clrFunction(p);
					p->argc = p->retc;
					for (k = 0; k < q->retc; k++){
						alias[getArg(p,k)] = getArg(q,k);
						p= pushArgument(mb,p, getArg(q,k));
					}
#ifdef DEBUG_OPT_COMMONTERMS_MORE
					mnstr_printf(cntxt->fdout, "COMMON MODIFIED EXPRESSION %d -> %d\n",i,j);
					printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
#endif
					actions++;
					break; /* end of search */
				}
			}
#ifdef DEBUG_OPT_COMMONTERMS_MORE
			else if ( mayhaveSideEffects(cntxt, mb, q, TRUE) || isUpdateInstruction(p)){
				mnstr_printf(cntxt->fdout, "COMMON SKIPPED %d %d\n", mayhaveSideEffects(cntxt, mb, q, TRUE) , isUpdateInstruction(p));
				printInstruction(cntxt->fdout, mb, 0, q, LIST_MAL_ALL);
			}
#endif
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(list);
	GDKfree(vars);
	GDKfree(old);
	GDKfree(alias);
	return actions;
}
