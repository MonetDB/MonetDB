/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
str
OPTcommonTermsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, prop, barrier= 0, cnt;
	InstrPtr p, q;
	int actions = 0;
	int limit, slimit;
	int *alias;
	InstrPtr *old = NULL;
	int *list;	
	/* link all final constant expressions in a list */
	/* it will help to find duplicate sql.bind calls */
	int *vars;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;
	(void) pci;
	alias = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	list = (int*) GDKzalloc(sizeof(int) * mb->stop);
	vars = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if ( alias == NULL || list == NULL || vars == NULL){
		msg = createException(MAL,"optimizer.commonTerms",MAL_MALLOC_FAIL);
		goto wrapup;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		msg = createException(MAL,"optimizer.commonTerms",MAL_MALLOC_FAIL);
		old = NULL;
		goto wrapup;
	}

	for ( i = 0; i < limit; i++) {
		p = old[i];

		for ( k = 0; k < p->argc; k++)
		if ( alias[getArg(p,k)] )
			getArg(p,k) = alias[getArg(p,k)];
			
		/* Link the statement to the previous use, based on the last argument.*/
		if ( p->retc < p->argc ){
			list[i] = vars[getArg(p,p->argc-1)];
			vars[getArg(p,p->argc-1)] = i;
		} 

		for ( k = 0; k < p->retc; k++)
			if( vars[getArg(p,k)] && p->barrier != RETURNsymbol){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
				fprintf(stderr, "#ERROR MULTIPLE ASSIGNMENTS[%d] ",i);
				fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
#endif
				pushInstruction(mb,p);
				barrier= TRUE; // no more optimization allowed
				break;
			}
		if( k < p->retc)
			continue;

		pushInstruction(mb,p);
		if (p->token == ENDsymbol){
			/* wrap up the remainder */
			for(i++; i<limit; i++)
				if( old[i]){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
					fprintf(stderr, "#FINALIZE[%d] ",i);
					fprintInstruction(stderr, mb, 0, old[i], LIST_MAL_ALL);
#endif
					pushInstruction(mb,old[i]);
			}
			break;
		}
		/*
		 * Any barrier block signals the end of this optimizer,
		 * because the impact of the block can affect the common code eliminated.
		 */
		barrier |= (p->barrier== BARRIERsymbol || p->barrier== CATCHsymbol || p->barrier == RETURNsymbol);
		/*
		 * Also block further optimization when you have seen an assert().
		 * This works particularly for SQL, because it is not easy to track
		 * the BAT identifier aliases to look for updates. The sql.assert
		 * at least tells us that an update is planned.
		 * Like all optimizer decisions, it is safe to stop.
		 */
		barrier |= getFunctionId(p) == assertRef;
		if (barrier || p->token == NOOPsymbol || p->token == ASSIGNsymbol /* || p->retc == p->argc */) {
#ifdef DEBUG_OPT_COMMONTERMS_MORE
				fprintf(stderr, "#COMMON SKIPPED[%d] %d %d\n",i, barrier, p->retc == p->argc);
#endif
			continue;
		}

		/* from here we have a candidate to look for a match */
#ifdef DEBUG_OPT_COMMONTERMS_MORE
		fprintf(stderr,"#TARGET CANDIDATE[%d] ",i);
		fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
#endif
		prop = mayhaveSideEffects(cntxt, mb, p,TRUE);
		cnt = i; /* / 128 < 32? 32 : mb->stop/128;	limit search depth */
		if ( !prop)
		for (j = list[i]; cnt > 0 && j ; cnt--, j = list[j]) 
			if ( getFunctionId(q=getInstrPtr(mb,j)) == getFunctionId(p) && getModuleId(q) == getModuleId(p)  ){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
			fprintf(stderr,"#CANDIDATE[%d->%d] %d %d", j, list[j], 
				hasSameSignature(mb, p, q, p->retc), 
				hasSameArguments(mb, p, q));
				mnstr_printf(cntxt->fdout," :%d %d %d=%d %d %d %d ", 
					q->token != ASSIGNsymbol ,
					list[getArg(q,q->argc-1)],i,
					!hasCommonResults(p, q), 
					!isUnsafeFunction(q),
					!isUpdateInstruction(q),
					isLinearFlow(q));
				printInstruction(cntxt->fdout, mb, 0, q, LIST_MAL_ALL);
#endif
				/*
				 * Simple assignments are not replaced either. They should be
				 * handled by the alias removal part. All arguments should
				 * be assigned their value before instruction p.
				 */
				if ( hasSameArguments(mb, p, q) && 
					hasSameSignature(mb, p, q, p->retc) && 
					!hasCommonResults(p, q) && 
					!isUnsafeFunction(q) && 
					!isUpdateInstruction(q) &&
					isLinearFlow(q) 
				   ) {
						if (safetyBarrier(p, q) ){
#ifdef DEBUG_OPT_COMMONTERMS_MORE
						fprintf(stderr,"#safetybarrier reached\n");
#endif
						break;
					}
					clrFunction(p);
					p->argc = p->retc;
					for (k = 0; k < q->retc; k++){
						alias[getArg(p,k)] = getArg(q,k);
						p= pushArgument(mb,p, getArg(q,k));
					}
#ifdef DEBUG_OPT_COMMONTERMS_MORE
					fprintf(stderr, "#MODIFIED EXPRESSION %d -> %d ",getArg(p,0),getArg(p,1));
					fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
#endif
					actions++;
					break; /* end of search */
				}
			}
#ifdef DEBUG_OPT_COMMONTERMS_MORE
			else if ( mayhaveSideEffects(cntxt, mb, q, TRUE) || isUpdateInstruction(p)){
				fprintf(stderr, "#COMMON SKIPPED %d %d ", mayhaveSideEffects(cntxt, mb, q, TRUE) , isUpdateInstruction(p));
				fprintInstruction(stderr, mb, 0, q, LIST_MAL_ALL);
			}
#endif
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        chkDeclarations(cntxt->fdout, mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","commonTerms",actions,usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

wrapup:
	if(alias) GDKfree(alias);
	if(list) GDKfree(list);
	if(vars) GDKfree(vars);
	if(old) GDKfree(old);
	return msg;
}
