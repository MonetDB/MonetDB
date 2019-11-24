/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_commonTerms.h"
#include "mal_exception.h"
#include "gdk_tracer.h"

 /*
 * Caveat. A lot of time was lost due to constants that are indistinguisable
 * at the surface level.  It requires the constant optimizer to be ran first.
 */

#define HASHinstruction(X)   getArg((X), (X)->argc-1)

str
OPTcommonTermsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, barrier= 0;
	InstrPtr p, q;
	int actions = 0;
	int limit, slimit;
	int duplicate;
	int *alias;
	int *hash;
	int *list;	

	InstrPtr *old = NULL;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;
	(void) pci;

	DEBUG(MAL_OPT_COMMONTERMS, "COMMONTERMS optimizer enter\n");

	alias = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	list = (int*) GDKzalloc(sizeof(int) * mb->stop);
	hash = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if ( alias == NULL || list == NULL || hash == NULL){
		msg = createException(MAL,"optimizer.commonTerms", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		msg = createException(MAL,"optimizer.commonTerms", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		old = NULL;
		goto wrapup;
	}

	for ( i = 0; i < limit; i++) {
		p = old[i];
		duplicate = 0;

		for ( k = 0; k < p->argc; k++)
			if ( alias[getArg(p,k)] )
				getArg(p,k) = alias[getArg(p,k)];
			
		if (p->token == ENDsymbol){
			pushInstruction(mb,p);
			/* wrap up the remainder */
			for(i++; i<limit; i++)
				if( old[i])
					pushInstruction(mb,old[i]);
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
		if (barrier || p->token == NOOPsymbol || p->token == ASSIGNsymbol) {
			DEBUG(MAL_OPT_COMMONTERMS, "Skipped[%d]: %d %d\n", i, barrier, p->retc == p->argc);
			pushInstruction(mb,p);
			continue;
		}

		/* when we enter a barrier block, we should ditch all previous instructions from consideration */
		if( p->barrier== BARRIERsymbol || p->barrier== CATCHsymbol || p->barrier == RETURNsymbol){
			memset(list, 0, sizeof(int) * mb->stop);
			memset(hash, 0, sizeof(int) * mb->vtop);
		}
		/* side-effect producing operators can never be replaced */
		/* the same holds for function calls without an argument, it is unclear where the results comes from (e.g. clock()) */
		if ( mayhaveSideEffects(cntxt, mb, p,TRUE) || p->argc == p->retc){
			DEBUG(MAL_OPT_COMMONTERMS, "Skipped[%d] side-effect: %d\n", i, p->retc == p->argc);
			pushInstruction(mb,p);
			continue;
		}

		/* from here we have a candidate to look for a match */
		DEBUG(MAL_OPT_COMMONTERMS, "Candidate[%d] look at list[%d] => %d\n",
									i, HASHinstruction(p), hash[HASHinstruction(p)]);
		debugInstruction(MAL_OPT_COMMONTERMS, mb, 0, p, LIST_MAL_ALL);

		/* Look into the hash structure for matching instructions */
		for (j = hash[HASHinstruction(p)];  j > 0  ; j = list[j]) 
			if ( (q= getInstrPtr(mb,j)) && getFunctionId(q) == getFunctionId(p) && getModuleId(q) == getModuleId(p)  ){
				DEBUG(MAL_OPT_COMMONTERMS, "Candidate[%d->%d] %d %d :%d %d %d=%d %d %d %d\n",
					j, list[j], 
					hasSameSignature(mb, p, q), 
					hasSameArguments(mb, p, q),
					q->token != ASSIGNsymbol ,
					list[getArg(q,q->argc-1)],i,
					!hasCommonResults(p, q), 
					!isUnsafeFunction(q),
					!isUpdateInstruction(q),
					isLinearFlow(q));
				debugInstruction(MAL_OPT_COMMONTERMS, mb, 0, q, LIST_MAL_ALL);

				/*
				 * Simple assignments are not replaced either. They should be
				 * handled by the alias removal part. All arguments should
				 * be assigned their value before instruction p.
				 */
				if ( hasSameArguments(mb, p, q) && 
					 hasSameSignature(mb, p, q) && 
					 !hasCommonResults(p, q) && 
					 !isUnsafeFunction(q) && 
					 !isUpdateInstruction(q) &&
					 isLinearFlow(q) 
					) {
					if (safetyBarrier(p, q) ){
						DEBUG(MAL_OPT_COMMONTERMS, "Safety barrier reached\n");
						break;
					}
					duplicate = 1;
					clrFunction(p);
					p->argc = p->retc;
					for (k = 0; k < q->retc; k++){
						alias[getArg(p,k)] = getArg(q,k);
						p= pushArgument(mb,p, getArg(q,k));
					}

					DEBUG(MAL_OPT_COMMONTERMS, "Modified expression %d -> %d ", getArg(p,0), getArg(p,1));
					debugInstruction(MAL_OPT_COMMONTERMS, mb, 0, p, LIST_MAL_ALL);

					actions++;
					break; /* end of search */
				}
			}

			else if(isUpdateInstruction(p)){
				DEBUG(MAL_OPT_COMMONTERMS, "Skipped: %d %d\n", mayhaveSideEffects(cntxt, mb, q, TRUE) , isUpdateInstruction(p));
				debugInstruction(MAL_OPT_COMMONTERMS, mb, 0, q, LIST_MAL_ALL);
			}

		if (duplicate){
			pushInstruction(mb,p);
			continue;
		} 
		/* update the hash structure with another candidate for re-use */
		DEBUG(MAL_OPT_COMMONTERMS, "Update hash[%d] - look at arg '%d' hash '%d' list '%d'\n",
									i, getArg(p,p->argc-1), HASHinstruction(p), hash[HASHinstruction(p)]);
		debugInstruction(MAL_OPT_COMMONTERMS, mb, 0, p, LIST_MAL_ALL);

		if ( !mayhaveSideEffects(cntxt, mb, p, TRUE) && p->argc != p->retc &&  isLinearFlow(p) && !isUnsafeFunction(p) && !isUpdateInstruction(p)){
			list[i] = hash[HASHinstruction(p)];
			hash[HASHinstruction(p)] = i;
			pushInstruction(mb,p);
		}
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
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
	if(hash) GDKfree(hash);
	if(old) GDKfree(old);

	debugFunction(MAL_OPT_COMMONTERMS, mb, 0, LIST_MAL_ALL);
	DEBUG(MAL_OPT_COMMONTERMS, "COMMONTERMS optimizer exit\n");
    
	return msg;
}
