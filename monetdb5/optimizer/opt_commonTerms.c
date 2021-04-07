/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_commonTerms.h"
#include "mal_exception.h"
 /*
 * Caveat. A lot of time was lost due to constants that are indistinguisable
 * at the surface level.  It requires the constant optimizer to be ran first.
 */

/* The key for finding common terms is that they share variables.
 * Therefore we skip all constants, except for a constant only situation.
 */

/*
 * Speed up simple insert operations by skipping the common terms.
*/

static int
isProjectConst(InstrPtr p)
{
	if (getModuleId(p)== algebraRef && getFunctionId(p)== projectRef)
		return TRUE;
	return FALSE;
}

static int
hashInstruction(MalBlkPtr mb, InstrPtr p)
{
	int i;
	for ( i = p->argc - 1 ; i >= p->retc; i--)
		if (! isVarConstant(mb,getArg(p,i)) )
			return getArg(p,i);
	if (isVarConstant(mb,getArg(p, p->retc)) )
		return p->retc;
	return -1;
}

str
OPTcommonTermsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, barrier= 0, bailout = 0;
	InstrPtr p, q;
	int actions = 0;
	int limit, slimit;
	int duplicate;
	int *alias = NULL;
	int *hash = NULL, h;
	int *list = NULL;
	str msg = MAL_SUCCEED;

	InstrPtr *old = NULL;
	char buf[256];
	lng usec = GDKusec();

	/* catch simple insert operations */
	if( isSimpleSQL(mb)){
		goto wrapup;
	}

	(void) cntxt;
	(void) stk;
	(void) pci;
	alias = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	list = (int*) GDKzalloc(sizeof(int) * mb->stop);
	hash = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if ( alias == NULL || list == NULL || hash == NULL){
		msg = createException(MAL,"optimizer.commonTerms", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		msg = createException(MAL,"optimizer.commonTerms", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
			TRC_DEBUG(MAL_OPTIMIZER, "Skipped[%d]: %d %d\n", i, barrier, p->retc == p->argc);
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
			TRC_DEBUG(MAL_OPTIMIZER, "Skipped[%d] side-effect: %d\n", i, p->retc == p->argc);
			pushInstruction(mb,p);
			continue;
		}
		/* simple SQL bind operations need not be merged, they are cheap and/or can be duplicated eliminated elsewhere cheaper */
		if( getModuleId(p) == sqlRef && getFunctionId(p) != tidRef){
			pushInstruction(mb,p);
			continue;
		}

		/* from here we have a candidate to look for a match */

		h = hashInstruction(mb, p);

		TRC_DEBUG(MAL_OPTIMIZER, "Candidate[%d] look at list[%d] => %d\n", i, h, hash[h]);
		traceInstruction(MAL_OPTIMIZER, mb, 0, p, LIST_MAL_ALL);

		if( h < 0){
			pushInstruction(mb,p);
			continue;
		}

		bailout = 1024 ;  // don't run over long collision list
		/* Look into the hash structure for matching instructions */
		for (j = hash[h];  j > 0 && bailout-- > 0  ; j = list[j])
			if ( (q= getInstrPtr(mb,j)) && getFunctionId(q) == getFunctionId(p) && getModuleId(q) == getModuleId(p)){
				TRC_DEBUG(MAL_OPTIMIZER, "Candidate[%d->%d] %d %d :%d %d %d=%d %d %d %d\n",
					j, list[j],
					hasSameSignature(mb, p, q),
					hasSameArguments(mb, p, q),
					q->token != ASSIGNsymbol ,
					list[getArg(q,q->argc-1)],i,
					!hasCommonResults(p, q),
					!isUnsafeFunction(q),
					!isUpdateInstruction(q),
					isLinearFlow(q));
				traceInstruction(MAL_OPTIMIZER, mb, 0, q, LIST_MAL_ALL);

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
					 !isProjectConst(q) && /* disable project(x,val), as its used for the result of case statements */
					 isLinearFlow(q)
					) {
					if (safetyBarrier(p, q) ){
						TRC_DEBUG(MAL_OPTIMIZER, "Safety barrier reached\n");
						break;
					}
					duplicate = 1;
					clrFunction(p);
					p->argc = p->retc;
					for (k = 0; k < q->retc; k++){
						alias[getArg(p,k)] = getArg(q,k);
						/* we know the arguments fit so the instruction can safely be patched */
						p= addArgument(mb,p, getArg(q,k));
					}

					TRC_DEBUG(MAL_OPTIMIZER, "Modified expression %d -> %d ", getArg(p,0), getArg(p,1));
					traceInstruction(MAL_OPTIMIZER, mb, 0, p, LIST_MAL_ALL);

					actions++;
					break; /* end of search */
				}
			}

			else if(isUpdateInstruction(p)){
				TRC_DEBUG(MAL_OPTIMIZER, "Skipped: %d %d\n", mayhaveSideEffects(cntxt, mb, q, TRUE) , isUpdateInstruction(p));
				traceInstruction(MAL_OPTIMIZER, mb, 0, q, LIST_MAL_ALL);
			}

		if (duplicate){
			pushInstruction(mb,p);
			continue;
		}
		/* update the hash structure with another candidate for re-use */
		TRC_DEBUG(MAL_OPTIMIZER, "Update hash[%d] - look at arg '%d' hash '%d' list '%d'\n", i, getArg(p,p->argc-1), h, hash[h]);
		traceInstruction(MAL_OPTIMIZER, mb, 0, p, LIST_MAL_ALL);

		if ( !mayhaveSideEffects(cntxt, mb, p, TRUE) && p->argc != p->retc &&  isLinearFlow(p) && !isUnsafeFunction(p) && !isUpdateInstruction(p)){
			list[i] = hash[h];
			hash[h] = i;
			pushInstruction(mb,p);
		}
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
        	msg = chkFlow(mb);
	if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","commonTerms",actions,usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

  wrapup:
	if(alias) GDKfree(alias);
	if(list) GDKfree(list);
	if(hash) GDKfree(hash);
	if(old) GDKfree(old);
	return msg;
}
