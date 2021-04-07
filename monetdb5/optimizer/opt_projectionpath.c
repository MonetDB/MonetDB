/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* author: M Kersten
 * Post-optimization of projection lists.
 */
#include "monetdb_config.h"
#include "opt_deadcode.h"
#include "opt_projectionpath.h"


// Common prefix reduction was not effective it is retained for
// future experiments.
//#define ELIMCOMMONPREFIX

#ifdef ELIMCOMMONPREFIX
static int
OPTprojectionPrefix(Client cntxt, MalBlkPtr mb)
{
	int i, j, k, maxmatch, actions=0;
	InstrPtr p,q,*old;
	int limit, slimit;
	InstrPtr *paths = NULL;
	int     *alias = NULL;

	(void) cntxt;
	old = mb->stmt;
	limit = mb->stop;
	slimit= mb->ssize;

	paths = (InstrPtr *) GDKzalloc(mb->vsize * sizeof(InstrPtr));
	if (paths == NULL)
		return 0;
	alias = (int*) GDKzalloc(mb->vsize * sizeof(int));
	if( alias == NULL){
		GDKfree(paths);
		return 0;
	}

	maxmatch = 0; // to collect maximum common paths
	/* Collect the projection paths achored at the same start */
	for( i=0; i< limit; i++){
		p = old[i];
		if ( getFunctionId(p) == projectionpathRef && p->argc > 3){
			k = getArg(p,1);
			if( paths[k] == 0)
				paths[k] = p;
			q = paths[k];
			// Calculate the number of almost identical paths
			if( q->argc == p->argc){
				for(j = q->retc; j<q->argc - 1; j++)
					if( getArg(p,j) != getArg(q,j))
						break;
				if( j == q->argc -1 ){
					alias[k] = alias[k] -1;
					if (alias[k] < maxmatch)
						maxmatch = alias[k];
				}
			}
		}
	}
	if (maxmatch == -1){
		GDKfree(alias);
		GDKfree(paths);
		return 0;
	}

	if (newMalBlkStmt(mb,mb->ssize) < 0){
		GDKfree(paths);
		GDKfree(alias);
		return 0;
	}


	for( i = 0; i < limit; i++){
		p = old[i];
		if ( getFunctionId(p) != projectionpathRef ){
			pushInstruction(mb,p);
			continue;
		}
		if( p->argc < 3){
			pushInstruction(mb,p);
			continue;
		}

		actions++;
		// the first one should be split if there is interest
		k = getArg(p,1);
		q = paths[k];
		if( alias[k] < 0){
			// inject the join prefix calculation
			q= copyInstruction(q);
			q->argc = q->argc -1;
			getArg(q,0) = newTmpVariable(mb, getArgType(mb,q, q->argc -1));
			pushInstruction(mb, q);
			alias[k] = getArg(q,0);
			q = copyInstruction(p);
			getArg(q,1) = alias[k];
			getArg(q,2) = getArg(q, q->argc -1);
			q->argc = 3;
			pushInstruction(mb,q);
			continue;
		}
		// check if we can replace the projectionpath with an alias
		k = getArg(p,1);
		q = paths[k];
		if( alias[k] >  0 && q->argc == p->argc){
			for(j = q->retc; j<q->argc - 1; j++)
				if( getArg(p,j) != getArg(q,j))
					break;
			if( j == q->argc - 1){
				// we found a common prefix, and it is the first one?
				getArg(p,1) = alias[k];
				getArg(p,2) = getArg(p, p->argc -1);
				p->argc = 3;
				pushInstruction(mb,p);
			}
		} else {
			pushInstruction(mb,p);
			continue;
		}

	}

	for(; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(paths);
	GDKfree(alias);
	//if(actions)
		//printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
	return actions;
}
#endif

str
OPTprojectionpathImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, actions=0, maxprefixlength=0;
	int *pc =0;
	InstrPtr q,r;
	InstrPtr *old=0;
	int *varcnt= 0;		/* use count */
	int limit,slimit;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;
	if ( mb->inlineProp)
		return MAL_SUCCEED;
	//if ( optimizerIsApplied(mb,"projectionpath") )
		//return 0;

	for( i = 0; i < mb->stop ; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) == algebraRef && ((getFunctionId(p) == projectionRef && p->argc == 3) || getFunctionId(p) == projectionpathRef) ){
			break;
		}
	}
	if ( i == mb->stop){
		goto wrapupall;
	}

	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb, 2 * mb->stop) < 0)
		throw(MAL,"optimizer.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* beware, new variables and instructions are introduced */
	pc= (int*) GDKzalloc(sizeof(int)* mb->vtop * 2); /* to find last assignment */
	varcnt= (int*) GDKzalloc(sizeof(int)* mb->vtop * 2);
	if (pc == NULL || varcnt == NULL ){
		msg = createException(MAL,"optimizer.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapupall;
	}

	/*
	 * Count the variable re-use  used as arguments first.
	 * A pass operation is not a real re-use
	 */
	for (i = 0; i<limit; i++){
		p= old[i];
		for(j=p->retc; j<p->argc; j++)
		if( ! (getModuleId(p) == languageRef && getFunctionId(p)== passRef))
			varcnt[getArg(p,j)]++;
	}

	/* assume a single pass over the plan, and only consider projection sequences
	 * beware, we are only able to deal with projections without candidate lists. (argc=3)
	 * We also should not change the type of the outcome, i.e. leaving the last argument untouched.
 	 */
	for (i = 0; i<limit; i++){
		p= old[i];
		if( getModuleId(p)== algebraRef && getFunctionId(p) == projectionRef && p->argc == 3){
			/*
			 * Try to expand its argument list with what we have found so far.
			 */
			int args = p->retc;
			for (j = p->retc; j < p->argc; j++) {
				if (pc[getArg(p,j)] &&
					(r = getInstrPtr(mb, pc[getArg(p, j)])) != NULL &&
					varcnt[getArg(p,j)] <= 1 &&
					getModuleId(r)== algebraRef &&
					(getFunctionId(r)== projectionRef ||
					 getFunctionId(r) == projectionpathRef))
					args += r->argc - r->retc;
				else
					args++;
			}
			if((q = copyInstructionArgs(p, args)) == NULL) {
				msg = createException(MAL,"optimizer.projectionpath", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapupall;
			}

			q->argc=p->retc;
			for(j=p->retc; j<p->argc; j++){
				if (pc[getArg(p,j)] )
					r = getInstrPtr(mb,pc[getArg(p,j)]);
				else
					r = 0;
				if (r && varcnt[getArg(p,j)] > 1 )
					r = 0;

				/* inject the complete sub-path */

				if ( getFunctionId(p) == projectionRef){
					if( r &&  getModuleId(r)== algebraRef && ( getFunctionId(r)== projectionRef  || getFunctionId(r)== projectionpathRef) ){
						for(k= r->retc; k<r->argc; k++)
							q = addArgument(mb,q,getArg(r,k));
					} else
						q = addArgument(mb,q,getArg(p,j));
				}
			}
			if(q->argc<= p->argc){
				/* no change */
				freeInstruction(q);
				goto wrapup;
			}
			/*
			 * Final type check and hardwire the result type, because that  can not be inferred directly from the signature
			 * We already know that all heads are void. Only the last element may have a non-oid type.
			 */
			for(j=1; j<q->argc-1; j++)
				if( getBatType(getArgType(mb,q,j)) != TYPE_oid  && getBatType(getArgType(mb,q,j)) != TYPE_void ){
					/* don't use the candidate list */
					freeInstruction(q);
					goto wrapup;
				}

			/* fix the type */
			setVarType(mb, getArg(q,0), newBatType(getBatType(getArgType(mb,q,q->argc-1))));
			if ( getFunctionId(q) == projectionRef )
				setFunctionId(q,projectionpathRef);
			q->typechk = TYPE_UNKNOWN;

			freeInstruction(p);
			p = q;
			/* keep track of the longest projection path */
			if ( p->argc  > maxprefixlength)
				maxprefixlength = p->argc;
			actions++;
		}
	wrapup:
		pushInstruction(mb,p);
		for(j=0; j< p->retc; j++)
		if( getModuleId(p)== algebraRef && ( getFunctionId(p)== projectionRef  || getFunctionId(p)== projectionpathRef) ){
			pc[getArg(p,j)]= mb->stop-1;
		}
	}

	for(; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);

	/* All complete projection paths have been constructed.
	 * There may be cases where there is a common prefix used multiple times.
	 * Especially in wide table projections and lengthy paths
	 * They can be located and replaced in the plan by factoring the common part out
	 * First experiments on tpch SF10- Q7, showed significant decrease in performance
	 * Also a run against SF-100 did not show improvement in Q7
 	 * Also there are collateral damages in the testweb.
	 if( OPTdeadcodeImplementation(cntxt, mb, 0, 0) == MAL_SUCCEED){
		actions += OPTprojectionPrefix(cntxt, mb);
	}
	 */

    /* Defense line against incorrect plans */
    if( actions > 0){
        msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
        	msg = chkFlow(mb);
	if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
wrapupall:
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","projectionpath",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);
	if (pc ) GDKfree(pc);
	if (varcnt ) GDKfree(varcnt);
	if(old) GDKfree(old);

	return msg;
}
