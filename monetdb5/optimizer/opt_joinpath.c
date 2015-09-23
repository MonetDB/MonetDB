/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * Post-optimization. After the join path has been constructed
 * we could search for common subpaths. This heuristic is to
 * remove any pair which is used more than once.
 * Inner paths are often foreign key walks.
 * The heuristics is sufficient for the code produced by SQL frontend.
 * The alternative is to search for all possible subpaths and materialize them.
 * For example, using recursion for all common paths.
 */
#include "monetdb_config.h"
#include "opt_joinpath.h"

typedef struct{
	int cnt;
	int lvar, rvar;
	str fcn;
	InstrPtr p;
} Candidate;

//#undef OPTDEBUGjoinPath 
//#define OPTDEBUGjoinPath  if(1)
/*
 * The join path type analysis should also be done at run time,
 * because the expressive power of MAL is insufficient to
 * enforce a proper join type list.
 * The current  costmodel is rather limited. It takes as default
 * the order presented and catches a few more cases that do not
 * lead to materialization of large views. This solved the earlier
 * problem noticable on TPC-H that join order was producing slower
 * results. Now, on TPCH-H, all queries run equal or better.
 * about 12 out of 84 joinpaths are improved with more than 10%.
 * (affecting Q2, Q5, Q7, Q8,Q11,Q13)
 */
static int
OPTjoinSubPath(Client cntxt, MalBlkPtr mb)
{
	int i,j,k,top=0, actions =0;
	InstrPtr q = NULL, p, *old;
	int limit, slimit;
	Candidate *candidate;

	candidate = (Candidate *) GDKzalloc(mb->stop * sizeof(Candidate));
	if ( candidate == NULL)
		return 0;
	(void) cntxt;

	/* collect all candidates */
	limit= mb->stop;
	slimit= mb->ssize;
	for(i=0, p= getInstrPtr(mb, i); i< limit; i++, p= getInstrPtr(mb, i))
		if ( getFunctionId(p) == leftfetchjoinPathRef)
			for ( j= p->retc; j< p->argc-1; j++){
				for (k= top-1; k >= 0 ; k--)
					if ( candidate[k].lvar == getArg(p,j) && candidate[k].rvar == getArg(p,j+1) && candidate[k].fcn == getFunctionId(p)){
						candidate[k].cnt++;
						break;
					}
				if (k < 0) k = top;
				if ( k == top && top < mb->stop ){
					candidate[k].cnt =1;
					candidate[k].lvar = getArg(p,j);
					candidate[k].rvar = getArg(p,j+1);
					candidate[k].fcn = getFunctionId(p);
					top++;
				}
			}

	if (top == 0) {
		GDKfree(candidate);
		return 0;
	}

	/* now inject and replace the subpaths */
	old = mb->stmt;
	if ( newMalBlkStmt(mb,mb->ssize) < 0) {
		GDKfree(candidate);
		return 0;
	}

	for(i=0, p= old[i]; i< limit; i++, p= old[i]) {
		if( getFunctionId(p)== leftfetchjoinPathRef)
			for ( j= p->retc ; j< p->argc-1; j++){
				for (k= top-1; k >= 0 ; k--)
					if ( candidate[k].lvar == getArg(p,j) && candidate[k].rvar == getArg(p,j+1) && candidate[k].fcn == getFunctionId(p) && candidate[k].cnt > 1){
						if ( candidate[k].p == 0 ) {
							if ( candidate[k].fcn == leftfetchjoinPathRef)
								q= newStmt(mb, algebraRef, leftfetchjoinRef);
							q= pushArgument(mb,q, candidate[k].lvar);
							q= pushArgument(mb,q, candidate[k].rvar);
							candidate[k].p = q;
						} 
						delArgument(p,j);
						getArg(p,j) = getArg(candidate[k].p,0);
						if ( p->argc == 3 ){
							if ( getFunctionId(p) == leftfetchjoinPathRef)
								setFunctionId(p, leftfetchjoinRef);
						}
						actions ++;
						OPTDEBUGjoinPath {
							mnstr_printf(cntxt->fdout,"re-use pair\n");
							printInstruction(cntxt->fdout,mb,0,candidate[k].p,0);
							printInstruction(cntxt->fdout,mb,0,p,0);
						}
						goto breakout;
					}
			}
	breakout:
		pushInstruction(mb,p);
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	
	GDKfree(old);
	GDKfree(candidate);
	/* there may be new opportunities to remove common expressions 
	   avoid the recursion
	*/
	if ( actions )
		return actions + OPTjoinSubPath(cntxt, mb);
	return actions;
}

int
OPTjoinPathImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, actions=0;
	int *pc;
	InstrPtr q,r;
	InstrPtr *old;
	int *varcnt;		/* use count */
	int limit,slimit;

	(void) cntxt;
	(void) stk;
	if (varGetProp(mb, getArg(mb->stmt[0], 0), inlineProp) != NULL)
		return 0;

	old= mb->stmt;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb,mb->ssize) < 0)
		return 0;

	/* beware, new variables and instructions are introduced */
	pc= (int*) GDKzalloc(sizeof(int)* mb->vtop * 2); /* to find last assignment */
	varcnt= (int*) GDKzalloc(sizeof(int)* mb->vtop * 2); 
	if (pc == NULL || varcnt == NULL){
		if (pc ) GDKfree(pc);
		if (varcnt ) GDKfree(varcnt);
		return 0;
	}
	/*
	 * Count the variable used as arguments first.
	 */
	for (i = 0; i<limit; i++){
		p= old[i];
		for(j=p->retc; j<p->argc; j++)
			varcnt[getArg(p,j)]++;
	}

	/* assume a single pass over the plan, and only consider projection sequences composed of leftfetchjoin
 	 */
	for (i = 0; i<limit; i++){
		p= old[i];
		if( getModuleId(p)== algebraRef && getFunctionId(p) == leftfetchjoinRef && p->argc ==3){
			/*
			 * Try to expand its argument list with what we have found so far.
			 * This creates a series of join paths, many of which will be removed during deadcode elimination.
			 */
			q= copyInstruction(p);
			q->argc=p->retc;
			for(j=p->retc; j<p->argc; j++){
				r= getInstrPtr(mb,pc[getArg(p,j)]);
				/*
				 * Don't inject a pattern when it is used more than once.
				 * For leftfetchjoin series we may benefitt
				 */
				if (r && varcnt[getArg(p,j)] > 1){
					OPTDEBUGjoinPath {
						mnstr_printf(cntxt->fdout,"#double use %d %d\n", getArg(p,j), varcnt[getArg(p,j)]);
						printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
					}
					r = 0;
				}
				
				OPTDEBUGjoinPath if (r) {
					mnstr_printf(cntxt->fdout,"#expand list \n");
					printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
					printInstruction(cntxt->fdout,mb, 0, q, LIST_MAL_ALL);
				}
				if ( getFunctionId(p) == leftfetchjoinRef){
					if( r &&  getModuleId(r)== algebraRef && ( getFunctionId(r)== leftfetchjoinRef  || getFunctionId(r)== leftfetchjoinPathRef) ){
						for(k= r->retc; k<r->argc; k++) 
							q = pushArgument(mb,q,getArg(r,k));
					} else 
						q = pushArgument(mb,q,getArg(p,j));
				}
			}
			OPTDEBUGjoinPath {
				chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
				mnstr_printf(cntxt->fdout,"#new [[left]fetch]joinPath instruction\n");
				printInstruction(cntxt->fdout,mb, 0, q, LIST_MAL_ALL);
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
				if( getColumnType(getArgType(mb,q,j)) != TYPE_oid  && getColumnType(getArgType(mb,q,j)) != TYPE_void ){
					/* don't use the candidate list */
					freeInstruction(q);
					goto wrapup;
				}

			/* fix the type */
			setVarUDFtype(mb, getArg(q,0));
			setVarType(mb, getArg(q,0), newBatType( TYPE_oid, getColumnType(getArgType(mb,q,q->argc-1))));
			if ( getFunctionId(q) == leftfetchjoinRef )
				setFunctionId(q,leftfetchjoinPathRef);
			freeInstruction(p);
			p= q;
			actions++;
		} 
	wrapup:
		pushInstruction(mb,p);
		for(j=0; j< p->retc; j++)
			pc[getArg(p,j)]= mb->stop-1;
	}
	for(; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
/* perform a second phase, trial code, many TPCH queries have subpaths of interest.
 * The count is meant to illustrate the impact
 */
	if (0 &&  actions )
		actions = 10000*actions+ OPTjoinSubPath(cntxt, mb);
	GDKfree(old);
	GDKfree(pc);
	if (varcnt ) GDKfree(varcnt);
	return actions;
}
