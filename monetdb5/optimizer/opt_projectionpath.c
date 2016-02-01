/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
#include "opt_projectionpath.h"

//#undef OPTDEBUGprojectionpath 
//#define OPTDEBUGprojectionpath  if(1)

int
OPTprojectionpathImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, actions=0;
	int *pc;
	InstrPtr q,r;
	InstrPtr *old;
	int *varcnt;		/* use count */
	int limit,slimit;

	(void) cntxt;
	(void) stk;
	if ( mb->inlineProp)
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

	/* assume a single pass over the plan, and only consider projection sequences 
 	 */
	for (i = 0; i<limit; i++){
		p= old[i];
		if( getModuleId(p)== algebraRef && getFunctionId(p) == projectionRef && p->argc ==3){
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
				 * For projection series we may benefitt
				 */
				if (r && varcnt[getArg(p,j)] > 1){
					OPTDEBUGprojectionpath{
						mnstr_printf(cntxt->fdout,"#double use %d %d\n", getArg(p,j), varcnt[getArg(p,j)]);
						printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
					}
					r = 0;
				}
				
				OPTDEBUGprojectionpath if (r) {
					mnstr_printf(cntxt->fdout,"#expand list \n");
					printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
					printInstruction(cntxt->fdout,mb, 0, q, LIST_MAL_ALL);
				}
				if ( getFunctionId(p) == projectionRef){
					if( r &&  getModuleId(r)== algebraRef && ( getFunctionId(r)== projectionRef  || getFunctionId(r)== projectionpathRef) ){
						for(k= r->retc; k<r->argc; k++) 
							q = pushArgument(mb,q,getArg(r,k));
					} else 
						q = pushArgument(mb,q,getArg(p,j));
				}
			}
			OPTDEBUGprojectionpath {
				chkTypes(cntxt->fdout, cntxt->nspace,mb,TRUE);
				mnstr_printf(cntxt->fdout,"#new [[left]fetch]projectionpath instruction\n");
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
			if ( getFunctionId(q) == projectionRef )
				setFunctionId(q,projectionpathRef);
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
	GDKfree(old);
	GDKfree(pc);
	if (varcnt ) GDKfree(varcnt);
	return actions;
}
