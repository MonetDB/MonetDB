/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * The statemens are all checked for being eligible for dataflow.
 */
#include "monetdb_config.h"
#include "opt_dataflow.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "manifold.h"

/*
 * dataflow processing incurs overhead and is only
 * relevant if multiple tasks kan be handled at the same time.
 * Also simple expressions dont had to be done in parallel.
 *
 * The garbagesink takes multiple variables whose endoflife is within
 * a dataflow block and who are used multiple times. They should be
 * garbage collected outside the parallel block.
 */

static int
simpleFlow(InstrPtr *old, int start, int last)
{
	int i, j, k, simple = TRUE;
	InstrPtr p = NULL, q;

	/* ignore trivial blocks */
	if ( last - start == 1)
		return TRUE;
	/* skip sequence of simple arithmetic first */
	for( ; simple && start < last; start++)  
	if ( old[start] ) {
		p= old[start];
		simple = getModuleId(p) == calcRef || getModuleId(p) == mtimeRef || getModuleId(p) == strRef || getModuleId(p)== mmathRef;
	}
	for( i = start; i < last; i++) 
	if ( old[i]) {
		q= old[i];
		simple = getModuleId(q) == calcRef || getModuleId(q) == mtimeRef || getModuleId(q) == strRef || getModuleId(q)== mmathRef;
		if( !simple)  {
			/* if not arithmetic than we should consume the previous result directly */
			for( j= q->retc; j < q->argc; j++)
				for( k =0; k < p->retc; k++)
					if( getArg(p,k) == getArg(q,j))
						simple= TRUE;
			if( !simple)
				return 0;
		}
		p = q;
	}
	return simple;
}

// take care of side-effects in updates
static void setAssigned(InstrPtr p, int k, int *assigned){
	if ( isUpdateInstruction(p) || hasSideEffects(p,TRUE))
		assigned[getArg(p,p->retc)] ++;
	assigned[getArg(p,k)]++;
}

static int
dflowAssignConflict(InstrPtr p, int pc, int *assigned, int *eolife)
{
	int j;
	/* flow blocks should be closed when we reach a point
	   where a variable is assigned  more then once
	*/
	for(j=0; j<p->retc; j++)
		if ( assigned[getArg(p,j)] )
			return 1;
	/* first argument of updates collect side-effects */
	if ( isUpdateInstruction(p) ){
		return eolife[getArg(p,p->retc)] != pc;
	}
	return 0;
}

/* Updates are permitted if it is a unique update on 
 * a BAT created in the context of this block
 * As far as we know, no SQL nor MAL test re-uses the
 * target BAT to insert again and subsequently calls dataflow.
 * In MAL scripts, they still can occur.
*/

/* a limited set of MAL instructions may appear in the dataflow block*/
static int
dataflowConflict(Client cntxt, MalBlkPtr mb,InstrPtr p) 
{
	if (p->token == ENDsymbol || 
	   (isMultiplex(p) && MANIFOLDtypecheck(cntxt,mb,p) == NULL) || 
	    blockCntrl(p) || blockStart(p) || blockExit(p))
		return TRUE;
	switch(p->token){
	case ASSIGNsymbol:
	case PATcall:
	case CMDcall:
	case FACcall:
	case FCNcall:
		return (hasSideEffects(p,FALSE) || isUnsafeFunction(p) );
	}
	return TRUE;
}

static int
dflowGarbagesink(MalBlkPtr mb, int var, InstrPtr *sink, int top){
	InstrPtr r;
	
	r = newInstruction(NULL,ASSIGNsymbol);
	getModuleId(r) = languageRef;
	getFunctionId(r) = passRef;
	getArg(r,0) = newTmpVariable(mb,TYPE_void);
	r= pushArgument(mb,r, var);
	sink[top++] = r;
	return top;
}

/* dataflow blocks are transparent, because they are always
   executed, either sequentially or in parallell */

int
OPTdataflowImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, start=1, conflict, actions=0, simple = TRUE;
	int flowblock= 0;
	InstrPtr *sink = NULL, *old = NULL, q;
	int limit, slimit, vlimit, top = 0;
	char *init = NULL;
	int *used = NULL, *assigned = NULL, *eolife = NULL;
	char  buf[256];
	lng usec = GDKusec();

	/* don't use dataflow on single processor systems */
	if (GDKnr_threads <= 1)
		return 0;

	if ( optimizerIsApplied(mb,"dataflow"))
		return 0;
	(void) stk;
	/* inlined functions will get their dataflow control later */
	if ( mb->inlineProp)
		return 0;
#ifdef DEBUG_OPT_DATAFLOW
		mnstr_printf(cntxt->fdout,"#dataflow input\n");
		printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
#endif

	vlimit = mb->vsize;
	eolife= (int*) GDKzalloc(vlimit * sizeof(int));
	init= (char*) GDKzalloc(mb->vtop);
	used= (int*) GDKzalloc(vlimit * sizeof(int));
	sink= (InstrPtr*) GDKzalloc(vlimit * sizeof(InstrPtr));
	assigned= (int*) GDKzalloc(vlimit * sizeof(int));
	if (eolife == NULL || init == NULL || used == NULL || sink == NULL || assigned == NULL)
		goto wrapup;
	
	limit= mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;
	// collect end of variable lives
	for (i = 0; i<limit; i++) {
		p = old[i];
		assert( p);
		for (j = 0; j < p->argc; j++)
			eolife[getArg(p,j)]= i;
	}

	// make sure we have space for the language.pass operation
	// for all variables within the barrier
	if ( newMalBlkStmt(mb, mb->ssize) <0 )
		goto wrapup;
	
	pushInstruction(mb,old[0]);

	/* inject new dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];
		assert(p);
		conflict = 0;

		if ( dataflowConflict(cntxt,mb,p) || (conflict = dflowAssignConflict(p,i,assigned,eolife)) )  {
#ifdef DEBUG_OPT_DATAFLOW
			mnstr_printf(cntxt->fdout,"#conflict %d dataflow %d dflowAssignConflict %d\n",i, dataflowConflict(cntxt,mb,p),dflowAssignConflict(p,i,assigned,eolife));
			printInstruction(cntxt->fdout, mb, 0, p, LIST_MAL_ALL);
#endif
			/* close previous flow block */
			if ( !(simple = simpleFlow(old,start,i))){
				for( j=start ; j<i; j++){
					q = old[j];
					// initialize variables used beyond the dataflow block
					for( k=0; k<q->retc; k++)
						if( eolife[getArg(q,k)] >= i && init[getArg(q,k)]==0){
							InstrPtr r= newAssignment(mb);
							getArg(r,0)= getArg(q,k);
							pushNil(mb,r,getArgType(mb,q,k));
							init[getArg(r,0)]=1;
						}
					// collect BAT variables garbage collected within the block 
					for( k=q->retc; k<q->argc; k++)
						if ( isaBatType(getVarType(mb,getArg(q,k))) ){
							if( eolife[getArg(q,k)] == j && used[getArg(q,k)]>=1 )
								top = dflowGarbagesink(mb, getArg(q,k), sink, top);
							else
							if( eolife[getArg(q,k)] < i )
								used[getArg(q,k)]++;
						}
				}
				flowblock = newTmpVariable(mb,TYPE_bit);
				q= newFcnCall(mb,languageRef,dataflowRef);\
				q->barrier= BARRIERsymbol;\
				getArg(q,0)= flowblock;\
			}
			//copyblock 
			for( j=start ; j<i; j++) 
				pushInstruction(mb,old[j]);
			// force the pending final garbage statements
			for( j=0; j<top; j++) 
				pushInstruction(mb,sink[j]);
			/* exit block */
			if ( ! simple){ 
				q= newAssignment(mb); 
				q->barrier= EXITsymbol; 
				getArg(q,0) = flowblock; 
			}
			// implicitly a new flow block starts
			(void) memset((char*)assigned, 0, vlimit * sizeof (int));
			(void) memset((char*) used, 0, vlimit * sizeof(int));
			top = 0;
			actions++;
			start = i+1;
			if ( ! blockStart(p) && !conflict  ){
				for ( k = 0; k < p->retc; k++)
					init[getArg(p,k)]=1;
				pushInstruction(mb,p);
				(void) memset((char*)assigned, 0, vlimit * sizeof (int));
				(void) memset((char*) used, 0, vlimit * sizeof(int));
				continue;
			} 
			if ( conflict ) 
				start --;
		}

		if (blockStart(p)){
			/* barrier blocks are kept out of the dataflow */
			/* assumes that barrier entry/exit pairs are correct. */
			/* A refinement is parallelize within a barrier block */
			int copy= 1;
			pushInstruction(mb,p);
			for ( k = 0; k < p->retc; k++)
				init[getArg(p,k)]=1;
			for ( i++; i<limit; i++) {
				p = old[i];
				for ( k = 0; k < p->retc; k++)
					init[getArg(p,k)]=1;
				pushInstruction(mb,p);

				if (blockStart(p))
					copy++;
				if (blockExit(p)) {
					copy--;
					if ( copy == 0) break;
				}
			}
			// reset admin
			(void) memset((char*)assigned, 0, vlimit * sizeof (int));
			(void) memset((char*) used, 0, vlimit * sizeof(int));
			start = i+1;
		} else {
			for ( k = 0; k < p->retc; k++)
				init[getArg(p,k)]=1;
		} 
		// remember you assigned to variables
		for ( k = 0; k < p->retc; k++)
			setAssigned(p,k,assigned);
	}
	/* take the remainder as is */
	for (; i<slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        chkDeclarations(cntxt->fdout, mb);
    }
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","dataflow",actions,GDKusec() - usec);
    newComment(mb,buf);

wrapup:
	if( eolife) GDKfree(eolife);
	if( init) GDKfree(init);
	if( used) GDKfree(used);
	if( sink) GDKfree(sink);
	if( assigned) GDKfree(assigned);
	if( old) GDKfree(old);

	return actions;
}
