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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/
/*
 * The statemens are all checked for being eligible for dataflow.
 */
#include "monetdb_config.h"
#include "opt_dataflow.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"

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

/* optimizers may remove the dataflow hints first */
void removeDataflow(MalBlkPtr mb)
{
	int i, k, flowblock=0, limit;
	InstrPtr p, *old;
	int *init= GDKzalloc(mb->vtop * sizeof(int)), skip = 0;
	char *delete= (char*) GDKzalloc(mb->stop);
	char *used= (char*) GDKzalloc(mb->vtop);

	if ( delete == 0 || init == 0 || used == 0)
		return;
	old = mb->stmt;
	limit = mb->stop;
	if ( newMalBlkStmt(mb, mb->ssize) <0 )
		return;
	/* remove the inlined dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];

		if (blockStart(p) ){
			if ( getModuleId(p) == languageRef &&
				getFunctionId(p) == dataflowRef){
				flowblock = getArg(p,0);
				delete[i] = 1;
			} else skip++;
		} else 
		if (blockExit(p) ){
			if ( skip )
				skip--;
			else
			if ( getArg(p,0) == flowblock) {
				flowblock = 0;
				delete[i] = 1;
			}
		} else {
			/* remember first initialization */
			for ( k = p->retc; k < p->argc; k++)
				used[getArg(p,k)] = 1;
			if ( init[getArg(p,0)]  && ! used[getArg(p,0)]) 
				/* remove the old initialization */
				delete[ init[getArg(p,0)]] = 1;
			init[getArg(p,0)] = i;
		}
	}
	/* remove the superflous variable initializations */
	/* when there are no auxillary barrier blocks */
	for (i = 0; i<limit; i++) 
		if ( delete[i] == 0 )
			pushInstruction(mb,old[i]);
		else freeInstruction(old[i]);
	GDKfree(init);
	GDKfree(old);
	GDKfree(used);
	GDKfree(delete);
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
int
dataflowConflict(InstrPtr p) {
	if ( p->token == ENDsymbol || getFunctionId(p) == multiplexRef || blockCntrl(p) || blockStart(p) || blockExit(p))	
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

	/* don't use dataflow on single processor systems */
	if (GDKnr_threads <= 1)
		return 0;

	(void) stk;
	/* inlined functions will get their dataflow control later */
	if ( varGetProp(mb, getArg(getInstrPtr(mb,0),0),inlineProp)!= NULL) 
		return 0;
	OPTDEBUGdataflow{
		mnstr_printf(cntxt->fdout,"#dataflow input\n");
		printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
	}

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

	if ( newMalBlkStmt(mb, mb->ssize+mb->vtop) <0 )
		goto wrapup;
	
	pushInstruction(mb,old[0]);

	/* inject new dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];
		assert(p);
		conflict = 0;

		if ( dataflowConflict(p) || (conflict = dflowAssignConflict(p,i,assigned,eolife)) )  {
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
				varSetProperty(mb, getArg(q,0), "transparent",0,0);
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
wrapup:
	if( eolife) GDKfree(eolife);
	if( init) GDKfree(init);
	if( used) GDKfree(used);
	if( sink) GDKfree(sink);
	if( assigned) GDKfree(assigned);
	if( old) GDKfree(old);
	return actions;
}
