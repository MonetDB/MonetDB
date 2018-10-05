/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * Dataflow processing incurs overhead and is only
 * relevant if multiple tasks kan be handled at the same time.
 * Also simple expressions dont have to be executed in parallel.
 *
 * The garbagesink contains variables whose endoflife is within
 * a dataflow block and who are used concurrently. 
 * They are garbage collected at the end of the parallel block.
 *
 * The dataflow analysis centers around the read/write use patterns of
 * the variables and the occurrence of side-effect bearing functions.
 * Any such function should break the dataflow block as it may rely
 * on the sequential order in the plan.
 *
 * The following state properties can be distinguished for all variables:
 * VARWRITE  - variable assigned a value in the dataflow block
 * VARREAD   - variable is used in an argument
 * VAR2READ  - variable is read in concurrent mode
 * VARBLOCK  - variable next use terminate the // block, set after encountering an update
 *
 * Only some combinations are allowed.
 */

#define VARFREE  0
#define VARWRITE 1
#define VARREAD  2
#define VARBLOCK 4
#define VAR2READ 8

typedef char *States;

#define setState(S,P,K,F)  ( assert(getArg(P,K) < vlimit), (S)[getArg(P,K)] |= F)
#define getState(S,P,K)  ((S)[getArg(P,K)])

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

/* Updates are permitted if it is a unique update on 
 * a BAT created in the context of this block
 * As far as we know, no SQL nor MAL test re-uses the
 * target BAT to insert again and subsequently calls dataflow.
 * In MAL scripts, they still can occur.
*/

/* a limited set of MAL instructions may appear in the dataflow block*/
static int
dataflowBreakpoint(Client cntxt, MalBlkPtr mb, InstrPtr p, States states)
{
	int j;

	if (p->token == ENDsymbol || p->barrier || isUnsafeFunction(p) || 
		(isMultiplex(p) && MANIFOLDtypecheck(cntxt,mb,p,0) == NULL) ){
#ifdef DEBUG_OPT_DATAFLOW
			fprintf(stderr,"#breakpoint on instruction\n");
#endif
			return TRUE;
		}

	/* flow blocks should be closed when we reach a point
	   where a variable is assigned  more then once or already
	   being read.
	*/
	for(j=0; j<p->retc; j++)
		if ( getState(states,p,j) & (VARWRITE | VARREAD | VARBLOCK)){
#ifdef DEBUG_OPT_DATAFLOW
			fprintf(stderr,"#breakpoint on argument %s state %d\n", getVarName(mb,getArg(p,j)), getState(states,p,j));
#endif
			return 1;
		}

	/* update instructions can be updated if the target variable
	 * has not been read in the block so far */
	if ( isUpdateInstruction(p) ){
		/* the SQL update functions change BATs that are not
		 * explicitly mentioned as arguments (and certainly not as the
		 * first argument), but that can still be available to the MAL
		 * program (see bugs.monetdb.org/6641) */
		if (getModuleId(p) == sqlRef)
			return 1;
#ifdef DEBUG_OPT_DATAFLOW
		if( getState(states,p,1) & (VARREAD | VARBLOCK))
			fprintf(stderr,"#breakpoint on update %s state %d\n", getVarName(mb,getArg(p,j)), getState(states,p,j));
#endif
		return getState(states,p,p->retc) & (VARREAD | VARBLOCK);
	}

	for(j=p->retc; j < p->argc; j++)
		if ( getState(states,p,j) & VARBLOCK){
#ifdef DEBUG_OPT_DATAFLOW
			if( getState(states,p,j) & VARREAD)
				fprintf(stderr,"#breakpoint on blocked var %s state %d\n", getVarName(mb,getArg(p,j)), getState(states,p,j));
#endif
			return 1;
		}
#ifdef DEBUG_OPT_DATAFLOW
	if( hasSideEffects(mb,p,FALSE))
		fprintf(stderr,"#breakpoint on sideeffect var %s %s.%s\n", getVarName(mb,getArg(p,j)), getModuleId(p), getFunctionId(p));
#endif
	return hasSideEffects(mb,p,FALSE);
}

/* Collect the BATs that are used concurrently to ensure that
 * there is a single point where they can be released
 */
static int
dflowGarbagesink(Client cntxt, MalBlkPtr mb, int var, InstrPtr *sink, int top)
{
	InstrPtr r;
	int i;
	for( i =0; i<top; i++)
		if( getArg(sink[i],1) == var)
			return top;
	(void) cntxt;
	
	r = newInstruction(NULL,languageRef, passRef);
	getArg(r,0) = newTmpVariable(mb,TYPE_void);
	r= pushArgument(mb,r, var);
	sink[top++] = r;
	return top;
}

/* dataflow blocks are transparent, because they are always
   executed, either sequentially or in parallel */

str
OPTdataflowImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, start=1, slimit, breakpoint, actions=0, simple = TRUE;
	int flowblock= 0;
	InstrPtr *sink = NULL, *old = NULL, q;
	int limit, vlimit, top = 0;
	States states;
	char  buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	/* don't use dataflow on single processor systems */
	if (GDKnr_threads <= 1)
		return MAL_SUCCEED;

	if ( optimizerIsApplied(mb,"dataflow"))
		return MAL_SUCCEED;
	(void) stk;
	/* inlined functions will get their dataflow control later */
	if ( mb->inlineProp)
		return MAL_SUCCEED;

#ifdef DEBUG_OPT_DATAFLOW
		fprintf(stderr,"#dataflow input\n");
		fprintFunction(stderr, mb, 0, LIST_MAL_ALL);
#endif

	vlimit = mb->vsize;
	states = (States) GDKzalloc(vlimit * sizeof(char));
	sink = (InstrPtr *) GDKzalloc(mb->stop * sizeof(InstrPtr));
	if (states == NULL || sink == NULL){
		msg= createException(MAL,"optimizer.dataflow", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto wrapup;
	}
	
	setVariableScope(mb);

	limit= mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		msg= createException(MAL,"optimizer.dataflow", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		actions = -1;
		goto wrapup;
	}
	pushInstruction(mb,old[0]);

	/* inject new dataflow barriers using a single pass through the program */
	for (i = 1; i<limit; i++) {
		p = old[i];
		assert(p);
		breakpoint = dataflowBreakpoint(cntxt, mb, p ,states);
		if ( breakpoint ){
			/* close previous flow block */
			simple = simpleFlow(old,start,i);
#ifdef DEBUG_OPT_DATAFLOW
			fprintf(stderr,"#breakpoint pc %d  %s\n",i, (simple?"simple":"") );
#endif
			if ( !simple){
				flowblock = newTmpVariable(mb,TYPE_bit);
				q= newFcnCall(mb,languageRef,dataflowRef);
				q->barrier= BARRIERsymbol;
				getArg(q,0)= flowblock;
				actions++;
			}
			// copyblock the collected statements 
			for( j=start ; j<i; j++) {
				q= old[j];
				pushInstruction(mb,q);
				// collect BAT variables garbage collected within the block 
				if( !simple)
					for( k=q->retc; k<q->argc; k++){
						if (getState(states,q,k) & VAR2READ &&  getEndScope(mb,getArg(q,k)) == j && isaBatType(getVarType(mb,getArg(q,k))) )
								top = dflowGarbagesink(cntxt, mb, getArg(q,k), sink, top);
					}
			}
			/* exit parallel block */
			if ( ! simple){ 
				// force the pending final garbage statements
				for( j=0; j<top; j++) 
					pushInstruction(mb,sink[j]);
				q= newAssignment(mb); 
				q->barrier= EXITsymbol; 
				getArg(q,0) = flowblock; 
			}
			if (p->token == ENDsymbol){
				for(; i < limit; i++)
					if( old[i])
						pushInstruction(mb,old[i]);
				break;
			}
			// implicitly a new flow block starts unless we have a hard side-effect
			memset((char*) states, 0, vlimit * sizeof(char));
			top = 0;
			if ( p->token == ENDsymbol  || (hasSideEffects(mb,p,FALSE) && !blockStart(p)) || isMultiplex(p)){
				start = i+1;
				pushInstruction(mb,p);
				continue;
			}
			start = i;
		}

		if (blockStart(p)){
			/* barrier blocks are kept out of the dataflow */
			/* assumes that barrier entry/exit pairs are correct. */
			/* A refinement is parallelize within a barrier block */
			int copy= 1;
			pushInstruction(mb,p);
			for ( i++; i<limit; i++) {
				p = old[i];
				pushInstruction(mb,p);

				if (blockStart(p))
					copy++;
				if (blockExit(p)) {
					copy--;
					if ( copy == 0) break;
				}
			}
			// reset admin
			start = i+1;
		} 
		// remember you assigned/read variables
		for ( k = 0; k < p->retc; k++)
			setState(states, p, k, VARWRITE);
		if( isUpdateInstruction(p) && (getState(states,p,1) == 0 || getState(states,p,1) & VARWRITE))
			setState(states, p,1, VARBLOCK);
		for ( k = p->retc; k< p->argc; k++)
		if( !isVarConstant(mb,getArg(p,k)) ){
			if( getState(states, p, k) & VARREAD)
				setState(states, p, k, VAR2READ);
			else
			if( getState(states, p, k) & VARWRITE)
				setState(states, p ,k, VARREAD);
		}
#ifdef DEBUG_OPT_DATAFLOW
		fprintf(stderr,"# variable states\n");
		fprintInstruction(stderr,mb, 0, p , LIST_MAL_ALL);
		for(k = 0; k < p->argc; k++)
			fprintf(stderr,"#%s %d\n", getVarName(mb,getArg(p,k)), states[getArg(p,k)] );
#endif
	}
	/* take the remainder as is */
	for (; i<slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
#ifdef DEBUG_OPT_DATAFLOW
		fprintf(stderr,"#dataflow output %s\n", mb->errors?"ERROR":"");
		fprintFunction(stderr, mb, 0, LIST_MAL_ALL);
#endif
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","dataflow",actions,usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

wrapup:
	if(states) GDKfree(states);
	if(sink)   GDKfree(sink);
	if(old)    GDKfree(old);

	return msg;
}
