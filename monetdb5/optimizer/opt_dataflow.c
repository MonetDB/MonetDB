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
 * Copyright August 2008-2012 MonetDB B.V.
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
	int i, j, simple = TRUE;
	InstrPtr p = NULL, q;

	/* skip simple first */
	for( ; simple && start < last; start++)  {
		p= old[start];
		simple = getModuleId(p) == calcRef || getModuleId(p) == mtimeRef || getModuleId(p) == strRef || getModuleId(p)== mmathRef;
	}
	for( i = start; i < last; i++) 
	if ( old[i]) {
		q= old[i];
		simple = getModuleId(q) == calcRef || getModuleId(q) == mtimeRef || getModuleId(q) == strRef || getModuleId(q)== mmathRef;
		if( !simple)  {
			simple = FALSE;
			for( j= q->retc; j < q->argc; j++)
				if( getArg(p,0) == getArg(q,j))
					simple= TRUE;
			if( !simple)
				return 0;
			p = q;
		}
	}
	return 1;
}

void removeDataflow(InstrPtr *old, int limit)
{
	int i, flowblock=0;
	InstrPtr p;
	/* remove the inlined dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];

		if (!flowblock && blockStart(p) && 
		    getModuleId(p) == languageRef &&
		    getFunctionId(p) == dataflowRef){
			flowblock = getArg(p,0);
			freeInstruction(p);
			old[i] = NULL;
		} else if (flowblock && blockExit(p) && getArg(p,0) == flowblock) {
			flowblock = 0;
			freeInstruction(p);
			old[i] = NULL;
		}
	}
}

static int
dflowAssignTest(Lifespan span, InstrPtr p, int i)
{
	int j;
	/* flow blocks should be closed (and not opened) when we reach a point
	   where a variable is assigned that is not the last
	*/
	for(j=0; j<p->retc; j++)
		if (getLastUpdate(span, getArg(p,j)) != i)
			return 1;
	return 0;
}

static int
dflowUpdateTest(Lifespan span, InstrPtr p, int i)
{
	/* Updates are permitted if it is a unique update on 
	 * a BAT created in the context of this block
	 * As far as we know, no SQL nor MAL test re-uses the
	 * target BAT to insert again and subsequently calls dataflow.
	 * In MAL scripts, they still can occur.
	*/
	(void) span;
	(void) i;
	if ( getModuleId(p) == batRef  &&
	   (getFunctionId(p) == insertRef ||
		getFunctionId(p) == inplaceRef ||
		getFunctionId(p) == appendRef ||
		getFunctionId(p) == updateRef ||
		getFunctionId(p) == replaceRef ||
		getFunctionId(p) == deleteRef ) )
			return FALSE;/* always */
	return FALSE;
}

/* a limited set of MAL instructions may appear in the dataflow block*/
static int
dflowInstruction(InstrPtr p) {
	switch(p->token){
	case ASSIGNsymbol:
	case PATcall:
	case CMDcall:
	case FACcall:
	case FCNcall:
		return ! (	hasSideEffects(p,FALSE) || isUnsafeFunction(p) || blockCntrl(p) );
	}
	return FALSE;
}

static int
dflowGarbagesink(MalBlkPtr mb, InstrPtr *old, int start, int last, int var, InstrPtr *sink, int top){
	InstrPtr p, q, r;
	int j,k;
	
	q= newInstruction(NULL,ASSIGNsymbol); 
	getModuleId(q) = languageRef;
	getFunctionId(q) = sinkRef;
	getArg(q,0)= newTmpVariable(mb,TYPE_void);
	q= pushArgument(mb, q, var);
	for ( j= start; j< last; j++){
		assert(top <mb->vsize);
		p = old[j];
		if ( p )
		for (k = p->retc; k< p->argc; k++)
			if ( getArg(p,k)== var) {
				r = newInstruction(NULL,ASSIGNsymbol);
				getModuleId(r) = languageRef;
				getFunctionId(r) = passRef;
				getArg(r,0) = newTmpVariable(mb,getArgType(mb,p,0));
				r= pushArgument(mb,r, getArg(p,0));
				sink[top++] = r;
				q= pushArgument(mb,q, getArg(r,0));
				break;
		}
	}
	assert(top <mb->vsize);
	sink[top++] = q;
	return top;
}

int
OPTdataflowImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, var, cnt, start=1,entries=0, actions=0;
	int flowblock= 0, dumbcopy=0;
	InstrPtr *sink, *old, q;
	int limit, slimit, top = 0;
	Lifespan span;
	char *init;
	int *usage;

	/* don't use dataflow on single processor systems */
	if (GDKnr_threads <= 1)
		return 0;

	(void) cntxt;
	(void) stk;
	/* inlined functions will get their dataflow control later */
	if ( varGetProp(mb, getArg(getInstrPtr(mb,0),0),inlineProp)!= NULL) 
		return 0;
	span= setLifespan(mb);
	if( span == NULL)
		return 0;
	init= (char*) GDKzalloc(mb->vtop);
	if ( init == NULL){
		GDKfree(span);
		return 0;
	}
	usage= (int*) GDKzalloc(mb->vtop * sizeof(int));
	if ( usage == NULL){
		GDKfree(span);
		GDKfree(init);
		return 0;
	}
	sink= (InstrPtr*) GDKzalloc(mb->vsize * sizeof(InstrPtr));
	if ( sink == NULL){
		GDKfree(span);
		GDKfree(init);
		GDKfree(usage);
		return 0;
	}

	limit= mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;
	if ( newMalBlkStmt(mb, mb->ssize+mb->vtop) <0 ){
		GDKfree(span);
		GDKfree(init);
		GDKfree(usage);
		GDKfree(sink);
		return 0;
	}
	pushInstruction(mb,old[0]);

	removeDataflow(old,limit);

	/* inject new dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];

		if (p == NULL)
			continue;

		if (p->token == ENDsymbol)
			break;
		if (!dflowInstruction(p) || (!dumbcopy && blockExit(p)) || dflowAssignTest(span,p,i) || dflowUpdateTest(span,p,i)){
			/* close old flow block */
			if (flowblock){
				int sf = simpleFlow(old,start,i);
				top = 0;
				if (!sf && entries > 1){
					for( j=start ; j<i; j++)
					if (old[j]) {
						for( k=0; k<old[j]->retc; k++)
						if( getBeginLifespan(span,getArg(old[j],k)) >= start && getEndLifespan(span,getArg(old[j],k)) >= i && init[getArg(old[j],k)]==0){
							InstrPtr r= newAssignment(mb);
							getArg(r,0)= getArg(old[j],k);
							pushNil(mb,r,getArgType(mb,old[j],k));
							init[getArg(old[j],k)]=1;
						}
						/* collect variables garbage collected within the block */
						for( k=old[j]->retc; k<old[j]->argc; k++)
							if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]==1 && !isVarConstant(mb, var) )
								top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
							else
							if( getEndLifespan(span,getArg(old[j],k)) < i && !isVarConstant(mb, var) )
								usage[getArg(old[j],k)]++;
						assert(top <mb->vsize);
					}
					q= newFcnCall(mb,languageRef,dataflowRef);
					q->barrier= BARRIERsymbol;
					getArg(q,0)= flowblock;
					/* dataflow blocks are transparent, because they are always
					   executed, either sequentially or in parallell */
					varSetProperty(mb, getArg(q,0), "transparent",0,0);
				}
				for( j=start ; j<i; j++)
					if (old[j])
						pushInstruction(mb,old[j]);
				for( j=0; j<top; j++)
						pushInstruction(mb,sink[j]);
				if (!sf && entries>1){
					q= newAssignment(mb);
					q->barrier= EXITsymbol;
					getArg(q,0) = flowblock;
				}
				/* inject the optional garbage sink statement */
				entries = 0;
				flowblock = 0;
				actions++;
			}
			pushInstruction(mb,p);
			continue;
		}

		if (blockStart(p)){
			dumbcopy++;
			if (dumbcopy == 1)
				/* close old flow block */
				if (flowblock){
					int sf = simpleFlow(old,start,i);
					top = 0;
					if (!sf && entries > 1){
						for( j=start ; j<i; j++)
						if (old[j]) {
							for( k=0; k<old[j]->retc; k++)
							if( getBeginLifespan(span,getArg(old[j],k)) >= start && getEndLifespan(span,getArg(old[j],k)) >= i && init[getArg(old[j],k)]==0){
								InstrPtr r= newAssignment(mb);
								getArg(r,0)= getArg(old[j],k);
								pushNil(mb,r,getArgType(mb,old[j],k));
								init[getArg(old[j],k)]=1;
							}
							/* collect variables garbagecollected in the block */
							for( k=old[j]->retc; k<old[j]->argc; k++)
							if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]==1 && !isVarConstant(mb, var) )
								top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
							else
							if( getEndLifespan(span,getArg(old[j],k)) < i && !isVarConstant(mb, var) )
								usage[getArg(old[j],k)]++;
						}
						q= newFcnCall(mb,languageRef,dataflowRef);
						q->barrier= BARRIERsymbol;
						getArg(q,0)= flowblock;
						/* dataflow blocks are transparent, because they are always
						   executed, either sequentially or in parallell */
						varSetProperty(mb, getArg(q,0), "transparent",0,0);
					}
					for( j=start ; j<i; j++)
						if (old[j])
							pushInstruction(mb,old[j]);
					assert(top <mb->vsize);
					/* inject the optional garbage sink statement */
					for( j=0; j<top; j++)
							pushInstruction(mb,sink[j]);
					if (!sf && entries>1){
						q= newAssignment(mb);
						q->barrier= EXITsymbol;
						getArg(q,0) = flowblock;
					}
					entries = 0;
					flowblock = 0;
					actions++;
				}
		}
		if (blockExit(p)) {
			assert(flowblock == 0);
			dumbcopy--;
			pushInstruction(mb,p);
			continue;
		}
		if (dumbcopy) {
			assert(flowblock == 0);
			pushInstruction(mb,p);
			continue;
		}
		if (flowblock == 0){
			flowblock = newTmpVariable(mb,TYPE_bit);
			entries = 0;
			start = i;
		}
		/* check if the instruction can start a flow */
		/* this should be a function call with multiple arguments */
		cnt = 0;
		if (getFunctionId(p))
			for(j=p->retc; j<p->argc; j++) 
				if ( isVarConstant(mb, getArg(p,j)) || getLastUpdate(span, getArg(p,j)) <= start)
					cnt++;
		if (cnt && dflowAssignTest(span,p,i))
			cnt = 0;

		if (cnt && cnt == p->argc-p->retc)
			entries++;
	}
	/* close old flow block */
	if (flowblock){
		int sf = simpleFlow(old,start,i);
		top = 0;
		if (!sf && entries > 1){
			for( j=start ; j<i; j++)
			if (old[j]) {
				for( k=0; k<old[j]->retc; k++)
				if( getBeginLifespan(span,getArg(old[j],k)) > start && getEndLifespan(span,getArg(old[j],k)) >= i && init[getArg(old[j],k)]==0){
					InstrPtr r= newAssignment(mb);
					getArg(r,0)= getArg(old[j],k);
					pushNil(mb,r,getArgType(mb,old[j],k));
					init[getArg(old[j],k)]=1;
				}
				for( k=old[j]->retc; k<old[j]->argc; k++)
				if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]==1 && !isVarConstant(mb, var) )
					top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
				else
				if( getEndLifespan(span,getArg(old[j],k)) < i && !isVarConstant(mb, var) )
					usage[getArg(old[j],k)]++;
			}
			q= newFcnCall(mb,languageRef,dataflowRef);
			q->barrier= BARRIERsymbol;
			getArg(q,0)= flowblock;
			/* dataflow blocks are transparent, because they are always
			   executed, either sequentially or in parallell */
			varSetProperty(mb, getArg(q,0), "transparent",0,0);
		}
		for( j=start ; j<i; j++)
			if (old[j])
				pushInstruction(mb,old[j]);
		assert(top <mb->vsize);
		/* inject the optional garbage sink statement */
		for( j=0; j<top; j++)
				pushInstruction(mb,sink[j]);
		if (!sf && entries>1){
			q= newAssignment(mb);
			q->barrier= EXITsymbol;
			getArg(q,0) = flowblock;
		}
		entries = 0;
		flowblock = 0;
		actions++;
	}
	/* take the remainder as is */
	for (; i<limit; i++) 
		if (old[i])
			pushInstruction(mb,old[i]);
	for (; i<slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	GDKfree(span);
	GDKfree(init);
	GDKfree(sink);
	GDKfree(usage);
	return actions;
}
