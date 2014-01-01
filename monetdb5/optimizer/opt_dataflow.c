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
 * Copyright August 2008-2014 MonetDB B.V.
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
			simple = FALSE;
			for( j= q->retc; j < q->argc; j++)
				if( getArg(p,0) == getArg(q,j))
					simple= TRUE;
			if( !simple)
				return 0;
		}
		p = q;
	}
	return 1;
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

/* Updates are permitted if it is a unique update on 
 * a BAT created in the context of this block
 * As far as we know, no SQL nor MAL test re-uses the
 * target BAT to insert again and subsequently calls dataflow.
 * In MAL scripts, they still can occur.
*/

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
	InstrPtr r;
	
	(void) start;
	(void) last;
	(void) old;
	r = newInstruction(NULL,ASSIGNsymbol);
	getModuleId(r) = languageRef;
	getFunctionId(r) = passRef;
	getArg(r,0) = newTmpVariable(mb,getVarType(mb,var));
	r= pushArgument(mb,r, var);
	sink[top++] = r;
	return top;
}

static void
DFLOWinitvars(MalBlkPtr mb, InstrPtr p,int start, int last, Lifespan span, char *init)
{
	int k;
	for( k=0; k<p->retc; k++)
	if( getBeginLifespan(span,getArg(p,k)) >= start && getEndLifespan(span,getArg(p,k)) >= last && init[getArg(p,k)]==0){
		InstrPtr r= newAssignment(mb);
		getArg(r,0)= getArg(p,k);
		pushNil(mb,r,getArgType(mb,p,k));
		init[getArg(p,k)]=1;
	}
}

#define copyblock() \
	for( j=start ; j<i; j++) if (old[j]) pushInstruction(mb,old[j]);\
	for( j=0; j<top; j++) pushInstruction(mb,sink[j]);

#define exitblock() \
	if (!sf && entries>1){ \
		q= newAssignment(mb); \
		q->barrier= EXITsymbol; \
		getArg(q,0) = flowblock; \
	}\
	memset((char*) usage, 0, vlimit);\
	entries = flowblock = 0;\
	actions++;

/* dataflow blocks are transparent, because they are always
   executed, either sequentially or in parallell */
#define startblock()\
	q= newFcnCall(mb,languageRef,dataflowRef);\
	q->barrier= BARRIERsymbol;\
	getArg(q,0)= flowblock;\
	varSetProperty(mb, getArg(q,0), "transparent",0,0);

int
OPTdataflowImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k, var, cnt, start=1,entries=0, actions=0;
	int flowblock= 0, dumbcopy=0;
	InstrPtr *sink, *old, q;
	int limit, slimit, vlimit, top = 0;
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
	usage= (int*) GDKzalloc(vlimit = mb->vtop * sizeof(int));
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

	//removeDataflow(mb); To be done explicit by optimizers

	/* inject new dataflow barriers */
	for (i = 1; i<limit; i++) {
		p = old[i];

		if (p == NULL)
			continue;

		if (p->token == ENDsymbol)
			break;
		if (!dflowInstruction(p) || (!dumbcopy && blockExit(p)) || dflowAssignTest(span,p,i) ){
			/* close old flow block */
			if (flowblock){
				int sf = simpleFlow(old,start,i);
				top = 0;
				if (!sf && entries > 1){
					for( j=start ; j<i; j++)
					if (old[j]) {
						DFLOWinitvars(mb, old[j], start, i, span, init);
						/* collect variables garbage collected within the block */
						for( k=old[j]->retc; k<old[j]->argc; k++)
							if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]>=1 && isaBatType(getVarType(mb,var)))
								top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
							else
							if( getEndLifespan(span,getArg(old[j],k)) < i && isaBatType(getVarType(mb,var)))
								usage[getArg(old[j],k)]++;
						assert(top <mb->vsize);
					}
					startblock();
				}
				copyblock();
				exitblock();
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
							DFLOWinitvars(mb, old[j], start, i, span, init);
							/* collect variables garbagecollected in the block */
							for( k=old[j]->retc; k<old[j]->argc; k++)
								if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]>=1 && isaBatType(getVarType(mb,var)))
									top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
								else
								if( getEndLifespan(span,getArg(old[j],k)) < i && isaBatType(getVarType(mb,var)))
									usage[getArg(old[j],k)]++;
						}
						startblock();
					}
					/* inject the optional garbage sink statement */
					copyblock();
					exitblock();
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
				DFLOWinitvars(mb, old[j], start, i, span, init);
				for( k=old[j]->retc; k<old[j]->argc; k++)
					if( getEndLifespan(span, var = getArg(old[j],k)) == j && usage[var]>=1 && isaBatType(getVarType(mb,var)))
						top = dflowGarbagesink(mb,old, start, i, getArg(old[j],k), sink,top);
					else
					if( getEndLifespan(span,getArg(old[j],k)) < i && isaBatType(getVarType(mb,var)))
						usage[getArg(old[j],k)]++;
			}
			startblock();
		}
		copyblock();
		exitblock();
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
