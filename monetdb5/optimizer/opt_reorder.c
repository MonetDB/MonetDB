/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * The dataflow reorder
 * MAL programs are largely logical descriptions of an execution plan.
 * After the mitosis and mergetable optimizers we have a large program, which when
 * executed as is, does not necessarily benefit from the locality
 * of data and operations. The problem is that the execution plan is
 * a DAG for which a topological order should be found that
 * minimizes the life time of variables and maximizes parallel execution.
 * This is an NP hard optimization problem. Therefore, we have
 * to rely on an affordable heuristic steps.
 *
 * The reorder optimizer transfers the breadth-first plans of
 * the mergetable into a depth-first traversal.
 * This increases cohesion for parallel execution.
 * It is performed by basic block that ends in a side-effect
 * bearing instruction.
 * Simply walking backward and pulling out subplans is then
 * the way to go. This step could be optimized somewhat
 * by giving preference to variables that are too far
 * away in the plan from their source. It is however not
 * explored.
 *
 * The secondary approach is to pull instructions to the
 * head of the plan if the dataflow such permits.
 * If you are not careful, you will end up with
 * the breadth first plan again.
 *
 * Beware, variables can be assigned a value multiple times.
 * The order this implies should be respected.
 *
 * Hidden dependencies occur als with e.g. sql.assert() calls.
 * Reordering them may easily lead to an assertion violation.
 * Therefore, reordering should be limited to basic blocks without
 * side-effects.
 */
#include "monetdb_config.h"
#include "opt_reorder.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
/*
 * Collect the statement dependencies in a table first
 * This can be done in linear time in size of the program.
 * Also check for barrier blocks. We only allow reordering
 * for a linear plan. Future extensions could consider
 * re-ordering basic blocks only.
 */
typedef struct{
	int cnt;
	int used;
	int pos,pos2;
	int stmt[FLEXIBLE_ARRAY_MEMBER];
} *Node, NodeRecord;

static void
OPTremoveDep(Node *list, int lim)
{
	int i;

	for (i=0; i< lim; i++)
		if (list[i])
			GDKfree(list[i]);
	GDKfree(list);
}

static Node *
OPTdependencies(Client cntxt, MalBlkPtr mb, int **Ulist)
{
	Node *list = (Node *) GDKzalloc(sizeof(Node) * mb->stop);
	int *var = (int*) GDKzalloc(sizeof(int) * mb->vtop), *uselist = NULL;
	int i,j,sz=0;
	InstrPtr p = NULL;
	int block = 0;
	
	(void) cntxt;

	if (list == NULL || var == NULL){
		if (list ) GDKfree(list);
		if (var) GDKfree(var);
		return NULL;
	}

	for ( i=0; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		block |= p->barrier != 0;
		list[i]= (Node) GDKzalloc(offsetof(NodeRecord, stmt) + sizeof(int) * p->argc);
		if (list[i] == NULL){
			OPTremoveDep(list, i);
			GDKfree(var);
			return 0;
		}
		list[i]->cnt = p->argc;
		for( j=p->retc; j<p->argc; j++) {
			list[i]->stmt[j] = var[getArg(p,j)];
			list[var[getArg(p,j)]]->used++;
		}
		/* keep the assignment order */
		for( j= 0; j < p->retc; j++) {
			if ( var[ getArg(p,j)] ) {
				//list[i]->stmt[j] = var [getArg(p,j)];
				// escape we should avoid reused variables.
				OPTremoveDep(list, i + 1);
				GDKfree(var);
				return 0;
			}
		}
		/* remember the last assignment */
		for( j=0; j<p->retc; j++)
			var[getArg(p,j)] = i;
	}
	/*
	 * 	mnstr_printf(cntxt->fdout,"DEPENDENCY TABLE\n");
	 * 	for(i=0;i<mb->stop; i++)
	 * 		if( list[i]->cnt){
	 * 			mnstr_printf(cntxt->fdout,"%s.%s [%d,%d]",
	 * 				mb->stmt[i]->modname,
	 * 				mb->stmt[i]->fcnname, i, list[i]->used);
	 * 			for(j=p->retc; j< list[i]->cnt; j++)
	 * 				mnstr_printf(cntxt->fdout, " %d", list[i]->stmt[j]);
	 * 			mnstr_printf(cntxt->fdout,"\n");
	 * 		}
	 */
	for(i=0;i<mb->stop; i++) {
		list[i]->pos = sz;
		list[i]->pos2 = sz;
		sz += list[i]->used;
	}
	if( sz == 0){
		OPTremoveDep(list, mb->stop);
		GDKfree(var);
		return NULL;
	}
	uselist = GDKzalloc(sizeof(int)*sz);
	if (!uselist) {
		OPTremoveDep(list, mb->stop);
		GDKfree(var);
		return NULL;
	}

	for(i=0;i<mb->stop; i++) {
		if (list[i]->cnt) {
			p= getInstrPtr(mb,i);
			for(j=p->retc; j< list[i]->cnt; j++) {
				uselist[list[list[i]->stmt[j]]->pos2] = i;
				list[list[i]->stmt[j]]->pos2++;
			}
		}
	}
	/*
	 * 	for(i=0, sz = 0; i<mb->stop; i++) {
	 * 		mnstr_printf(cntxt->fdout,"%d is used by", i);
	 * 		for(j=0; j<list[i]->used; j++, sz++)
	 * 			mnstr_printf(cntxt->fdout," %d", uselist[sz]);
	 * 		mnstr_printf(cntxt->fdout,"\n");
	 * 	}
	 */

	if ( block ){
		OPTremoveDep(list, mb->stop);
		GDKfree(uselist);
		GDKfree(var);
		return NULL;
	}
	GDKfree(var);
	*Ulist = uselist;
	return list;
}

static int
OPTbreadthfirst(Client cntxt, MalBlkPtr mb, int pc, int max, InstrPtr old[], Node dep[], int *uselist)
{
	int i;
	InstrPtr p;

	if (pc > max)
		return 0;

	p = old[pc];
	if (p == NULL)
		return 0;

	if (THRhighwater())
		return -1;

	for (i= p->retc; i< dep[pc]->cnt; i++)
		if (OPTbreadthfirst(cntxt, mb, dep[pc]->stmt[i], max, old, dep, uselist) < 0)
			return -1;
	if (old[pc] != NULL) {
		old[pc] = 0;
		pushInstruction(mb, p);
	}
	if (getModuleId(p) == groupRef)
		for (i = 0; i< dep[pc]->used; i++)
			if (OPTbreadthfirst(cntxt, mb, uselist[dep[pc]->pos+i], max, old, dep, uselist) < 0)
				return -1;
	return 0;
}

/* SQL appends are collected to create a better dataflow block */
/* alternatively, we should postpone all mcv-chained actions */
static int
OPTpostponeAppends(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j,k=0, actions =0, last=-1;
	InstrPtr *old, *appends;
	int limit;
	(void) cntxt;
	(void) stk;
	(void) p;

	appends =(InstrPtr*) GDKzalloc(mb->ssize * sizeof(InstrPtr));
	if( appends == NULL)
		return 0;
	limit= mb->stop;
	old = mb->stmt;
	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(appends);
		return 0;
	}
	for( i=0; i<limit; i++){
		if ( getModuleId(old[i]) == sqlRef && getFunctionId(old[i]) == appendRef){
			last = i;
		}
	}
	for( i=0; i<limit; i++){
		if ( getModuleId(old[i]) == sqlRef && getFunctionId(old[i]) == appendRef){
			// only postpone under strict conditions
			assert( isVarConstant(mb,getArg(old[i],2)));
			assert( isVarConstant(mb,getArg(old[i],3)));
			assert( isVarConstant(mb,getArg(old[i],4)));
			if( actions )
				pushInstruction(mb, old[i]);
			else {
				if (k > 0 &&  getArg(old[i],1) == getArg(appends[k-1],0))
					appends[k++]= old[i];
				else {
					for(j=0; j<k; j++)
						pushInstruction(mb,appends[j]);
					pushInstruction(mb, old[i]);
					actions++;
				}
			}
			continue;
		}
		if ( i == last){
			actions++;
			for(j=0; j<k; j++)
				pushInstruction(mb,appends[j]);
		}
		pushInstruction(mb,old[i]);
	}
	for( ; i<limit; i++){
		pushInstruction(mb,old[i]);
	}
	GDKfree(appends);
	GDKfree(old);
	return actions;
}

str
OPTreorderImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,j, start;
	InstrPtr *old;
	int limit, slimit, *uselist = NULL;
	Node *dep;
	char buf[256];
	lng usec= GDKusec();
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) stk;
	dep = OPTdependencies(cntxt,mb,&uselist);
	if ( dep == NULL)
		return MAL_SUCCEED;
	limit= mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;
	if ( newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(uselist);
		OPTremoveDep(dep, limit);
		throw(MAL,"optimizer.reorder", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	
	pushInstruction(mb,old[0]);
	old[0]=0;

	start=1;
	for (i=1; i<limit; i++){
		p= old[i];
		if ( p == 0)
			continue;
		if( p->token == ENDsymbol)
			break;
		if( hasSideEffects(mb, p,FALSE) || isUnsafeFunction(p) || p->barrier ){
			if (OPTbreadthfirst(cntxt, mb, i, i, old, dep, uselist) < 0)
				break;
			/* remove last instruction and keep for later */
			if (p == mb->stmt[mb->stop-1]) {
				p= mb->stmt[mb->stop-1];
				mb->stmt[mb->stop-1]=0;
				mb->stop--;
			} else {
				p = 0;
			}
			/* collect all seen sofar by backward grouping */
			/* since p has side-effects, we should secure all seen sofar */
			for(j=i-1; j>=start;j--) {
#ifdef DEBUG_OPT_REORDER
				if( old[j]){
					fprintf(stderr,"leftover: %d",start+1);
					fprintInstruction(stderr,mb,0,old[j],LIST_MAL_DEBUG);
				}
#endif
				if (OPTbreadthfirst(cntxt, mb, j, i, old, dep, uselist) < 0) {
					i = limit;	/* cause break from outer loop */
					break;
				}
			}
			if (p)
				pushInstruction(mb,p);
			start = i+1;
		}
	}
	for(i=0; i<limit; i++)
		if (old[i])
			pushInstruction(mb,old[i]);
	for(; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	OPTremoveDep(dep, limit);
	GDKfree(uselist);
	GDKfree(old);
	(void) OPTpostponeAppends(cntxt, mb, 0, 0);

    /* Defense line against incorrect plans */
    if( 1){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","reorder",1,usec);
    newComment(mb,buf);
	addtoMalBlkHistory(mb);

	return msg;
}
