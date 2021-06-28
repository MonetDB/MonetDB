/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
 * the mergetable into a multi-phase execution plan.
 * This increases cohesion for parallel execution.
 * A subquery is processed completely as quickly as possible.
 * Only when the subquery is stalled for available input, the
 * threads may start working on another subquery.
 *
 */
#include "monetdb_config.h"
#include "opt_reorder.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "opt_mitosis.h"


/* Insert the instruction immediately after a previous instruction that
 * generated an argument needed.
 * If non can be found, add it to the end.
 * Be aware of side-effect instructions, they may not be skipped.
 */
str
OPTreorderImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
    int i,j,k, blkcnt = 1, pc = 0;
    InstrPtr *old = NULL;
    int limit, slimit, *depth = NULL;
    char buf[256];
    lng usec= GDKusec();
    str msg = MAL_SUCCEED;
	InstrPtr *blocks[MAXSLICES] ={0};
	int top[MAXSLICES] ={0};

	(void) blocks;
	(void) top;
	for(i=0; i< MAXSLICES; i++) top[i] = 0;
    if( isOptimizerUsed(mb, "mitosis") <= 0){
        goto wrapup;
    }
    (void) cntxt;
    (void) stk;

    limit= mb->stop;
    slimit= mb->ssize;
    old = mb->stmt;

    depth = (int*) GDKzalloc(mb->vtop * sizeof(int));
    if( depth == NULL){
        throw(MAL,"optimizer.reorder", SQLSTATE(HY013) MAL_MALLOC_FAIL);
    }

    if ( newMalBlkStmt(mb, mb->ssize) < 0) {
        GDKfree(depth);
        throw(MAL,"optimizer.reorder", SQLSTATE(HY013) MAL_MALLOC_FAIL);
    }

    /* Mark the parameters as constants as beloning to depth 0; */
    for( i =0; i< limit; i++){
        p = old[i];
		if( !p) {
			//mnstr_printf(cntxt->fdout, "empty stmt:pc %d \n", i);
			continue;
		}
		if( p->token == ENDsymbol)
			break;
		k = 0;
		if( getModuleId(p) == sqlRef && getFunctionId(p) == tidRef && p->argc == 6){
			if (depth[getArg(p,0)] == 0){
				k =  getVarConstant(mb, getArg(p, p->argc-2)).val.ival;
				assert( k < MAXSLICES);
				depth[getArg(p,0)] = k;
			}
		} else
		if( getModuleId(p) == sqlRef && getFunctionId(p) == bindRef && p->argc == 8){
			if (depth[getArg(p,0)] == 0){
				k =  getVarConstant(mb, getArg(p, p->argc-2)).val.ival;
				assert( k < MAXSLICES);
				depth[getArg(p,0)] = k;
			} 
		} else{
			for(j= p->retc; j <p->argc; j++){
				if (depth[getArg(p,j)] > k)
					k = depth[getArg(p,j)];
			}
			for(j=0; j< p->retc; j++)
				if( depth[getArg(p,j)] == 0)
					depth[getArg(p,j)] = k;
		}

		if( top[k] == 0){
			blocks[k] = GDKzalloc(limit * sizeof(InstrPtr));
			if( blocks[k] == NULL){
				for(i=0; i< blkcnt; i++)
					if( top[i])
						GDKfree(blocks[i]);
				GDKfree(depth);
				GDKfree(old);
				throw(MAL,"optimizer.reorder", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		blocks[k][top[k]] = p;
		top[k]= top[k] +1;
		//mnstr_printf(cntxt->fdout, "block[%d] :%d:",i, k);
		//printInstruction(cntxt->fdout, mb, stk, p, LIST_MAL_DEBUG);
		if( k > blkcnt)
			blkcnt = k;
    }

	for(k =0; k <= blkcnt; k++)
		for(j=0; j < top[k]; j++){
			p =  blocks[k][j];
			p->pc = pc++;
			pushInstruction(mb, p);
		}

    for(; i<limit; i++)
        if (old[i])
            pushInstruction(mb,old[i]);
    for(; i<slimit; i++)
        if (old[i])
            freeInstruction(old[i]);

    /* Defense line against incorrect plans */
    msg = chkTypes(cntxt->usermodule, mb, FALSE);
    if (!msg)
            msg = chkFlow(mb);
    if (!msg)
            msg = chkDeclarations(mb);
        /* keep all actions taken as a post block comment */
	//mnstr_printf(cntxt->fdout,"REORDER RESULT ");
	//printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
wrapup:
	for(i=0; i< blkcnt; i++)
		if( top[i])
			GDKfree(blocks[i]);
    GDKfree(depth);
    GDKfree(old);
    usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","reorder",1,usec);
    newComment(mb,buf);
    addtoMalBlkHistory(mb);
    return msg;
}

