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


/* Insert the instruction immediately after a previous instruction that
 * generated an argument needed.
 * If non can be found, add it to the end.
 * Be aware of side-effect instructions, they may not be skipped.
 */
static void
insertInstruction(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int start)
{               
        int i,j,k;
        InstrPtr q;
		(void) stk;

        if( hasSideEffects(mb, p, FALSE)){
                pushInstruction(mb,p);
                return;
        }
        for(i = mb->stop -1; i>0 && i>start; i--)
        {
                q= getInstrPtr(mb, i);
                if( hasSideEffects(mb, q, FALSE))
                        break;
				for (j=0; j< q->retc; j++){
					for(k= p->retc; k <p->argc; k++){
						if( getArg(q,j) == getArg(p,k)){
							/* found a location to insert the new instruction */
							for(i++; i>0 && i< mb->stop; i++){
								q= getInstrPtr(mb,i);
								mb->stmt[i]= p;
								p = q;
							}
							pushInstruction(mb,p);
							return;
						}
					}
				}
        }
        pushInstruction(mb,p);
}

str
OPTreorderImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
    int i,j,k, maxphase = 1, phase=0, startblk=0;
    InstrPtr *old = NULL;
    int limit, slimit, *used = NULL;
    char buf[256];
    lng usec= GDKusec();
    str msg = MAL_SUCCEED;
    if( isOptimizerUsed(mb, "mitosis") <= 0){
        goto wrapup;
    }
    (void) cntxt;
    (void) stk;

    limit= mb->stop;
    slimit= mb->ssize;
    old = mb->stmt;

    used = (int*) GDKzalloc(mb->vtop * sizeof(int));
    if( used == NULL){
        throw(MAL,"optimizer.reorder", SQLSTATE(HY013) MAL_MALLOC_FAIL);
    }

    if ( newMalBlkStmt(mb, mb->ssize) < 0) {
        GDKfree(used);
        throw(MAL,"optimizer.reorder", SQLSTATE(HY013) MAL_MALLOC_FAIL);
    }

    /* Mark the parameters as constants as beloning to phase 1; */
    for( i =0; i< limit; i++){
        p = old[i];
        for( j= p->retc; j< p->argc; j++)
            if(i == 0 ||  isVarConstant(mb, getArg(p,j)))
                used[ getArg(p,j)] = maxphase;
    }
	pushInstruction(mb, old[0]);
	old[0] = 0;
    for( phase = 0; phase < maxphase; phase ++){
		startblk = mb->stop;
        for (i=1; i<limit; i++){
            p= old[i];
            if ( p == 0)
                continue;
            if( p->token == ENDsymbol)
                break;
            if( getModuleId(p) == sqlRef && getFunctionId(p) == tidRef && p->argc == 6){
                if (used[getArg(p,0)] == 0){
					k =  getVarConstant(mb, getArg(p, p->argc-2)).val.ival + 2;
					maxphase =  getVarConstant(mb, getArg(p, p->argc-1)).val.ival + 2;
					used[getArg(p,0)] = k;
					//mnstr_printf(cntxt->fdout, "tid : %d %d\n",  used[getArg(p,0)], maxphase);
					continue;
				}
            }
            if( getModuleId(p) == sqlRef && getFunctionId(p) == bindRef && p->argc == 8){
                if (used[getArg(p,0)] == 0){
					k =  getVarConstant(mb, getArg(p, p->argc-2)).val.ival + 2;
					maxphase =  getVarConstant(mb, getArg(p, p->argc-1)).val.ival + 2;
					used[getArg(p,0)] = k;
					//mnstr_printf(cntxt->fdout, "bind : %d %d\n",  used[getArg(p,0)], maxphase);
					continue;
                } 
            }
            /* locate instruction that does not have a blocking variable */
            k = 0;
            for(j= 0; j<p->argc; j++){
                if( used[getArg(p,j)] > k)
                    k = used[getArg(p,j)];
            }
            /* If this instruction belongs to phase */
            if ( k == phase){
                //mnstr_printf(cntxt->fdout, "inject %d pc %d max found: %d:",  phase, p->pc, k);
                //printInstruction(cntxt->fdout, mb, stk, p, LIST_MAL_DEBUG);
                //pushInstruction(mb,p);
                insertInstruction(mb, stk, p, startblk);
                old[i] = 0;
            }
            for( j = 0; j< p->retc; j++)
            if( used[getArg(p,j)] == 0){
                //mnstr_printf(cntxt->fdout, "var[%d]  : %d %d\n", i,  used[getArg(p,j)], k);
                used[getArg(p,j)] = k;
            }
        }
    }
   for(i=0; i<limit; i++)
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
wrapup:
    GDKfree(used);
    GDKfree(old);
    usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","reorder",1,usec);
    newComment(mb,buf);
    addtoMalBlkHistory(mb);
    return msg;
}

