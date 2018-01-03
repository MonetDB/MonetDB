/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* example: select count(distinct userid) from hits_10m;
 * Massage aggregate operations using value based partitioning
    (o,g,c):= group.groupdone(d);
    a:= aggr.subcount(o,o,g,false);

    (p1,p2,p3,p4):= partition.hash(d);
    (o1,g1,c1):= group.groupdone(p1);
    (o2,g2,c2):= group.groupdone(p2);
    (o3,g3,c3):= group.groupdone(p3);
    (o4,g4,c4):= group.groupdone(p4);
    o := mat.pack(o1,o2,o3,o4);
    g := mat.pack(g1,g2,g3,g4);
    c := mat.pack(c1,c2,c3,c4);
    a1:= aggr.subcount(o1,o1,g1,false);
    a2:= aggr.subcount(o2,o2,g2,false);
    a3:= aggr.subcount(o3,o3,g3,false);
    a4:= aggr.subcount(o4,o4,g4,false);
    m := mat.pack(a1,a2,a3,a4);

 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_partition.h"

//#define _PARTITION_DEBUG_

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')
#define MAXPIECES 128

str
OPTpartitionImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, j, slimit, limit, actions=0;
	InstrPtr qo, qg, qc, qh, q, *old = 0;
	lng usec = GDKusec();
	char buf[256];
	int vlimit;
       	int pieces = 2; // GDKnr_threads ? (GDKnr_threads > MAXPIECES? MAXPIECES: GDKnr_threads) : 1;
	int **vars; 

	(void) stk;
	(void) cntxt;

	limit = mb->stop;
	slimit = mb->ssize;
	vlimit = mb->vsize;
	/* check for aggregates */
	for ( i = 0; i < limit; i++){
		p= getInstrPtr(mb,i);
		if( getModuleId(p) == groupRef && ( getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef))
			actions ++;
	}
	if( actions == 0)
		goto finished;

	vars = (int **) GDKzalloc(sizeof(int*) * vlimit);
	if( vars == NULL)
		throw(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize + 32) < 0){
		GDKfree(vars);
		throw(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* construct the new plan */
	for ( i = 0; i < limit; i++)
	{
		p= old[i];
		if( getModuleId(p) == groupRef && ( getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef)){
#ifdef _PARTITION_DEBUG_
			fprintf(stderr,"#Found partition candidate %d\n", p->pc);
#endif
#ifdef _SINGLE_HASH_
			qh = newStmt(mb, partitionRef, hashRef);
			qh->retc =0;
			qh->argc =0;
			for( j = 0; j < pieces; j++)
				qh = pushReturn(mb, qh, newTmpVariable(mb, getArgType(mb, p, p->retc)));
			pushArgument(mb, qh, getArg(p,p->retc));
#else
			/* do parallel partition construction */
			qh = newInstruction(mb, languageRef, passRef);
			qh->retc = qh->argc = 0;
			for( j = 0; j < pieces; j++){
				q = newStmt(mb, partitionRef, hashRef);
				q = pushArgument(mb, q,  getArg(p,p->retc));
				q = pushInt(mb, q, j);
				q = pushInt(mb, q, pieces);
				qh = pushReturn(mb, qh, getArg(q,0));
			}
#endif
			qo = newInstruction(mb, matRef, packRef);
			getArg(qo,0) =  getArg(p,0);
			qg = newInstruction(mb, matRef, packRef);
			getArg(qg,0) = getArg(p,1);
			qc = newInstruction(mb, matRef, packRef);
			getArg(qc,0) = getArg(p,2);

			vars[getArg(p,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,1)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,2)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			for( j = 0; j < pieces; j++){
				q = newStmt(mb, groupRef, getFunctionId(p));
				vars[getArg(p,0)][j] = newTmpVariable(mb, getArgType(mb,p,0));
				vars[getArg(p,1)][j] = newTmpVariable(mb, getArgType(mb,p,1));
				vars[getArg(p,2)][j] = newTmpVariable(mb, getArgType(mb,p,2));
				getArg(q,0) = vars[getArg(p,0)][j];
				q = pushReturn(mb, q, vars[getArg(p,1)][j]);
				q = pushReturn(mb, q, vars[getArg(p,2)][j]);
				pushArgument(mb, q, getArg(qh, j));
				qo = pushArgument(mb, qo, getArg(q,0));
				qg = pushArgument(mb, qg, getArg(q,1));
				qc = pushArgument(mb, qc, getArg(q,2));
			}
			pushInstruction(mb, qo);
			pushInstruction(mb, qg);
			pushInstruction(mb, qc);
		} else
		if( getModuleId(p) == aggrRef && getFunctionId(p) == subcountRef && vars[getArg(p, p->retc)] != NULL){
			/* subcount over the individual groups */
			qo = newInstruction(mb, matRef, packRef);
			getArg(qo,0) =  getArg(p,0);
			vars[getArg(p,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			for( j = 0; j < pieces; j++){
				q = newStmt(mb, aggrRef, subcountRef);
				q = pushArgument(mb, q, vars[getArg(p,1)][j]);
				q = pushArgument(mb, q, vars[getArg(p,2)][j]);
				q = pushArgument(mb, q, vars[getArg(p,3)][j]);
				q = pushArgument(mb, q, getArg(p,p->argc-1));
				qo = pushArgument(mb, qo, getArg(q,0));
			}
			pushInstruction(mb, qo);
		} else
			pushInstruction(mb,p);
	}

	if( getFunctionId(qh) == passRef)
		freeInstruction(qh);
	for( ; i < slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
#ifdef _PARTITION_DEBUG_
	fprintFunction(stderr,mb, 0, LIST_MAL_ALL);
#endif

	/* Defense line against incorrect plans */
	if( actions ){
		chkTypes(cntxt->usermodule, mb, FALSE);
		chkFlow(mb);
		chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment and update statics */
	GDKfree(old);
	for( i = 0; i< vlimit; i++)
		if( vars[i])
			GDKfree(vars[i]);
	GDKfree(vars);
finished:
	usec= GDKusec() - usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec", "partition", actions, usec);
	newComment(mb,buf);
	addtoMalBlkHistory(mb);

	return MAL_SUCCEED;
}
