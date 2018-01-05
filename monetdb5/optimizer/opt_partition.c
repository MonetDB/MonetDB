/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#define _PARTITION_DEBUG_

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')
#define MAXPIECES 128

str
OPTpartitionImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, j, slimit, limit, actions=0, hashed = 0;
	InstrPtr qo, qg, qc, q, *old = 0;
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
		if( getModuleId(p) == groupRef && ( getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef || getFunctionId(p) == subgroupdoneRef))
			actions ++;
	}
	if( actions == 0)
		goto finished;

	vars = (int **) GDKzalloc(sizeof(int*) * vlimit);
	if( vars == NULL)
		throw(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize + 5 * actions) < 0){
		GDKfree(vars);
		throw(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* construct the new plan */
	for ( i = 0; i < limit; i++)
	{	int part;
		p= old[i];
		if( getModuleId(p) == groupRef && ( getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef)){
#ifdef _PARTITION_DEBUG_
			fprintf(stderr,"#Found partition candidate group/groupdone %d and var %d\n", p->pc, p->argv[p->retc]);
#endif
			part = getArg(p, p->retc);
			if( vars[part] == NULL){
				vars[part]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
				hashed = 0;
			} else hashed = 1;
			if( hashed ==0){
#ifdef _SINGLE_HASH_
				q = newStmt(mb, partitionRef, hashRef);
				q->retc =0;
				q->argc =0;
				for( j = 0; j < pieces; j++){
					int k = newTmpVariable(mb, getArgType(mb, p, part));
					q = pushReturn(mb, q, k);
					vars[part][j] = k;
				}
				pushArgument(mb, q, part);
#else
				/* do parallel partition construction on all keys */
				for( j = 0; j < pieces; j++){
					q = newStmt(mb, partitionRef, hashRef);
					q = pushArgument(mb, q,  part);
					q = pushInt(mb, q, j);
					q = pushInt(mb, q, pieces);
					vars[part][j] = getArg(q,0);
				}
#endif
			}
			qo = newInstruction(mb, matRef, packRef);
			getArg(qo,0) =  getArg(p,0);
			vars[getArg(qo,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			qg = newInstruction(mb, matRef, packRef);
			getArg(qg,0) = getArg(p,1);
			vars[getArg(qg,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			qc = newInstruction(mb, matRef, packRef);
			getArg(qc,0) = getArg(p,2);
			vars[getArg(qc,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);

			vars[getArg(p,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,1)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,2)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			for( j = 0; j < pieces; j++){
				vars[getArg(p,0)][j] = newTmpVariable(mb, getArgType(mb,p,0));
				vars[getArg(p,1)][j] = newTmpVariable(mb, getArgType(mb,p,1));
				vars[getArg(p,2)][j] = newTmpVariable(mb, getArgType(mb,p,2));
				q = newStmt(mb, groupRef, getFunctionId(p));
				getArg(q,0) = vars[getArg(p,0)][j];
				q = pushReturn(mb, q, vars[getArg(p,1)][j]);
				q = pushReturn(mb, q, vars[getArg(p,2)][j]);
				pushArgument(mb, q, vars[part][j]);
				qo = pushArgument(mb, qo, getArg(q,0));
				vars[getArg(qo,0)][j] =getArg(q,0);
				qg = pushArgument(mb, qg, getArg(q,1));
				vars[getArg(qg,0)][j] =getArg(q,1);
				qc = pushArgument(mb, qc, getArg(q,2));
				vars[getArg(qc,0)][j] =getArg(q,2);
			}
			pushInstruction(mb, qo);
			pushInstruction(mb, qg);
			pushInstruction(mb, qc);
		} else
		if( getModuleId(p) == groupRef && getFunctionId(p) == subgroupdoneRef && vars[getArg(p,3)]  && vars[getArg(p,4)])
		{
#ifdef _PARTITION_DEBUG_
			fprintf(stderr,"#Found partition candidate subgroupdone %d and var %d, %d\n", p->pc, getArg(p,3), getArg(p,4));
#endif
			qo = newInstruction(mb, matRef, packRef);
			getArg(qo,0) =  getArg(p,0);
			vars[getArg(qo,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			qg = newInstruction(mb, matRef, packRef);
			getArg(qg,0) = getArg(p,1);
			vars[getArg(qg,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			qc = newInstruction(mb, matRef, packRef);
			getArg(qc,0) = getArg(p,2);
			vars[getArg(qc,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);

			vars[getArg(p,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,1)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			vars[getArg(p,2)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			for( j = 0; j < pieces; j++){
				vars[getArg(p,0)][j] = newTmpVariable(mb, getArgType(mb,p,0));
				vars[getArg(p,1)][j] = newTmpVariable(mb, getArgType(mb,p,1));
				vars[getArg(p,2)][j] = newTmpVariable(mb, getArgType(mb,p,2));
				q = newStmt(mb, groupRef, subgroupdoneRef);
				getArg(q,0) = vars[getArg(p,0)][j];
				q = pushReturn(mb, q, vars[getArg(p,1)][j]);
				q = pushReturn(mb, q, vars[getArg(p,2)][j]);
				q = pushArgument(mb, q, vars[getArg(p, 3)][j]);
				q = pushArgument(mb, q, vars[getArg(p, 4)][j]);
				qo = pushArgument(mb, qo, getArg(q,0));
				vars[getArg(qo,0)][j] =getArg(q,0);
				qg = pushArgument(mb, qg, getArg(q,1));
				vars[getArg(qg,0)][j] =getArg(q,0);
				qc = pushArgument(mb, qc, getArg(q,2));
				vars[getArg(qc,0)][j] =getArg(q,0);
			}
			pushInstruction(mb, qo);
			pushInstruction(mb, qg);
			pushInstruction(mb, qc);
		} else
		if( getModuleId(p) == aggrRef && getFunctionId(p) == subcountRef && vars[getArg(p, p->retc)] != NULL){
#ifdef _PARTITION_DEBUG_
			fprintf(stderr,"#Found partition candidate subcount %d and var %dd\n", p->pc, getArg(p,p->retc));
#endif
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
		if( getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef && vars[getArg(p, 1)] && vars[getArg(p,2)]){
#ifdef _PARTITION_DEBUG_
			fprintf(stderr,"#Found partition projection %d and var %dd\n", p->pc, getArg(p, 1));
#endif
			vars[getArg(p,0)]= (int*) GDKzalloc(sizeof(int) * MAXPIECES);
			for( j = 0; j < pieces; j++){
				vars[getArg(p,0)][j] = newTmpVariable(mb, getArgType(mb,p,0));
				q = newStmt(mb, algebraRef, projectionRef);
				getArg(q,0) = vars[getArg(p,0)][j];
				q = pushArgument(mb, q, vars[getArg(p,1)][j]);
				q = pushArgument(mb, q, vars[getArg(p,2)][j]);
			}

		} else
			pushInstruction(mb,p);
	}

	for( ; i < slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
#ifdef _PARTITION_DEBUG_
	for(i = 0; i < vlimit; i++)
		if(vars[i]){
			fprintf(stderr,"#[%d]", i);
			for( j=0; j<pieces; j++)
				fprintf(stderr,"%3d ",vars[i][j]);
			fprintf(stderr,"\n");
		}
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
