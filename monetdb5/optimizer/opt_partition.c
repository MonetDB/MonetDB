/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* example: select count(distinct userid) from hits_10m;
 * Massage aggregate operations using value based partitioning
    X_16 := algebra.projection(X_4, X_15);
    (X_17, X_18, X_19) := group.groupdone(X_16);
    X_20 := algebra.projection(X_18, X_16);
    X_21 := aggr.count(X_20, true);

   The new could should become:
     X_16 := algebra.projection(X_4, X_15);
    (P1,P2,P3,P4) := bat.partition(X_16)
    (O1, G1 , S1) := group.groupdone(P1);
    (O2, G2 , S2) := group.groupdone(P2);
    (O3, G3 , S3) := group.groupdone(P3);
    (O4, G4 , S4) := group.groupdone(P4);
    X_17 := mat.pack(O1,O3,O3,O4)
    X_18 := mat.pack(G1,G3,G3,G4)
    X_19 := mat.pack(S1,S3,S3,S4)

    Q1 := algebra.projection(G1, P1);
    Q2 := algebra.projection(G2, P2);
    Q3 := algebra.projection(G3, P3);
    Q4 := algebra.projection(G4, P4);
    X_20 := mat.pack(Q1,Q2,Q3,Q4);

    X_21 := aggr.count(X_20, true);
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_partition.h"

#define isCandidateList(M,P,I) ((M)->var[getArg(P,I)].id[0]== 'C')

str
OPTpartitionImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, slimit, limit, actions=0;
	InstrPtr *old = 0;
	lng usec = GDKusec();
	char buf[256];

	(void) stk;
	(void) cntxt;

	limit = mb->stop;
	slimit = mb->ssize;
	/* check for aggregates */
	for ( i = 0; i < limit; i++){
		p= getInstrPtr(mb,i);
		if( getFunctionId(p) == groupdoneRef  && getModuleId(p)== groupRef && p->argc == 2){
			actions ++;
		}
	}
	if( actions == 0)
		goto finished;

	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL,"optimizer.postfix", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	/* construct the new plan */
	for ( i = 0; i < limit; i++)
	{
		p= old[i];
		pushInstruction(mb,p);
	}
	for( ;i < slimit; i++)
		if( old[i])
			freeInstruction(old[i]);

	/* Defense line against incorrect plans */
	if( actions ){
		chkTypes(cntxt->usermodule, mb, FALSE);
		chkFlow(mb);
		chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment and update statics */
	GDKfree(old);
finished:
	usec= GDKusec() - usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec", "postfix", actions, usec);
	newComment(mb,buf);
	addtoMalBlkHistory(mb);

	return MAL_SUCCEED;
}
