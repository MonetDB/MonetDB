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
 * Post-optimization. After the join path has been constructed
 * we could search for common subpaths. This heuristic is to
 * remove any pair which is used more than once.
 * Inner paths are often foreign key walks.
 * The heuristics is sufficient for the code produced by SQL frontend.
 * The alternative is to search for all possible subpaths and materialize them.
 * For example, using recursion for all common paths.
 */
#include "monetdb_config.h"
#include "joinpath.h"
//#include "cluster.h"

/*
 * The join path optimizer takes a join sequence and
 * attempts to minimize the intermediate result.
 * The choice depends on a good estimate of intermediate
 * results using properties.
 * For the time being, we use a simplistic model, based
 * on the assumption that most joins are foreign key joins anyway.
 *
 * We use a sample based approach for sizeable  tables.
 * The model is derived from the select statement. However, we did not succeed.
 * The code is now commented for future improvement.
 *
 * Final conclusion from this exercise is:
 * The difference between the join input size and the join output size is not
 * the correct (or unique) metric which should be used to decide which order
 * should be used in the joinPath.
 * A SMALL_OPERAND is preferrable set to those cases where the table
 * fits in the cache. This depends on the cache size and operand type.
 * For the time being we limit ourself to a default of 1Kelements
 */
/*#define SAMPLE_THRESHOLD_lOG 17*/

#define SMALL_OPERAND	1024

static BUN
ALGjoinCost(Client cntxt, BAT *l, BAT *r, int flag)
{
	BUN lc, rc;
	BUN cost=0;
#if 0
	BUN lsize,rsize;
	BAT *lsample, *rsample, *j; 
#endif

	(void) flag;
	(void) cntxt;
	lc = BATcount(l);
	rc = BATcount(r);
#if 0	
	/* The sampling method */
	if(flag < 2 && ( lc > 100000 || rc > 100000)){
		lsize= MIN(lc/100, (1<<SAMPLE_THRESHOLD_lOG)/3);
		lsample= BATsample(l,lsize);
		BBPreclaim(lsample);
		rsize= MIN(rc/100, (1<<SAMPLE_THRESHOLD_lOG)/3);
		rsample= BATsample(r,rsize);
		BBPreclaim(rsample);
		j= BATjoin(l,r, MAX(lsize,rsize));
		lsize= BATcount(j);
		BBPreclaim(j);
		return lsize;
	}
#endif

	/* first use logical properties to estimate upper bound of result size */
	if (l->tkey && r->hkey)
		cost = MIN(lc,rc);
	else
	if (l->tkey)
		cost = rc;
	else
	if (r->hkey)
		cost = lc;
	else
	if (lc * rc >= BUN_MAX)
		cost = BUN_MAX;
	else
		cost = lc * rc;

	/* then use physical properties to rank costs */
	if (BATtdense(l) && BAThdense(r))
		/* densefetchjoin -> sequential access */
		cost /= 7;
	else
	if (BATtordered(l) && BAThdense(r))
		/* orderedfetchjoin > sequential access */
		cost /= 6;
	else
	if (BATtdense(l) && BAThordered(r) && flag != 0 /* no leftjoin */)
		/* (reversed-) orderedfetchjoin -> sequential access */
		cost /= 6;
	else
	if (BAThdense(r) && rc <= SMALL_OPERAND)
		/* fetchjoin with random access in L1 */
		cost /= 5;
	else
	if (BATtdense(l) && lc <= SMALL_OPERAND && flag != 0 /* no leftjoin */)
		/* (reversed-) fetchjoin with random access in L1 */
		cost /= 5;
	else
	if (BATtordered(l) && BAThordered(r))
		/* mergejoin > sequential access */
		cost /= 4;
	else
	if (BAThordered(r) && rc <= SMALL_OPERAND)
		/* binary-lookup-join with random access in L1 */
		cost /= 3;
	else
	if (BATtordered(l) && lc <= SMALL_OPERAND && flag != 0 /* no leftjoin */)
		/* (reversed-) binary-lookup-join with random access in L1 */
		cost /= 3;
	else
	if ((BAThordered(r) && lc <= SMALL_OPERAND) || (BATtordered(l) && rc <= SMALL_OPERAND))
		/* sortmergejoin with sorting in L1 */
		cost /= 3;
	else
	if (rc <= SMALL_OPERAND)
		/* hashjoin with hashtable in L1 */
		cost /= 3;
	else
	if (lc <= SMALL_OPERAND && flag != 0 /* no leftjoin */)
		/* (reversed-) hashjoin with hashtable in L1 */
		cost /= 3;
	else
	if (BAThdense(r))
		/* fetchjoin with random access beyond L1 */
		cost /= 2;
	else
	if (BATtdense(l) && flag != 0 /* no leftjoin */)
		/* (reversed-) fetchjoin with random access beyond L1 */
		cost /= 2;
	else
		/* hashjoin with hashtable larger than L1 */
		/* sortmergejoin with sorting beyond L1 */
		cost /= 1;

	ALGODEBUG
		fprintf(stderr,"#batjoin cost ?"BUNFMT"\n",cost);
	return cost;
}

BAT *
ALGjoinPathBody(Client cntxt, int top, BAT **joins, int flag)
{
	BAT *b = NULL;
	BUN estimate, e = 0;
	int i, j, k;
	int *postpone= (int*) GDKzalloc(sizeof(int) *top);
	int postponed=0;

	/* solve the join by pairing the smallest first */
	while (top > 1) {
		j = 0;
		estimate = ALGjoinCost(cntxt,joins[0],joins[1],flag);
		ALGODEBUG
			fprintf(stderr,"#joinPath estimate join(%d,%d) %d cnt="BUNFMT" %s\n", joins[0]->batCacheid, 
				joins[1]->batCacheid,(int)estimate, BATcount(joins[0]), postpone[0]?"postpone":"");
		for (i = 1; i < top - 1; i++) {
			e = ALGjoinCost(cntxt,joins[i], joins[i + 1],flag);
			ALGODEBUG
				fprintf(stderr,"#joinPath estimate join(%d,%d) %d cnt="BUNFMT" %s\n", joins[i]->batCacheid, 
					joins[i+1]->batCacheid,(int)e,BATcount(joins[i]),  postpone[i]?"postpone":"");
			if (e < estimate &&  ( !(postpone[i] && postpone[i+1]) || postponed<top)) {
				estimate = e;
				j = i;
			}
		}
		/*
		 * @-
		 * BEWARE. you may not use a size estimation, because it
		 * may fire a BATproperty check in a few cases.
		 * In case a join fails, we may try another order first before
		 * abandoning the task. It can handle cases where a Cartesian product emerges.
		 *
		 * A left-join sequence only requires the result to be sorted
		 * against the first operand. For all others operand pairs, the cheapest join suffice.
		 */

		switch(flag){
		case 0:
			if ( j == 0) {
				b = BATleftjoin(joins[j], joins[j + 1], BATcount(joins[j]));
				break;
			}
		case 1:
			b = BATjoin(joins[j], joins[j + 1], (BATcount(joins[j]) < BATcount(joins[j + 1])? BATcount(joins[j]):BATcount(joins[ j + 1])));
			break;
		case 2:
			b = BATsemijoin(joins[j], joins[j + 1]);
			break;
		case 3:
			b = BATleftfetchjoin(joins[j], joins[j + 1], BATcount(joins[j]));
		}
		if (b==NULL){
			if ( postpone[j] && postpone[j+1]){
				for( --top; top>=0; top--)
					BBPreleaseref(joins[top]->batCacheid);
				GDKfree(postpone);
				return NULL;
			}
			postpone[j] = TRUE;
			postpone[j+1] = TRUE;
			postponed = 0;
			for( k=0; k<top; k++)
				postponed += postpone[k]== TRUE;
			if ( postponed == top){
				for( --top; top>=0; top--)
					BBPreleaseref(joins[top]->batCacheid);
				GDKfree(postpone);
				return NULL;
			}
			/* clear the GDKerrors and retry */
			if( cntxt->errbuf )
				cntxt->errbuf[0]=0;
			continue;
		} else {
			/* reset the postponed joins */
			for( k=0; k<top; k++)
				postpone[k]=FALSE;
			if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
			postponed = 0;
		}
		ALGODEBUG{
			if (b ) {
				fprintf(stderr, "#joinPath %d:= join(%d,%d)"
				" arguments %d (cnt= "BUNFMT") against (cnt "BUNFMT") cost "BUNFMT"\n", 
					b->batCacheid, joins[j]->batCacheid, joins[j + 1]->batCacheid,
					j, BATcount(joins[j]),  BATcount(joins[j+1]), e);
			}
		}

		if ( b == 0 ){
			for( --top; top>=0; top--)
				BBPreleaseref(joins[top]->batCacheid);
			GDKfree(postpone);
			return 0;
		}
		BBPdecref(joins[j]->batCacheid, FALSE);
		BBPdecref(joins[j+1]->batCacheid, FALSE);
		joins[j] = b;
		top--;
		for (i = j + 1; i < top; i++)
			joins[i] = joins[i + 1];
	}
	GDKfree(postpone);
	b = joins[0];
	if (b && !(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	return b;
}

str
ALGjoinPath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,*bid,top=0;
	int *r = (int*) getArgReference(stk, pci, 0);
	BAT *b, **joins = (BAT**)GDKmalloc(pci->argc*sizeof(BAT*)); 
	int error = 0;
	str joinPathRef = putName("joinPath",8);
	str semijoinPathRef = putName("semijoinPath",12);
	str leftjoinPathRef = putName("leftjoinPath",12);

	if ( joins == NULL)
		throw(MAL, "algebra.joinPath", MAL_MALLOC_FAIL);
	(void)mb;
	for (i = pci->retc; i < pci->argc; i++) {
		bid = (int *) getArgReference(stk, pci, i);
		b = BATdescriptor(*bid);
		if (  b && top ) {
			if ( !(joins[top-1]->ttype == b->htype) &&
			     !(joins[top-1]->ttype == TYPE_void && b->htype == TYPE_oid) &&
			     !(joins[top-1]->ttype == TYPE_oid && b->htype == TYPE_void) ) {
				b= NULL;
				error = 1;
			}
		}
		if ( b == NULL) {
			for( --top; top>=0; top--)
				BBPreleaseref(joins[top]->batCacheid);
			GDKfree(joins);
			throw(MAL, "algebra.joinPath", error? SEMANTIC_TYPE_MISMATCH: INTERNAL_BAT_ACCESS);
		}
		joins[top++] = b;
	}
	ALGODEBUG{
		char *ps;
		ps = instruction2str(mb, 0, pci, 0);
		fprintf(stderr,"#joinpath %s\n", ps ? ps : "");
		GDKfree(ps);
	}
	if ( getFunctionId(pci) == joinPathRef)
		b= ALGjoinPathBody(cntxt,top,joins, 1);
	else
	if ( getFunctionId(pci) == leftjoinPathRef)
		b= ALGjoinPathBody(cntxt,top,joins, 0); 
	else
	if ( getFunctionId(pci) == semijoinPathRef)
		b= ALGjoinPathBody(cntxt,top,joins, 2);
	else
		b= ALGjoinPathBody(cntxt,top,joins, 3); 

	GDKfree(joins);
	if ( b)
		BBPkeepref( *r = b->batCacheid);
	else
		throw(MAL, "algebra.joinPath", INTERNAL_OBJ_CREATE);
	return MAL_SUCCEED;
}
