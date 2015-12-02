/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
ALGjoinCost(Client cntxt, BAT *l, BAT *r)
{
	BUN lc, rc;
	BUN cost=0;

	(void) cntxt;
	lc = BATcount(l);
	rc = BATcount(r);

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
	if (BATtdense(l) && BAThordered(r))
		/* (reversed-) orderedfetchjoin -> sequential access */
		cost /= 6;
	else
	if (BAThdense(r) && rc <= SMALL_OPERAND)
		/* fetchjoin with random access in L1 */
		cost /= 5;
	else
	if (BATtdense(l) && lc <= SMALL_OPERAND)
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
	if (BATtordered(l) && lc <= SMALL_OPERAND)
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
	if (lc <= SMALL_OPERAND)
		/* (reversed-) hashjoin with hashtable in L1 */
		cost /= 3;
	else
	if (BAThdense(r))
		/* fetchjoin with random access beyond L1 */
		cost /= 2;
	else
	if (BATtdense(l))
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


static BAT *
ALGjoinPathBody(Client cntxt, int top, BAT **joins)
{
	BAT *b = NULL;
	BUN estimate, e = 0;
	int i, j, k;
	int *postpone= (int*) GDKzalloc(sizeof(int) *top);
	int postponed=0;

	if(postpone == NULL){
		GDKerror("joinPathBody" MAL_MALLOC_FAIL);
		return NULL;
	}


	/* solve the join by pairing the smallest first */
	while (top > 1) {
		j = 0;
		estimate = ALGjoinCost(cntxt,joins[0],joins[1]);
		ALGODEBUG
			fprintf(stderr,"#joinPath estimate join(%d,%d) %d cnt="BUNFMT" %s\n", joins[0]->batCacheid, 
				joins[1]->batCacheid,(int)estimate, BATcount(joins[0]), postpone[0]?"postpone":"");
		for (i = 1; i < top - 1; i++) {
			e = ALGjoinCost(cntxt,joins[i], joins[i + 1]);
			ALGODEBUG
				fprintf(stderr,"#joinPath estimate join(%d,%d) %d cnt="BUNFMT" %s\n", joins[i]->batCacheid, 
					joins[i+1]->batCacheid,(int)e,BATcount(joins[i]),  postpone[i]?"postpone":"");
			if (e < estimate &&  ( !(postpone[i] && postpone[i+1]) || postponed<top)) {
				estimate = e;
				j = i;
			}
		}
		/*
		 * BEWARE. you may not use a size estimation, because it
		 * may fire a BATproperty check in a few cases.
		 * In case a join fails, we may try another order first before
		 * abandoning the task. It can handle cases where a Cartesian product emerges.
		 *
		 * A left-join sequence only requires the result to be sorted
		 * against the first operand. For all others operand pairs, the cheapest join suffice.
		 */

		b = BATproject(joins[j], joins[j + 1]);
		ALGODEBUG{
			fprintf(stderr,"#joinpath step produces "BUNFMT"\n", BATcount(b));
		}
		if (b==NULL){
			if ( postpone[j] && postpone[j+1]){
				for( --top; top>=0; top--)
					BBPunfix(joins[top]->batCacheid);
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
					BBPunfix(joins[top]->batCacheid);
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
			if (!(b->batDirty&2)) BATsetaccess(b, BAT_READ);
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
				BBPunfix(joins[top]->batCacheid);
			GDKfree(postpone);
			return 0;
		}
		BBPunfix(joins[j]->batCacheid);
		BBPunfix(joins[j+1]->batCacheid);
		joins[j] = b;
		top--;
		for (i = j + 1; i < top; i++)
			joins[i] = joins[i + 1];
	}
	GDKfree(postpone);
	b = joins[0];
	if (b && !(b->batDirty&2)) BATsetaccess(b, BAT_READ);
	return b;
}

str
ALGjoinPath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,top=0, empty = 0;
	bat *bid;
	bat *r = getArgReference_bat(stk, pci, 0);
	BAT *b, **joins = (BAT**)GDKmalloc(pci->argc*sizeof(BAT*)); 
	int error = 0;

	assert(pci->argc > 1);
	if ( joins == NULL)
		throw(MAL, "algebra.joinPath", MAL_MALLOC_FAIL);
	(void)mb;
	for (i = pci->retc; i < pci->argc; i++) {
		bid = getArgReference_bat(stk, pci, i);
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
				BBPunfix(joins[top]->batCacheid);
			GDKfree(joins);
			throw(MAL, "algebra.joinPath", "%s", error? SEMANTIC_TYPE_MISMATCH: INTERNAL_BAT_ACCESS);
		}
		empty += BATcount(b) == 0;
		joins[top++] = b;
	}

	ALGODEBUG{
		char *ps;
		ps = instruction2str(mb, 0, pci, LIST_MAL_ALL);
		fprintf(stderr,"#joinpath %s\n", (ps ? ps : ""));
		GDKfree(ps);
	}
	if ( empty){
		// any empty step produces an empty result
		b = BATnew( TYPE_void, joins[top-1]->ttype, 0, TRANSIENT);
		if( b == NULL){
			GDKerror("joinChain" MAL_MALLOC_FAIL);
			return NULL;
		}
		/* be optimistic, inherit the properties  */
		BATseqbase(b,0);
		BATsettrivprop(b);

		for( --top; top>=0; top--)
			BBPunfix(joins[top]->batCacheid);
	} else {
		b = ALGjoinPathBody(cntxt,top,joins); 
	}

	GDKfree(joins);
	if ( b)
		BBPkeepref( *r = b->batCacheid);
	else
		throw(MAL, "algebra.joinPath", INTERNAL_OBJ_CREATE);
	return MAL_SUCCEED;
}
