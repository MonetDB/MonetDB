/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "projectionpath.h"

/*
 * The projection path optimizer takes a projection sequence and
 * should attempt to minimize the intermediate result.
 */

static BAT *
ALGprojectionpathBody(Client cntxt, int top, BAT **joins)
{
	BAT *b = NULL;
	BUN e = 0;
	int i, j, k;
	int *postpone= (int*) GDKzalloc(sizeof(int) *top);
	int postponed=0;

	if(postpone == NULL){
		GDKerror("projectionpathBody" MAL_MALLOC_FAIL);
		return NULL;
	}


	/* solve the projection by pairing the smallest first */
	while (top > 1) {
		j = 0;

		b = BATproject(joins[j], joins[j + 1]);
		ALGODEBUG if(b){
			fprintf(stderr,"#projection step produces "BUNFMT"\n", BATcount(b));
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
				fprintf(stderr, "#projectionpath %d:= join(%d,%d)"
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
ALGprojectionpath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,top=0, empty = 0;
	bat *bid;
	bat *r = getArgReference_bat(stk, pci, 0);
	BAT *b, **joins = (BAT**)GDKmalloc(pci->argc*sizeof(BAT*)); 
	int error = 0;

	assert(pci->argc > 1);
	if ( joins == NULL)
		throw(MAL, "algebra.projectionpath", MAL_MALLOC_FAIL);
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
			throw(MAL, "algebra.projectionpath", "%s", error? SEMANTIC_TYPE_MISMATCH: INTERNAL_BAT_ACCESS);
		}
		empty += BATcount(b) == 0;
		joins[top++] = b;
	}

	ALGODEBUG{
		char *ps;
		ps = instruction2str(mb, 0, pci, LIST_MAL_ALL);
		fprintf(stderr,"#projectionpath %s\n", (ps ? ps : ""));
		GDKfree(ps);
	}
	if ( empty){
		// any empty step produces an empty result
		b = BATnew( TYPE_void, joins[top-1]->ttype, 0, TRANSIENT);
		if( b == NULL){
			GDKerror("algebra.projectionpath" MAL_MALLOC_FAIL);
			return NULL;
		}
		/* be optimistic, inherit the properties  */
		BATseqbase(b,0);
		BATsettrivprop(b);

		for( --top; top>=0; top--)
			BBPunfix(joins[top]->batCacheid);
	} else {
		b = ALGprojectionpathBody(cntxt,top,joins); 
	}

	GDKfree(joins);
	if ( b)
		BBPkeepref( *r = b->batCacheid);
	else
		throw(MAL, "algebra.projectionpath", INTERNAL_OBJ_CREATE);
	return MAL_SUCCEED;
}
