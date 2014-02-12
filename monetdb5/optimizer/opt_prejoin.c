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

#include "monetdb_config.h"
#include "opt_prejoin.h"
#include "math.h"
/*
 * The prejoin implementation should only become active if we
 * expect that we may end up with an IO access for each
 * possible lookup.
 */
str
ALGprejoin(int *rl, int *rr, int *l, int *r){
	BAT *bl,*br,*bn;
	BUN lpages, rpages;

	*rl = *rr = 0;
	if( (bl= BATdescriptor(*l)) == NULL ){
		throw(MAL, "algebra.prejoin", INTERNAL_BAT_ACCESS);
	}
	if( (br= BATdescriptor(*r)) == NULL ){
		BBPreleaseref(bl->batCacheid);
		throw(MAL, "algebra.prejoin", INTERNAL_BAT_ACCESS);
	}
	lpages= (BUN) ((bl->H->heap.size + bl->T->heap.size)/MT_pagesize());
	rpages= (BUN) ((br->H->heap.size + br->T->heap.size)/MT_pagesize());

	if( bl->batPersistence != TRANSIENT || 	/* no change in persistent */
		BATtordered(bl) ||		/* ordered tails are fine */
		bl->batSharecnt ||		/* avoid dependent views */
		rpages + lpages <= GDKmem_cursize()/MT_pagesize()  ||	/* small operands are ok*/
		(dbl)BATcount(bl) < ( 2* rpages * log((dbl)rpages)) ){
		BBPkeepref(*rl = bl->batCacheid);
		BBPkeepref(*rr = br->batCacheid);
		return MAL_SUCCEED;
	}
	ALGODEBUG{
	fprintf(stderr,"Prejoin tuples=" BUNFMT "pages" BUNFMT "," BUNFMT"\n",
		BATcount(bl), lpages, rpages);
	}
	bn= BATmirror(BATsort(BATmirror(bl)));
	BBPkeepref(*rr = br->batCacheid);
	BBPkeepref(*rl = bn->batCacheid);
	BBPreleaseref(bl->batCacheid);
	return MAL_SUCCEED;
}

int 
OPTprejoinImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit;
	InstrPtr q, p=0, *old= mb->stmt;
	int actions = 0;

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	limit= mb->stop;
	if ( newMalBlkStmt(mb, mb->ssize) < 0 )
		return 0;

	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++) {
		p= old[i];
		if( getModuleId(p)== algebraRef && getFunctionId(p)== joinRef &&
			getHeadType(getArgType(mb,p,1)) == TYPE_oid  &&
			getTailType(getArgType(mb,p,1)) == TYPE_oid ){
			q= newStmt(mb,algebraRef, "prejoin");
			setArgType(mb,q,0,getArgType(mb,p,1));
			q= pushReturn(mb,q,newTmpVariable(mb, getArgType(mb,p,2)));
			q= pushArgument(mb,q,getArg(p,1));
			q= pushArgument(mb,q,getArg(p,2));
			getArg(p,1)= getArg(q,0);
			getArg(p,2)= getArg(q,1);
			actions++;
		} 
		pushInstruction(mb,p);
	}
	/* we may have uncovered new use-less operations */
	/*chkProgram(cntxtx->fdout, cntxt->nspace,mb);*/
	GDKfree(old);
	return actions;
}
