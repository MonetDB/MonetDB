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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "groups.h"
#include "group.h"

/*
 * The groups optimizer takes a sequence and attempts to minimize the intermediate result.
 * The choice depends on a good estimate of intermediate results using properties.
 * We start by just performing the underlying MAL instructions in sequence as requested
 * This will lead to better locality of BAT access.
 */
typedef struct{
	int *arg;
	BAT *b;
} Elm;

str
GRPmulticolumngroup(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *grp = (int*) getArgReference(stk,pci,0);
	int *ext = (int*) getArgReference(stk,pci,1);
	int *hist = (int*) getArgReference(stk,pci,2);
	int i, j, oldgrp;
	str msg = MAL_SUCCEED;
	lng *sizes = (lng*) GDKzalloc(sizeof(lng) * pci->argc), l;
	bat *bid = (bat*) GDKzalloc(sizeof(bat) * pci->argc), bi;
	BUN *cnt = (BUN*) GDKzalloc(sizeof(BUN) * pci->argc), c;
	BAT *b, *sample, *uniq;

	for( i=3; i< pci->argc; i++){
		bid[i] = *(int *) getArgReference(stk, pci, i);
		b = BATdescriptor(bid[i]);
		if ( b ){
			cnt[i]= BATcount(b);
			sample = BATsample(b,1000);
			if ( sample) {
				uniq = BATkunique( BATmirror(sample));
				if ( uniq){
					sizes[i] = (lng) BATcount(uniq);
					BBPreleaseref(uniq->batCacheid);
				}
				BBPreleaseref(sample->batCacheid);
			}
			BBPreleaseref(bid[i]);
		}
	}

	/* for (i=3; i<pci->argc; i++)
		mnstr_printf(cntxt->fdout,"# before[%d] "LLFMT"\n",i, sizes[i]); */
	/* sort order may have influences */
	/* SF100 Q16 showed < ordering is 2 times faster as > ordering */
	for ( i = 3; i< pci->argc; i++)
	for ( j = i+1; j<pci->argc; j++)
	if ( sizes[j] < sizes[i]){
		l = sizes[j]; sizes[j]= sizes[i]; sizes[i]= l;
		bi = bid[j]; bid[j]= bid[i]; bid[i]= bi;
		c = cnt[j]; cnt[j]= cnt[i]; cnt[i]= c;
	}
	/* for (i=2; i<pci->argc; i++)
		mnstr_printf(cntxt->fdout,"# after [%d] "LLFMT"\n",i, sizes[i]); */

	/* (grp,ext,his) := group.subgroup(..) */
	*grp = 0;
	*ext = 0;
	msg = GRPsubgroup1(grp, ext, hist, &bid[3]);
	if ( msg != MAL_SUCCEED){
		GDKfree(sizes);
		GDKfree(bid);
		GDKfree(cnt);
		return msg;
	}
	/* check group count */
	b = BATdescriptor(*grp);
	if (  b && BATcount(b) != cnt[3]) {
		BBPreleaseref(*grp);
		b = 0;
		/* (grp,ext,hist) := group.subgroupdone(arg,grp) */
		for ( i=4; i < pci->argc; i++){
			oldgrp= *grp;
			msg = GRPsubgroup2(grp, ext, hist, &bid[i], &oldgrp);
			if ( msg == MAL_SUCCEED){
				BBPdecref(oldgrp, TRUE);
			} else break;
			/* check group count */
			b = BATdescriptor(*grp);
			if ( b && BATcount(b) == cnt[i])
				break;
		}
	} 
	if (b) 
		BBPreleaseref(*grp);
	GDKfree(sizes);
	GDKfree(bid);
	GDKfree(cnt);
	(void) cntxt;
	(void) mb;
	return msg;
}
