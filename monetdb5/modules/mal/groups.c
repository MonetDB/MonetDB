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
#include "groups.h"
#include "group.h"

/*
 * The groups optimizer takes a grouping sequence and attempts to
 * minimize the intermediate result.  The choice depends on a good
 * estimate of intermediate results using properties.
 */

str
GRPmulticolumngroup(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *grp = (bat *) getArgReference(stk, pci, 0);
	bat *ext = (bat *) getArgReference(stk, pci, 1);
	bat *hist = (bat *) getArgReference(stk, pci, 2);
	int i, j;
	bat oldgrp, oldext, oldhist;
	str msg = MAL_SUCCEED;
	lng *sizes = (lng *) GDKzalloc(sizeof(lng) * pci->argc), l;
	bat *bid = (bat *) GDKzalloc(sizeof(bat) * pci->argc), bi;
	BAT *b, *sample, *uniq;
	BUN count = 0;

	assert(pci->argc >= 4);
	for (i = 3; i < pci->argc; i++) {
		bid[i] = *(int *) getArgReference(stk, pci, i);
		b = BATdescriptor(bid[i]);
		if (b) {
			sizes[i] = count = BATcount(b);
			sample = BATsample(b, 1000);
			if (sample) {
				uniq = BATkunique(BATmirror(sample));
				if (uniq) {
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
	for (i = 3; i < pci->argc; i++)
		for (j = i + 1; j < pci->argc; j++)
			if (sizes[j] < sizes[i]) {
				l = sizes[j];
				sizes[j] = sizes[i];
				sizes[i] = l;
				bi = bid[j];
				bid[j] = bid[i];
				bid[i] = bi;
			}
	/* for (i=2; i<pci->argc; i++)
	   mnstr_printf(cntxt->fdout,"# after [%d] "LLFMT"\n",i, sizes[i]); */

	/* (grp,ext,hist) := group.subgroup(..) */
	*grp = 0;
	*ext = 0;
	*hist = 0;
	msg = GRPsubgroup1(grp, ext, hist, &bid[3]);
	i = 4;
	if (msg == MAL_SUCCEED && pci->argc > 4)
		do {
			/* early break when there are as many groups as histogram entries */
			b = BATdescriptor(*hist);
			if (b) {
				j = BATcount(b) == count;
				BBPreleaseref(*hist);
				if (j)
					break;
			}

			/* (grp,ext,hist) := group.subgroup(arg,grp,ext,hist) */
			oldgrp = *grp;
			oldext = *ext;
			oldhist = *hist;
			*grp = 0;
			*ext = 0;
			*hist = 0;
			msg = GRPsubgroup4(grp, ext, hist, &bid[i], &oldgrp, &oldext, &oldhist);
			BBPdecref(oldgrp, TRUE);
			BBPdecref(oldext, TRUE);
			BBPdecref(oldhist, TRUE);
		} while (msg == MAL_SUCCEED && ++i < pci->argc);
	GDKfree(sizes);
	GDKfree(bid);
	(void) cntxt;
	(void) mb;
	return msg;
}
