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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f algebraExtensions
 * @v 2.0
 * @a M.L.Kersten, P. Boncz
 * @+ Algebra Extensions
 * The algebra extensions rely
 * on the MAL runtime setting, but logically belong to the kernel/algebra
 * module.
 */
#include "monetdb_config.h"
#include "algebraExtensions.h"

str
ALGprojectCstBody(bat *result, int *bid, ptr *p, int tt){
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.project", INTERNAL_BAT_ACCESS);
	}

	if (ATOMvarsized(tt)) {
		if (p == 0 || *(str *) p == 0)
			p = (ptr *) str_nil;
		else
			p = *(ptr **) p;
	}
	bn = BATconst(b, tt, p);
	BBPunfix(b->batCacheid);
	if (bn) {
		*result = bn->batCacheid;
		BBPkeepref(bn->batCacheid);
		return MAL_SUCCEED;
	}
	throw(MAL, "bbp.project", INTERNAL_OBJ_CREATE);
}

str
ALGprojectCst(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *result = (int *) getArgReference(stk, pci, 0);
	int *bid = (int *) getArgReference(stk, pci, 1);
	ptr *p = (ptr *) getArgReference(stk, pci, 2);
	int tt = getArgType(mb, pci, 2);

	(void) cntxt;
	return ALGprojectCstBody(result, bid, p, tt);
}
