/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "cudf.h"

static str
CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped);

str CUDFevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return CUDFeval(cntxt, mb, stk, pci, 0);
}

str CUDFevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return CUDFeval(cntxt, mb, stk, pci, 1);
}

str CUDFprelude(void *ret) {
	(void) ret;
	return MAL_SUCCEED;
}


static str
CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped) {
	return MAL_SUCCEED;
}
