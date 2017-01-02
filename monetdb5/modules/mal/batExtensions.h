/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _BAT_EXTENSIONS_
#define _BAT_EXTENSIONS_

#include "mal_client.h"
#include "mal_interpreter.h"
#include "bat5.h"
#include "algebra.h"

mal_export str CMDBATnewColumn(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mal_export str CMDBATnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mal_export str CMDBATnew_persistent(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p);
mal_export str CMDBATsingle(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDBATpartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDBATpartition2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDBATimprints(void *ret, bat *bid);
mal_export str CMDBATimprintsize(lng *ret, bat *bid);

#endif /* _BAT_EXTENSIONS_ */
