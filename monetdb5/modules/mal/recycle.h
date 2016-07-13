/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _RECYCLE_
#define _RECYCLE_

#include "mal.h"
#include "mal_instruction.h"
#include "bat5.h"

mal_export str RECYCLEdumpWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str RECYCLEsetCache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RECYCLEdropWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

// SQL updates should trigger recycler cleanup operations
mal_export str RECYCLEresetBATwrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RECYCLEappendSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RECYCLEdeleteSQL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif
