/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _OLTP_H
#define _OLTP_H

#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

#define _DEBUG_OLTP_

mal_export str OLTPinit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPenable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPdisable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPtable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str OLTPis_enabled(int *ret);
#endif /* _OLTP_H */
