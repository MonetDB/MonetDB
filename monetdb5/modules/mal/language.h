/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @+ Dummy code
 */
#ifndef _LANGUAGE_H
#define _LANGUAGE_H
#include "mal.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_dataflow.h"

mal_export str CMDraise(str *ret, str *msg);
mal_export str MALassertBit(void *ret, bit *val, str *msg);
mal_export str MALassertStr(void *ret, str *val, str *msg);
mal_export str MALassertOid(void *ret, oid *val, str *msg);
mal_export str MALassertSht(void *ret, sht *val, str *msg);
mal_export str MALassertInt(void *ret, int *val, str *msg);
mal_export str MALassertLng(void *ret, lng *val, str *msg);
#ifdef HAVE_HGE
mal_export str MALassertHge(void *ret, hge *val, str *msg);
#endif
mal_export str MALstartDataflow( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MALpass( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MALgarbagesink( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDregisterFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDcallString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDcallFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDcallBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDevalFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MALassertTriple(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif /* _LANGUAGE_H */
