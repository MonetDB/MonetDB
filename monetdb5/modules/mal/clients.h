/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @+ Monet client data
 * Portions of the client record can be directly obtained for
 * backward compatibility. The routine clientInfo provides more
 * detailed information.
 */
#ifndef _CLIENTS_H
#define _CLIENTS_H
#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define clients_export extern __declspec(dllimport)
#else
#define clients_export extern __declspec(dllexport)
#endif
#else
#define clients_export extern
#endif

clients_export str CLTsetListing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetClientId(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTusers(bat *ret);
clients_export str CLTsetHistory(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTquit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTLogin(bat *ret, bat *nme);
clients_export str CLTLastCommand(bat *ret);
clients_export str CLTActions(bat *ret);
clients_export str CLTTime(bat *ret);
clients_export str CLTInfo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsuspend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsetSessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTwakeup(void *ret, int *id);

clients_export str CLTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTmd5sum(str *ret, str *pw);
clients_export str CLTsha1sum(str *ret, str *pw);
clients_export str CLTripemd160sum(str *ret, str *pw);
clients_export str CLTsha2sum(str *ret, str *pw, int *bits);
clients_export str CLTbackendsum(str *ret, str *pw);
clients_export str CLTaddUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTremoveUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetPasswordHash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTchangeUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTchangePassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsetPassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTcheckPermission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTgetUsers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
clients_export str CLTsessions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _CLIENTS_H */
