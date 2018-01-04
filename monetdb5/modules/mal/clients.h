/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

mal_export str CLTsetListing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetClientId(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTusers(bat *ret);
mal_export str CLTquit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTLogin(bat *ret, bat *nme);
mal_export str CLTLastCommand(bat *ret);
mal_export str CLTActions(bat *ret);
mal_export str CLTInfo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsuspend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsetSessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTwakeup(void *ret, int *id);

mal_export str CLTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTmd5sum(str *ret, str *pw);
mal_export str CLTsha1sum(str *ret, str *pw);
mal_export str CLTripemd160sum(str *ret, str *pw);
mal_export str CLTsha2sum(str *ret, str *pw, int *bits);
mal_export str CLTbackendsum(str *ret, str *pw);
mal_export str CLTaddUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTremoveUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetPasswordHash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTchangeUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTchangePassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsetPassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTcheckPermission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTgetUsers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CLTsessions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _CLIENTS_H */
