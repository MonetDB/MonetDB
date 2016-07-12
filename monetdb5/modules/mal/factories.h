/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @+ Implementation code
 */
#ifndef _FACTORIES_H
#define _FACTORIES_H

#include "mal.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

mal_export str FCTgetPlants(bat *ret, bat *ret2);
mal_export str FCTgetCaller(int *ret);
mal_export str FCTgetOwners(bat *ret);
mal_export str FCTgetArrival(bat *ret);
mal_export str FCTgetDeparture(bat *ret);
mal_export str FCTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _FACTORIES_H */
