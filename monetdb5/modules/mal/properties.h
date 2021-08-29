/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _PROPS_H
#define _PROPS_H
#include "mal.h"
#include "mal_interpreter.h"

mal_export str PROPget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str PROPbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _PROPS_H */
