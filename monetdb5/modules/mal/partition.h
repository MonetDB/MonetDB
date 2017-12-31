/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _PART_H
#define _PART_H

#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

mal_export str PARThash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str PARThashkeyed(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str PARTslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str PARTslicekeyed(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _PART_H */
