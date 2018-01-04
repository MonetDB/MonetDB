/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _OPT_PROJECTIONPATH_
#define _OPT_PROJECTIONPATH_

#include "mal_client.h"
#include "mal_interpreter.h"

mal_export str ALGprojectionpath(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
